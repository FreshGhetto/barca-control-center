from __future__ import annotations

import argparse
import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from db_sync import _clamp_num, _parse_order_detail_report, get_db_dsn
from reparto_sizes import infer_reparto_from_path, normalize_reparto

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


SEASON_RE = re.compile(r"(?P<season>\d{2}[a-z])", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import storico dettaglio articoli (marchio/colore/materiale/venduto periodo) nel DB BARCA."
    )
    parser.add_argument(
        "--detail-dir",
        type=Path,
        required=True,
        help="Cartella con i CSV aggiuntivi tipo ANALISI ARTICOLI + Raffronta con venduto nel periodo.",
    )
    return parser.parse_args()


def _require_psycopg():
    if psycopg is None:
        raise RuntimeError("psycopg non installato. Esegui: pip install -r requirements.txt")


def _txt(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _article(value: Any) -> Optional[str]:
    text = _txt(value)
    return text.upper() if text else None


def _season_from_path(path: Path) -> str:
    match = SEASON_RE.search(path.stem)
    if not match:
        raise ValueError(f"Stagione non rilevata da {path.name}")
    return match.group("season").lower()


def _module_from_season(code: str) -> str:
    season_char = str(code or "").strip().lower()[-1:]
    if season_char in {"i", "e"}:
        return "current"
    if season_char in {"y", "g"}:
        return "continuativa"
    raise ValueError(f"Modulo non rilevato per stagione {code}")


def _season_sort_key(code: str):
    raw = str(code or "").strip().lower()
    year = int(raw[:2]) + 2000 if len(raw) >= 2 and raw[:2].isdigit() else -1
    season_rank = {"y": 0, "g": 0, "i": 1, "e": 1}.get(raw[-1:] or "", 9)
    return year, season_rank, raw


def _collect_frames(detail_dir: Path) -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
    frames: List[Tuple[pd.DataFrame, Dict[str, Any]]] = []
    for path in sorted(detail_dir.glob("*.csv"), key=lambda item: _season_sort_key(_season_from_path(item))):
        season = _season_from_path(path)
        module = _module_from_season(season)
        df = _parse_order_detail_report(path)
        if df.empty:
            continue
        frames.append(
            (
                df,
                {
                    "season": season,
                    "module": module,
                    "reparto": infer_reparto_from_path(path),
                    "path": path,
                    "rows": int(len(df)),
                },
            )
        )
    return frames


def _build_dim_articles(frames: List[Tuple[pd.DataFrame, Dict[str, Any]]]) -> List[Tuple[Any, ...]]:
    merged: Dict[str, Dict[str, Optional[str]]] = {}
    for df, _meta in frames:
        for _, row in df.iterrows():
            article_code = _article(row.get("Codice_Articolo"))
            if not article_code:
                continue
            rec = merged.setdefault(
                article_code,
                {
                    "description": None,
                    "reparto": None,
                    "categoria": None,
                    "tipologia": None,
                    "marchio": None,
                    "colore": None,
                    "materiale": None,
                },
            )
            mapping = {
                "description": row.get("Descrizione"),
                "reparto": row.get("Reparto") or _meta.get("reparto"),
                "categoria": row.get("Categoria"),
                "tipologia": row.get("Tipologia"),
                "marchio": row.get("Marchio"),
                "colore": row.get("Colore"),
                "materiale": row.get("Materiale"),
            }
            for key, value in mapping.items():
                text = _txt(value)
                if text and not rec[key]:
                    rec[key] = text
    return [
        (
            article_code,
            payload["description"],
            normalize_reparto(payload["reparto"]),
            payload["categoria"],
            payload["tipologia"],
            payload["marchio"],
            payload["colore"],
            payload["materiale"],
        )
        for article_code, payload in sorted(merged.items())
    ]


def _build_order_source_rows(run_id: str, frames: List[Tuple[pd.DataFrame, Dict[str, Any]]]) -> List[Tuple[Any, ...]]:
    rows: List[Tuple[Any, ...]] = []
    for df, meta in frames:
        season = meta["season"]
        module = meta["module"]
        for _, row in df.iterrows():
            article_code = _article(row.get("Codice_Articolo"))
            if not article_code:
                continue
            rows.append(
                (
                    run_id,
                    module,
                    season,
                    article_code,
                    _txt(row.get("Categoria")),
                    _txt(row.get("Tipologia")),
                    _txt(row.get("Marchio")),
                    _txt(row.get("Colore")),
                    _txt(row.get("Materiale")),
                    _txt(row.get("Descrizione")),
                    _clamp_num(row.get("Venduto_Totale"), low=0.0),
                    _clamp_num(row.get("Venduto_Periodo"), low=0.0),
                    _clamp_num(row.get("Giacenza"), low=0.0),
                    _clamp_num(row.get("Venduto_Extra"), low=0.0) or 0.0,
                    _txt(row.get("Fascia_Prezzo")),
                    _clamp_num(row.get("Prezzo_Listino"), low=0.0),
                    _clamp_num(row.get("Prezzo_Acquisto"), low=0.0),
                    _clamp_num(row.get("Prezzo_Vendita"), low=0.0),
                )
            )
    return rows


def main():
    args = parse_args()
    _require_psycopg()

    detail_dir = args.detail_dir.resolve()
    if not detail_dir.exists():
        raise SystemExit(f"Cartella non trovata: {detail_dir}")

    frames = _collect_frames(detail_dir)
    if not frames:
        raise SystemExit("Nessun file dettaglio valido trovato.")

    dsn = get_db_dsn()
    run_id = str(uuid.uuid4())
    dim_articles = _build_dim_articles(frames)
    order_source_rows = _build_order_source_rows(run_id, frames)
    metadata = {
        "detail_history_only": True,
        "source_mode": "external_order_detail_history",
        "source_dir": str(detail_dir),
        "seasons": [meta["season"] for _, meta in frames],
        "files": [
            {
                "season": meta["season"],
                "module": meta["module"],
                "path": str(meta["path"]),
                "rows": meta["rows"],
            }
            for _, meta in frames
        ],
        "counts": {
            "dim_article": len(dim_articles),
            "fact_order_source": len(order_source_rows),
            "fact_order_source_size": 0,
        },
    }

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_run (run_id, run_type, status, metadata) VALUES (%s, %s, 'running', %s::jsonb)",
                (run_id, "detail_history_sync", json.dumps(metadata)),
            )

            if dim_articles:
                cur.executemany(
                    """
                    INSERT INTO dim_article (article_code, description, reparto, categoria, tipologia, marchio, colore, materiale)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (article_code) DO UPDATE SET
                      description = COALESCE(EXCLUDED.description, dim_article.description),
                      reparto = COALESCE(EXCLUDED.reparto, dim_article.reparto),
                      categoria = COALESCE(EXCLUDED.categoria, dim_article.categoria),
                      tipologia = COALESCE(EXCLUDED.tipologia, dim_article.tipologia),
                      marchio = COALESCE(EXCLUDED.marchio, dim_article.marchio),
                      colore = COALESCE(EXCLUDED.colore, dim_article.colore),
                      materiale = COALESCE(EXCLUDED.materiale, dim_article.materiale),
                      updated_at = NOW()
                    """,
                    dim_articles,
                )

            if order_source_rows:
                cur.executemany(
                    """
                    INSERT INTO fact_order_source (
                      run_id, module, season_code, article_code, categoria, tipologia, marchio, colore, materiale,
                      descrizione, venduto_totale, venduto_periodo, giacenza, venduto_extra, fascia_prezzo,
                      prezzo_listino, prezzo_acquisto, prezzo_vendita
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, module, season_code, article_code) DO UPDATE SET
                      categoria = EXCLUDED.categoria,
                      tipologia = EXCLUDED.tipologia,
                      marchio = EXCLUDED.marchio,
                      colore = EXCLUDED.colore,
                      materiale = EXCLUDED.materiale,
                      descrizione = EXCLUDED.descrizione,
                      venduto_totale = EXCLUDED.venduto_totale,
                      venduto_periodo = EXCLUDED.venduto_periodo,
                      giacenza = EXCLUDED.giacenza,
                      venduto_extra = EXCLUDED.venduto_extra,
                      fascia_prezzo = EXCLUDED.fascia_prezzo,
                      prezzo_listino = EXCLUDED.prezzo_listino,
                      prezzo_acquisto = EXCLUDED.prezzo_acquisto,
                      prezzo_vendita = EXCLUDED.prezzo_vendita
                    """,
                    order_source_rows,
                )

            cur.execute(
                """
                UPDATE etl_run
                SET status = 'completed', metadata = %s::jsonb, finished_at = NOW()
                WHERE run_id = %s::uuid
                """,
                (json.dumps(metadata), run_id),
            )
        conn.commit()

    print(json.dumps({"run_id": run_id, "status": "completed", "counts": metadata["counts"]}, indent=2))


if __name__ == "__main__":
    main()
