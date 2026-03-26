from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None


def _require_psycopg():
    if psycopg is None:
        raise RuntimeError("psycopg non installato. Esegui: pip install -r requirements.txt")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def get_db_dsn() -> str:
    host = _env("BARCA_DB_HOST")
    port = _env("BARCA_DB_PORT", "5432")
    dbname = _env("BARCA_DB_NAME")
    user = _env("BARCA_DB_USER")
    password = _env("BARCA_DB_PASSWORD")
    sslmode = _env("BARCA_DB_SSLMODE", "prefer")
    miss = [k for k, v in {
        "BARCA_DB_HOST": host,
        "BARCA_DB_NAME": dbname,
        "BARCA_DB_USER": user,
        "BARCA_DB_PASSWORD": password,
    }.items() if not v]
    if miss:
        raise ValueError(
            f"Variabili DB mancanti: {miss}. "
            "Imposta BARCA_DB_HOST, BARCA_DB_NAME, BARCA_DB_USER, BARCA_DB_PASSWORD."
        )
    return f"host={host} port={port} dbname={dbname} user={user} password={password} sslmode={sslmode}"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _txt(v: Any) -> Optional[str]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    return s if s else None


def _class_label(v: Any) -> Optional[str]:
    s = _txt(v)
    if not s:
        return None
    if s in {":", "-", "--", "N/D", "ND", "N.D.", "NULL"}:
        return None
    return s


def _article(v: Any) -> Optional[str]:
    s = _txt(v)
    return s.upper() if s else None


def _shop(v: Any) -> Optional[str]:
    s = _txt(v)
    return s.upper() if s else None


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _i(v: Any) -> Optional[int]:
    x = _f(v)
    return int(round(x)) if x is not None else None


def _clamp_num(v: Any, *, low: Optional[float] = None, high: Optional[float] = None) -> Optional[float]:
    x = _f(v)
    if x is None:
        return None
    if low is not None:
        x = max(float(low), x)
    if high is not None:
        x = min(float(high), x)
    return float(x)


def _b(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _dt(v: Any):
    if v is None:
        return None
    try:
        t = pd.to_datetime(v, errors="coerce")
    except Exception:
        return None
    if pd.isna(t):
        return None
    return t.to_pydatetime() if hasattr(t, "to_pydatetime") else None


def _size(col: str) -> Optional[int]:
    suf = col.split("_")[-1]
    return int(suf) if suf.isdigit() else None


def _season_sort_key(code: str) -> Tuple[int, int, str]:
    raw = str(code or "").strip().lower()
    m = re.search(r"(\d{2,4})", raw)
    year = int(m.group(1)) if m else -1
    if 0 <= year < 100:
        year += 2000
    season_char = next((ch for ch in reversed(raw) if ch.isalpha()), "")
    season_rank = {"y": 0, "g": 0, "i": 1, "e": 1}.get(season_char, 9)
    return year, season_rank, raw


def _cfg_shops(root: Path) -> Dict[str, Dict[str, Any]]:
    cfg = next(
        (
            p for p in [
                root / "config" / "lista-negozi_integrato.xlsx",
                root / "config" / "lista-negozi.xlsx",
            ] if p.exists()
        ),
        None,
    )
    if cfg is None:
        return {}
    try:
        df = pd.read_excel(cfg, sheet_name=0)
    except Exception:
        return {}
    cols = {str(c).strip().lower(): c for c in df.columns}
    c_sig = next((c for k, c in cols.items() if "sig" in k or "cod" in k or "shop" in k), None)
    c_name = next((c for k, c in cols.items() if "negozi" in k or "negozio" in k or "name" in k), None)
    c_fas = next((c for k, c in cols.items() if "fascia" in k), None)
    c_mq = next((c for k, c in cols.items() if "mq" in k or "metri" in k), None)
    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        code = _shop(r.get(c_sig)) if c_sig else None
        if not code:
            continue
        out[code] = {
            "shop_name": _txt(r.get(c_name)) if c_name else None,
            "fascia": _i(r.get(c_fas)) if c_fas else None,
            "mq": _f(r.get(c_mq)) if c_mq else None,
        }
    return out


def _merge_art(dest: Dict[str, Dict[str, Optional[str]]], code: Any, row: Dict[str, Any]):
    a = _article(code)
    if not a:
        return
    rec = dest.setdefault(
        a,
        {"description": None, "categoria": None, "tipologia": None, "marchio": None, "colore": None, "materiale": None},
    )
    for k in rec.keys():
        v = _class_label(row.get(k)) if k in {"categoria", "tipologia"} else _txt(row.get(k))
        if v and not rec[k]:
            rec[k] = v


def _fill_missing_classifications(
    frames: List[Tuple[pd.DataFrame, Dict[str, Any]]],
) -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
    if not frames:
        return frames

    samples: List[pd.DataFrame] = []
    for df, _meta in frames:
        if df is None or df.empty:
            continue
        work = df.copy()
        if "Codice_Articolo" not in work.columns:
            continue
        work["Codice_Articolo"] = work["Codice_Articolo"].map(_article)
        work["Categoria"] = work.get("Categoria", pd.Series(index=work.index, dtype="object")).map(_class_label)
        work["Tipologia"] = work.get("Tipologia", pd.Series(index=work.index, dtype="object")).map(_class_label)
        work["Marchio"] = work.get("Marchio", pd.Series(index=work.index, dtype="object")).map(_txt)
        work["Descrizione"] = work.get("Descrizione", pd.Series(index=work.index, dtype="object")).map(_txt)
        samples.append(work[["Codice_Articolo", "Categoria", "Tipologia", "Marchio", "Descrizione"]])

    if not samples:
        return frames

    source_df = pd.concat(samples, ignore_index=True).drop_duplicates()

    article_cat: Dict[str, str] = {}
    article_tip: Dict[str, str] = {}
    article_groups = source_df.groupby("Codice_Articolo", dropna=True)
    for article_code, grp in article_groups:
        cats = {v for v in grp["Categoria"].dropna().tolist() if v}
        tips = {v for v in grp["Tipologia"].dropna().tolist() if v}
        if len(cats) == 1:
            article_cat[str(article_code)] = next(iter(cats))
        if len(tips) == 1:
            article_tip[str(article_code)] = next(iter(tips))

    pair_cat: Dict[Tuple[str, str], str] = {}
    pair_tip: Dict[Tuple[str, str], str] = {}
    pair_df = source_df.dropna(subset=["Marchio", "Descrizione"]).copy()
    if not pair_df.empty:
        for key, grp in pair_df.groupby(["Marchio", "Descrizione"], dropna=True):
            cats = {v for v in grp["Categoria"].dropna().tolist() if v}
            tips = {v for v in grp["Tipologia"].dropna().tolist() if v}
            if len(cats) == 1:
                pair_cat[(str(key[0]), str(key[1]))] = next(iter(cats))
            if len(tips) == 1:
                pair_tip[(str(key[0]), str(key[1]))] = next(iter(tips))

    out_frames: List[Tuple[pd.DataFrame, Dict[str, Any]]] = []
    for df, meta in frames:
        if df is None or df.empty or "Codice_Articolo" not in df.columns:
            out_frames.append((df, meta))
            continue
        out = df.copy()
        out["Codice_Articolo"] = out["Codice_Articolo"].map(_article)
        out["Marchio"] = out.get("Marchio", pd.Series(index=out.index, dtype="object")).map(_txt)
        out["Descrizione"] = out.get("Descrizione", pd.Series(index=out.index, dtype="object")).map(_txt)
        out["Categoria"] = out.get("Categoria", pd.Series(index=out.index, dtype="object")).map(_class_label)
        out["Tipologia"] = out.get("Tipologia", pd.Series(index=out.index, dtype="object")).map(_class_label)

        for idx in out.index:
            article_code = _article(out.at[idx, "Codice_Articolo"])
            marchio = _txt(out.at[idx, "Marchio"])
            descr = _txt(out.at[idx, "Descrizione"])
            pair_key = (marchio, descr) if marchio and descr else None

            if not _class_label(out.at[idx, "Categoria"]):
                fill_cat = article_cat.get(article_code) if article_code else None
                if not fill_cat and pair_key:
                    fill_cat = pair_cat.get(pair_key)
                if fill_cat:
                    out.at[idx, "Categoria"] = fill_cat

            if not _class_label(out.at[idx, "Tipologia"]):
                fill_tip = article_tip.get(article_code) if article_code else None
                if not fill_tip and pair_key:
                    fill_tip = pair_tip.get(pair_key)
                if fill_tip:
                    out.at[idx, "Tipologia"] = fill_tip

        out_frames.append((out, meta))
    return out_frames


def _order_jobs(out_orders: Path, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    c_season = _txt(summary.get("current", {}).get("season")) or "unknown"
    y_season = _txt(summary.get("continuativa", {}).get("season")) or "unknown"
    jobs = [
        ("orders_current_previsione_math.csv", "current", "math", c_season),
        ("orders_continuativa_previsione_math.csv", "continuativa", "math", y_season),
        ("orders_continuativa_previsione_rf.csv", "continuativa", "rf", y_season),
        ("orders_continuativa_previsione_ibrida.csv", "continuativa", "hybrid", y_season),
    ]
    out: List[Dict[str, Any]] = []
    for fn, module, mode, season in jobs:
        p = out_orders / fn
        if p.exists():
            out.append({"path": p, "module": module, "mode": mode, "season": season})
    return out


def _order_source_jobs(out_orders: Path, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    c_season = _txt(summary.get("current", {}).get("season")) or "unknown"
    y_season = _txt(summary.get("continuativa", {}).get("season")) or "unknown"
    jobs = [
        ("orders_current_dati_originali.csv", "current", c_season),
        ("orders_continuativa_dati_originali.csv", "continuativa", y_season),
    ]
    out: List[Dict[str, Any]] = []
    for fn, module, season in jobs:
        p = out_orders / fn
        if p.exists():
            out.append({"path": p, "module": module, "season": season})
    return out


def _order_output_path(out_orders: Path, file_ref: Any) -> Optional[Path]:
    file_txt = _txt(file_ref)
    if not file_txt:
        return None
    p = Path(file_txt)
    if not p.is_absolute():
        p = out_orders / p
    return p


def _order_source_history_jobs(out_orders: Path, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = summary.get("historical_sources") if isinstance(summary, dict) else []
    if not isinstance(items, list):
        return []

    out: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        module = _txt(item.get("module"))
        season = _txt(item.get("season"))
        path = _order_output_path(out_orders, item.get("file"))
        if not module or not season or path is None or not path.exists():
            continue
        out.append({"path": path, "module": module, "season": season})
    return out


def _season_module_from_code(code: Any) -> Optional[str]:
    raw = (_txt(code) or "").lower()
    if not raw:
        return None
    season_char = next((ch for ch in reversed(raw) if ch.isalpha()), "")
    if season_char in {"i", "e"}:
        return "current"
    if season_char in {"y", "g"}:
        return "continuativa"
    return None


def _catalog_source_history_frames(
    dsn: str,
    existing_keys: Sequence[Tuple[str, str]],
) -> Tuple[List[Tuple[pd.DataFrame, Dict[str, str]]], List[Dict[str, Any]]]:
    normalized_keys = {
        ((str(module or "").strip().lower()), (str(season or "").strip().lower()))
        for module, season in existing_keys
        if str(module or "").strip() and str(season or "").strip()
    }

    sql = """
        SELECT
          s.season_code,
          s.article_code,
          COALESCE(NULLIF(s.categoria, ''), a.categoria) AS categoria,
          COALESCE(NULLIF(s.tipologia, ''), a.tipologia) AS tipologia,
          a.marchio,
          s.color AS colore,
          a.materiale,
          COALESCE(NULLIF(s.description, ''), a.description) AS descrizione,
          COALESCE(s.ven, 0) AS venduto_qty,
          COALESCE(s.giac, 0) AS giacenza,
          p.price_listino,
          p.price_saldo
        FROM vw_catalog_article_store_current s
        LEFT JOIN dim_article a
          ON a.article_code = s.article_code
        LEFT JOIN vw_catalog_price_current p
          ON p.season_code = s.season_code
         AND p.article_code = s.article_code
        WHERE s.store_code = 'XX'
          AND s.season_code IS NOT NULL
          AND UPPER(s.season_code) <> 'UNKNOWN'
        ORDER BY s.season_code, s.article_code
    """

    frames: List[Tuple[pd.DataFrame, Dict[str, str]]] = []
    jobs: List[Dict[str, Any]] = []
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    if not rows:
        return frames, jobs

    raw_df = pd.DataFrame(
        rows,
        columns=[
            "season_code",
            "article_code",
            "categoria",
            "tipologia",
            "marchio",
            "colore",
            "materiale",
            "descrizione",
            "venduto_qty",
            "giacenza",
            "price_listino",
            "price_saldo",
        ],
    )

    for season_code, season_df in raw_df.groupby("season_code", sort=True):
        season_txt = _txt(season_code)
        module = _season_module_from_code(season_txt)
        if not season_txt or not module:
            continue
        normalized_key = (module.lower(), season_txt.lower())
        if normalized_key in normalized_keys:
            continue

        out_df = pd.DataFrame(
            {
                "Codice_Articolo": season_df["article_code"].map(_article),
                "Categoria": season_df["categoria"].map(_txt),
                "Tipologia": season_df["tipologia"].map(_txt),
                "Marchio": season_df["marchio"].map(_txt),
                "Colore": season_df["colore"].map(_txt),
                "Materiale": season_df["materiale"].map(_txt),
                "Descrizione": season_df["descrizione"].map(_txt),
                # Il catalogo storico non separa venduto periodo/totale come i bundle ordini.
                # Per il confronto stagionale usiamo il venduto accumulato del report per entrambi.
                "Venduto_Totale": pd.to_numeric(season_df["venduto_qty"], errors="coerce").fillna(0.0),
                "Venduto_Periodo": pd.to_numeric(season_df["venduto_qty"], errors="coerce").fillna(0.0),
                "Giacenza": pd.to_numeric(season_df["giacenza"], errors="coerce").fillna(0.0),
                "Venduto_Extra": 0.0,
                "Fascia_Prezzo": None,
                "Prezzo_Listino": pd.to_numeric(season_df["price_listino"], errors="coerce"),
                "Prezzo_Vendita": pd.to_numeric(season_df["price_saldo"], errors="coerce"),
            }
        )
        out_df = _apply_price_band(out_df)
        out_df = out_df[out_df["Codice_Articolo"].notna()].copy()
        if out_df.empty:
            continue
        frames.append((out_df, {"module": module, "season": season_txt, "source": "catalog_snapshot"}))
        jobs.append(
            {
                "module": module,
                "season": season_txt,
                "source": "catalog_snapshot",
                "file": f"catalog://vw_catalog_article_store_current/{season_txt}",
                "rows": int(len(out_df)),
            }
        )

    return frames, jobs


def _catalog_price_snapshot_df(dsn: str) -> pd.DataFrame:
    sql = """
        SELECT
          season_code,
          article_code,
          price_listino,
          price_saldo
        FROM vw_catalog_price_current
        WHERE season_code IS NOT NULL
          AND article_code IS NOT NULL
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["season_code", "article_code", "price_listino", "price_saldo"])
    df = pd.DataFrame(rows, columns=["season_code", "article_code", "price_listino", "price_saldo"])
    df["season_code"] = df["season_code"].map(lambda v: (_txt(v) or "").upper())
    df["article_code"] = df["article_code"].map(_article)
    return df


def _read_csv_head(path: Path, max_chars: int = 8192) -> str:
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
            return f.read(max_chars)
    except Exception:
        return ""


def _extract_season_code(text: str, filename: str) -> Optional[str]:
    m = re.search(r"STAGIONE[:\s]*([0-9]{2}[A-Z])", str(text or ""), flags=re.IGNORECASE)
    if m:
        return m.group(1).lower()
    m = re.search(r"([0-9]{2}[a-zA-Z])(?:_|$)", str(filename or ""), flags=re.IGNORECASE)
    if m:
        return m.group(1).lower()
    return None


def _parse_csv_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".")
    try:
        number = float(text)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def _parse_csv_int(value: Any) -> Optional[int]:
    number = _parse_csv_number(value)
    return int(round(number)) if number is not None else None


def _discover_order_detail_reports(root: Path) -> List[Dict[str, Any]]:
    orders_root = root / "input" / "orders"
    if not orders_root.exists():
        return []

    reports_by_season: Dict[str, Dict[str, Any]] = {}
    for path in orders_root.rglob("*.csv"):
        name = path.name.lower()
        if re.search(r"_sd_[1-4]\.csv$", name):
            continue
        if name.endswith("prezzo_acq-ven.csv"):
            continue
        head = _read_csv_head(path)
        upper = head.upper()
        if "ANALISI ARTICOLI" not in upper:
            continue
        if "RAFFRONTA CON VENDUTO NEL PERIODO" not in upper:
            continue
        if "COLORE" not in upper or "MATERIALE" not in upper or "MARCHIO" not in upper:
            continue
        season = _extract_season_code(head, path.name)
        module = _season_module_from_code(season)
        if not season or not module:
            continue
        candidate = {
            "path": path,
            "season": season,
            "module": module,
        }
        current = reports_by_season.get(season)
        if current is None or path.stat().st_mtime > current["path"].stat().st_mtime:
            reports_by_season[season] = candidate

    return [
        reports_by_season[season]
        for season in sorted(reports_by_season.keys(), key=_season_sort_key)
    ]


def _discover_native_order_bundle_seasons(root: Path) -> set[str]:
    orders_root = root / "input" / "orders"
    if not orders_root.exists():
        return set()

    pattern = re.compile(r"^(?P<season>.+?)_sd_(?P<part>[1234])\.csv$", re.IGNORECASE)
    grouped: Dict[str, set[int]] = {}
    for path in orders_root.glob("*.csv"):
        match = pattern.match(path.name)
        if not match:
            continue
        season = (_txt(match.group("season")) or "").lower()
        part = int(match.group("part"))
        grouped.setdefault(season, set()).add(part)

    return {season for season, parts in grouped.items() if {1, 2, 3}.issubset(parts)}


def _parse_order_detail_report(path: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    current_reparto = current_colore = current_materiale = current_marchio = None

    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        for row in csv.reader(f):
            if "ARTICOLO" not in row:
                continue
            elements = row[row.index("ARTICOLO") + 1 :]
            if "TOTALI :" in elements:
                elements = elements[: elements.index("TOTALI :")]
            article_indices = [
                idx
                for idx, val in enumerate(elements)
                if "/" in (val.strip().split()[0] if val.strip().split() else "")
                and any(c.isdigit() for c in (val.strip().split()[0] if val.strip().split() else ""))
            ]
            last_idx = 0
            for idx in article_indices:
                pre_chunk = elements[last_idx:idx]
                pre_elements = [
                    item.strip()
                    for item in pre_chunk
                    if item.strip()
                    and not re.match(r"^-?\d+(?:,\d+)?$|^%$", item.strip())
                    and not item.startswith("SUBTOTALE")
                    and not item.startswith("VALORE")
                    and not item.startswith("COSTO")
                ]
                if len(pre_elements) >= 1:
                    current_marchio = pre_elements[-1]
                if len(pre_elements) >= 2:
                    current_materiale = pre_elements[-2]
                if len(pre_elements) >= 3:
                    current_colore = pre_elements[-3]
                if len(pre_elements) >= 4:
                    current_reparto = pre_elements[-4]

                raw_article = elements[idx].strip()
                if not raw_article:
                    last_idx = idx + 1
                    continue
                code = raw_article.split()[0].strip()
                if not _article(code):
                    last_idx = idx + 1
                    continue
                description = re.sub(r"\s+", " ", raw_article[len(code) :].strip()) or None
                rows.append(
                    {
                        "Codice_Articolo": _article(code),
                        "Categoria": None,
                        "Tipologia": None,
                        "Marchio": re.sub(r"\s+", " ", current_marchio).strip() if current_marchio else None,
                        "Colore": re.sub(r"\s+", " ", current_colore).strip() if current_colore else None,
                        "Materiale": re.sub(r"\s+", " ", current_materiale).strip() if current_materiale else None,
                        "Descrizione": description,
                        "Venduto_Totale": _parse_csv_int(elements[idx + 2]) if idx + 2 < len(elements) else None,
                        "Venduto_Periodo": _parse_csv_int(elements[idx + 3]) if idx + 3 < len(elements) else None,
                        "Giacenza": _parse_csv_int(elements[idx + 4]) if idx + 4 < len(elements) else None,
                        "Venduto_Extra": 0,
                        "Prezzo_Listino": None,
                        "Prezzo_Acquisto": None,
                        "Prezzo_Vendita": None,
                        "Fascia_Prezzo": None,
                        "_Reparto": re.sub(r"\s+", " ", current_reparto).strip() if current_reparto else None,
                    }
                )
                last_idx = idx + 1

    columns = [
        "Codice_Articolo",
        "Categoria",
        "Tipologia",
        "Marchio",
        "Colore",
        "Materiale",
        "Descrizione",
        "Venduto_Totale",
        "Venduto_Periodo",
        "Giacenza",
        "Venduto_Extra",
        "Prezzo_Listino",
        "Prezzo_Acquisto",
        "Prezzo_Vendita",
        "Fascia_Prezzo",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows)
    for col in ("Venduto_Totale", "Venduto_Periodo", "Giacenza", "Venduto_Extra"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    def _first_non_empty(series: pd.Series) -> Optional[str]:
        for value in series:
            text = _txt(value)
            if text:
                return text
        return None

    agg = {
        "Categoria": _first_non_empty,
        "Tipologia": _first_non_empty,
        "Marchio": _first_non_empty,
        "Colore": _first_non_empty,
        "Materiale": _first_non_empty,
        "Descrizione": _first_non_empty,
        "Venduto_Totale": "max",
        "Venduto_Periodo": "max",
        "Giacenza": "max",
        "Venduto_Extra": "max",
        "Prezzo_Listino": "max",
        "Prezzo_Acquisto": "max",
        "Prezzo_Vendita": "max",
        "Fascia_Prezzo": _first_non_empty,
    }
    out = df.groupby("Codice_Articolo", as_index=False).agg(agg)
    for col in ("Venduto_Totale", "Venduto_Periodo", "Giacenza", "Venduto_Extra"):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out.reindex(columns=columns)


def _order_detail_history_frames(root: Path) -> Tuple[List[Tuple[pd.DataFrame, Dict[str, Any]]], List[Dict[str, Any]]]:
    frames: List[Tuple[pd.DataFrame, Dict[str, Any]]] = []
    jobs: List[Dict[str, Any]] = []
    for item in _discover_order_detail_reports(root):
        df = _parse_order_detail_report(item["path"])
        if df.empty:
            continue
        meta = {
            "module": item["module"],
            "season": item["season"],
            "source": "detail_report",
            "path": item["path"],
        }
        frames.append((df, meta))
        jobs.append(
            {
                "module": item["module"],
                "season": item["season"],
                "source": "detail_report",
                "file": str(item["path"]),
                "rows": int(len(df)),
            }
        )
    return frames, jobs


def _overlay_order_detail(
    base: pd.DataFrame,
    detail: pd.DataFrame,
    *,
    detail_authoritative: bool = False,
) -> pd.DataFrame:
    if detail is None or detail.empty:
        return base.copy() if base is not None else pd.DataFrame()
    if base is None or base.empty:
        return detail.copy()

    out = detail.copy() if detail_authoritative else base.copy()
    det = detail.copy()
    base_df = base.copy()
    out["Codice_Articolo"] = out.get("Codice_Articolo", pd.Series(dtype="object")).map(_article)
    det["Codice_Articolo"] = det.get("Codice_Articolo", pd.Series(dtype="object")).map(_article)
    base_df["Codice_Articolo"] = base_df.get("Codice_Articolo", pd.Series(dtype="object")).map(_article)
    out = out[out["Codice_Articolo"].notna()].copy()
    det = det[det["Codice_Articolo"].notna()].copy()
    base_df = base_df[base_df["Codice_Articolo"].notna()].copy()

    out = out.set_index("Codice_Articolo")
    det = det.set_index("Codice_Articolo")
    base_df = base_df.set_index("Codice_Articolo")
    target_index = det.index if detail_authoritative else out.index.union(det.index)
    out = out.reindex(target_index)
    det = det.reindex(target_index)
    base_df = base_df.reindex(target_index)

    for col in det.columns.union(base_df.columns):
        if col not in out.columns:
            out[col] = pd.NA

    for col in ("Marchio", "Colore", "Materiale", "Descrizione"):
        if col not in out.columns:
            out[col] = pd.NA
        if col not in det.columns:
            continue
        preferred = det[col].apply(_txt)
        fallback = base_df[col].apply(_txt) if col in base_df.columns else out[col].apply(_txt)
        out[col] = preferred.where(preferred.notna(), fallback)

    for col in ("Categoria", "Tipologia", "Fascia_Prezzo"):
        if col not in out.columns:
            out[col] = pd.NA
        preferred = out[col].apply(_txt)
        fallback = base_df[col].apply(_txt) if col in base_df.columns else (
            det[col].apply(_txt) if col in det.columns else pd.Series(pd.NA, index=out.index, dtype="object")
        )
        out[col] = preferred.where(preferred.notna(), fallback)

    for col in ("Venduto_Totale", "Venduto_Periodo", "Giacenza"):
        if col not in out.columns:
            out[col] = pd.NA
        if col not in det.columns:
            continue
        preferred = pd.to_numeric(det[col], errors="coerce")
        fallback = pd.to_numeric(base_df[col], errors="coerce") if col in base_df.columns else pd.to_numeric(out[col], errors="coerce")
        out[col] = preferred.where(preferred.notna(), fallback)

    for col in ("Venduto_Extra", "Prezzo_Listino", "Prezzo_Acquisto", "Prezzo_Vendita"):
        if col not in out.columns:
            out[col] = pd.NA
        preferred = pd.to_numeric(out[col], errors="coerce")
        if col in base_df.columns:
            fallback = pd.to_numeric(base_df[col], errors="coerce")
        elif col in det.columns:
            fallback = pd.to_numeric(det[col], errors="coerce")
        else:
            fallback = pd.Series(np.nan, index=out.index, dtype="float64")
        out[col] = preferred.where(preferred.notna(), fallback)

    out = out.reset_index()
    return _apply_price_band(out)


def _merge_order_source_frames(
    base_frames: List[Tuple[pd.DataFrame, Dict[str, Any]]],
    detail_frames: List[Tuple[pd.DataFrame, Dict[str, Any]]],
    *,
    native_bundle_seasons: Optional[set[str]] = None,
) -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
    merged: Dict[Tuple[str, str], Tuple[pd.DataFrame, Dict[str, Any]]] = {}
    native_bundle_seasons = {str(s).strip().lower() for s in (native_bundle_seasons or set()) if str(s).strip()}

    for df, meta in base_frames:
        key = ((_txt(meta.get("module")) or "").lower(), (_txt(meta.get("season")) or "").lower())
        if not all(key):
            continue
        merged[key] = (df.copy(), dict(meta))

    for detail_df, detail_meta in detail_frames:
        key = ((_txt(detail_meta.get("module")) or "").lower(), (_txt(detail_meta.get("season")) or "").lower())
        if not all(key):
            continue
        if key in merged:
            base_df, base_meta = merged[key]
            base_source = (_txt(base_meta.get("source")) or "orders_source").lower()
            season_key = key[1]
            merged[key] = (
                _overlay_order_detail(
                    base_df,
                    detail_df,
                    detail_authoritative=(base_source == "catalog_snapshot" or season_key not in native_bundle_seasons),
                ),
                base_meta,
            )
        else:
            merged[key] = (detail_df.copy(), dict(detail_meta))

    return [
        merged[key]
        for key in sorted(merged.keys(), key=lambda item: _season_sort_key(item[1]))
    ]


def _is_valid_price_band(value: Any) -> bool:
    text = _txt(value) or ""
    return bool(re.match(r"^\d+\s*-\s*\d+$", text))


def _price_band_label(value: Any) -> Optional[str]:
    x = _f(value)
    if x is None or x <= 0:
        return None
    whole = int(x)
    if whole < 20:
        return "0-19"
    upper = (whole // 10) * 10 + 9
    lower = upper - 9
    return f"{lower}-{upper}"


def _apply_price_band(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "Fascia_Prezzo" not in out.columns:
        out["Fascia_Prezzo"] = None
    out["Fascia_Prezzo"] = out["Fascia_Prezzo"].astype("object")

    base_price = pd.Series(pd.NA, index=out.index, dtype="object")
    for col in ("Prezzo_Listino", "Prezzo_Vendita", "Prezzo_Acquisto"):
        if col not in out.columns:
            continue
        values = pd.to_numeric(out[col], errors="coerce")
        base_price = base_price.where(base_price.notna(), values)

    derived = base_price.map(_price_band_label)
    current_band = out["Fascia_Prezzo"].map(lambda v: _txt(v) or "")
    needs_fill = current_band.eq("") | ~current_band.map(_is_valid_price_band)
    out.loc[needs_fill, "Fascia_Prezzo"] = derived.loc[needs_fill]
    return out


def _enrich_order_source_frame(
    df: pd.DataFrame,
    meta: Dict[str, Any],
    catalog_prices: pd.DataFrame,
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["Codice_Articolo"] = out.get("Codice_Articolo", pd.Series(dtype="object")).map(_article)
    season = (_txt(meta.get("season")) or "").upper()
    if not season or catalog_prices.empty:
        return out

    price_slice = catalog_prices.loc[catalog_prices["season_code"] == season, ["article_code", "price_listino", "price_saldo"]]
    if price_slice.empty:
        return out

    out = out.merge(price_slice, left_on="Codice_Articolo", right_on="article_code", how="left")
    if "Prezzo_Listino" not in out.columns:
        out["Prezzo_Listino"] = None
    if "Prezzo_Vendita" not in out.columns:
        out["Prezzo_Vendita"] = None
    out["Prezzo_Listino"] = pd.to_numeric(out["Prezzo_Listino"], errors="coerce").where(
        pd.to_numeric(out["Prezzo_Listino"], errors="coerce").notna(),
        pd.to_numeric(out["price_listino"], errors="coerce"),
    )
    out["Prezzo_Vendita"] = pd.to_numeric(out["Prezzo_Vendita"], errors="coerce").where(
        pd.to_numeric(out["Prezzo_Vendita"], errors="coerce").notna(),
        pd.to_numeric(out["price_saldo"], errors="coerce"),
    )
    out = out.drop(columns=["article_code", "price_listino", "price_saldo"], errors="ignore")
    return _apply_price_band(out)


def run_db_sync(
    root: Path,
    *,
    create_schema: bool = False,
    schema_path: Optional[Path] = None,
    run_type: str = "manual_sync",
    verbose: bool = True,
) -> Dict[str, Any]:
    _require_psycopg()
    root = root.resolve()
    out = root / "output"
    out_orders = out / "orders"
    schema = schema_path or (root / "db" / "schema.sql")
    dsn = get_db_dsn()
    run_id = uuid.uuid4()

    clean_sales = _read_csv(out / "clean_sales.csv")
    clean_stock = _read_csv(out / "clean_articles.csv")
    transfers = _read_csv(out / "suggested_transfers.csv")
    features = _read_csv(out / "features_after.csv")
    ingest = _read_json(out / "ingest" / "ingest_report_latest.json")
    ord_summary = _read_json(out_orders / "orders_summary.json")
    jobs = _order_jobs(out_orders, ord_summary)
    ord_frames = [(_read_csv(j["path"]), j) for j in jobs]
    source_jobs = _order_source_jobs(out_orders, ord_summary)
    history_source_jobs = _order_source_history_jobs(out_orders, ord_summary)

    merged_source_jobs: List[Dict[str, Any]] = []
    seen_source_keys = set()
    for job in source_jobs + history_source_jobs:
        key = (job.get("module"), job.get("season"))
        if key in seen_source_keys:
            continue
        seen_source_keys.add(key)
        merged_source_jobs.append(job)

    catalog_price_df = _catalog_price_snapshot_df(dsn)
    raw_ord_source_frames = [(_read_csv(j["path"]), j) for j in merged_source_jobs]
    catalog_history_frames, catalog_history_jobs = _catalog_source_history_frames(
        dsn,
        [(j.get("module"), j.get("season")) for j in merged_source_jobs],
    )
    detail_history_frames, detail_history_jobs = _order_detail_history_frames(root)
    native_bundle_seasons = _discover_native_order_bundle_seasons(root)
    merged_ord_source_frames = _merge_order_source_frames(
        raw_ord_source_frames + catalog_history_frames,
        detail_history_frames,
        native_bundle_seasons=native_bundle_seasons,
    )
    merged_ord_source_frames = _fill_missing_classifications(merged_ord_source_frames)
    ord_source_frames = [(_enrich_order_source_frame(df, meta, catalog_price_df), meta) for df, meta in merged_ord_source_frames]

    cfg = _cfg_shops(root)
    shop_codes = set(cfg.keys())
    for df, cols in [
        (clean_sales, ["Shop"]),
        (clean_stock, ["Shop"]),
        (features, ["Shop"]),
        (transfers, ["From", "To"]),
    ]:
        for c in cols:
            if c in df.columns:
                for v in df[c].dropna().tolist():
                    s = _shop(v)
                    if s:
                        shop_codes.add(s)
    dim_shops = [
        (s, _txt(cfg.get(s, {}).get("shop_name")), _i(cfg.get(s, {}).get("fascia")), _f(cfg.get(s, {}).get("mq")))
        for s in sorted(shop_codes)
    ]

    arts: Dict[str, Dict[str, Optional[str]]] = {}
    if "Article" in clean_stock.columns:
        for _, r in clean_stock.iterrows():
            _merge_art(
                arts,
                r.get("Article"),
                {"description": r.get("Description"), "categoria": None, "tipologia": None, "marchio": None, "colore": None, "materiale": None},
            )
    if "Article" in clean_sales.columns:
        for _, r in clean_sales.iterrows():
            _merge_art(
                arts,
                r.get("Article"),
                {"description": None, "categoria": None, "tipologia": None, "marchio": None, "colore": None, "materiale": None},
            )
    for df in [d for d, _ in ord_source_frames if not d.empty] + [d for d, _ in ord_frames if not d.empty]:
        for _, r in df.iterrows():
            _merge_art(
                arts,
                r.get("Codice_Articolo"),
                {
                    "description": r.get("Descrizione"),
                    "categoria": r.get("Categoria"),
                    "tipologia": r.get("Tipologia"),
                    "marchio": r.get("Marchio"),
                    "colore": r.get("Colore"),
                    "materiale": r.get("Materiale"),
                },
            )
    dim_articles = [(k, v["description"], v["categoria"], v["tipologia"], v["marchio"], v["colore"], v["materiale"]) for k, v in sorted(arts.items())]

    sales_map: Dict[Tuple[str, str], Tuple[Any, ...]] = {}
    for _, r in clean_sales.iterrows():
        a, s = _article(r.get("Article")), _shop(r.get("Shop"))
        if not a or not s:
            continue
        sales_map[(a, s)] = (
            run_id, _dt(r.get("snapshot_at")), a, s, _f(r.get("Consegnato_Qty")), _f(r.get("Venduto_Qty")),
            _f(r.get("Periodo_Qty")), _f(r.get("Altro_Venduto_Qty")), _clamp_num(r.get("Sellout_Percent"), low=0.0, high=250.0),
            _clamp_num(r.get("Sellout_Clamped"), low=0.0, high=100.0), _f(r.get("Valore_1")), _f(r.get("Valore_2")), _f(r.get("Valore_3")), _f(r.get("Valore_4")),
        )
    sales_rows = list(sales_map.values())

    stock_map: Dict[Tuple[str, str], Tuple[Any, ...]] = {}
    for _, r in clean_stock.iterrows():
        a, s = _article(r.get("Article")), _shop(r.get("Shop"))
        if not a or not s:
            continue
        stock_map[(a, s)] = (
            run_id, _dt(r.get("snapshot_at")), a, s, _f(r.get("Ricevuto")), _f(r.get("Giacenza")), _f(r.get("Consegnato")), _f(r.get("Venduto")),
            _clamp_num(r.get("Sellout_Percent"), low=0.0, high=250.0), _f(r.get("Size_35")), _f(r.get("Size_36")), _f(r.get("Size_37")), _f(r.get("Size_38")),
            _f(r.get("Size_39")), _f(r.get("Size_40")), _f(r.get("Size_41")), _f(r.get("Size_42")), _f(r.get("Valore_Giac")),
        )
    stock_rows = list(stock_map.values())

    tr_map: Dict[Tuple[str, int, str, str, str], float] = {}
    for _, r in transfers.iterrows():
        a, z, fshop, tshop = _article(r.get("Article")), _i(r.get("Size")), _shop(r.get("From")), _shop(r.get("To"))
        qty, reason = _f(r.get("Qty")), _txt(r.get("Reason")) or "UNSPECIFIED"
        if not a or z is None or not fshop or not tshop or qty is None:
            continue
        k = (a, int(z), fshop, tshop, reason)
        tr_map[k] = tr_map.get(k, 0.0) + float(qty)
    tr_rows = [(run_id, a, z, fs, ts, rs, q) for (a, z, fs, ts, rs), q in tr_map.items()]

    feat_map: Dict[Tuple[str, str], Tuple[Any, ...]] = {}
    for _, r in features.iterrows():
        a, s = _article(r.get("Article")), _shop(r.get("Shop"))
        if not a or not s:
            continue
        feat_map[(a, s)] = (
            run_id, a, s, _i(r.get("Fascia")), _b(r.get("IsOutlet")), _txt(r.get("Role")), _f(r.get("DemandRaw")), _f(r.get("DemandRule")),
            _f(r.get("DemandAI")), _f(r.get("DemandBlendWeight")), _f(r.get("DemandHybrid")), _txt(r.get("DemandModelMode")),
            _f(r.get("DemandModelQualityR2")), _f(r.get("Periodo_Qty")), _f(r.get("Stock_after")), _f(r.get("ShopCapacityPairs")),
            _f(r.get("ShopCapacityTarget")), _f(r.get("ShopFreeCapacityAfter")), _txt(r.get("ShopCapacitySource")),
            _f(r.get("CapacityBlockedMoves")), _f(r.get("OpsBlockedMoves")), _f(r.get("ShopInboundBudget")), _f(r.get("ShopOutboundBudget")),
            _f(r.get("ShopInboundUsed")), _f(r.get("ShopOutboundUsed")), _f(r.get("Size_35")), _f(r.get("Size_36")),
            _f(r.get("Size_37")), _f(r.get("Size_38")), _f(r.get("Size_39")), _f(r.get("Size_40")), _f(r.get("Size_41")), _f(r.get("Size_42")),
        )
    feat_rows = list(feat_map.values())

    ord_main: Dict[Tuple[str, str, str, str], Tuple[Any, ...]] = {}
    ord_size: Dict[Tuple[str, str, str, str, int], float] = {}
    for df, meta in ord_frames:
        module, mode, season = meta["module"], meta["mode"], meta["season"]
        tot_col = "Ibrido_Totale" if mode == "hybrid" else "Da_Acquistare_Totale"
        pred_col = "Predizione_Vendite" if mode == "math" else ("Vendita_Totale_Prevista" if mode == "rf" else None)
        pref = "Ibrido_" if mode == "hybrid" else "Acquistare_"
        size_cols = [c for c in df.columns if c.startswith(pref) and _size(c) is not None]
        for _, r in df.iterrows():
            a = _article(r.get("Codice_Articolo"))
            if not a:
                continue
            ord_main[(module, season, mode, a)] = (
                run_id, module, season, mode, a, _clamp_num(r.get(tot_col), low=0.0),
                _clamp_num(r.get(pred_col), low=0.0) if pred_col else None, _clamp_num(r.get("Prezzo_Acquisto"), low=0.0), _clamp_num(r.get("Budget_Acquisto"), low=0.0),
            )
            for c in size_cols:
                z, q = _size(c), _clamp_num(r.get(c), low=0.0)
                if z is None or q is None:
                    continue
                k = (module, season, mode, a, int(z))
                ord_size[k] = ord_size.get(k, 0.0) + float(q)
    ord_main_rows = list(ord_main.values())
    ord_size_rows = [(run_id, m, s, mo, a, z, q) for (m, s, mo, a, z), q in ord_size.items() if abs(q) > 1e-12]

    ord_src_main: Dict[Tuple[str, str, str], Tuple[Any, ...]] = {}
    ord_src_size: Dict[Tuple[str, str, str, int], float] = {}
    for df, meta in ord_source_frames:
        module, season = meta["module"], meta["season"]
        size_cols = [c for c in df.columns if c.startswith("Venduto_") and _size(c) is not None]
        for _, r in df.iterrows():
            a = _article(r.get("Codice_Articolo"))
            if not a:
                continue
            ord_src_main[(module, season, a)] = (
                run_id,
                module,
                season,
                a,
                _txt(r.get("Categoria")),
                _txt(r.get("Tipologia")),
                _txt(r.get("Marchio")),
                _txt(r.get("Colore")),
                _txt(r.get("Materiale")),
                _txt(r.get("Descrizione")),
                _clamp_num(r.get("Venduto_Totale"), low=0.0),
                _clamp_num(r.get("Venduto_Periodo"), low=0.0),
                _clamp_num(r.get("Giacenza"), low=0.0),
                _clamp_num(r.get("Venduto_Extra"), low=0.0),
                _txt(r.get("Fascia_Prezzo")),
                _clamp_num(r.get("Prezzo_Listino"), low=0.0),
                _clamp_num(r.get("Prezzo_Acquisto"), low=0.0),
                _clamp_num(r.get("Prezzo_Vendita"), low=0.0),
            )
            for c in size_cols:
                z, q = _size(c), _clamp_num(r.get(c), low=0.0)
                if z is None or q is None:
                    continue
                k = (module, season, a, int(z))
                ord_src_size[k] = ord_src_size.get(k, 0.0) + float(q)
    ord_src_main_rows = list(ord_src_main.values())
    ord_src_size_rows = [
        (run_id, m, s, a, z, q)
        for (m, s, a, z), q in ord_src_size.items()
        if abs(q) > 1e-12
    ]

    ing_rows = []
    for r in ingest.get("rows", []) if isinstance(ingest.get("rows"), list) else []:
        if isinstance(r, dict):
            ing_rows.append((run_id, _txt(r.get("source")) or "", _txt(r.get("target")), _txt(r.get("kind")), _txt(r.get("status")) or "unknown", _txt(r.get("note"))))

    if verbose:
        print(f"[DB] start run={run_id}")
    conn = psycopg.connect(dsn)
    try:
        if create_schema:
            if verbose:
                print(f"[DB] apply schema {schema}")
            with conn.cursor() as cur:
                cur.execute(schema.read_text(encoding="utf-8"))
            conn.commit()

        base_meta = {
            "root": str(root),
            "create_schema": bool(create_schema),
            "orders_jobs": [{"module": j["module"], "mode": j["mode"], "season": j["season"], "file": str(j["path"])} for j in jobs],
            "order_source_jobs": [{"module": j["module"], "season": j["season"], "file": str(j["path"])} for j in source_jobs],
            "order_source_history_jobs": [{"module": j["module"], "season": j["season"], "file": str(j["path"])} for j in history_source_jobs],
            "catalog_source_history_jobs": catalog_history_jobs,
            "order_detail_history_jobs": detail_history_jobs,
        }
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_run (run_id, run_type, status, metadata) VALUES (%s, %s, 'running', %s::jsonb)",
                (run_id, run_type, json.dumps(base_meta, ensure_ascii=False)),
            )
        conn.commit()

        def _exec_many(sql: str, rows: Sequence[Tuple[Any, ...]]) -> int:
            if not rows:
                return 0
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            return len(rows)

        counts = {
            "dim_shop": _exec_many(
                """
                INSERT INTO dim_shop (shop_code, shop_name, fascia, mq) VALUES (%s, %s, %s, %s)
                ON CONFLICT (shop_code) DO UPDATE SET
                  shop_name=COALESCE(EXCLUDED.shop_name, dim_shop.shop_name),
                  fascia=COALESCE(EXCLUDED.fascia, dim_shop.fascia),
                  mq=COALESCE(EXCLUDED.mq, dim_shop.mq),
                  updated_at=NOW()
                """,
                dim_shops,
            ),
            "dim_article": _exec_many(
                """
                INSERT INTO dim_article (article_code, description, categoria, tipologia, marchio, colore, materiale)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (article_code) DO UPDATE SET
                  description=COALESCE(EXCLUDED.description, dim_article.description),
                  categoria=COALESCE(EXCLUDED.categoria, dim_article.categoria),
                  tipologia=COALESCE(EXCLUDED.tipologia, dim_article.tipologia),
                  marchio=COALESCE(EXCLUDED.marchio, dim_article.marchio),
                  colore=COALESCE(EXCLUDED.colore, dim_article.colore),
                  materiale=COALESCE(EXCLUDED.materiale, dim_article.materiale),
                  updated_at=NOW()
                """,
                dim_articles,
            ),
            "fact_sales_snapshot": _exec_many(
                """
                INSERT INTO fact_sales_snapshot (
                  run_id, snapshot_at, article_code, shop_code, consegnato_qty, venduto_qty, periodo_qty, altro_venduto_qty,
                  sellout_percent, sellout_clamped, valore_1, valore_2, valore_3, valore_4
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, article_code, shop_code) DO UPDATE SET
                  snapshot_at=EXCLUDED.snapshot_at, consegnato_qty=EXCLUDED.consegnato_qty, venduto_qty=EXCLUDED.venduto_qty,
                  periodo_qty=EXCLUDED.periodo_qty, altro_venduto_qty=EXCLUDED.altro_venduto_qty,
                  sellout_percent=EXCLUDED.sellout_percent, sellout_clamped=EXCLUDED.sellout_clamped,
                  valore_1=EXCLUDED.valore_1, valore_2=EXCLUDED.valore_2, valore_3=EXCLUDED.valore_3, valore_4=EXCLUDED.valore_4
                """,
                sales_rows,
            ),
            "fact_stock_snapshot": _exec_many(
                """
                INSERT INTO fact_stock_snapshot (
                  run_id, snapshot_at, article_code, shop_code, ricevuto, giacenza, consegnato, venduto, sellout_percent,
                  size_35, size_36, size_37, size_38, size_39, size_40, size_41, size_42, valore_giac
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, article_code, shop_code) DO UPDATE SET
                  snapshot_at=EXCLUDED.snapshot_at, ricevuto=EXCLUDED.ricevuto, giacenza=EXCLUDED.giacenza, consegnato=EXCLUDED.consegnato,
                  venduto=EXCLUDED.venduto, sellout_percent=EXCLUDED.sellout_percent,
                  size_35=EXCLUDED.size_35, size_36=EXCLUDED.size_36, size_37=EXCLUDED.size_37, size_38=EXCLUDED.size_38,
                  size_39=EXCLUDED.size_39, size_40=EXCLUDED.size_40, size_41=EXCLUDED.size_41, size_42=EXCLUDED.size_42,
                  valore_giac=EXCLUDED.valore_giac
                """,
                stock_rows,
            ),
            "fact_transfer_suggestion": _exec_many(
                """
                INSERT INTO fact_transfer_suggestion (run_id, article_code, size, from_shop_code, to_shop_code, reason, qty)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, article_code, size, from_shop_code, to_shop_code, reason) DO UPDATE SET qty=EXCLUDED.qty
                """,
                tr_rows,
            ),
            "fact_feature_state": _exec_many(
                """
                INSERT INTO fact_feature_state (
                  run_id, article_code, shop_code, fascia, is_outlet, role, demand_raw, demand_rule, demand_ai, demand_blend_weight, demand_hybrid,
                  demand_model_mode, demand_model_quality_r2, periodo_qty, stock_after, shop_capacity_pairs, shop_capacity_target, shop_free_capacity_after,
                  shop_capacity_source, capacity_blocked_moves, ops_blocked_moves, shop_inbound_budget, shop_outbound_budget, shop_inbound_used, shop_outbound_used,
                  size_35, size_36, size_37, size_38, size_39, size_40, size_41, size_42
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, article_code, shop_code) DO UPDATE SET
                  fascia=EXCLUDED.fascia, is_outlet=EXCLUDED.is_outlet, role=EXCLUDED.role, demand_raw=EXCLUDED.demand_raw, demand_rule=EXCLUDED.demand_rule,
                  demand_ai=EXCLUDED.demand_ai, demand_blend_weight=EXCLUDED.demand_blend_weight, demand_hybrid=EXCLUDED.demand_hybrid,
                  demand_model_mode=EXCLUDED.demand_model_mode, demand_model_quality_r2=EXCLUDED.demand_model_quality_r2,
                  periodo_qty=EXCLUDED.periodo_qty, stock_after=EXCLUDED.stock_after, shop_capacity_pairs=EXCLUDED.shop_capacity_pairs,
                  shop_capacity_target=EXCLUDED.shop_capacity_target, shop_free_capacity_after=EXCLUDED.shop_free_capacity_after,
                  shop_capacity_source=EXCLUDED.shop_capacity_source, capacity_blocked_moves=EXCLUDED.capacity_blocked_moves,
                  ops_blocked_moves=EXCLUDED.ops_blocked_moves, shop_inbound_budget=EXCLUDED.shop_inbound_budget, shop_outbound_budget=EXCLUDED.shop_outbound_budget,
                  shop_inbound_used=EXCLUDED.shop_inbound_used, shop_outbound_used=EXCLUDED.shop_outbound_used,
                  size_35=EXCLUDED.size_35, size_36=EXCLUDED.size_36, size_37=EXCLUDED.size_37, size_38=EXCLUDED.size_38,
                  size_39=EXCLUDED.size_39, size_40=EXCLUDED.size_40, size_41=EXCLUDED.size_41, size_42=EXCLUDED.size_42
                """,
                feat_rows,
            ),
            "fact_order_forecast": _exec_many(
                """
                INSERT INTO fact_order_forecast (
                  run_id, module, season_code, mode, article_code, totale_qty, predizione_vendite, prezzo_acquisto, budget_acquisto
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, module, season_code, mode, article_code) DO UPDATE SET
                  totale_qty=EXCLUDED.totale_qty, predizione_vendite=EXCLUDED.predizione_vendite,
                  prezzo_acquisto=EXCLUDED.prezzo_acquisto, budget_acquisto=EXCLUDED.budget_acquisto
                """,
                ord_main_rows,
            ),
            "fact_order_forecast_size": _exec_many(
                """
                INSERT INTO fact_order_forecast_size (run_id, module, season_code, mode, article_code, size, qty)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, module, season_code, mode, article_code, size) DO UPDATE SET qty=EXCLUDED.qty
                """,
                ord_size_rows,
            ),
            "fact_order_source": _exec_many(
                """
                INSERT INTO fact_order_source (
                  run_id, module, season_code, article_code, categoria, tipologia, marchio, colore, materiale,
                  descrizione, venduto_totale, venduto_periodo, giacenza, venduto_extra, fascia_prezzo, prezzo_listino, prezzo_acquisto, prezzo_vendita
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, module, season_code, article_code) DO UPDATE SET
                  categoria=EXCLUDED.categoria, tipologia=EXCLUDED.tipologia, marchio=EXCLUDED.marchio, colore=EXCLUDED.colore,
                  materiale=EXCLUDED.materiale, descrizione=EXCLUDED.descrizione, venduto_totale=EXCLUDED.venduto_totale,
                  venduto_periodo=EXCLUDED.venduto_periodo, giacenza=EXCLUDED.giacenza, venduto_extra=EXCLUDED.venduto_extra,
                  fascia_prezzo=EXCLUDED.fascia_prezzo, prezzo_listino=EXCLUDED.prezzo_listino,
                  prezzo_acquisto=EXCLUDED.prezzo_acquisto, prezzo_vendita=EXCLUDED.prezzo_vendita
                """,
                ord_src_main_rows,
            ),
            "fact_order_source_size": _exec_many(
                """
                INSERT INTO fact_order_source_size (run_id, module, season_code, article_code, size, venduto_qty)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, module, season_code, article_code, size) DO UPDATE SET venduto_qty=EXCLUDED.venduto_qty
                """,
                ord_src_size_rows,
            ),
            "ingest_file_log": _exec_many(
                """
                INSERT INTO ingest_file_log (run_id, source_path, target_path, file_kind, status, note)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                ing_rows,
            ),
        }
        done_meta = {**base_meta, "status": "completed", "counts": counts}
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE etl_run SET status='completed', finished_at=NOW(), metadata=%s::jsonb WHERE run_id=%s",
                (json.dumps(done_meta, ensure_ascii=False), run_id),
            )
        conn.commit()
        out_summary = {"run_id": str(run_id), "status": "completed", "counts": counts}
        if verbose:
            print(json.dumps(out_summary, indent=2, ensure_ascii=False))
        return out_summary
    except Exception as exc:
        conn.rollback()
        fail_meta = {"root": str(root), "status": "failed", "error": str(exc)}
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE etl_run SET status='failed', finished_at=NOW(), metadata=%s::jsonb WHERE run_id=%s",
                (json.dumps(fail_meta, ensure_ascii=False), run_id),
            )
        conn.commit()
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync output CSV -> PostgreSQL")
    p.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    p.add_argument("--create-schema", action="store_true")
    p.add_argument("--schema-path", type=Path, default=None)
    p.add_argument("--run-type", type=str, default="manual_sync")
    p.add_argument("--quiet", action="store_true")
    return p.parse_args()


def main():
    a = parse_args()
    try:
        res = run_db_sync(
            root=a.root,
            create_schema=bool(a.create_schema),
            schema_path=a.schema_path,
            run_type=a.run_type,
            verbose=not a.quiet,
        )
        if a.quiet:
            print(json.dumps(res, ensure_ascii=False))
    except Exception as exc:
        print(f"[DB] ERRORE: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
