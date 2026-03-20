from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from db_sync import get_db_dsn

try:
    import psycopg
except Exception:  # pragma: no cover - optional import guard
    psycopg = None


def _require_psycopg():
    if psycopg is None:
        raise RuntimeError("psycopg non installato. Esegui: pip install -r requirements.txt")


def _fetch_df(conn, sql: str, params: tuple[Any, ...]) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _pick_orders_source_run_id(conn, explicit_run_id: Optional[str]) -> str:
    if explicit_run_id:
        q = """
            SELECT r.run_id
            FROM etl_run r
            WHERE r.run_id = %s::uuid
              AND EXISTS (SELECT 1 FROM fact_order_forecast f WHERE f.run_id = r.run_id)
            LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(q, (explicit_run_id,))
            row = cur.fetchone()
        if not row:
            raise ValueError(f"run_id {explicit_run_id} non trovato o senza dati ordini.")
        return str(row[0])

    q = """
        SELECT r.run_id
        FROM etl_run r
        WHERE r.status = 'completed'
          AND EXISTS (SELECT 1 FROM fact_order_forecast f WHERE f.run_id = r.run_id)
        ORDER BY COALESCE(r.finished_at, r.started_at) DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(q)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("Nessuna run DB completata con dati ordini disponibili.")
    return str(row[0])


def _safe_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _build_forecast_df(
    main_df: pd.DataFrame,
    size_df: pd.DataFrame,
    *,
    module: str,
    mode: str,
) -> Optional[pd.DataFrame]:
    m = main_df[(main_df["module"] == module) & (main_df["mode"] == mode)].copy()
    if m.empty:
        return None
    m = m.sort_values(["article_code"]).reset_index(drop=True)

    out = pd.DataFrame(
        {
            "Codice_Articolo": m["article_code"],
            "Categoria": m["categoria"].fillna(""),
            "Tipologia": m["tipologia"].fillna(""),
            "Marchio": m["marchio"].fillna(""),
            "Colore": m["colore"].fillna(""),
            "Materiale": m["materiale"].fillna(""),
            "Descrizione": m["description"].fillna(""),
        }
    )

    if mode == "hybrid":
        out["Ibrido_Totale"] = _safe_num(m["totale_qty"]).round().astype(int)
    else:
        out["Da_Acquistare_Totale"] = _safe_num(m["totale_qty"]).round().astype(int)

    if mode == "math":
        out["Predizione_Vendite"] = _safe_num(m["predizione_vendite"]).round(1)
    elif mode == "rf":
        out["Vendita_Totale_Prevista"] = _safe_num(m["predizione_vendite"]).round(1)

    if m["prezzo_acquisto"].notna().any():
        out["Prezzo_Acquisto"] = pd.to_numeric(m["prezzo_acquisto"], errors="coerce")
    if m["budget_acquisto"].notna().any():
        out["Budget_Acquisto"] = pd.to_numeric(m["budget_acquisto"], errors="coerce").round(2)

    s = size_df[(size_df["module"] == module) & (size_df["mode"] == mode)].copy()
    if not s.empty:
        s["size"] = pd.to_numeric(s["size"], errors="coerce")
        s = s.dropna(subset=["size"])
        s["size"] = s["size"].astype(int)
        pvt = (
            s.pivot_table(index="article_code", columns="size", values="qty", aggfunc="sum", fill_value=0.0)
            .sort_index(axis=1)
        )
        size_prefix = "Ibrido_" if mode == "hybrid" else "Acquistare_"
        for sz in pvt.columns.tolist():
            col_name = f"{size_prefix}{int(sz)}"
            out[col_name] = (
                pd.to_numeric(out["Codice_Articolo"].map(pvt[sz]), errors="coerce")
                .fillna(0.0)
                .round()
                .astype(int)
            )

    priority_cols = [
        "Codice_Articolo",
        "Categoria",
        "Tipologia",
        "Marchio",
        "Colore",
        "Materiale",
        "Descrizione",
        "Predizione_Vendite",
        "Vendita_Totale_Prevista",
        "Da_Acquistare_Totale",
        "Ibrido_Totale",
        "Prezzo_Acquisto",
        "Budget_Acquisto",
    ]
    size_cols = sorted(
        [
            c
            for c in out.columns
            if (c.startswith("Acquistare_") or c.startswith("Ibrido_")) and c.split("_")[-1].isdigit()
        ],
        key=lambda c: (0 if c.startswith("Acquistare_") else 1, int(c.split("_")[-1])),
    )
    ordered_cols = [c for c in priority_cols if c in out.columns] + [c for c in size_cols if c in out.columns]
    remaining = [c for c in out.columns if c not in ordered_cols]
    return out[ordered_cols + remaining]


def _build_source_df(
    source_main_df: pd.DataFrame,
    source_size_df: pd.DataFrame,
    *,
    module: str,
) -> Optional[pd.DataFrame]:
    m = source_main_df[source_main_df["module"] == module].copy()
    if m.empty:
        return None
    m = m.sort_values(["article_code"]).reset_index(drop=True)
    out = pd.DataFrame(
        {
            "Codice_Articolo": m["article_code"],
            "Categoria": m["categoria"].fillna(""),
            "Tipologia": m["tipologia"].fillna(""),
            "Marchio": m["marchio"].fillna(""),
            "Colore": m["colore"].fillna(""),
            "Materiale": m["materiale"].fillna(""),
            "Descrizione": m["descrizione"].fillna(""),
            "Venduto_Totale": _safe_num(m["venduto_totale"]).round().astype(int),
            "Venduto_Periodo": _safe_num(m["venduto_periodo"]).round().astype(int),
            "Giacenza": _safe_num(m["giacenza"]).round().astype(int),
            "Venduto_Extra": _safe_num(m["venduto_extra"]).round().astype(int),
        }
    )
    if m["prezzo_acquisto"].notna().any():
        out["Prezzo_Acquisto"] = pd.to_numeric(m["prezzo_acquisto"], errors="coerce")

    s = source_size_df[source_size_df["module"] == module].copy()
    if not s.empty:
        s["size"] = pd.to_numeric(s["size"], errors="coerce")
        s = s.dropna(subset=["size"])
        s["size"] = s["size"].astype(int)
        pvt = (
            s.pivot_table(index="article_code", columns="size", values="venduto_qty", aggfunc="sum", fill_value=0.0)
            .sort_index(axis=1)
        )
        for sz in pvt.columns.tolist():
            col_name = f"Venduto_{int(sz)}"
            out[col_name] = (
                pd.to_numeric(out["Codice_Articolo"].map(pvt[sz]), errors="coerce")
                .fillna(0.0)
                .round()
                .astype(int)
            )

    vend_size_cols = sorted(
        [c for c in out.columns if c.startswith("Venduto_") and c.split("_")[-1].isdigit()],
        key=lambda c: int(c.split("_")[-1]),
    )
    priority_cols = [
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
        "Prezzo_Acquisto",
    ]
    ordered = [c for c in priority_cols if c in out.columns] + vend_size_cols
    remaining = [c for c in out.columns if c not in ordered]
    return out[ordered + remaining]


def export_orders_outputs_from_db(
    *,
    output_dir: Path,
    source_run_id: Optional[str] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Rebuild orders output files from DB facts to enable DB-first operational runs.
    """
    _require_psycopg()
    output_orders = output_dir / "orders"
    output_orders.mkdir(parents=True, exist_ok=True)

    dsn = get_db_dsn()
    with psycopg.connect(dsn) as conn:
        run_id = _pick_orders_source_run_id(conn, source_run_id)
        main_df = _fetch_df(
            conn,
            """
            SELECT
              f.module,
              f.season_code,
              f.mode,
              f.article_code,
              f.totale_qty,
              f.predizione_vendite,
              f.prezzo_acquisto,
              f.budget_acquisto,
              da.categoria,
              da.tipologia,
              da.marchio,
              da.colore,
              da.materiale,
              da.description
            FROM fact_order_forecast f
            LEFT JOIN dim_article da ON da.article_code = f.article_code
            WHERE f.run_id = %s::uuid
            ORDER BY f.module, f.mode, f.article_code
            """,
            (run_id,),
        )
        size_df = _fetch_df(
            conn,
            """
            SELECT
              module,
              season_code,
              mode,
              article_code,
              size,
              qty
            FROM fact_order_forecast_size
            WHERE run_id = %s::uuid
            ORDER BY module, mode, article_code, size
            """,
            (run_id,),
        )
        source_main_df = _fetch_df(
            conn,
            """
            SELECT
              module,
              season_code,
              article_code,
              categoria,
              tipologia,
              marchio,
              colore,
              materiale,
              descrizione,
              venduto_totale,
              venduto_periodo,
              giacenza,
              venduto_extra,
              prezzo_acquisto
            FROM fact_order_source
            WHERE run_id = %s::uuid
            ORDER BY module, article_code
            """,
            (run_id,),
        )
        source_size_df = _fetch_df(
            conn,
            """
            SELECT
              module,
              season_code,
              article_code,
              size,
              venduto_qty
            FROM fact_order_source_size
            WHERE run_id = %s::uuid
            ORDER BY module, article_code, size
            """,
            (run_id,),
        )

    if main_df.empty:
        summary = {
            "enabled": False,
            "reason": "no_orders_in_source_run",
            "source": "db",
            "source_run_id": run_id,
            "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        }
        (output_orders / "orders_summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        (output_orders / "orders_run_log.txt").write_text(
            f"[{dt.datetime.now().strftime('%H:%M:%S')}] Nessun dato ordini trovato in run {run_id}\n",
            encoding="utf-8",
        )
        return summary

    file_map = {
        ("current", "math"): output_orders / "orders_current_previsione_math.csv",
        ("continuativa", "math"): output_orders / "orders_continuativa_previsione_math.csv",
        ("continuativa", "rf"): output_orders / "orders_continuativa_previsione_rf.csv",
        ("continuativa", "hybrid"): output_orders / "orders_continuativa_previsione_ibrida.csv",
    }

    written: Dict[tuple[str, str], Path] = {}
    totals: Dict[tuple[str, str], int] = {}
    rows_count: Dict[tuple[str, str], int] = {}
    for (module, mode), path in file_map.items():
        df_out = _build_forecast_df(main_df, size_df, module=module, mode=mode)
        if df_out is None or df_out.empty:
            continue
        df_out.to_csv(path, index=False)
        written[(module, mode)] = path
        rows_count[(module, mode)] = int(len(df_out))
        if mode == "hybrid" and "Ibrido_Totale" in df_out.columns:
            totals[(module, mode)] = int(pd.to_numeric(df_out["Ibrido_Totale"], errors="coerce").fillna(0).sum())
        elif "Da_Acquistare_Totale" in df_out.columns:
            totals[(module, mode)] = int(
                pd.to_numeric(df_out["Da_Acquistare_Totale"], errors="coerce").fillna(0).sum()
            )
        else:
            totals[(module, mode)] = 0

    source_written: Dict[str, Path] = {}
    source_rows: Dict[str, int] = {}
    source_file_map = {
        "current": output_orders / "orders_current_dati_originali.csv",
        "continuativa": output_orders / "orders_continuativa_dati_originali.csv",
    }
    for module, path in source_file_map.items():
        df_src = _build_source_df(source_main_df, source_size_df, module=module)
        if df_src is None or df_src.empty:
            continue
        df_src.to_csv(path, index=False)
        source_written[module] = path
        source_rows[module] = int(len(df_src))

    def _season_for(module: str) -> str:
        vals = pd.concat(
            [
                main_df.loc[main_df["module"] == module, "season_code"],
                source_main_df.loc[source_main_df["module"] == module, "season_code"],
            ],
            ignore_index=True,
        )
        vals = vals.dropna().astype(str).str.strip().tolist()
        return vals[0] if vals else "unknown"

    current_files = [p.name for k, p in written.items() if k[0] == "current"]
    continuativa_files = [p.name for k, p in written.items() if k[0] == "continuativa" and k[1] == "math"]
    full_files = [p.name for k, p in written.items() if k[0] == "continuativa" and k[1] in {"rf", "hybrid"}]
    current_source_file = source_written.get("current")
    continuativa_source_file = source_written.get("continuativa")
    season_values = (
        pd.concat([main_df.get("season_code", pd.Series(dtype=object)), source_main_df.get("season_code", pd.Series(dtype=object))])
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    summary: Dict[str, Any] = {
        "enabled": True,
        "source": "db",
        "source_run_id": run_id,
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "bundles_detected": sorted(season_values),
        "current": {
            "season": _season_for("current"),
            "articles_input": source_rows.get("current", rows_count.get(("current", "math"), 0)),
            "totale_math": totals.get(("current", "math"), 0),
            "output_files": current_files,
            "source_output_files": [current_source_file.name] if current_source_file else [],
        },
        "continuativa": {
            "season": _season_for("continuativa"),
            "articles_input": source_rows.get("continuativa", rows_count.get(("continuativa", "math"), 0)),
            "totale_math": totals.get(("continuativa", "math"), 0),
            "output_files": continuativa_files,
            "source_output_files": [continuativa_source_file.name] if continuativa_source_file else [],
            "full": {
                "enabled": bool(full_files),
                "totale_rf": totals.get(("continuativa", "rf"), 0),
                "totale_ibrido": totals.get(("continuativa", "hybrid"), 0),
                "output_files": full_files,
            },
        },
    }

    (output_orders / "orders_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    log_lines = [
        f"[{dt.datetime.now().strftime('%H:%M:%S')}] Modulo ordini DB-first: source_run_id={run_id}",
    ]
    for (module, mode), path in written.items():
        log_lines.append(
            f"[{dt.datetime.now().strftime('%H:%M:%S')}] WRITE {path.name} "
            f"(module={module}, mode={mode}, rows={rows_count.get((module, mode), 0)}, "
            f"totale={totals.get((module, mode), 0)})"
        )
    for module, path in source_written.items():
        log_lines.append(
            f"[{dt.datetime.now().strftime('%H:%M:%S')}] WRITE {path.name} "
            f"(module={module}, mode=source, rows={source_rows.get(module, 0)})"
        )
    (output_orders / "orders_run_log.txt").write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    if verbose:
        print(f"[ORDERS-DB] source_run_id={run_id} files={len(written) + len(source_written)}")
    return summary
