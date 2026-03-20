from __future__ import annotations

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


def _fetch_df(conn, sql: str, params: Optional[tuple[Any, ...]] = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _pick_source_run_id(conn, explicit_run_id: Optional[str]) -> str:
    if explicit_run_id:
        q = """
            SELECT r.run_id
            FROM etl_run r
            WHERE r.run_id = %s::uuid
              AND EXISTS (SELECT 1 FROM fact_sales_snapshot s WHERE s.run_id = r.run_id)
              AND EXISTS (SELECT 1 FROM fact_stock_snapshot t WHERE t.run_id = r.run_id)
            LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(q, (explicit_run_id,))
            row = cur.fetchone()
        if not row:
            raise ValueError(f"run_id {explicit_run_id} non trovato o senza snapshot sales/stock.")
        return str(row[0])

    q = """
        SELECT r.run_id
        FROM etl_run r
        WHERE r.status = 'completed'
          AND EXISTS (SELECT 1 FROM fact_sales_snapshot s WHERE s.run_id = r.run_id)
          AND EXISTS (SELECT 1 FROM fact_stock_snapshot t WHERE t.run_id = r.run_id)
        ORDER BY COALESCE(r.finished_at, r.started_at) DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(q)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("Nessuna run DB completata con snapshot sales/stock disponibili.")
    return str(row[0])


def export_latest_clean_inputs_from_db(
    *,
    clean_sales_csv: Path,
    clean_stock_csv: Path,
    source_run_id: Optional[str] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Export clean_sales/clean_articles from the latest DB run (or explicit run_id)
    so the allocator pipeline can run with DB as primary source.
    """
    _require_psycopg()
    dsn = get_db_dsn()

    with psycopg.connect(dsn) as conn:
        run_id = _pick_source_run_id(conn, source_run_id)

        sales_df = _fetch_df(
            conn,
            """
            SELECT
              snapshot_at,
              article_code AS "Article",
              shop_code AS "Shop",
              consegnato_qty AS "Consegnato_Qty",
              venduto_qty AS "Venduto_Qty",
              periodo_qty AS "Periodo_Qty",
              altro_venduto_qty AS "Altro_Venduto_Qty",
              sellout_percent AS "Sellout_Percent",
              sellout_clamped AS "Sellout_Clamped",
              valore_1 AS "Valore_1",
              valore_2 AS "Valore_2",
              valore_3 AS "Valore_3",
              valore_4 AS "Valore_4"
            FROM fact_sales_snapshot
            WHERE run_id = %s::uuid
            ORDER BY article_code, shop_code
            """,
            (run_id,),
        )

        stock_df = _fetch_df(
            conn,
            """
            SELECT
              fs.snapshot_at,
              fs.article_code AS "Article",
              COALESCE(da.description, '') AS "Description",
              fs.shop_code AS "Shop",
              fs.ricevuto AS "Ricevuto",
              fs.giacenza AS "Giacenza",
              fs.consegnato AS "Consegnato",
              fs.venduto AS "Venduto",
              fs.sellout_percent AS "Sellout_Percent",
              fs.size_35 AS "Size_35",
              fs.size_36 AS "Size_36",
              fs.size_37 AS "Size_37",
              fs.size_38 AS "Size_38",
              fs.size_39 AS "Size_39",
              fs.size_40 AS "Size_40",
              fs.size_41 AS "Size_41",
              fs.size_42 AS "Size_42",
              fs.valore_giac AS "Valore_Giac"
            FROM fact_stock_snapshot fs
            LEFT JOIN dim_article da ON da.article_code = fs.article_code
            WHERE fs.run_id = %s::uuid
            ORDER BY fs.article_code, fs.shop_code
            """,
            (run_id,),
        )

    clean_sales_csv.parent.mkdir(parents=True, exist_ok=True)
    clean_stock_csv.parent.mkdir(parents=True, exist_ok=True)
    sales_df.to_csv(clean_sales_csv, index=False)
    stock_df.to_csv(clean_stock_csv, index=False)

    summary = {
        "source_run_id": run_id,
        "sales_rows": int(len(sales_df)),
        "stock_rows": int(len(stock_df)),
        "clean_sales_csv": str(clean_sales_csv),
        "clean_stock_csv": str(clean_stock_csv),
    }
    if verbose:
        print(
            f"[DB-SOURCE] run_id={run_id} -> sales={len(sales_df)} righe, "
            f"stock={len(stock_df)} righe"
        )
    return summary

