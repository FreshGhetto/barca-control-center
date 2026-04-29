"""Aggiunge indici per velocizzare la query dashboard trasferimenti."""
import sys
sys.path.insert(0, '.')
from barca_control_center.db_sync import get_db_dsn
import psycopg

dsn = get_db_dsn()
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        # Indice per la query: WHERE run_id = X ORDER BY qty DESC LIMIT N
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_transfer_run_qty
            ON public.fact_transfer_suggestion (run_id, qty DESC);
        """)
        print("Indice idx_transfer_run_qty OK")

        # Indice per fact_feature_state: già ha pkey su (run_id, article_code, shop_code)
        # ma confirmiamo che sia usabile
        cur.execute("""
            SELECT pg_size_pretty(pg_relation_size('fact_feature_state')) AS ffs_size,
                   pg_size_pretty(pg_relation_size('fact_transfer_suggestion')) AS fts_size
        """)
        row = cur.fetchone()
        print(f"Dimensioni tabelle: fact_feature_state={row[0]}, fact_transfer_suggestion={row[1]}")
    conn.commit()
print("Indici OK")

