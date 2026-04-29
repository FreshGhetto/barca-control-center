import os, sys
try:
    import dotenv; dotenv.load_dotenv()
except ImportError:
    pass

import psycopg

dsn = f"host={os.environ['BARCA_DB_HOST']} port={os.environ.get('BARCA_DB_PORT','5432')} dbname={os.environ['BARCA_DB_NAME']} user={os.environ['BARCA_DB_USER']} password={os.environ['BARCA_DB_PASSWORD']} sslmode={os.environ.get('BARCA_DB_SSLMODE','prefer')} connect_timeout=10"

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tablename, indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename IN (
                'fact_feature_state', 'fact_transfer_suggestion',
                'fact_stock_snapshot', 'fact_sales_snapshot'
            )
            ORDER BY tablename, indexname
        """)
        for row in cur.fetchall():
            print(f"{row[0]}: {row[1]}")
            print(f"  {row[2]}")

        # Conta le righe per dare un'idea del volume
        print("\n--- Conteggio righe per run più recente ---")
        cur.execute("""
            SELECT run_id FROM public.etl_run
            WHERE lower(coalesce(status,'')) IN ('completed','success','done','ok')
            ORDER BY coalesce(finished_at, started_at) DESC
            LIMIT 1
        """)
        r = cur.fetchone()
        if r:
            rid = r[0]
            print(f"run_id: {rid}")
            for tbl in ['fact_transfer_suggestion','fact_feature_state','fact_stock_snapshot','fact_sales_snapshot']:
                cur.execute(f"SELECT COUNT(*) FROM public.{tbl} WHERE run_id = %s::uuid", (rid,))
                print(f"  {tbl}: {cur.fetchone()[0]} righe")

