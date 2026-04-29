import os, sys, time
try:
    import dotenv; dotenv.load_dotenv()
except ImportError:
    pass
import psycopg

dsn = f"host={os.environ['BARCA_DB_HOST']} port={os.environ.get('BARCA_DB_PORT','5432')} dbname={os.environ['BARCA_DB_NAME']} user={os.environ['BARCA_DB_USER']} password={os.environ['BARCA_DB_PASSWORD']} sslmode={os.environ.get('BARCA_DB_SSLMODE','prefer')} connect_timeout=10"

indexes_to_create = [
    # fact_order_source: usato in JOIN su (run_id, module, article_code, season_code)
    "CREATE INDEX IF NOT EXISTS idx_order_source_run_join ON public.fact_order_source (run_id, module, article_code, season_code)",
    "CREATE INDEX IF NOT EXISTS idx_order_source_run_module ON public.fact_order_source (run_id, module)",
    # fact_order_forecast: filtro su run_id, JOIN su (run_id, module, article_code, season_code)
    "CREATE INDEX IF NOT EXISTS idx_order_forecast_run ON public.fact_order_forecast (run_id)",
    "CREATE INDEX IF NOT EXISTS idx_order_forecast_run_article ON public.fact_order_forecast (run_id, article_code)",
    # fact_feature_state: già ha PK su (run_id, article_code, shop_code) ma aggiungiamo per (run_id, article_code)
    "CREATE INDEX IF NOT EXISTS idx_feature_run_article ON public.fact_feature_state (run_id, article_code)",
    # fact_transfer_suggestion: indice per article_code lookup
    "CREATE INDEX IF NOT EXISTS idx_transfer_run_article ON public.fact_transfer_suggestion (run_id, article_code)",
]

with psycopg.connect(dsn) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        for sql in indexes_to_create:
            name = sql.split("idx_")[1].split(" ")[0]
            t0 = time.time()
            cur.execute(sql)
            print(f"  [{time.time()-t0:.2f}s] idx_{name} OK")

        print("\nVerifica indici su order tables:")
        cur.execute("""
            SELECT tablename, indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename IN ('fact_order_source', 'fact_order_forecast')
            ORDER BY tablename, indexname
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")

print("\nFatto. Ri-misuro le query lente...")

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT run_id FROM public.etl_run
            WHERE lower(coalesce(status,'')) IN ('completed','success','done','ok')
            ORDER BY coalesce(finished_at, started_at) DESC LIMIT 1
        """)
        rid = str(cur.fetchone()[0])

        t0 = time.time()
        cur.execute("""
            WITH fos AS MATERIALIZED (
              SELECT module, article_code, season_code, fascia_prezzo
              FROM public.fact_order_source WHERE run_id = %s::uuid
            )
            SELECT fo.module, fo.season_code, fo.article_code,
                   os.fascia_prezzo, fo.totale_qty
            FROM public.fact_order_forecast fo
            LEFT JOIN fos os ON os.module=fo.module AND os.article_code=fo.article_code AND os.season_code=fo.season_code
            WHERE fo.run_id = %s::uuid
            ORDER BY totale_qty DESC NULLS LAST LIMIT 200
        """, (rid, rid))
        rows = cur.fetchall()
        print(f"  [{time.time()-t0:.3f}s] Orders CTE join: {len(rows)} righe")

        t0 = time.time()
        cur.execute("""
            WITH fos AS MATERIALIZED (
              SELECT module, article_code, season_code,
                     fascia_prezzo, prezzo_listino, prezzo_vendita
              FROM public.fact_order_source WHERE run_id = %s::uuid
            )
            SELECT COALESCE(NULLIF(os.fascia_prezzo, ''), 'n/a') AS label,
                   SUM(COALESCE(fo.totale_qty, 0)) AS value
            FROM public.fact_order_forecast fo
            LEFT JOIN fos os ON os.module=fo.module AND os.article_code=fo.article_code AND os.season_code=fo.season_code
            WHERE fo.run_id = %s::uuid
            GROUP BY COALESCE(NULLIF(os.fascia_prezzo, ''), 'n/a')
        """, (rid, rid))
        rows = cur.fetchall()
        print(f"  [{time.time()-t0:.3f}s] Orders price band chart: {len(rows)} righe")

