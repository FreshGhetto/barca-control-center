import os, time
try:
    import dotenv; dotenv.load_dotenv()
except ImportError:
    pass
import psycopg

dsn = (
    f"host={os.environ['BARCA_DB_HOST']} "
    f"port={os.environ.get('BARCA_DB_PORT','5432')} "
    f"dbname={os.environ['BARCA_DB_NAME']} "
    f"user={os.environ['BARCA_DB_USER']} "
    f"password={os.environ['BARCA_DB_PASSWORD']} "
    f"sslmode={os.environ.get('BARCA_DB_SSLMODE','prefer')} "
    f"connect_timeout=10"
)

with psycopg.connect(dsn) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:

        # ANALYZE per aggiornare le statistiche del planner
        print("ANALYZE fact_order_source...")
        t0 = time.time()
        cur.execute("ANALYZE public.fact_order_source")
        print(f"  fatto [{time.time()-t0:.2f}s]")

        print("ANALYZE fact_order_forecast...")
        t0 = time.time()
        cur.execute("ANALYZE public.fact_order_forecast")
        print(f"  fatto [{time.time()-t0:.2f}s]")

        # Recupera il run_id più recente completato
        cur.execute("""
            SELECT run_id FROM public.etl_run
            WHERE lower(coalesce(status,'')) IN ('completed','success','done','ok')
            ORDER BY coalesce(finished_at, started_at) DESC LIMIT 1
        """)
        rid = str(cur.fetchone()[0])
        print(f"\nrun_id usato: {rid}")

        # Conta righe per dimensionare il problema
        cur.execute("SELECT count(*) FROM public.fact_order_source WHERE run_id = %s::uuid", (rid,))
        print(f"fact_order_source righe: {cur.fetchone()[0]}")
        cur.execute("SELECT count(*) FROM public.fact_order_forecast WHERE run_id = %s::uuid", (rid,))
        print(f"fact_order_forecast righe: {cur.fetchone()[0]}")

        print()

        # ---- Query 1: CTE join (la più lenta) ----
        t0 = time.time()
        cur.execute("""
            WITH fos AS MATERIALIZED (
              SELECT module, article_code, season_code, fascia_prezzo
              FROM public.fact_order_source WHERE run_id = %s::uuid
            )
            SELECT fo.module, fo.season_code, fo.article_code,
                   os.fascia_prezzo, fo.totale_qty
            FROM public.fact_order_forecast fo
            LEFT JOIN fos os
              ON os.module = fo.module
             AND os.article_code = fo.article_code
             AND os.season_code = fo.season_code
            WHERE fo.run_id = %s::uuid
            ORDER BY totale_qty DESC NULLS LAST LIMIT 200
        """, (rid, rid))
        rows = cur.fetchall()
        print(f"[{time.time()-t0:.3f}s] CTE join LIMIT 200: {len(rows)} righe")

        # ---- Query 2: price band chart ----
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
            LEFT JOIN fos os
              ON os.module = fo.module
             AND os.article_code = fo.article_code
             AND os.season_code = fo.season_code
            WHERE fo.run_id = %s::uuid
            GROUP BY COALESCE(NULLIF(os.fascia_prezzo, ''), 'n/a')
        """, (rid, rid))
        rows = cur.fetchall()
        print(f"[{time.time()-t0:.3f}s] Price band chart: {len(rows)} righe")

        # ---- EXPLAIN ANALYZE sulla query più lenta ----
        print("\n--- EXPLAIN ANALYZE CTE join ---")
        cur.execute("""
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            WITH fos AS MATERIALIZED (
              SELECT module, article_code, season_code, fascia_prezzo
              FROM public.fact_order_source WHERE run_id = %s::uuid
            )
            SELECT fo.module, fo.season_code, fo.article_code,
                   os.fascia_prezzo, fo.totale_qty
            FROM public.fact_order_forecast fo
            LEFT JOIN fos os
              ON os.module = fo.module
             AND os.article_code = fo.article_code
             AND os.season_code = fo.season_code
            WHERE fo.run_id = %s::uuid
            ORDER BY totale_qty DESC NULLS LAST LIMIT 200
        """, (rid, rid))
        for row in cur.fetchall():
            print(row[0])

