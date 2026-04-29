import os, sys, time
try:
    import dotenv; dotenv.load_dotenv()
except ImportError:
    pass
import psycopg

dsn = f"host={os.environ['BARCA_DB_HOST']} port={os.environ.get('BARCA_DB_PORT','5432')} dbname={os.environ['BARCA_DB_NAME']} user={os.environ['BARCA_DB_USER']} password={os.environ['BARCA_DB_PASSWORD']} sslmode={os.environ.get('BARCA_DB_SSLMODE','prefer')} connect_timeout=10"

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        # Prendi il run più recente con trasferimenti
        cur.execute("""
            SELECT run_id FROM public.etl_run
            WHERE lower(coalesce(status,'')) IN ('completed','success','done','ok')
            ORDER BY coalesce(finished_at, started_at) DESC LIMIT 1
        """)
        rid = str(cur.fetchone()[0])
        print(f"run_id: {rid}")

        queries = {
            "KPI counts": ("""
                SELECT
                  COUNT(DISTINCT article_code) AS articles,
                  COUNT(DISTINCT to_shop_code) AS shops,
                  SUM(qty) AS total_qty
                FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid
            """, (rid,)),

            "Transfer chart to": ("""
                SELECT to_shop_code AS label, SUM(qty) AS value
                FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid
                GROUP BY to_shop_code ORDER BY value DESC LIMIT 12
            """, (rid,)),

            "Transfer table 50k+JOIN": ("""
                WITH top_t AS (
                  SELECT article_code, size, from_shop_code, to_shop_code,
                         COALESCE(reason, '') AS reason, qty
                  FROM public.fact_transfer_suggestion
                  WHERE run_id = %s::uuid
                  ORDER BY qty DESC, article_code ASC LIMIT 50000
                )
                SELECT t.article_code, t.size, t.from_shop_code, t.to_shop_code,
                       ff.observed_sales_signal, tf.missing_core_sizes, t.qty
                FROM top_t t
                LEFT JOIN public.fact_feature_state ff
                  ON ff.run_id = %s::uuid AND ff.article_code = t.article_code AND ff.shop_code = t.from_shop_code
                LEFT JOIN public.fact_feature_state tf
                  ON tf.run_id = %s::uuid AND tf.article_code = t.article_code AND tf.shop_code = t.to_shop_code
                ORDER BY t.qty DESC
            """, (rid, rid, rid)),

            "Orders CTE join": ("""
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
            """, (rid, rid)),

            "Feature state critical": ("""
                SELECT article_code, shop_code, demand_hybrid - stock_after AS deficit
                FROM public.fact_feature_state
                WHERE run_id = %s::uuid AND demand_hybrid > stock_after
                ORDER BY deficit DESC LIMIT 200
            """, (rid,)),
        }

        for name, (sql, params) in queries.items():
            t0 = time.time()
            cur.execute(sql, params)
            rows = cur.fetchall()
            elapsed = time.time() - t0
            print(f"  [{elapsed:.3f}s] {name}: {len(rows)} righe")

