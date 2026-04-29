"""Profila OGNI query della dashboard per trovare quella lenta."""
import sys, time
sys.path.insert(0, '.')
from barca_control_center.db_sync import get_db_dsn
import psycopg

dsn = get_db_dsn()
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT run_id FROM etl_run ORDER BY started_at DESC LIMIT 1")
        rid = str(cur.fetchone()[0])
        print(f"run_id: {rid}\n")

        def tq(label, sql, params=()):
            t0 = time.time()
            cur.execute(sql, params)
            rows = cur.fetchall()
            t1 = time.time()
            print(f"[{t1-t0:.3f}s] {label}: {len(rows)} righe")
            return rows

        tq("kpi transfer_rows", "SELECT count(*) FROM fact_transfer_suggestion WHERE run_id=%s::uuid", (rid,))
        tq("kpi transfer_qty", "SELECT COALESCE(sum(qty),0) FROM fact_transfer_suggestion WHERE run_id=%s::uuid", (rid,))
        tq("kpi feature_rows", "SELECT count(*) FROM fact_feature_state WHERE run_id=%s::uuid", (rid,))
        tq("kpi critical_rows", """
            SELECT count(*) FROM fact_feature_state
            WHERE run_id=%s::uuid AND demand_hybrid IS NOT NULL AND stock_after IS NOT NULL
            AND (demand_hybrid - stock_after) > 0
        """, (rid,))
        tq("chart transfer_to", """
            SELECT to_shop_code AS label, SUM(qty) AS value FROM fact_transfer_suggestion
            WHERE run_id=%s::uuid GROUP BY to_shop_code ORDER BY value DESC LIMIT 12
        """, (rid,))
        tq("chart transfer_from", """
            SELECT from_shop_code, SUM(qty) FROM fact_transfer_suggestion
            WHERE run_id=%s::uuid GROUP BY from_shop_code ORDER BY SUM(qty) DESC LIMIT 12
        """, (rid,))
        tq("chart transfer_reason", """
            SELECT COALESCE(reason,'n/a'), SUM(qty) FROM fact_transfer_suggestion
            WHERE run_id=%s::uuid GROUP BY COALESCE(reason,'n/a') ORDER BY SUM(qty) DESC LIMIT 12
        """, (rid,))
        tq("table transfer_rows (CTE 2000)", """
            WITH top_t AS (
              SELECT article_code, size, from_shop_code, to_shop_code, COALESCE(reason,'') AS reason, qty
              FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid ORDER BY qty DESC LIMIT 2000
            )
            SELECT t.article_code, COALESCE(da.reparto,''), COALESCE(da.categoria,''), COALESCE(da.marchio,''),
                   t.size, t.from_shop_code,
                   ff.observed_sales_signal, ff.zero_sales_source_candidate,
                   t.to_shop_code, tf.observed_sales_signal, tf.missing_core_sizes, tf.destination_priority_score,
                   t.reason, t.qty
            FROM top_t t
            LEFT JOIN public.dim_article da ON da.article_code = t.article_code
            LEFT JOIN public.fact_feature_state ff ON ff.run_id=%s::uuid AND ff.article_code=t.article_code AND ff.shop_code=t.from_shop_code
            LEFT JOIN public.fact_feature_state tf ON tf.run_id=%s::uuid AND tf.article_code=t.article_code AND tf.shop_code=t.to_shop_code
        """, (rid, rid, rid))

        tq("table order_proposals LIMIT 200", """
            SELECT fo.module, fo.season_code, fo.mode, fo.article_code,
                   os.fascia_prezzo, os.prezzo_listino, os.prezzo_vendita,
                   fo.totale_qty, fo.predizione_vendite, fo.budget_acquisto
            FROM public.fact_order_forecast fo
            LEFT JOIN public.fact_order_source os
              ON os.run_id=fo.run_id AND os.module=fo.module AND os.article_code=fo.article_code
              AND os.season_code IS NOT DISTINCT FROM fo.season_code
            WHERE fo.run_id=%s::uuid ORDER BY totale_qty DESC NULLS LAST, article_code ASC LIMIT 200
        """, (rid,))

        tq("table critical_articles LIMIT 200", """
            SELECT article_code, shop_code, demand_hybrid, stock_after, (demand_hybrid - stock_after) AS deficit
            FROM public.fact_feature_state
            WHERE run_id=%s::uuid AND demand_hybrid IS NOT NULL AND stock_after IS NOT NULL
            AND (demand_hybrid - stock_after) > 0
            ORDER BY deficit DESC LIMIT 200
        """, (rid,))

        # next_current_candidates
        tq("next_current_candidates", """
            WITH prev_run AS (
                SELECT run_id FROM etl_run
                WHERE run_id != %s::uuid ORDER BY started_at DESC LIMIT 1
            )
            SELECT fo.article_code,
                   fo.totale_qty AS qty_current,
                   fo.totale_qty AS qty_prev,
                   (fo.totale_qty - fo.totale_qty) AS delta_vs_stock
            FROM public.fact_order_forecast fo
            WHERE fo.run_id = %s::uuid
            LIMIT 200
        """, (rid, rid))

print("\nProfiling completato.")

