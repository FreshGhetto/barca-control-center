"""Profila la query dashboard trasferimenti."""
import sys, time
sys.path.insert(0, '.')
from barca_control_center.db_sync import get_db_dsn
import psycopg

dsn = get_db_dsn()
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        # Trova il run_id più recente
        cur.execute("SELECT run_id FROM etl_run ORDER BY started_at DESC LIMIT 1")
        rid = str(cur.fetchone()[0])
        print(f"run_id: {rid}")

        # Test 1: solo transfer suggestion senza join
        t0 = time.time()
        cur.execute("""
            SELECT article_code, size, from_shop_code, to_shop_code, reason, qty
            FROM public.fact_transfer_suggestion
            WHERE run_id = %s::uuid
            ORDER BY qty DESC LIMIT 2000
        """, (rid,))
        rows_basic = cur.fetchall()
        t1 = time.time()
        print(f"Test 1 (solo transfer, LIMIT 2000): {len(rows_basic)} righe in {t1-t0:.3f}s")

        # Test 2: transfer + dim_article JOIN
        t0 = time.time()
        cur.execute("""
            WITH top_t AS (
              SELECT article_code, size, from_shop_code, to_shop_code, COALESCE(reason,'') AS reason, qty
              FROM public.fact_transfer_suggestion
              WHERE run_id = %s::uuid
              ORDER BY qty DESC LIMIT 2000
            )
            SELECT t.article_code,
                   COALESCE(da.reparto,'') AS reparto,
                   COALESCE(da.categoria,'') AS categoria,
                   COALESCE(da.marchio,'') AS marchio,
                   t.size, t.from_shop_code, t.to_shop_code, t.reason, t.qty
            FROM top_t t
            LEFT JOIN public.dim_article da ON da.article_code = t.article_code
        """, (rid,))
        rows_with_da = cur.fetchall()
        t1 = time.time()
        print(f"Test 2 (+ dim_article, LIMIT 2000): {len(rows_with_da)} righe in {t1-t0:.3f}s")

        # Test 3: con doppio JOIN fact_feature_state (CTE approach)
        t0 = time.time()
        cur.execute("""
            WITH top_t AS (
              SELECT article_code, size, from_shop_code, to_shop_code, COALESCE(reason,'') AS reason, qty
              FROM public.fact_transfer_suggestion
              WHERE run_id = %s::uuid
              ORDER BY qty DESC LIMIT 2000
            )
            SELECT t.article_code,
                   COALESCE(da.reparto,'') AS reparto,
                   COALESCE(da.categoria,'') AS categoria,
                   COALESCE(da.marchio,'') AS marchio,
                   t.size, t.from_shop_code,
                   ff.observed_sales_signal AS from_obs,
                   ff.zero_sales_source_candidate AS from_zero,
                   t.to_shop_code,
                   tf.observed_sales_signal AS to_obs,
                   tf.missing_core_sizes AS to_missing,
                   tf.destination_priority_score AS to_score,
                   t.reason, t.qty
            FROM top_t t
            LEFT JOIN public.dim_article da ON da.article_code = t.article_code
            LEFT JOIN public.fact_feature_state ff
              ON ff.run_id = %s::uuid AND ff.article_code = t.article_code AND ff.shop_code = t.from_shop_code
            LEFT JOIN public.fact_feature_state tf
              ON tf.run_id = %s::uuid AND tf.article_code = t.article_code AND tf.shop_code = t.to_shop_code
        """, (rid, rid, rid))
        rows_full = cur.fetchall()
        t1 = time.time()
        print(f"Test 3 (+ fact_feature_state x2, LIMIT 2000): {len(rows_full)} righe in {t1-t0:.3f}s")

print("Profiling completato.")

