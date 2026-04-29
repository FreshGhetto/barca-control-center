"""Profila le query ottimizzate con CTE MATERIALIZED."""
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

        # 1. orders_by_price_band (CTE)
        tq("orders_by_price_band (CTE)", """
            WITH fos AS MATERIALIZED (
              SELECT module, article_code, season_code, fascia_prezzo
              FROM public.fact_order_source WHERE run_id = %s::uuid
            )
            SELECT COALESCE(NULLIF(os.fascia_prezzo,''),'n/a') AS label,
                   SUM(COALESCE(fo.totale_qty,0)) AS value
            FROM public.fact_order_forecast fo
            LEFT JOIN fos os ON os.module=fo.module AND os.article_code=fo.article_code AND os.season_code=fo.season_code
            WHERE fo.run_id = %s::uuid
            GROUP BY COALESCE(NULLIF(os.fascia_prezzo,''),'n/a')
        """, (rid, rid))

        # 2. order_rows (CTE)
        tq("order_rows LIMIT 200 (CTE)", """
            WITH fos AS MATERIALIZED (
              SELECT module, article_code, season_code, fascia_prezzo, prezzo_listino, prezzo_vendita
              FROM public.fact_order_source WHERE run_id = %s::uuid
            )
            SELECT fo.module, fo.season_code, fo.mode, fo.article_code,
                   os.fascia_prezzo, os.prezzo_listino, os.prezzo_vendita,
                   fo.totale_qty, fo.predizione_vendite, fo.budget_acquisto
            FROM public.fact_order_forecast fo
            LEFT JOIN fos os
              ON os.module=fo.module AND os.article_code=fo.article_code AND os.season_code=fo.season_code
            WHERE fo.run_id = %s::uuid
            ORDER BY totale_qty DESC NULLS LAST, article_code ASC LIMIT 200
        """, (rid, rid))

        # 3. next_current (CTE materializzata)
        tq("next_current_all (CTE)", """
            WITH latest_cont AS (
                SELECT season_code FROM public.fact_order_source
                WHERE run_id = %s::uuid AND module = 'continuativa' AND season_code IS NOT NULL
                GROUP BY season_code ORDER BY season_code DESC LIMIT 1
            ),
            fos_current AS MATERIALIZED (
                SELECT article_code, season_code, venduto_periodo
                FROM public.fact_order_source WHERE run_id = %s::uuid AND module = 'current'
            ),
            global_factor AS (
                SELECT COALESCE(AVG(CASE WHEN COALESCE(os.venduto_periodo,0)>0 THEN fo.totale_qty/NULLIF(os.venduto_periodo,0) ELSE NULL END),1.0) AS factor
                FROM public.fact_order_forecast fo
                JOIN fos_current os ON os.article_code=fo.article_code AND os.season_code=fo.season_code
                WHERE fo.run_id=%s::uuid AND fo.module='current' AND fo.mode='math'
            ),
            fos_current_cat AS MATERIALIZED (
                SELECT fo.article_code, fo.season_code, fo.totale_qty, os.venduto_periodo, os2.categoria, os2.tipologia
                FROM public.fact_order_forecast fo
                JOIN fos_current os ON os.article_code=fo.article_code AND os.season_code=fo.season_code
                LEFT JOIN public.fact_order_source os2 ON os2.run_id=fo.run_id AND os2.module=fo.module AND os2.article_code=fo.article_code AND os2.season_code=fo.season_code
                WHERE fo.run_id=%s::uuid AND fo.module='current' AND fo.mode='math'
            ),
            factor_by_attr AS (
                SELECT categoria, tipologia,
                       AVG(CASE WHEN COALESCE(venduto_periodo,0)>0 THEN totale_qty/NULLIF(venduto_periodo,0) ELSE NULL END) AS factor
                FROM fos_current_cat GROUP BY categoria, tipologia
            )
            SELECT c.season_code AS from_cont_season, c.article_code, c.fascia_prezzo,
                   COALESCE(c.venduto_periodo,0), COALESCE(c.giacenza,0)
            FROM public.fact_order_source c
            JOIN latest_cont lc ON lc.season_code=c.season_code
            LEFT JOIN factor_by_attr fa ON fa.categoria IS NOT DISTINCT FROM c.categoria AND fa.tipologia IS NOT DISTINCT FROM c.tipologia
            CROSS JOIN global_factor gf
            WHERE c.run_id=%s::uuid AND c.module='continuativa'
            ORDER BY COALESCE(c.venduto_periodo,0) DESC, c.article_code ASC
        """, (rid, rid, rid, rid, rid))

        print("\nTotale stimato (senza overhead): tutte le query sopra")
print("Fatto.")

