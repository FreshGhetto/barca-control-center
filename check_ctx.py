import sys, os
sys.path.insert(0, os.path.dirname(__file__))
os.chdir(os.path.dirname(__file__))

from barca_control_center.db_sync import get_db_dsn
import psycopg

dsn = get_db_dsn()
conn = psycopg.connect(dsn)
cur = conn.cursor()

RUN_ID = "fb165dfc-2e1d-4b09-9caa-b5327368d508"  # run più recente

print("=" * 60)
print("FACT_ORDER_SOURCE — dati di INPUT (storico ordini passati)")
print("=" * 60)
cur.execute("""
SELECT season_code, module, article_code, 
       venduto_periodo, giacenza, prezzo_acquisto
FROM public.fact_order_source
WHERE run_id = %s::uuid
ORDER BY season_code, module
LIMIT 5
""", (RUN_ID,))
rows = cur.fetchall()
for r in rows:
    print(f"  stagione={r[0]}  modulo={r[1]}  art={r[2]}  venduto={r[3]}  giacenza={r[4]}  prezzo_acq={r[5]}")

cur.execute("SELECT COUNT(DISTINCT season_code) FROM public.fact_order_source WHERE run_id=%s::uuid", (RUN_ID,))
print(f"\n  → Stagioni distinte in questo run: {cur.fetchone()[0]}")
cur.execute("SELECT DISTINCT season_code FROM public.fact_order_source WHERE run_id=%s::uuid ORDER BY season_code", (RUN_ID,))
print(f"  → Stagioni: {[r[0] for r in cur.fetchall()]}")

print()
print("=" * 60)
print("FACT_ORDER_FORECAST — dati di OUTPUT (previsione calcolata)")
print("=" * 60)
cur.execute("""
SELECT fo.season_code, fo.module, fo.article_code, 
       fo.totale_qty, fo.budget_acquisto, fo.mode
FROM public.fact_order_forecast fo
WHERE fo.run_id = %s::uuid
ORDER BY fo.season_code, fo.module
LIMIT 5
""", (RUN_ID,))
rows = cur.fetchall()
for r in rows:
    print(f"  stagione={r[0]}  modulo={r[1]}  art={r[2]}  qty={r[3]}  budget={r[4]}  mode={r[5]}")

cur.execute("SELECT COUNT(DISTINCT season_code) FROM public.fact_order_forecast WHERE run_id=%s::uuid", (RUN_ID,))
print(f"\n  → Stagioni distinte in questo run: {cur.fetchone()[0]}")
cur.execute("SELECT DISTINCT season_code FROM public.fact_order_forecast WHERE run_id=%s::uuid ORDER BY season_code", (RUN_ID,))
print(f"  → Stagioni: {[r[0] for r in cur.fetchall()]}")

print()
print("=" * 60)
print("FACT_TRANSFER_SUGGESTION — spostamenti calcolati (output)")
print("=" * 60)
cur.execute("""
SELECT from_shop_code, to_shop_code, article_code, qty
FROM public.fact_transfer_suggestion
WHERE run_id = %s::uuid
LIMIT 5
""", (RUN_ID,))
rows = cur.fetchall()
for r in rows:
    print(f"  da={r[0]} → a={r[1]}  art={r[2]}  qty={r[3]}")
cur.execute("SELECT COUNT(*) FROM public.fact_transfer_suggestion WHERE run_id=%s::uuid", (RUN_ID,))
print(f"\n  → Totale spostamenti in questo run: {cur.fetchone()[0]:,}")
print("  (I trasferimenti NON hanno season_code)")

print()
print("=" * 60)
print("RIEPILOGO GENERALE")
print("=" * 60)
print("""
  fact_order_SOURCE  → storico ordini passati (INPUT della pipeline)
                       Serve come 'base' per calcolare i nuovi ordini.
                       Contiene: quanto è stato venduto nelle stagioni 22/23/24/25,
                       le giacenze, i prezzi. La pipeline legge questi dati
                       per prevedere gli ordini futuri.

  fact_order_FORECAST → previsione ordini CALCOLATA (OUTPUT della pipeline)
                        Solo per le stagioni correnti (ora: 25I + 25Y).
                        Contiene: quante unità ordinare per ogni articolo,
                        il budget, il modulo (corrente/continuativa).

  I filtro stagione nella dashboard filtra solo FORECAST, non SOURCE.
  Quindi: il dropdown mostra 22/23/24 perché esistono in SOURCE,
  ma selezionarli fa andare ordini/qty/budget a 0 (nessun forecast).
""")
conn.close()
