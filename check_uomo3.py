"""Controlla SCARPE UOMO in fact_order_forecast e stock pipelines - v3."""
import psycopg

conn = psycopg.connect(dbname='barca', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()

# Colonne di fact_order_forecast
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='fact_order_forecast' ORDER BY ordinal_position")
cols = [r[0] for r in cur.fetchall()]
print(f"Colonne fact_order_forecast: {cols}\n")

# Reparti in fact_order_forecast
cur.execute("""
SELECT da.reparto, COUNT(DISTINCT fo.article_code)
FROM fact_order_forecast fo
LEFT JOIN dim_article da ON da.article_code = fo.article_code
WHERE fo.run_id = (SELECT run_id FROM fact_order_forecast ORDER BY created_at DESC LIMIT 1)
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print("Reparti in fact_order_forecast:")
for row in cur.fetchall():
    print(f"  {repr(row[0])}: {row[1]} articoli")

# SCARPE UOMO nei run storici fact_stock_snapshot
cur.execute("""
SELECT fss.run_id, COUNT(DISTINCT fss.article_code)
FROM fact_stock_snapshot fss
JOIN dim_article da ON da.article_code = fss.article_code
WHERE da.reparto = 'SCARPE UOMO'
GROUP BY fss.run_id ORDER BY COUNT(*) DESC LIMIT 10
""")
rows = cur.fetchall()
print(f"\nRun con SCARPE UOMO in fact_stock_snapshot: {len(rows)}")
for row in rows:
    print(f"  {str(row[0])[:8]}... | {row[1]}")
if not rows:
    print("  NESSUNO - il file di stock non contiene mai scarpe uomo!")

# Reparti aggregati fact_stock_snapshot
cur.execute("""
SELECT da.reparto, COUNT(DISTINCT fss.article_code)
FROM fact_stock_snapshot fss
LEFT JOIN dim_article da ON da.article_code = fss.article_code
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print("\nReparti in fact_stock_snapshot (tutti i run aggregati):")
for row in cur.fetchall():
    print(f"  {repr(row[0])}: {row[1]}")

conn.close()

