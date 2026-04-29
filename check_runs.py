"""Trova quale run aveva SCARPE UOMO e cosa c'era nei file di origine."""
import psycopg

conn = psycopg.connect(dbname='barca', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()

# Trova il run che aveva più SCARPE UOMO
cur.execute("""
SELECT fss.run_id, r.started_at, r.finished_at, r.status,
       COUNT(DISTINCT fss.article_code) as uomo_count
FROM fact_stock_snapshot fss
JOIN dim_article da ON da.article_code = fss.article_code
JOIN etl_run r ON r.run_id = fss.run_id
WHERE da.reparto = 'SCARPE UOMO'
GROUP BY fss.run_id, r.started_at, r.finished_at, r.status
ORDER BY uomo_count DESC LIMIT 5
""")
print("Run con piu SCARPE UOMO in stock:")
for row in cur.fetchall():
    print(f"  run={str(row[0])[:8]}... | started={row[1]} | uomo={row[4]}")

print()

# Controlla il run corrente (senza UOMO) vs run storici (con UOMO)
cur.execute("""
SELECT r.run_id, r.started_at,
       COUNT(DISTINCT fss.article_code) as total_articles,
       SUM(CASE WHEN da.reparto = 'SCARPE DONNA' THEN 1 ELSE 0 END) as donna_count,
       SUM(CASE WHEN da.reparto = 'SCARPE UOMO' THEN 1 ELSE 0 END) as uomo_count
FROM fact_stock_snapshot fss
JOIN dim_article da ON da.article_code = fss.article_code
JOIN etl_run r ON r.run_id = fss.run_id
GROUP BY r.run_id, r.started_at
ORDER BY r.started_at DESC LIMIT 10
""")
print("Ultimi 10 run - donna vs uomo in stock:")
for row in cur.fetchall():
    print(f"  {str(row[0])[:8]}... | {row[1].strftime('%Y-%m-%d %H:%M') if row[1] else 'N/A'} | total={row[2]} | donna={row[3]} | uomo={row[4]}")

print()

# Controlla il contenuto degli ingest_log o file sorgente
cur.execute("""
SELECT column_name FROM information_schema.columns
WHERE table_name = 'etl_run' ORDER BY ordinal_position
""")
cols = [r[0] for r in cur.fetchall()]
print(f"Colonne etl_run: {cols}")

# Controlla etl_run per gli ultimi run
cur.execute("SELECT * FROM etl_run ORDER BY started_at DESC LIMIT 5")
rows = cur.fetchall()
if rows:
    for row in rows[:3]:
        print(f"  {row}")

conn.close()

