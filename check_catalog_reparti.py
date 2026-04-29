"""Controlla i reparti nel catalogo (vw_catalog_article_store_current)."""
import psycopg

conn = psycopg.connect(dbname='barca', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()

# 1. Reparti nel catalogo corrente
cur.execute("""
SELECT DISTINCT reparto, COUNT(*) as n
FROM vw_catalog_article_store_current
WHERE store_code = 'XX' AND reparto IS NOT NULL AND reparto <> ''
GROUP BY reparto ORDER BY n DESC
""")
rows = cur.fetchall()
print("Reparti in vw_catalog_article_store_current (catalogo):")
for row in rows:
    print(f"  {repr(row[0])}: {row[1]}")
if not rows:
    print("  VUOTO o nessun reparto trovato!")

print()

# 2. Conta i record nel catalogo
cur.execute("SELECT COUNT(*) FROM vw_catalog_article_store_current WHERE store_code = 'XX'")
total = cur.fetchone()[0]
print(f"Totale articoli nel catalogo corrente: {total}")

print()

# 3. Controlla se la vista esiste
cur.execute("""
SELECT table_name FROM information_schema.views
WHERE table_name LIKE 'vw_catalog%'
ORDER BY table_name
""")
views = [r[0] for r in cur.fetchall()]
print(f"Viste catalogo disponibili: {views}")

print()

# 4. Controlla fact_catalog_article_store_snapshot
cur.execute("""
SELECT run_id, COUNT(*) as n
FROM fact_catalog_article_store_snapshot
WHERE store_code = 'XX'
GROUP BY run_id ORDER BY n DESC LIMIT 5
""")
rows = cur.fetchall()
print("Top 5 run in fact_catalog_article_store_snapshot:")
for row in rows:
    print(f"  {str(row[0])[:8]}...: {row[1]} articoli")
if not rows:
    print("  VUOTO")

print()

# 5. Reparti per run nel catalogo snapshot
cur.execute("""
SELECT run_id, reparto, COUNT(*) as n
FROM fact_catalog_article_store_snapshot
WHERE store_code = 'XX' AND reparto IS NOT NULL AND reparto <> ''
GROUP BY run_id, reparto ORDER BY run_id, n DESC LIMIT 20
""")
rows = cur.fetchall()
print("Reparti per run in fact_catalog_article_store_snapshot:")
for row in rows:
    print(f"  {str(row[0])[:8]}... | {repr(row[1])}: {row[2]}")
if not rows:
    print("  VUOTO")

conn.close()

