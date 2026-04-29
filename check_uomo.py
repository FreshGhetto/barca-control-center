"""Controlla perché SCARPE UOMO non compare nei transfer suggestions."""
import psycopg

conn = psycopg.connect(dbname='barca', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()

# 1. Quanti articoli SCARPE UOMO esistono in dim_article
cur.execute("""
SELECT reparto, COUNT(*) FROM dim_article
WHERE reparto IS NOT NULL
GROUP BY reparto ORDER BY COUNT(*) DESC
""")
print("Reparti in dim_article:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

print()

# 2. Controlla se SCARPE UOMO è in fact_feature_state (stock)
cur.execute("""
SELECT da.reparto, COUNT(DISTINCT ffs.article_code)
FROM fact_feature_state ffs
JOIN dim_article da ON da.article_code = ffs.article_code
WHERE ffs.run_id = (SELECT run_id FROM fact_feature_state ORDER BY created_at DESC LIMIT 1)
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print("Reparti in fact_feature_state (stock, ultimo run):")
for row in cur.fetchall():
    print(f"  {repr(row[0])}: {row[1]} articoli")

print()

# 3. Controlla se SCARPE UOMO è in fact_order_source (catalogo ordini)
cur.execute("""
SELECT da.reparto, COUNT(DISTINCT fos.article_code)
FROM fact_order_source fos
JOIN dim_article da ON da.article_code = fos.article_code
WHERE fos.run_id = (SELECT run_id FROM fact_order_source ORDER BY created_at DESC LIMIT 1)
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print("Reparti in fact_order_source (catalogo, ultimo run):")
for row in cur.fetchall():
    print(f"  {repr(row[0])}: {row[1]} articoli")

print()

# 4. Controlla qualche articolo SCARPE UOMO per capire cosa sta succedendo
cur.execute("""
SELECT da.article_code, da.reparto, da.categoria, da.marchio,
       EXISTS(SELECT 1 FROM fact_feature_state ffs WHERE ffs.article_code = da.article_code) as in_stock,
       EXISTS(SELECT 1 FROM fact_order_source fos WHERE fos.article_code = da.article_code) as in_catalog
FROM dim_article da
WHERE da.reparto = 'SCARPE UOMO'
LIMIT 10
""")
print("Campione articoli SCARPE UOMO:")
for row in cur.fetchall():
    print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]} | in_stock={row[4]} | in_catalog={row[5]}")

conn.close()

