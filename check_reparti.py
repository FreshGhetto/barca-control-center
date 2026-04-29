"""Controlla reparti disponibili nel DB e nei transfer suggestions."""
import psycopg

conn = psycopg.connect(dbname='barca', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()

# Controlla i reparti disponibili in dim_article
cur.execute("SELECT reparto, COUNT(*) FROM dim_article GROUP BY reparto ORDER BY COUNT(*) DESC")
print('Reparti in dim_article:')
for row in cur.fetchall():
    print(f'  {repr(row[0])}: {row[1]} articoli')

print()

# Controlla i reparti nei transfer suggestions dell'ultimo run
cur.execute("""
SELECT da.reparto, COUNT(*), SUM(ft.qty)
FROM fact_transfer_suggestion ft
JOIN dim_article da ON da.article_code = ft.article_code
WHERE ft.run_id = (SELECT run_id FROM fact_transfer_suggestion ORDER BY created_at DESC LIMIT 1)
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print('Reparti nei transfer suggestions (ultimo run):')
for row in cur.fetchall():
    print(f'  {repr(row[0])}: {row[1]} righe, {row[2]} paia')

print()

# Controlla le categorie
cur.execute("""
SELECT da.categoria, COUNT(*), SUM(ft.qty)
FROM fact_transfer_suggestion ft
JOIN dim_article da ON da.article_code = ft.article_code
WHERE ft.run_id = (SELECT run_id FROM fact_transfer_suggestion ORDER BY created_at DESC LIMIT 1)
GROUP BY da.categoria ORDER BY COUNT(*) DESC LIMIT 20
""")
print('Categorie nei transfer suggestions:')
for row in cur.fetchall():
    print(f'  {repr(row[0])}: {row[1]} righe, {row[2]} paia')

print()

# Controlla cosa c'è in order_proposals per reparto (fact_order_forecast)
cur.execute("""
SELECT da.reparto, COUNT(DISTINCT fo.article_code), SUM(fo.qty_suggested)
FROM fact_order_forecast fo
JOIN dim_article da ON da.article_code = fo.article_code
WHERE fo.run_id = (SELECT run_id FROM fact_order_forecast ORDER BY created_at DESC LIMIT 1)
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print('Reparti in fact_order_forecast (ultimo run):')
for row in cur.fetchall():
    print(f'  {repr(row[0])}: {row[1]} articoli, {row[2]} qty suggerita')

conn.close()

