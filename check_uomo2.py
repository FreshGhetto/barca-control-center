"""Controlla SCARPE UOMO in fact_order_forecast e stock pipelines - v2."""
import psycopg

conn = psycopg.connect(dbname='barca', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()

# Controlla le colonne di fact_order_forecast
cur.execute("""
SELECT column_name FROM information_schema.columns
WHERE table_name = 'fact_order_forecast'
ORDER BY ordinal_position
""")
cols = [r[0] for r in cur.fetchall()]
print(f"Colonne di fact_order_forecast: {cols}")
print()

# Controlla fact_order_forecast per reparto
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
print()

# Articoli SCARPE UOMO in fact_stock_snapshot (tutti i run)
cur.execute("""
SELECT fss.run_id, COUNT(DISTINCT fss.article_code)
FROM fact_stock_snapshot fss
JOIN dim_article da ON da.article_code = fss.article_code  
WHERE da.reparto = 'SCARPE UOMO'
GROUP BY fss.run_id ORDER BY COUNT(*) DESC LIMIT 10
""")
rows = cur.fetchall()
print("Run con SCARPE UOMO in fact_stock_snapshot (tutti i run):")
for row in rows:
    print(f"  run_id={str(row[0])[:8]}... | {row[1]} articoli")
if not rows:
    print("  NESSUNO!")

# Reparti storici aggregati in fact_stock_snapshot
cur.execute("""
SELECT da.reparto, COUNT(DISTINCT fss.article_code)
FROM fact_stock_snapshot fss
LEFT JOIN dim_article da ON da.article_code = fss.article_code
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print()
print("Reparti in fact_stock_snapshot (TUTTI i run aggregati):")
for row in cur.fetchall():
    print(f"  {repr(row[0])}: {row[1]} articoli")
conn.close()

# ---- VECCHIA VERSIONE eliminata ----
# 1. Controlla fact_order_forecast per reparto
if False:
    cur.execute("""
SELECT da.reparto, COUNT(DISTINCT fo.article_code), SUM(fo.qty_suggested)
FROM fact_order_forecast fo
LEFT JOIN dim_article da ON da.article_code = fo.article_code
WHERE fo.run_id = (SELECT run_id FROM fact_order_forecast ORDER BY created_at DESC LIMIT 1)
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print("Reparti in fact_order_forecast (ordini, ultimo run):")
for row in cur.fetchall():
    print(f"  {repr(row[0])}: {row[1]} articoli, qty={row[2]}")

print()

# 2. Controlla la pipeline di ingest - quale file di stock è stato usato
cur.execute("""
SELECT source_file, created_at, reparto, COUNT(DISTINCT article_code) as n_articles
FROM fact_stock_snapshot
WHERE run_id = (SELECT run_id FROM fact_stock_snapshot ORDER BY snapshot_at DESC LIMIT 1)
GROUP BY source_file, created_at, reparto
ORDER BY created_at DESC LIMIT 20
""")
print("Fonti in fact_stock_snapshot:")
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f"  {row[0]} | {row[2]} | {row[3]} articoli")
else:
    # Prova senza source_file
    print("  (colonna source_file non disponibile, provo altre query)")
    cur.execute("""
    SELECT DISTINCT snapshot_at FROM fact_stock_snapshot
    WHERE run_id = (SELECT run_id FROM fact_stock_snapshot ORDER BY snapshot_at DESC LIMIT 1)
    LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"  snapshot_at: {row[0]}")

print()

# 3. Controlla la colonna Reparto nel file stock originale via ingest log
cur.execute("""
SELECT column_name FROM information_schema.columns
WHERE table_name = 'fact_stock_snapshot'
ORDER BY ordinal_position
""")
cols = [r[0] for r in cur.fetchall()]
print(f"Colonne di fact_stock_snapshot: {cols}")

print()

# 4. Campione di righe in fact_stock_snapshot con articoli SCARPE UOMO
cur.execute("""
SELECT fss.article_code, da.reparto, fss.giacenza
FROM fact_stock_snapshot fss
JOIN dim_article da ON da.article_code = fss.article_code  
WHERE fss.run_id = (SELECT run_id FROM fact_stock_snapshot ORDER BY snapshot_at DESC LIMIT 1)
  AND da.reparto = 'SCARPE UOMO'
LIMIT 5
""")
rows = cur.fetchall()
print(f"Articoli SCARPE UOMO in fact_stock_snapshot: {len(rows)}")
for row in rows:
    print(f"  {row[0]} | {row[1]} | giacenza={row[2]}")

# 5. Controlla gli articoli SCARPE UOMO più recenti nei run storici
cur.execute("""
SELECT fss.run_id, COUNT(DISTINCT fss.article_code)
FROM fact_stock_snapshot fss
JOIN dim_article da ON da.article_code = fss.article_code  
WHERE da.reparto = 'SCARPE UOMO'
GROUP BY fss.run_id
ORDER BY COUNT(*) DESC LIMIT 10
""")
rows = cur.fetchall()
print()
print("Run con SCARPE UOMO in fact_stock_snapshot (tutti i run):")
for row in rows:
    print(f"  run_id={str(row[0])[:8]}... | {row[1]} articoli")

conn.close()

