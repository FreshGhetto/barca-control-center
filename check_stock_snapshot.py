"""Controlla fact_stock_snapshot per SCARPE UOMO."""
import psycopg

conn = psycopg.connect(dbname='barca', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()

# Controlla fact_stock_snapshot per reparti
cur.execute("""
SELECT da.reparto, COUNT(DISTINCT fss.article_code), SUM(fss.giacenza)
FROM fact_stock_snapshot fss
LEFT JOIN dim_article da ON da.article_code = fss.article_code
WHERE fss.run_id = (SELECT run_id FROM fact_stock_snapshot ORDER BY snapshot_at DESC LIMIT 1)
GROUP BY da.reparto ORDER BY COUNT(*) DESC
""")
print("Reparti in fact_stock_snapshot (ultimo run):")
for row in cur.fetchall():
    print(f"  {repr(row[0])}: {row[1]} articoli, giacenza={row[2]}")

print()

# Controlla il run_id usato da db_inputs
cur.execute("""
SELECT r.run_id, r.status, r.started_at, r.finished_at,
       EXISTS(SELECT 1 FROM fact_sales_snapshot s WHERE s.run_id = r.run_id) as has_sales,
       EXISTS(SELECT 1 FROM fact_stock_snapshot t WHERE t.run_id = r.run_id) as has_stock
FROM etl_run r
WHERE r.status = 'completed'
ORDER BY COALESCE(r.finished_at, r.started_at) DESC
LIMIT 5
""")
print("Ultimi run completati con sales/stock:")
for row in cur.fetchall():
    print(f"  {row[0]} | {row[1]} | {row[2]} | has_sales={row[4]} | has_stock={row[5]}")

print()

# Controlla il run più recente con sales+stock
cur.execute("""
SELECT r.run_id
FROM etl_run r
WHERE r.status = 'completed'
  AND EXISTS (SELECT 1 FROM fact_sales_snapshot s WHERE s.run_id = r.run_id)
  AND EXISTS (SELECT 1 FROM fact_stock_snapshot t WHERE t.run_id = r.run_id)
ORDER BY COALESCE(r.finished_at, r.started_at) DESC
LIMIT 1
""")
row = cur.fetchone()
if row:
    source_run_id = str(row[0])
    print(f"Source run_id usato dall'allocator: {source_run_id}")

    # Controlla di nuovo su questo run
    cur.execute("""
    SELECT da.reparto, COUNT(DISTINCT fss.article_code), SUM(fss.giacenza)
    FROM fact_stock_snapshot fss
    LEFT JOIN dim_article da ON da.article_code = fss.article_code
    WHERE fss.run_id = %s::uuid
    GROUP BY da.reparto ORDER BY COUNT(*) DESC
    """, (source_run_id,))
    print(f"\nReparti in fact_stock_snapshot per source_run_id {source_run_id[:8]}...:")
    for r2 in cur.fetchall():
        print(f"  {repr(r2[0])}: {r2[1]} articoli, giacenza={r2[2]}")

conn.close()

