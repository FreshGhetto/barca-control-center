import sys
sys.path.insert(0, '.')
from barca_control_center.db_sync import get_db_dsn
import psycopg

dsn = get_db_dsn()
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(NULLIF(reparto, '')) as has_reparto,
                COUNT(NULLIF(categoria, '')) as has_categoria,
                COUNT(NULLIF(marchio, '')) as has_marchio
            FROM dim_article
        """)
        row = cur.fetchone()
        print(f'total={row[0]}, has_reparto={row[1]}, has_categoria={row[2]}, has_marchio={row[3]}')

        cur.execute("SELECT DISTINCT reparto FROM dim_article WHERE reparto IS NOT NULL AND reparto != '' ORDER BY reparto LIMIT 10")
        print('reparti:', [r[0] for r in cur.fetchall()])

        cur.execute("SELECT DISTINCT categoria FROM dim_article WHERE categoria IS NOT NULL AND categoria != '' ORDER BY categoria LIMIT 15")
        print('categorie:', [r[0] for r in cur.fetchall()])

        cur.execute("SELECT DISTINCT marchio FROM dim_article WHERE marchio IS NOT NULL AND marchio != '' ORDER BY marchio LIMIT 15")
        print('marchi:', [r[0] for r in cur.fetchall()])

        # Check transfer table
        cur.execute("""
            SELECT COUNT(*) FROM fact_transfer_suggestion
        """)
        print(f'transfer_rows: {cur.fetchone()[0]}')

        # Check a sample with joined dim_article
        cur.execute("""
            SELECT t.article_code, da.reparto, da.categoria, da.marchio, t.qty
            FROM fact_transfer_suggestion t
            LEFT JOIN dim_article da ON da.article_code = t.article_code
            LIMIT 5
        """)
        for r in cur.fetchall():
            print(r)

