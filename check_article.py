import os, sys

try:
    import dotenv; dotenv.load_dotenv()
except ImportError:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'barca_control_center'))

host = os.environ.get('BARCA_DB_HOST', '')
port = os.environ.get('BARCA_DB_PORT', '5432')
dbname = os.environ.get('BARCA_DB_NAME', '')
user = os.environ.get('BARCA_DB_USER', '')
password = os.environ.get('BARCA_DB_PASSWORD', '')
sslmode = os.environ.get('BARCA_DB_SSLMODE', 'prefer')

dsn = f"host={host} port={port} dbname={dbname} user={user} password={password} sslmode={sslmode} connect_timeout=5"
import psycopg
conn = psycopg.connect(dsn)
print('Connected OK')

latest_run = '7522b138-6745-44a9-992a-32de029d125e'
article = '68/502042BE'

with conn.cursor() as cur:
    # Total transfer rows in latest run
    cur.execute("SELECT COUNT(*) FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid", (latest_run,))
    total = cur.fetchone()[0]
    print(f'Total transfer rows in latest run: {total}')

    # The article's rows sorted by qty DESC - where does it rank?
    cur.execute("""
        SELECT article_code, qty, ROW_NUMBER() OVER (ORDER BY qty DESC, article_code ASC) AS rank_pos
        FROM public.fact_transfer_suggestion
        WHERE run_id = %s::uuid AND UPPER(article_code) = UPPER(%s)
        ORDER BY qty DESC
        LIMIT 5
    """, (latest_run, article))
    rows = cur.fetchall()
    print(f'Article rows with rank position:')
    for r in rows:
        print(f'  article={r[0]}, qty={r[1]}, rank_pos={r[2]}')

    # What's the min qty at position 2000?
    cur.execute("""
        SELECT qty, article_code
        FROM public.fact_transfer_suggestion
        WHERE run_id = %s::uuid
        ORDER BY qty DESC, article_code ASC
        LIMIT 1 OFFSET 1999
    """, (latest_run,))
    row2000 = cur.fetchone()
    print(f'Row at position 2000: {row2000}')

    # How many rows with qty >= 1 (same as our article)?
    cur.execute("SELECT qty, COUNT(*) FROM public.fact_transfer_suggestion WHERE run_id = %s::uuid GROUP BY qty ORDER BY qty DESC", (latest_run,))
    print('Qty distribution:')
    for r in cur.fetchall():
        print(f'  qty={r[0]}, count={r[1]}')

conn.close()
print('Done.')
