"""Verifica i dati SCARPE UOMO nei file sd e nel fact_order_source."""
import psycopg, csv, collections

conn = psycopg.connect(dbname='barca', user='postgres', password='postgres', host='localhost')
cur = conn.cursor()

# 1. Controlla fact_order_source per reparto e stagione
cur.execute("""
SELECT da.reparto, fos.season_code, COUNT(DISTINCT fos.article_code)
FROM fact_order_source fos
LEFT JOIN dim_article da ON da.article_code = fos.article_code
WHERE fos.run_id = (SELECT run_id FROM fact_order_source ORDER BY created_at DESC LIMIT 1)
GROUP BY da.reparto, fos.season_code ORDER BY da.reparto, fos.season_code
""")
rows = cur.fetchall()
print("fact_order_source per reparto+stagione (ultimo run):")
for row in rows:
    print(f"  {repr(row[0])} | {row[1]}: {row[2]} articoli")

print()

# 2. Conta le righe SCARPE UOMO in ciascun file SD
for fname in ['26e_sd_1.csv', '26e_sd_2.csv', '26e_sd_3.csv', '26e_sd_4.csv',
              '26g_sd_1.csv', '26g_sd_2.csv', '26g_sd_3.csv', '26g_sd_4.csv']:
    try:
        path = rf'C:\Users\ufficio2\Desktop\barca\barca-control-center\input\orders\{fname}'
        counts = collections.Counter()
        with open(path, 'r', encoding='latin1', errors='ignore') as f:
            for row in csv.reader(f):
                for cell in row:
                    c = str(cell).strip().upper()
                    if 'SCARPE DONNA' in c: counts['DONNA'] += 1
                    elif 'SCARPE UOMO' in c: counts['UOMO'] += 1
                    elif 'TENNIS UNISEX' in c: counts['TENNIS'] += 1
        print(f'{fname}: {dict(counts)}')
    except FileNotFoundError:
        pass

conn.close()

