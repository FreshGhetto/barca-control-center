import csv, sys, os
sys.path.insert(0, os.path.dirname(__file__))
from barca_control_center.parse_data_v2 import normalize_shop_code, is_article_code, VALID_CODES_DEFAULT

filepath = r'C:\Users\ufficio2\Desktop\stampe da aggiungere\vendite_scarpe_26e.csv'
O = 33
valid = set(VALID_CODES_DEFAULT)

with open(filepath, encoding='latin1', errors='ignore') as f:
    all_rows = list(csv.reader(f))

print(f"Totale righe: {len(all_rows)}")
print()

# Trova tutte le righe "cambio negozio" (col O = shop code)
shop_rows = []
for i, row in enumerate(all_rows):
    if len(row) <= O + 1:
        continue
    cell_a = (row[O] or '').strip()
    cell_b = (row[O+1] or '').strip()
    shop_a = normalize_shop_code(cell_a)
    is_shop = bool(shop_a and shop_a in valid)
    is_art_b = is_article_code(cell_b.split(' ')[0])
    if is_shop:
        shop_rows.append((i, shop_a, cell_a[:25], cell_b[:25], is_art_b))

print(f"Righe con shop in col 33: {len(shop_rows)}")
print()
print("Righe di cambio negozio (prime 60):")
for row_i, shop, cell_a, cell_b, is_art_b in shop_rows[:60]:
    ok = "✅ Case A" if is_art_b else f"❌ col34='{cell_b}'"
    print(f"  riga {row_i:5d}: shop={shop:4s} ({cell_a:20s}) → {ok}")

