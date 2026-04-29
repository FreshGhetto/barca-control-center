import sys, csv
sys.path.insert(0, '.')
from barca_control_center.parse_data_v2 import (
    _detect_situazione_format, _parse_stock_situazione,
    _detect_stock_size_labels, is_article_code_any, normalize_shop_code, VALID_CODES_DEFAULT
)

fname = 'input/stock_26g_2026-04.csv'
valid = set(VALID_CODES_DEFAULT)

with open(fname, 'r', encoding='utf-8', errors='ignore') as f:
    all_rows = list(csv.reader(f))

print(f"Righe totali: {len(all_rows)}")
detected = _detect_situazione_format(all_rows)
print(f"Formato rilevato: {detected}")

size_labels = _detect_stock_size_labels(all_rows[:120])
print(f"Size labels: {size_labels}")

# Debug: guarda cosa succede nelle prime 40 righe
print("\nPrime 40 righe analisi col0/col4:")
current_article = None
for i, row in enumerate(all_rows[:50]):
    if not row or len(row) < 5:
        print(f"  {i:3d}: (riga corta)")
        continue
    c0 = (row[0] or '').strip()
    c4 = (row[4] or '').strip()
    art = c0.split(' ')[0] if c0 else ''
    is_art = is_article_code_any(art)
    shop = normalize_shop_code(c4)
    is_shop = bool(shop and shop in valid)
    if is_art:
        current_article = art
    if is_art or is_shop:
        print(f"  {i:3d}: art={repr(c0[:20]):25s} shop={repr(c4[:15]):18s}  is_art={is_art}  is_shop={is_shop}  current={current_article}")

# Prova il parse
print("\nRisultato _parse_stock_situazione:")
rows = _parse_stock_situazione(all_rows, valid, size_labels, 'DONNA', None)
print(f"  Righe parsate: {len(rows)}")
if rows:
    print(f"  Prime 3:")
    for r in rows[:3]:
        print(f"    {r}")

