import sys, csv
sys.path.insert(0, '.')
from barca_control_center.parse_data_v2 import _detect_situazione_format, normalize_shop_code, VALID_CODES_DEFAULT

fname = 'input/stock_26g_2026-04.csv'
valid = set(VALID_CODES_DEFAULT)

# Leggi con encodings diversi
for enc in ['utf-8', 'latin1', 'cp1252']:
    try:
        with open(fname, 'r', encoding=enc, errors='ignore') as f:
            rows = list(csv.reader(f))
        print(f"[enc={enc}] Lette {len(rows)} righe")
        # Prima 30 righe: stampa quelle con qualcosa in col 4
        detected = _detect_situazione_format(rows)
        print(f"  _detect_situazione_format = {detected}")
        for i, row in enumerate(rows[:35]):
            if i == 0: continue
            upper = [str(c or '').strip().upper() for c in row]
            if 'NEG' in upper or 'GIAC' in upper:
                print(f"  riga {i}: {row[:10]}")
        break
    except Exception as e:
        print(f"[enc={enc}] ERRORE: {e}")
        continue

