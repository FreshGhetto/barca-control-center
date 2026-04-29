import csv, sys, os
sys.path.insert(0, os.path.dirname(__file__))
from barca_control_center.parse_data_v2 import normalize_shop_code, VALID_CODES_DEFAULT

filepath = r'C:\Users\ufficio2\Desktop\stampe da aggiungere\vendite_scarpe_26e.csv'
O = 33  # DATA_OFFSET

shop_codes_seen = {}
with open(filepath, encoding='latin1', errors='ignore') as f:
    for row in csv.reader(f):
        if len(row) <= O:
            continue
        cell = (row[O] or '').strip()
        if not cell:
            continue
        # E' un codice negozio se normalizzato è in VALID_CODES o se la cella ha aspetto di negozio
        code = normalize_shop_code(cell)
        # Cerca pattern: testo breve (max 10 chars) seguito da spazio e descrizione
        parts = cell.split()
        if parts and len(parts[0]) <= 5 and parts[0].isupper() and len(parts) > 1:
            raw_code = parts[0]
            if raw_code not in shop_codes_seen:
                shop_codes_seen[raw_code] = cell

print("Potenziali codici negozio trovati in colonna 33:")
for code, full in sorted(shop_codes_seen.items()):
    in_list = code in VALID_CODES_DEFAULT or normalize_shop_code(code) in VALID_CODES_DEFAULT
    print(f"  {code:8s} → '{full[:30]}'  {'✅ in lista' if in_list else '❌ NON in lista'}")

