import csv, sys
filepath = r'C:\Users\ufficio2\Desktop\stampe da aggiungere\vendite_scarpe_26e.csv'
with open(filepath, encoding='latin1', errors='ignore') as f:
    rows = list(csv.reader(f))

print(f"Totale righe: {len(rows)}")
print(f"Colonne riga 1: {len(rows[0])}")
print()
print("=== Riga 1 — solo celle non vuote ===")
for i, v in enumerate(rows[0]):
    if v.strip():
        print(f"  [{i:2d}] {repr(v[:60])}")

print()
print("=== Riga 2 — colonne dalla 30 in poi ===")
for i, v in enumerate(rows[1][30:], start=30):
    print(f"  [{i:2d}] {repr(v[:60])}")

print()
print("=== Riga 9 (con %) — colonne dalla 30 in poi ===")
for i, v in enumerate(rows[8][30:], start=30):
    print(f"  [{i:2d}] {repr(v[:60])}")

