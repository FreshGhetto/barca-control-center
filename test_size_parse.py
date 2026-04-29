# Test della nuova funzione _parse_size_label e _find_table_header
import sys
sys.path.insert(0, r"C:\Users\ufficio2\Desktop\barca\barca-control-center")

from barca_control_center.catalog_excel import _parse_size_label, _find_table_header

print("=== Test _parse_size_label ===")
cases = [
    (39,      "39"),       # int
    (39.0,    "39"),       # float-as-int → normalizzato
    (38.5,    "38.5"),     # mezza taglia
    ("39",    "39"),       # stringa
    ("39.0",  "39"),       # stringa float-as-int
    ("38,5",  "38.5"),     # virgola
    (None,    None),       # vuoto
    ("",      None),       # stringa vuota
    ("NEG",   None),       # testo non-taglia → None
    ("XS",    "XS"),       # taglia alfanumerica
    ("M",     "M"),
    ("XL",    "XL"),
    (42,      "42"),
    (35,      "35"),
]
all_ok = True
for value, expected in cases:
    result = _parse_size_label(value)
    ok = result == expected
    if not ok:
        all_ok = False
    status = "✓" if ok else "✗ FAIL"
    print(f"  {status}  _parse_size_label({value!r}) = {result!r}  (atteso: {expected!r})")

print()
print("=== Test _find_table_header con colonna vuota tra 38 e 40 ===")
# Simula la riga header dell'Excel: NEG, GIAC, CON, VEN, %VEN, 35, 36, 37, 38, [vuota], 40, 39, 42, 41
header_row = [None, None, None, "NEG", "GIAC", "CON", "VEN", "%VEN", 35, 36, 37, 38, None, 40, 39, 42, 41]
result = _find_table_header(header_row)
if result:
    import json
    size_cols = json.loads(result["size_cols_json"])
    print(f"  size_cols: {size_cols}")
    print(f"  '39' presente: {'39' in size_cols}")
    print(f"  col index di '39': {size_cols.get('39', 'NON TROVATO')}")
    print(f"  col index di '40': {size_cols.get('40', 'NON TROVATO')}")
    print(f"  col index di '38': {size_cols.get('38', 'NON TROVATO')}")
    # La colonna None (indice 12) NON deve essere in size_cols
    none_col_in_sizes = any(idx == 12 for idx in size_cols.values())
    print(f"  colonna vuota (idx 12) NON associata a taglie: {not none_col_in_sizes}")
    if "39" in size_cols and size_cols["39"] == 14 and not none_col_in_sizes:
        print("  ✓ Header parsato correttamente!")
    else:
        print("  ✗ PROBLEMA nel parsing!")
        all_ok = False
else:
    print("  ✗ FAIL: _find_table_header non ha rilevato l'header!")
    all_ok = False

print()
print("=== Test con taglie float (39.0, 38.0) ===")
header_float = [None, "NEG", "GIAC", "CON", "VEN", "%VEN", 35.0, 36.0, 37.0, 38.0, None, 40.0, 39.0, 42.0, 41.0]
result2 = _find_table_header(header_float)
if result2:
    import json
    sc2 = json.loads(result2["size_cols_json"])
    print(f"  size_cols float-header: {sc2}")
    found_39 = "39" in sc2
    print(f"  '39' trovata da header float: {found_39}")
    if not found_39:
        print("  ✗ BUG: 39.0 non viene riconosciuta!")
        all_ok = False
    else:
        print("  ✓ Float headers gestiti correttamente!")
else:
    print("  ✗ FAIL: header non rilevato con float!")
    all_ok = False

print()
print("RISULTATO FINALE:", "✓ TUTTI OK" if all_ok else "✗ CI SONO PROBLEMI")

