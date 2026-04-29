"""Test per verificare la fix del bug size shift."""
import sys
import json
sys.path.insert(0, '.')

from barca_control_center.catalog_excel import parse_situazione_articoli_excel, ensure_xlsx

xlsx = ensure_xlsx(r'C:\Users\ufficio2\Desktop\stampe da aggiungere\26e_excel.xls')
df = parse_situazione_articoli_excel(xlsx)

art = '68/1043ABBL'
subset = df[df['articolo'] == art]
print(f'=== Articolo {art} ===')
print(f'Righe: {len(subset)}')
print()

errors = []

def check(label, sizes, expected):
    for size_str, expected_val in expected.items():
        actual = sizes.get(size_str, -999)
        status = "OK" if actual == expected_val else "FAIL"
        if status == "FAIL":
            errors.append(f"  {label}: taglia {size_str} attesa {expected_val}, trovata {actual}")
        print(f"  {label} taglia {size_str}: {actual} (atteso {expected_val}) [{status}]")

# Test AR: Excel R21 - col11=1(37), col12=2(38), col13=1(39), tutto il resto 0
ar = subset[subset['neg'] == 'AR'].iloc[0]
sizes_ar = json.loads(ar.sizes_json)
print(f'AR: giac={ar.giac:.0f} con={ar.con:.0f} ven={ar.ven:.0f}')
check("AR", sizes_ar, {"35": 0.0, "36": 0.0, "37": 1.0, "38": 2.0, "39": 1.0, "40": 0.0})
print()

# Test EU: Excel R29 - col10=1(36), col11=1(37), col12=1(38), col13=2(39), col14=1(40), col15=1(41)
eu = subset[subset['neg'] == 'EU'].iloc[0]
sizes_eu = json.loads(eu.sizes_json)
print(f'EU: giac={eu.giac:.0f} con={eu.con:.0f} ven={eu.ven:.0f}')
check("EU", sizes_eu, {"35": 0.0, "36": 1.0, "37": 1.0, "38": 1.0, "39": 2.0, "40": 1.0, "41": 1.0})
print()

# Test BO (nessun %): Excel R23 - col11=1(37), col12=1(38), col13=1(39), col14=1(40)
bo = subset[subset['neg'] == 'BO'].iloc[0]
sizes_bo = json.loads(bo.sizes_json)
print(f'BO: giac={bo.giac:.0f} con={bo.con:.0f} ven={bo.ven:.0f}')
check("BO", sizes_bo, {"35": 0.0, "36": 0.0, "37": 1.0, "38": 1.0, "39": 1.0, "40": 1.0})
print()

# Test WEB: Excel R41 - col14=1(40)
web = subset[subset['neg'] == 'WEB'].iloc[0]
sizes_web = json.loads(web.sizes_json)
print(f'WEB: giac={web.giac:.0f} con={web.con:.0f} ven={web.ven:.0f}')
check("WEB", sizes_web, {"40": 1.0, "35": 0.0, "36": 0.0, "37": 0.0, "38": 0.0, "39": 0.0})
print()

# Test SPW: Excel R37 - col10=1(36)
spw = subset[subset['neg'] == 'SPW'].iloc[0]
sizes_spw = json.loads(spw.sizes_json)
print(f'SPW: giac={spw.giac:.0f}')
check("SPW", sizes_spw, {"36": 1.0, "35": 0.0, "37": 0.0})
print()

if errors:
    print("=== ERRORI ===")
    for e in errors:
        print(e)
    sys.exit(1)
else:
    print("=== TUTTI I TEST SUPERATI ===")
