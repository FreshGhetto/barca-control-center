"""
Legge il .xls direttamente con xlrd (senza conversione pandas) per vedere
le celle esatte e capire il vero offset header→dati.
"""
import sys
sys.path.insert(0, '.')
import xlrd

xls_path = r"C:\Users\ufficio2\Desktop\stampe da aggiungere\26e_excel.xls"
wb = xlrd.open_workbook(xls_path)
ws = wb.sheets()[0]

print(f"Sheet: {ws.name}, rows={ws.nrows}, cols={ws.ncols}")
print()

# Stampa le righe 18-30 (l'header e le prime righe dati) con tutti i valori
for r in range(17, 32):
    row_values = ws.row_values(r)
    non_empty = [(c, v) for c, v in enumerate(row_values) if v != '' and v is not None]
    print(f"row {r:3}: {non_empty}")

