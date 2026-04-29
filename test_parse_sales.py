import sys, os
sys.path.insert(0, os.path.dirname(__file__))
os.chdir(os.path.dirname(__file__))

from barca_control_center.parse_data_v2 import parse_sales

filepath = r'C:\Users\ufficio2\Desktop\stampe da aggiungere\vendite_scarpe_26e.csv'
output   = r'C:\Users\ufficio2\Desktop\barca\barca-control-center\output\test_sales_26e.csv'

import os; os.makedirs(os.path.dirname(output), exist_ok=True)

df = parse_sales(filepath, output)
print(f"Righe parsate:       {len(df)}")
print(f"Articoli distinti:   {df['Article'].nunique()}")
print(f"Negozi distinti:     {df['Shop'].nunique()}")
print(f"Negozi trovati:      {sorted(df['Shop'].unique().tolist())}")
print()
print("Prime 10 righe:")
print(df[['Article','Shop','Consegnato_Qty','Venduto_Qty','Periodo_Qty','Sellout_Percent']].head(10).to_string(index=False))
print()
print("Campione con venduto > 0:")
print(df[df['Venduto_Qty'] > 0][['Article','Shop','Consegnato_Qty','Venduto_Qty','Periodo_Qty','Sellout_Percent']].head(10).to_string(index=False))

