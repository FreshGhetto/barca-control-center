import sys, tempfile, os
sys.path.insert(0, '.')
from barca_control_center.parse_data_v2 import parse_articles

for fname in ['input/stock_26e_2026-04.csv', 'input/stock_26g_2026-04.csv']:
    tmp = tempfile.mktemp(suffix='.csv')
    df = parse_articles(fname, tmp)
    if df.empty:
        print(f"{fname}: VUOTO")
    else:
        n_art = df['Article'].nunique()
        n_shop = df['Shop'].nunique()
        shops = sorted(df['Shop'].unique())
        print(f"{fname}: {len(df)} righe, {n_art} articoli, {n_shop} negozi")
        print(f"  Negozi: {shops}")
        print(df[['Article','Shop','Giacenza','Consegnato','Venduto','Size_38']].head(5).to_string())
    try:
        os.unlink(tmp)
    except:
        pass
    print()

