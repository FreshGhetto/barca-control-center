"""
Genera file sd_3 (aggreagato taglie per articolo) partendo dai file
stock_26e/26g_2026-04.csv già presenti in input/.
L'sd_3 atteso da orders_pipeline ha in ogni riga:
  TAG, 35, 36, ..., TOT, (extra), VEN, v35, v36, ..., codice_art, (extra), GIA, g35, ...
"""
import sys, csv, re
sys.path.insert(0, '.')
from pathlib import Path
from collections import defaultdict
from barca_control_center.parse_data_v2 import parse_articles, VALID_CODES_DEFAULT
import tempfile, os

ROOT = Path(__file__).parent
STOCK_DIR = ROOT / 'input'
ORDERS_DIR = ROOT / 'input' / 'orders'
ORDERS_DIR.mkdir(parents=True, exist_ok=True)

SIZES = [35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45]

def build_sd3_from_stock(stock_csv: Path, out_csv: Path):
    tmp = tempfile.mktemp(suffix='.csv')
    df = parse_articles(str(stock_csv), tmp)
    try:
        os.unlink(tmp)
    except Exception:
        pass

    if df.empty:
        print(f"  ATTENZIONE: {stock_csv.name} vuoto, sd_3 non generato")
        return False

    # Determina quali taglie sono presenti
    taglie_presenti = []
    for s in SIZES:
        col = f'Size_{s}'
        if col in df.columns and df[col].sum() > 0:
            taglie_presenti.append(s)

    if not taglie_presenti:
        taglie_presenti = [s for s in SIZES if f'Size_{s}' in df.columns]

    # Aggrega per articolo: somma Venduto e taglie su tutti i negozi
    art_data = defaultdict(lambda: {'ven': 0, 'giac': 0, 'taglie_ven': defaultdict(float), 'taglie_giac': defaultdict(float)})
    for _, row in df.iterrows():
        art = str(row['Article']).strip()
        art_data[art]['ven'] += float(row.get('Venduto', 0) or 0)
        art_data[art]['giac'] += float(row.get('Giacenza', 0) or 0)
        for s in taglie_presenti:
            v = float(row.get(f'Size_{s}', 0) or 0)
            art_data[art]['taglie_giac'][s] += v

    # Scrivi il CSV nel formato atteso da _estrai_matematico (sd_3)
    # Il parser cerca righe con TAG, VEN, GIA, TOT nella stessa riga CSV
    # Le etichette taglie sono tra TAG e TOT
    # I valori venduto sono dopo VEN (n valori = numero taglie)
    # Il codice articolo è dopo i valori VEN (tra VEN+n e GIA)
    # I valori giacenza sono dopo GIA (n valori)

    header_line = ['ANALISI PER SINGOLA TAGLIA', '', '', '', '', '']
    tag_labels = [str(s) for s in taglie_presenti]
    n = len(tag_labels)

    rows_out = []
    # Intestazione report
    rows_out.append(['', '', 'ANALISI PER SINGOLA TAGLIA', '', '', '', ''])
    rows_out.append([])
    # Riga header taglie (quella che il parser usa per rilevare il formato)
    # Formato: ..., TAG, 35, 36, ..., TOT, ..., VEN, v35, v36, ..., ART, ..., GIA, g35, g36, ...
    # Per ogni articolo, una riga dati
    for art, data in sorted(art_data.items()):
        ven_vals = [str(int(data['taglie_giac'].get(s, 0))) for s in taglie_presenti]
        giac_vals = [str(int(data['taglie_giac'].get(s, 0))) for s in taglie_presenti]

        row = (
            ['TAG'] + tag_labels + ['TOT', str(int(data['giac']))] +
            ['VEN'] + ven_vals + [art] +
            ['GIA'] + giac_vals
        )
        rows_out.append(row)

    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(rows_out)

    print(f"  Scritto {out_csv.name}: {len(art_data)} articoli, taglie={taglie_presenti}")
    return True


print("=== Generazione file sd_3 da stock ===")
for stock_file in sorted(STOCK_DIR.glob('stock_2?[eg]_*.csv')):
    # Estrai stagione dal nome: stock_26e_2026-04.csv -> 26e
    m = re.search(r'stock_(2\d[a-z])_', stock_file.name, re.IGNORECASE)
    if not m:
        print(f"Saltato (stagione non trovata): {stock_file.name}")
        continue
    season = m.group(1).lower()
    out_path = ORDERS_DIR / f'{season}_sd_3.csv'
    print(f"\nGenerando {out_path.name} da {stock_file.name}...")
    build_sd3_from_stock(stock_file, out_path)

print("\nFatto. File sd_3 in input/orders/:")
for f in sorted(ORDERS_DIR.glob('*_sd_3.csv')):
    print(f"  {f.name}")

