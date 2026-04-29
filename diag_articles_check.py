"""
Confronta il numero di articoli distinti per stagione tra tutti i file in input/orders/.
"""
import sys, csv, re
sys.path.insert(0, '.')
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent
ORDERS_DIR = ROOT / 'input' / 'orders'
STOCK_DIR = ROOT / 'input'

ARTICLE_RE = re.compile(r'^\d{1,5}/[^/\s]+$')
ARTICLE_ANY_RE = re.compile(r'^\d{6,9}$')

def is_art(v):
    v = str(v or '').strip().split(' ')[0]
    return bool(ARTICLE_RE.match(v)) or bool(ARTICLE_ANY_RE.match(v))

def count_articles_csv(path: Path, encoding='utf-8') -> set:
    arts = set()
    try:
        with open(path, encoding=encoding, errors='replace') as f:
            for row in csv.reader(f):
                for cell in row:
                    v = str(cell or '').strip().split(' ')[0]
                    if is_art(v):
                        arts.add(v)
    except Exception as e:
        print(f"  ERRORE lettura {path.name}: {e}")
    return arts

print("=" * 70)
print("CONFRONTO ARTICOLI PER STAGIONE")
print("=" * 70)

seasons = set()
for f in ORDERS_DIR.glob('*.csv'):
    m = re.match(r'^(2\d[a-z])', f.name, re.IGNORECASE)
    if m:
        seasons.add(m.group(1).lower())

for season in sorted(seasons):
    print(f"\nStagione: {season.upper()}")
    files = {
        'sd_1 (marchio/totali)': ORDERS_DIR / f'{season}_sd_1.csv',
        'sd_2 (colore)':         ORDERS_DIR / f'{season}_sd_2.csv',
        'sd_3 (taglie)':         ORDERS_DIR / f'{season}_sd_3.csv',
        'sd_4 (listino)':        ORDERS_DIR / f'{season}_sd_4.csv',
        'prezzi':                ORDERS_DIR / f'{season}_prezzo_acq-ven.csv',
    }
    # Cerca anche il file stock
    stock_files = list(STOCK_DIR.glob(f'stock_{season}_*.csv'))
    if stock_files:
        files[f'stock ({stock_files[0].name})'] = stock_files[0]

    article_sets = {}
    for label, path in files.items():
        if not path.exists():
            print(f"  {label:35s}: FILE MANCANTE ({path.name})")
            continue
        arts = count_articles_csv(path)
        article_sets[label] = arts
        print(f"  {label:35s}: {len(arts):4d} articoli")

    # Trova la reference (sd_1 ha il set più completo di solito)
    if len(article_sets) >= 2:
        keys = list(article_sets.keys())
        ref_key = next((k for k in keys if 'sd_1' in k), keys[0])
        ref_set = article_sets[ref_key]
        print(f"\n  Confronto vs '{ref_key}' ({len(ref_set)} art):")
        for label, arts in article_sets.items():
            if label == ref_key:
                continue
            only_ref = ref_set - arts
            only_other = arts - ref_set
            common = ref_set & arts
            if only_ref or only_other:
                print(f"    {label:35s}: comuni={len(common)}, solo in sd_1={len(only_ref)}, solo in {label.split('(')[0].strip()}={len(only_other)}")
            else:
                print(f"    {label:35s}: OK identici ({len(common)} art)")

print("\n" + "=" * 70)

