
import csv
import re
from pathlib import Path
import pandas as pd

VALID_CODES_DEFAULT = [
    'AR','AU','BO','BS','CA','CO','EU','EU2','LN','MC','MI',
    'NV','OR','PD','PM','RI','RM','SC','SD','SM','SPW','NO','ME2',
    'TV','VR','WEB','W','M4','MR','MP','SP'
]

ARTICLE_CODE_RE = re.compile(r'^\d{2}/\S+')

def clean_number(s):
    """Italian number format: 1.234,56 -> 1234.56. Also handles blanks and '-'."""
    if s is None:
        return 0.0
    if not isinstance(s, str):
        try:
            return float(s)
        except Exception:
            return 0.0
    s = s.strip()
    if s == '' or s == '-':
        return 0.0
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return 0.0

def clean_non_negative(s):
    return max(0.0, clean_number(s))

def normalize_shop_code(shop_str: str) -> str:
    if not shop_str:
        return ''
    code = shop_str.strip().split(' ')[0].strip().upper()
    # Aliases
    aliases = {
        'W': 'WEB',
        'NU': 'NV',
        'M2': 'ME2',
    }
    if code in aliases:
        return aliases[code]
    return code

def is_article_code(value: str) -> bool:
    if not value:
        return False
    return bool(ARTICLE_CODE_RE.match(value.strip().split(' ')[0]))

def _find_article_cell(row):
    """Return (idx, article_code, article_full_cell) or (None, None, None)."""
    for idx, cell in enumerate(row):
        val = (cell or '').strip()
        if not val:
            continue
        # Article codes like '59/XXXX' or '25/XXXX'
        if is_article_code(val):
            # keep only the code part
            code = val.split(' ')[0]
            return idx, code, val
    return None, None, None

def _is_numeric_like(value) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return True
    s = str(value).strip()
    if s == '' or s == '-':
        return False
    # Italian numbers, integers or decimals.
    return bool(re.match(r'^-?\d{1,3}(\.\d{3})*(,\d+)?$|^-?\d+(,\d+)?$', s))

def _candidate_shop_positions(row, valid_codes, article_idx=None):
    """
    Return ordered candidate tuples (shop_code, idx).
    Priorities:
      1) right after detected article code
      2) known report window where NEGOZIO usually appears (30..50)
      3) full-row fallback scan
    """
    candidates = []
    seen = set()

    def add_candidate(idx, cell, priority):
        if idx is None or idx < 0 or idx >= len(row):
            return
        raw = (cell or '').strip()
        if not raw:
            return
        code = normalize_shop_code(raw)
        if not code or code not in valid_codes:
            return
        key = (code, idx)
        if key in seen:
            return
        seen.add(key)
        candidates.append((priority, idx, code))

    if article_idx is not None:
        add_candidate(article_idx + 1, row[article_idx + 1] if article_idx + 1 < len(row) else '', 0)

    # Typical "ANALISI ARTICOLI" window.
    for idx in range(30, min(len(row), 50)):
        add_candidate(idx, row[idx], 1)

    # Full-row fallback.
    for idx, cell in enumerate(row):
        add_candidate(idx, cell, 2)

    candidates.sort(key=lambda x: (x[0], x[1]))
    return [(code, idx) for _, idx, code in candidates]

def _find_article_shop_block_in_stock_row(row, valid_codes):
    """
    Locate a valid (article, shop_idx) block in stock rows.
    Supports both standard narrow rows (article at 16, shop at 18) and wide rows where
    article/shop shift to the right (e.g. campaign rows with extra headers).
    """
    candidates = []

    for idx, cell in enumerate(row):
        raw = (cell or '').strip()
        if not raw:
            continue
        code = raw.split(' ')[0]
        if not is_article_code(code):
            continue

        for offset in (2, 1, 3):
            sidx = idx + offset
            if sidx >= len(row):
                continue
            shop = normalize_shop_code((row[sidx] or '').strip())
            if not shop or shop not in valid_codes:
                continue

            numeric_hits = 0
            for j in range(sidx + 1, min(len(row), sidx + 11)):
                if _is_numeric_like(row[j]):
                    numeric_hits += 1
            if numeric_hits < 4:
                continue

            candidates.append((numeric_hits, idx, code, sidx, shop))

    if not candidates:
        return None, None, None, None

    # Prefer richer numeric neighborhood, then left-most article occurrence.
    candidates.sort(key=lambda x: (-x[0], x[1]))
    _, aidx, article, sidx, shop = candidates[0]
    return aidx, article, sidx, shop

def parse_sales(filepath, output_path, valid_codes=None, snapshot_at=None):
    """
    Parses "ANALISI ARTICOLI" export.
    Robust rule: SELL-OUT% value is the number immediately BEFORE a literal '%' cell.
    Layout near article+shop usually:
      <article+desc>, <shop>, Consegnato, Venduto, Periodo, AltroVenduto, <Sellout>, '%', Valore_1...
    """
    valid_codes = set(valid_codes or VALID_CODES_DEFAULT)

    rows = []
    current_article = None

    with open(filepath, 'r', encoding='latin1', errors='ignore') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 10:
                continue

            art_idx, art_code, _ = _find_article_cell(row)
            if art_code:
                current_article = art_code

            if not current_article:
                continue

            parsed = False
            for shop_code, shop_idx in _candidate_shop_positions(row, valid_codes, article_idx=art_idx):
                # Search for literal '%' in following cells.
                pct_idx = None
                for j in range(shop_idx + 1, min(len(row), shop_idx + 30)):
                    if (row[j] or '').strip() == '%':
                        pct_idx = j
                        break
                if pct_idx is None or pct_idx - 1 < 0:
                    continue

                # Sellout is the value immediately before '%'
                sellout = clean_number(row[pct_idx - 1])

                # Metrics between shop and sellout marker.
                nums = []
                for j in range(shop_idx + 1, pct_idx - 1):
                    v = (row[j] or '').strip()
                    if v == '':
                        continue
                    if 'TOTALI' in v.upper():
                        break
                    nums.append(clean_number(v))

                # Need at least 2 metrics to consider row valid.
                if len(nums) < 2:
                    continue
                while len(nums) < 4:
                    nums.append(0.0)
                consegnato, venduto, periodo, altro = (
                    max(0.0, nums[0]),
                    max(0.0, nums[1]),
                    max(0.0, nums[2]),
                    max(0.0, nums[3]),
                )

                # Values after % (optional)
                vals = []
                for j in range(pct_idx + 1, min(len(row), pct_idx + 8)):
                    v = (row[j] or '').strip()
                    if not v or 'TOTALI' in v.upper():
                        break
                    vals.append(clean_number(v))
                while len(vals) < 4:
                    vals.append(0.0)

                record = {
                    'snapshot_at': snapshot_at,
                    'Article': current_article,
                    'Shop': shop_code,
                    'Consegnato_Qty': consegnato,
                    'Venduto_Qty': venduto,
                    'Periodo_Qty': periodo,
                    'Altro_Venduto_Qty': altro,
                    'Sellout_Percent': sellout,
                    'Sellout_Clamped': max(0.0, min(100.0, sellout)),
                    'Valore_1': max(0.0, vals[0]),
                    'Valore_2': max(0.0, vals[1]),
                    'Valore_3': max(0.0, vals[2]),
                    'Valore_4': max(0.0, vals[3]),
                }
                rows.append(record)
                parsed = True
                break

            if not parsed:
                continue

    df = pd.DataFrame(rows)
    if not df.empty:
        # Keep latest parse per article-shop in case a line is matched twice in noisy raw exports.
        df = df.drop_duplicates(subset=['Article', 'Shop'], keep='last')
    df.to_csv(output_path, index=False)
    return df

def parse_articles(filepath, output_path, valid_codes=None, snapshot_at=None):
    """
    Parses 'SITUAZIONE ARTICOLI' export (stock + sizes).
    Expected cols: Article in 16, Desc in 17, Shop in 18, metrics 19.. and sizes 24..
    """
    valid_codes = set(valid_codes or VALID_CODES_DEFAULT)

    rows = []
    with open(filepath, 'r', encoding='latin1', errors='ignore') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if not row or len(row) < 20:
                continue

            art_idx, article, shop_idx, shop = _find_article_shop_block_in_stock_row(row, valid_codes)
            if not article or art_idx is None or shop_idx is None:
                continue

            desc = (row[art_idx + 1] or '').strip() if art_idx + 1 < len(row) else ''
            m0 = shop_idx + 1
            record = {
                'snapshot_at': snapshot_at,
                'Article': article,
                'Description': desc,
                'Shop': shop,
                'Ricevuto': clean_non_negative(row[m0]) if len(row) > m0 else 0.0,
                'Giacenza': clean_non_negative(row[m0 + 1]) if len(row) > m0 + 1 else 0.0,
                'Consegnato': clean_non_negative(row[m0 + 2]) if len(row) > m0 + 2 else 0.0,
                'Venduto': clean_non_negative(row[m0 + 3]) if len(row) > m0 + 3 else 0.0,
                'Sellout_Percent': clean_number(row[m0 + 4]) if len(row) > m0 + 4 else 0.0,
                'Size_35': clean_non_negative(row[m0 + 5]) if len(row) > m0 + 5 else 0.0,
                'Size_36': clean_non_negative(row[m0 + 6]) if len(row) > m0 + 6 else 0.0,
                'Size_37': clean_non_negative(row[m0 + 7]) if len(row) > m0 + 7 else 0.0,
                'Size_38': clean_non_negative(row[m0 + 8]) if len(row) > m0 + 8 else 0.0,
                'Size_39': clean_non_negative(row[m0 + 9]) if len(row) > m0 + 9 else 0.0,
                'Size_40': clean_non_negative(row[m0 + 10]) if len(row) > m0 + 10 else 0.0,
                'Size_41': clean_non_negative(row[m0 + 11]) if len(row) > m0 + 11 else 0.0,
                'Size_42': clean_non_negative(row[m0 + 12]) if len(row) > m0 + 12 else 0.0,
                'Valore_Giac': clean_non_negative(row[m0 + 13]) if len(row) > m0 + 13 else 0.0,
            }
            rows.append(record)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=['Article', 'Shop'], keep='last')
    df.to_csv(output_path, index=False)
    return df

if __name__ == "__main__":
    # Example usage for local testing
    in_sales = 'input/sales.csv'
    in_stock = 'input/stock.csv'
    out_sales = 'output/clean_sales.csv'
    out_stock = 'output/clean_articles.csv'
    if Path(in_sales).exists():
        parse_sales(in_sales, out_sales)
        print(f"Wrote {out_sales}")
    if Path(in_stock).exists():
        parse_articles(in_stock, out_stock)
        print(f"Wrote {out_stock}")
