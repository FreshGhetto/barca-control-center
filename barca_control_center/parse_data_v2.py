
import csv
import re
from pathlib import Path
import pandas as pd
from .excel_size_alignment import detect_size_data_shift
from .reparto_sizes import SUPPORTED_SIZES, infer_reparto_from_path, infer_reparto_from_values

VALID_CODES_DEFAULT = [
    'AR','AU','BO','BS','CA','CO','EU','EU2','LN','MC','MI',
    'NV','OR','PD','PM','RI','RM','SC','SD','SM','SPW','NO','ME2',
    'TV','VR','WEB','W','M4','MR','MP','SP'
]

# Accept article prefixes up to 5 digits; the $ anchor ensures the token ends here,
# which rejects dates like 27/04/2026 (they contain a second slash and extra digits).
ARTICLE_CODE_RE = re.compile(r'^\d{1,5}/[^/\s]+$')
ARTICLE_CODE_NUMERIC_RE = re.compile(r'^\d{6,9}$')  # codici numerici puri (es. Birkenstock: 1018999)
SIZE_LABEL_RE = re.compile(r'^\d{2}$')

def clean_number(s):
    """
    Accetta sia formato italiano (1.234,56) che inglese (1234.56).
    Regola: se c'è sia '.' che ',' → italiano (1.234,56 → 1234.56).
    Se c'è solo ',' → italiano decimale (1234,56 → 1234.56).
    Se c'è solo '.' → inglese decimale (1234.56 → 1234.56).
    """
    if s is None:
        return 0.0
    if not isinstance(s, str):
        try:
            return float(s)
        except Exception:
            return 0.0
    s = s.strip()
    if s == '' or s == '-' or s == '%':
        return 0.0
    has_dot = '.' in s
    has_comma = ',' in s
    if has_dot and has_comma:
        # Italiano: 1.234,56
        s = s.replace('.', '').replace(',', '.')
    elif has_comma:
        # Italiano decimale: 1234,56
        s = s.replace(',', '.')
    # else: solo punto (inglese) o intero → nessuna modifica
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

def is_article_code_any(value: str) -> bool:
    """Come is_article_code ma include anche codici numerici puri (Birkenstock 1018999...)."""
    if not value:
        return False
    token = value.strip().split(' ')[0]
    return bool(ARTICLE_CODE_RE.match(token)) or bool(ARTICLE_CODE_NUMERIC_RE.match(token))

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

def _detect_situazione_format(all_rows: list) -> bool:
    """
    Rileva il formato 'SITUAZIONE ARTICOLI' con intestazione NEG in col 4 e GIAC in col 5.
    Tipicamente: col0=articolo, col4=NEG/shop, col5=GIAC, col6=CON, col7=VEN, col8=%VEN.
    """
    for row in all_rows[:120]:
        if len(row) < 9:
            continue
        upper = [str(c or '').strip().upper() for c in row]
        if 'NEG' in upper and 'GIAC' in upper and 'VEN' in upper:
            neg_idx = next((i for i, v in enumerate(upper) if v == 'NEG'), None)
            giac_idx = next((i for i, v in enumerate(upper) if v == 'GIAC'), None)
            if neg_idx is not None and giac_idx is not None and neg_idx == 4 and giac_idx == 5:
                return True
    return False


def _parse_stock_situazione(all_rows: list, valid_codes: set, size_col_map: dict,
                             reparto: str, snapshot_at) -> list:
    """
    Parser dedicato formato SITUAZIONE ARTICOLI:
    - Prima riga articolo: col0=codice, col2=desc, col4=shop, col5=GIAC, col6=CON,
                           col7=VEN, col8=%VEN, col9='%' (skip), col10+=taglie
    - Righe successive stesso articolo: col0 vuoto, col4=shop, stesse metriche
    - Gestisce taglie fuori ordine e colonne vuote usando le posizioni reali dal header.
    """
    rows = []
    current_article = None
    current_desc = ''

    for row in all_rows:
        if not row or len(row) < 9:
            continue

        cell0 = (row[0] or '').strip()
        art_token = cell0.split(' ')[0] if cell0 else ''

        if is_article_code_any(art_token):
            current_article = art_token
            current_desc = (row[2] or '').strip()

        if not current_article:
            continue

        shop_raw = (row[4] or '').strip() if len(row) > 4 else ''
        shop = normalize_shop_code(shop_raw)
        if not shop or shop not in valid_codes:
            continue

        # Verifica che ci siano valori numerici dopo lo shop (accetta sia formato italiano che inglese)
        def _is_num(v):
            try:
                float(str(v or '').strip().replace(',', '.'))
                return str(v or '').strip() not in ('', '-', '%')
            except Exception:
                return False

        numeric_count = sum(1 for j in range(5, min(len(row), 12)) if _is_num(row[j]))
        if numeric_count < 2:
            continue

        def sg(idx):
            return row[idx] if len(row) > idx else ''

        # col5=GIAC, col6=CON, col7=VEN, col8=%VEN, col9='%'(skip), col10+=taglie
        record = {
            'snapshot_at':     snapshot_at,
            'Article':         current_article,
            'Description':     current_desc,
            'Reparto':         reparto or '',
            'Shop':            shop,
            'Ricevuto':        0.0,
            'Giacenza':        clean_non_negative(sg(5)),
            'Consegnato':      clean_non_negative(sg(6)),
            'Venduto':         clean_non_negative(sg(7)),
            'Sellout_Percent': clean_number(sg(8)),
            'Valore_Giac':     0.0,
        }
        for size in SUPPORTED_SIZES:
            record[f'Size_{size}'] = 0.0
        # Usa le posizioni reali dal header: evita errori con taglie fuori ordine o colonne vuote
        size_shift = detect_size_data_shift(
            row,
            size_col_map,
            normalize=lambda value: str(value or '').strip(),
        )
        for size, col_idx in size_col_map.items():
            aligned_col_idx = col_idx + size_shift
            record[f'Size_{size}'] = (
                clean_non_negative(sg(aligned_col_idx)) if len(row) > aligned_col_idx else 0.0
            )
        rows.append(record)

    return rows


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

        for offset in (2, 1, 3, 4):
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


def _detect_stock_size_col_map(rows) -> dict:
    """
    Ritorna {size: col_idx} con le posizioni di colonna REALI dal header.
    Gestisce correttamente taglie fuori ordine e colonne vuote intermedie.
    Fallback: mapping contiguo dal col 10.
    """
    for row in rows:
        upper_row = [str(cell or '').strip().upper() for cell in row]
        if 'NEG' not in upper_row or 'GIAC' not in upper_row or 'VEN' not in upper_row:
            continue
        col_map = {}
        for col_idx, cell in enumerate(row):
            text = str(cell or '').strip()
            # xlrd converte i numeri Excel in float: "35.0" → normalizza a "35"
            if text.endswith('.0') and text[:-2].isdigit():
                text = text[:-2]
            if not SIZE_LABEL_RE.match(text):
                continue
            try:
                size = int(text)
            except Exception:
                continue
            if size in SUPPORTED_SIZES and size not in col_map:
                col_map[size] = col_idx
        if col_map:
            return col_map
    # Fallback: posizioni contigue a partire da col 10
    return {size: 10 + i for i, size in enumerate([35, 36, 37, 38, 39, 40, 41, 42])}


def _detect_stock_size_labels(rows):
    col_map = _detect_stock_size_col_map(rows)
    # Ritorna le taglie nell'ordine in cui appaiono nel header (per compat. parser classico)
    return [size for size, _ in sorted(col_map.items(), key=lambda x: x[1])]

_REPEATED_HEADER_DATA_OFFSET = 33  # colonne header ripetute su ogni riga


def _detect_repeated_header_format(all_rows: list) -> bool:
    """
    True se il file ha l'intero header del report ripetuto su ogni riga
    (formato "ANALISI ARTICOLI" per negozio con header fisso a 33 colonne).
    """
    matches = sum(
        1 for row in all_rows[:30]
        if len(row) > 1 and 'ANALISI ARTICOLI' in (row[1] or '').upper()
    )
    return matches >= 5


def _parse_sales_repeated_header(all_rows: list, valid_codes: set, snapshot_at) -> list:
    """
    Parsa il formato donde ogni riga CSV contiene 33 colonne fisse di header
    seguite dai dati articolo-negozio:
      Caso A (prima riga del negozio): col[33]=shop, col[34]=article, col[35..39]=CONS,VEND,PERIO,GIAC,VEN%
      Caso B (righe successive):       col[33]=article,              col[34..38]=CONS,VEND,PERIO,GIAC,VEN%
    """
    O = _REPEATED_HEADER_DATA_OFFSET
    result = []
    current_shop = None

    for row in all_rows:
        if len(row) <= O + 4:
            continue

        cell_a = (row[O] if len(row) > O else '').strip()
        cell_b = (row[O + 1] if len(row) > O + 1 else '').strip()

        shop_a = normalize_shop_code(cell_a)
        is_shop_a = bool(shop_a and shop_a in valid_codes)
        is_art_a = is_article_code_any(cell_a)
        is_art_b = is_article_code_any(cell_b)

        if is_shop_a and is_art_b:
            # Caso A: nuova riga di negozio
            current_shop = shop_a
            article_code = cell_b.split(' ')[0]
            v = O + 2        # i valori iniziano da col 35
        elif is_art_a:
            # Caso B: articolo aggiuntivo stesso negozio
            article_code = cell_a.split(' ')[0]
            v = O + 1        # i valori iniziano da col 34
        else:
            continue

        if not current_shop:
            continue

        def sg(i):
            return row[i] if len(row) > i else ''

        consegnato = clean_non_negative(sg(v))
        venduto    = clean_non_negative(sg(v + 1))
        periodo    = clean_non_negative(sg(v + 2))
        giacenza   = clean_non_negative(sg(v + 3))
        sellout    = clean_number(sg(v + 4))

        result.append({
            'snapshot_at':       snapshot_at,
            'Article':           article_code,
            'Shop':              current_shop,
            'Consegnato_Qty':    consegnato,
            'Venduto_Qty':       venduto,
            'Periodo_Qty':       periodo,
            'Altro_Venduto_Qty': giacenza,
            'Sellout_Percent':   sellout,
            'Sellout_Clamped':   max(0.0, min(100.0, sellout)),
            'Valore_1': 0.0, 'Valore_2': 0.0, 'Valore_3': 0.0, 'Valore_4': 0.0,
        })

    return result


def parse_sales(filepath, output_path, valid_codes=None, snapshot_at=None):
    """
    Parses "ANALISI ARTICOLI" export.
    Supporta due formati:
      1) Formato "header ripetuto": ogni riga ha 33 colonne fisse di header + dati articolo-negozio
      2) Formato classico: righe miste con header separato
    """
    valid_codes = set(valid_codes or VALID_CODES_DEFAULT)

    # Leggi tutto il file una volta sola
    with open(filepath, 'r', encoding='latin1', errors='ignore') as f:
        all_rows = list(csv.reader(f))

    # Rileva e usa il parser specializzato per il formato "header ripetuto"
    if _detect_repeated_header_format(all_rows):
        rows = _parse_sales_repeated_header(all_rows, valid_codes, snapshot_at)
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.drop_duplicates(subset=['Article', 'Shop'], keep='last')
        df.to_csv(output_path, index=False)
        return df

    # --- Parser classico (formato originale) ---
    rows = []
    current_article = None

    for row in all_rows:
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

            # Metrics between shop/article and sellout marker.
            vals_start = max(shop_idx, art_idx if art_idx is not None else shop_idx) + 1
            nums = []
            for j in range(vals_start, pct_idx - 1):
                v = (row[j] or '').strip()
                if v == '':
                    continue
                if 'TOTALI' in v.upper():
                    break
                nums.append(clean_number(v))

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
    Supporta due formati:
    - Formato classico (narrow): Article in 16, Shop in 18, metrics 19..
    - Formato SITUAZIONE XLS:   Article in 0, Shop in 4, metrics 5..
    """
    valid_codes = set(valid_codes or VALID_CODES_DEFAULT)
    # Prova più encodings: i CSV convertiti da XLS usano utf-8, i CSV originali usano latin1
    all_rows = None
    for enc in ('utf-8-sig', 'utf-8', 'latin1', 'cp1252'):
        try:
            with open(filepath, 'r', encoding=enc, errors='strict') as f:
                all_rows = list(csv.reader(f))
            break
        except (UnicodeDecodeError, LookupError):
            continue
    if all_rows is None:
        with open(filepath, 'r', encoding='latin1', errors='ignore') as f:
            all_rows = list(csv.reader(f))

    size_col_map = _detect_stock_size_col_map(all_rows[:120])
    size_labels = [size for size, _ in sorted(size_col_map.items(), key=lambda x: x[1])]
    reparto = infer_reparto_from_path(filepath)
    if not reparto:
        for row in all_rows[:40]:
            reparto = infer_reparto_from_values(row)
            if reparto:
                break

    # Rilevamento automatico formato
    if _detect_situazione_format(all_rows):
        rows = _parse_stock_situazione(all_rows, valid_codes, size_col_map, reparto, snapshot_at)
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.drop_duplicates(subset=['Article', 'Shop'], keep='last')
        df.to_csv(output_path, index=False)
        return df

    # --- Parser classico ---
    # Calcola offset relativi di ogni taglia rispetto alla prima colonna taglia nel header.
    # Gestisce taglie fuori ordine e colonne vuote intermedie.
    if size_col_map:
        first_size_col = min(size_col_map.values())
        size_rel_offsets = {size: (col - first_size_col) for size, col in size_col_map.items()}
    else:
        size_rel_offsets = {size: i for i, size in enumerate([35, 36, 37, 38, 39, 40, 41, 42])}

    rows = []
    for i, row in enumerate(all_rows):
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
            'Reparto': reparto or '',
            'Shop': shop,
            'Ricevuto': clean_non_negative(row[m0]) if len(row) > m0 else 0.0,
            'Giacenza': clean_non_negative(row[m0 + 1]) if len(row) > m0 + 1 else 0.0,
            'Consegnato': clean_non_negative(row[m0 + 2]) if len(row) > m0 + 2 else 0.0,
            'Venduto': clean_non_negative(row[m0 + 3]) if len(row) > m0 + 3 else 0.0,
            'Sellout_Percent': clean_number(row[m0 + 4]) if len(row) > m0 + 4 else 0.0,
            'Valore_Giac': clean_non_negative(row[m0 + 5 + len(size_rel_offsets)]) if len(row) > m0 + 5 + len(size_rel_offsets) else 0.0,
        }
        for idx, size in enumerate(SUPPORTED_SIZES):
            record[f'Size_{size}'] = 0.0
        # Usa gli offset relativi dal header: corregge taglie fuori ordine e colonne vuote
        for size, rel_offset in size_rel_offsets.items():
            col = m0 + 5 + rel_offset
            record[f'Size_{size}'] = clean_non_negative(row[col]) if len(row) > col else 0.0
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
