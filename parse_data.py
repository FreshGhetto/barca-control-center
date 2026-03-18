import pandas as pd
import csv
import re
from pathlib import Path

ARTICLE_CODE_RE = re.compile(r'^\d{2}/\S+')

def is_article_code(value):
    if not value:
        return False
    return bool(ARTICLE_CODE_RE.match(value.strip().split(' ')[0]))

def clean_number(s):
    if not isinstance(s, str):
        return s
    # Italian format: 1.234,56 -> 1234.56
    # Remove dots (thousands), replace comma with dot
    s = s.strip()
    if s == '' or s == '-': return 0
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except:
        return 0

def parse_sales(filepath, output_path):
    print(f"Parsing Sales Data from {filepath}...")
    rows = []
    current_article = None
    
    with open(filepath, 'r', encoding='latin1') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if len(row) < 30: continue
            
            # Find the article code (usually starts with "59/" or "XX/")
            article_idx = -1
            for idx, cell in enumerate(row):
                val = cell.strip()
                if is_article_code(val):
                    article_idx = idx
                    # Clean the article code: "59/CODE   DESCRIPTION" -> "59/CODE"
                    # We split by space and take the first part
                    current_article = val.split(' ')[0]
                    break
            
            if not current_article: continue
            
            # Identify shop and data offset
            shop = ""
            offset = 0
            
            if article_idx != -1:
                # Article found in this row. Shop is the next column.
                if article_idx + 1 < len(row):
                    shop = row[article_idx + 1].strip()
                    # Mapping: article_idx + 1 is Shop
                    # Data (Con.) starts at article_idx + 2
                    # The original index 37 (Shop) with Article at 36 had offset 1 relative to 36.
                    # So data starts at shop_idx + 1.
                    data_start_idx = article_idx + 2
            else:
                # No article code in this row. Use last current_article.
                # Heuristic: Shop name is usually in col 36 (if shifted by 1 relative to NEGOZIO title)
                # Let's search for something that looks like a shop name inCols 34-38
                for idx in range(34, min(39, len(row))):
                    val = row[idx].strip()
                    if re.match(r'^[A-Z]{2}\s', val) or val in ["WEB NEGOZIO WEB", "SPW SPED DOMICILIO TRIANG WEB"]:
                        shop = val
                        data_start_idx = idx + 1
                        break
            
            if not shop: continue
            
            # Approved Shop Code Whitelist (consistent with parse_articles)
            VALID_CODES = [
                'AR', 'AU', 'BO', 'BS', 'CA', 'CO', 'EU', 'LN', 'MC', 'MI', 
                'NV', 'OR', 'PD', 'PM', 'RI', 'RM', 'SC', 'SD', 'SM', 'SPW', 
                'TV', 'VR', 'WEB'
            ]
            
            # Extract short code from shop string: "AR  ARESE" -> "AR"
            shop_code = shop.split(' ')[0].strip()
            
            if shop_code not in VALID_CODES:
                continue
            
            # Extract data
            try:
                record = {
                    'Article': current_article,
                    'Shop': shop_code,
                    'Consegnato_Qty': clean_number(row[data_start_idx]),
                    'Venduto_Qty': clean_number(row[data_start_idx+1]),
                    'Periodo_Qty': clean_number(row[data_start_idx+2]),
                    'Altro_Venduto_Qty': clean_number(row[data_start_idx+3]),
                    'Giacenza_Qty': clean_number(row[data_start_idx+4]),
                    'Sellout_Percent': clean_number(row[data_start_idx+6]), 
                    'Valore_1': clean_number(row[data_start_idx+7]),
                    'Valore_2': clean_number(row[data_start_idx+8]),
                    'Valore_3': clean_number(row[data_start_idx+9]), 
                    'Valore_4': clean_number(row[data_start_idx+10])
                }
                rows.append(record)
            except IndexError:
                continue
            
    df = pd.DataFrame(rows)
    print(f"Extracted {len(df)} sales records.")
    df.to_csv(output_path, index=False)

def parse_articles(filepath, output_path):
    print(f"Parsing Article Data from {filepath}...")
    rows = []
    with open(filepath, 'r', encoding='latin1') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0: continue # Skip header line
            if len(row) < 30: continue
            
            # In 'situaz', granular rows have text in Col 18 (Shop) and Code in 16
            # Cols 0-15 are metadata repeated or empty
            
            article = row[16].strip()
            desc = row[17].strip()
            shop = row[18].strip()
            
            # Approved Shop Code Whitelist (consistent with parse_sales)
            VALID_CODES = [
                'AR', 'AU', 'BO', 'BS', 'CA', 'CO', 'EU', 'LN', 'MC', 'MI', 
                'NV', 'OR', 'PD', 'PM', 'RI', 'RM', 'SC', 'SD', 'SM', 'SPW', 
                'TV', 'VR', 'WEB'
            ]
            
            if shop not in VALID_CODES or not is_article_code(article):
                continue
            
            # Metrics from Col 19
            # 19: Ric
            # 20: Giac
            # 21: Con
            # 22: Ven
            # 23: % Ven
            
            record = {
                'Article': article,
                'Description': desc,
                'Shop': shop,
                'Ricevuto': clean_number(row[19]),
                'Giacenza': clean_number(row[20]),
                'Consegnato': clean_number(row[21]),
                'Venduto': clean_number(row[22]),
                'Sellout_Percent': clean_number(row[23]),
                'Size_35': clean_number(row[24]),
                'Size_36': clean_number(row[25]),
                'Size_37': clean_number(row[26]),
                'Size_38': clean_number(row[27]),
                'Size_39': clean_number(row[28]),
                'Size_40': clean_number(row[29]),
                'Size_41': clean_number(row[30]),
                'Size_42': clean_number(row[31]),
                'Valore_Giac': clean_number(row[32]) if len(row) > 32 else 0, # Guessing location
            }
            rows.append(record)

    df = pd.DataFrame(rows)
    print(f"Extracted {len(df)} article records.")
    df.to_csv(output_path, index=False)

def _newest_file(folder: Path, prefix: str):
    files = sorted(folder.glob(f"{prefix}_*.csv"))
    return files[-1] if files else None

if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    input_dir = root / "input"
    output_dir = root / "output"
    raw_dir = root / "data" / "raw_original"
    output_dir.mkdir(exist_ok=True)

    sales_src = _newest_file(input_dir, "sales")
    stock_src = _newest_file(input_dir, "stock")

    # Fallback for archived original exports.
    if sales_src is None:
        legacy_sales = raw_dir / "controlling vendite scarpe donna 25i_periodo1.1_4.2.csv"
        if legacy_sales.exists():
            sales_src = legacy_sales
    if stock_src is None:
        legacy_stock = raw_dir / "situaz articoli scarpe donna 25i.csv"
        if legacy_stock.exists():
            stock_src = legacy_stock

    if sales_src:
        parse_sales(sales_src, output_dir / "clean_sales.csv")
    else:
        print("No sales source found (input/sales_YYYY-MM.csv or data/raw_original legacy file).")

    if stock_src:
        parse_articles(stock_src, output_dir / "clean_articles.csv")
    else:
        print("No stock source found (input/stock_YYYY-MM.csv or data/raw_original legacy file).")
