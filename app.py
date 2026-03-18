
from pathlib import Path
import datetime as dt
import pandas as pd
import numpy as np

from parse_data_v2 import parse_sales, parse_articles
from allocator_v1 import run_allocation

def newest_file(folder: Path, prefix: str) -> Path:
    files = sorted(folder.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"Nessun file trovato in {folder} con pattern {prefix}_YYYY-MM.csv")
    return files[-1]

def load_valid_shop_codes(shops_cfg: Path):
    try:
        shops = pd.read_excel(shops_cfg, sheet_name=0)
        cols = [str(c).strip().lower() for c in shops.columns]
        sig_col = None
        for c in cols:
            if "sig" in c or "cod" in c or "shop" in c:
                sig_col = shops.columns[cols.index(c)]
                break
        if sig_col is None:
            return None
        codes = (
            shops[sig_col]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"W": "WEB"})
        )
        codes = sorted({c for c in codes if c and c != "NAN"})
        return codes or None
    except Exception:
        return None

def harmonize_clean_outputs(clean_sales: Path, clean_stock: Path):
    sales = pd.read_csv(clean_sales)
    stock = pd.read_csv(clean_stock)
    report_rows = []

    # 1) Remove inert stock-only articles (all-zero rows, no sales counterpart).
    sales_articles = set(sales["Article"].astype(str))
    size_cols = [c for c in stock.columns if c.startswith("Size_")]
    base_cols = ["Ricevuto", "Giacenza", "Consegnato", "Venduto"]
    existing_base_cols = [c for c in base_cols if c in stock.columns]
    signal_cols = existing_base_cols + size_cols
    if signal_cols:
        signal = stock[signal_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).clip(lower=0.0).sum(axis=1)
    else:
        signal = pd.Series(np.zeros(len(stock)), index=stock.index)
    stock = stock.copy()
    stock["__signal__"] = signal

    stock_only_articles = sorted(set(stock["Article"].astype(str)) - sales_articles)
    inert_articles = []
    for art in stock_only_articles:
        tot = float(stock.loc[stock["Article"].astype(str) == art, "__signal__"].sum())
        if tot <= 0.0:
            inert_articles.append(art)
            report_rows.append({"kind": "drop_inert_stock_only_article", "article": art, "qty_signal": tot, "note": "Removed inert stock-only article (all zero)."})
    if inert_articles:
        stock = stock[~stock["Article"].astype(str).isin(inert_articles)].copy()

    # 2) Add synthetic zero-sales rows for remaining stock-only active articles.
    sales_articles = set(sales["Article"].astype(str))
    remaining_stock_only = sorted(set(stock["Article"].astype(str)) - sales_articles)
    if remaining_stock_only:
        add = stock[stock["Article"].astype(str).isin(remaining_stock_only)][["snapshot_at", "Article", "Shop"]].drop_duplicates().copy()
        sales_cols = list(sales.columns)
        for c in sales_cols:
            if c in add.columns:
                continue
            add[c] = 0.0
        # Keep only target schema order.
        add = add[sales_cols]
        # Normalize types for numeric columns.
        for c in sales_cols:
            if c in ("snapshot_at", "Article", "Shop"):
                continue
            add[c] = pd.to_numeric(add[c], errors="coerce").fillna(0.0)
        sales = pd.concat([sales, add], ignore_index=True)

        for art in remaining_stock_only:
            n = int((add["Article"].astype(str) == art).sum())
            report_rows.append({"kind": "add_synthetic_zero_sales", "article": art, "qty_signal": n, "note": "Added synthetic zero-sales rows for active stock-only article."})

    # Cleanup and write back.
    stock = stock.drop(columns=["__signal__"], errors="ignore")
    sales = sales.drop_duplicates(subset=["Article", "Shop"], keep="last")
    stock = stock.drop_duplicates(subset=["Article", "Shop"], keep="last")
    sales.to_csv(clean_sales, index=False)
    stock.to_csv(clean_stock, index=False)

    report = pd.DataFrame(report_rows)
    return report

def main():
    root = Path(__file__).resolve().parent
    inp = root / "input"
    out = root / "output"
    cfg = root / "config"
    out.mkdir(exist_ok=True)

    sales_file = newest_file(inp, "sales")
    stock_file = newest_file(inp, "stock")
    shops_cfg = cfg / "lista-negozi_integrato.xlsx"
    if not shops_cfg.exists():
        shops_cfg = cfg / "lista-negozi.xlsx"

    snapshot_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("=== BARCA Stock Allocation Engine ===")
    print("1) Put files in .\\input\\ as:")
    print("   - sales_YYYY-MM.csv  (ANALISI ARTICOLI)")
    print("   - stock_YYYY-MM.csv  (SITUAZIONE ARTICOLI)")
    print()
    print(f"Sales file : {sales_file.name}")
    print(f"Stock file : {stock_file.name}")
    print(f"Shop config: {shops_cfg.name}")
    print()

    clean_sales = out / "clean_sales.csv"
    clean_stock = out / "clean_articles.csv"
    valid_codes = load_valid_shop_codes(shops_cfg)

    parse_sales(str(sales_file), str(clean_sales), valid_codes=valid_codes, snapshot_at=snapshot_at)
    parse_articles(str(stock_file), str(clean_stock), valid_codes=valid_codes, snapshot_at=snapshot_at)
    align_report = harmonize_clean_outputs(clean_sales, clean_stock)
    align_report.to_csv(out / "alignment_report.csv", index=False)

    print("Parsing completato. Avvio allocazione...")
    run_allocation(clean_sales, clean_stock, shops_cfg, out)

    print("\nFatto. Output in .\\output\\")
    print(" - clean_sales.csv")
    print(" - clean_articles.csv")
    print(" - alignment_report.csv")
    print(" - suggested_transfers.csv")
    print(" - suggested_transfers_detailed.csv")
    print(" - shipment_plan.csv")
    print(" - shipment_summary.csv")
    print(" - features_after.csv")
    print(" - demand_diagnostics.csv")

if __name__ == "__main__":
    main()
