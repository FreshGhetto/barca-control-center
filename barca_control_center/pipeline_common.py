from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def newest_file(folder: Path, prefix: str) -> Path:
    files = sorted(folder.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"Nessun file trovato in {folder} con pattern {prefix}_YYYY-MM.csv")
    return files[-1]


def all_files(folder: Path, prefix: str) -> list:
    """Restituisce tutti i CSV con il prefisso dato, ordinati per nome."""
    return sorted(folder.glob(f"{prefix}_*.csv"))


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
    sales, stock, report = harmonize_clean_frames(sales, stock)
    sales.to_csv(clean_sales, index=False)
    stock.to_csv(clean_stock, index=False)
    return report


def harmonize_clean_frames(sales: pd.DataFrame, stock: pd.DataFrame):
    sales = sales.copy()
    stock = stock.copy()
    report_rows = []

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
            report_rows.append(
                {
                    "kind": "drop_inert_stock_only_article",
                    "article": art,
                    "qty_signal": tot,
                    "note": "Removed inert stock-only article (all zero).",
                }
            )
    if inert_articles:
        stock = stock[~stock["Article"].astype(str).isin(inert_articles)].copy()

    sales_articles = set(sales["Article"].astype(str))
    remaining_stock_only = sorted(set(stock["Article"].astype(str)) - sales_articles)
    if remaining_stock_only:
        add = stock[stock["Article"].astype(str).isin(remaining_stock_only)][["snapshot_at", "Article", "Shop"]].drop_duplicates().copy()
        sales_cols = list(sales.columns)
        for c in sales_cols:
            if c in add.columns:
                continue
            add[c] = 0.0
        add = add[sales_cols]
        for c in sales_cols:
            if c in ("snapshot_at", "Article", "Shop"):
                continue
            add[c] = pd.to_numeric(add[c], errors="coerce").fillna(0.0)
        sales = pd.concat([sales, add], ignore_index=True)

        for art in remaining_stock_only:
            n = int((add["Article"].astype(str) == art).sum())
            report_rows.append(
                {
                    "kind": "add_synthetic_zero_sales",
                    "article": art,
                    "qty_signal": n,
                    "note": "Added synthetic zero-sales rows for active stock-only article.",
                }
            )

    stock = stock.drop(columns=["__signal__"], errors="ignore")
    sales = sales.drop_duplicates(subset=["Article", "Shop"], keep="last")
    stock = stock.drop_duplicates(subset=["Article", "Shop"], keep="last")

    report = pd.DataFrame(report_rows)
    return sales, stock, report
