from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd

from db_inputs import load_latest_clean_inputs_from_db
from reparto_sizes import (
    SUPPORTED_SIZES,
    infer_size_columns as infer_supported_size_columns,
    normalize_reparto,
    required_core_sizes,
)

SIZES = list(SUPPORTED_SIZES)
SHOP_ALIASES = {"W": "WEB", "NU": "NV", "M2": "ME2"}
ONLINE = "WEB"
WAREHOUSE = "M4"


def infer_size_columns(df: pd.DataFrame) -> list[str]:
    return infer_supported_size_columns(df.columns)


def _normalize_shop_value(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    return SHOP_ALIASES.get(text, text)


def _normalize_shop_series(series: pd.Series) -> pd.Series:
    return series.map(_normalize_shop_value)


def _normalize_shop_meta(shop_meta: Optional[Any]) -> Dict[str, Dict[str, Any]]:
    if shop_meta is None:
        return {}
    if isinstance(shop_meta, dict):
        return {_normalize_shop_value(k): dict(v or {}) for k, v in shop_meta.items()}
    if isinstance(shop_meta, pd.DataFrame):
        frame = shop_meta.copy()
        if "Shop" not in frame.columns:
            return {}
        frame["Shop"] = _normalize_shop_series(frame["Shop"])
        out: Dict[str, Dict[str, Any]] = {}
        for _, row in frame.iterrows():
            shop = _normalize_shop_value(row.get("Shop"))
            if not shop:
                continue
            out[shop] = row.to_dict()
        return out
    return {}


def _safe_fascia_rank(value: Any) -> int:
    try:
        if pd.isna(value):
            return 99
        return int(float(value))
    except Exception:
        return 99


def _required_core_sizes(
    fascia: Any,
    *,
    reparto: Any = None,
    available_sizes: Optional[Iterable[Any]] = None,
) -> list[int]:
    return required_core_sizes(fascia, reparto=reparto, available_sizes=available_sizes)


def _coerce_numeric(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def load_shop_priority_frame(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)
    cols = {str(col).strip().lower(): col for col in df.columns}

    shop_col = None
    fascia_col = None
    mq_col = None
    for key, col in cols.items():
        if shop_col is None and any(token in key for token in ("sigla", "sig", "cod", "shop", "negozio")):
            shop_col = col
        if fascia_col is None and "fascia" in key:
            fascia_col = col
        if mq_col is None and "mq" in key:
            mq_col = col

    if shop_col is None:
        raise ValueError("Colonna shop/sigla non trovata nella configurazione negozi.")

    out = pd.DataFrame({"Shop": _normalize_shop_series(df[shop_col])})
    out["Fascia"] = pd.to_numeric(df[fascia_col], errors="coerce") if fascia_col is not None else np.nan
    out["Mq"] = pd.to_numeric(df[mq_col], errors="coerce") if mq_col is not None else np.nan
    out = out[out["Shop"] != ""].drop_duplicates(subset=["Shop"], keep="last")
    return out


def validate_stock_snapshot_integrity(articles_df: pd.DataFrame) -> pd.DataFrame:
    articles = articles_df.copy()
    if articles.empty:
        return pd.DataFrame(
            columns=[
                "Article",
                "Shop",
                "SizeStockSum",
                "Giacenza",
                "Venduto",
                "Consegnato",
                "SizeVsGiacenzaGap",
                "FlowBalanceGap",
                "SizeVsGiacenzaOk",
                "FlowBalanceOk",
            ]
        )

    size_cols = infer_size_columns(articles)
    numeric_cols = size_cols + ["Giacenza", "Venduto", "Consegnato"]
    articles = _coerce_numeric(articles, numeric_cols)
    articles["Article"] = articles["Article"].astype(str).str.strip()
    articles["Shop"] = _normalize_shop_series(articles["Shop"])
    articles["SizeStockSum"] = articles[size_cols].sum(axis=1) if size_cols else 0.0
    articles["SizeVsGiacenzaGap"] = articles["SizeStockSum"] - articles["Giacenza"]
    articles["FlowBalanceGap"] = (articles["Giacenza"] + articles["Venduto"]) - articles["Consegnato"]
    articles["SizeVsGiacenzaOk"] = articles["SizeVsGiacenzaGap"].abs() <= 0.25
    articles["FlowBalanceOk"] = articles["FlowBalanceGap"].abs() <= 0.25

    return articles[
        [
            "Article",
            "Shop",
            "SizeStockSum",
            "Giacenza",
            "Venduto",
            "Consegnato",
            "SizeVsGiacenzaGap",
            "FlowBalanceGap",
            "SizeVsGiacenzaOk",
            "FlowBalanceOk",
        ]
    ].sort_values(["Article", "Shop"])


def build_article_shop_transfer_signals(
    articles_df: pd.DataFrame,
    sales_df: pd.DataFrame,
    *,
    shop_meta: Optional[Any] = None,
    low_stock_threshold: float = 6.0,
) -> pd.DataFrame:
    articles = articles_df.copy()
    sales = sales_df.copy()
    meta = _normalize_shop_meta(shop_meta)
    size_cols = infer_size_columns(articles)
    if not size_cols:
        size_cols = [f"Size_{size}" for size in SIZES]
        for col in size_cols:
            if col not in articles.columns:
                articles[col] = 0.0

    articles["Article"] = articles["Article"].astype(str).str.strip()
    articles["Shop"] = _normalize_shop_series(articles["Shop"])
    if "Reparto" not in articles.columns:
        articles["Reparto"] = ""
    articles["Reparto"] = articles["Reparto"].map(lambda value: normalize_reparto(value) or "")
    articles = _coerce_numeric(articles, ["Giacenza", "Venduto", *size_cols])

    sales["Article"] = sales["Article"].astype(str).str.strip()
    sales["Shop"] = _normalize_shop_series(sales["Shop"])
    sales = _coerce_numeric(sales, ["Periodo_Qty", "Venduto_Qty"])
    sales_rollup = (
        sales.groupby(["Article", "Shop"], as_index=False)[["Periodo_Qty", "Venduto_Qty"]]
        .max()
        .sort_values(["Article", "Shop"])
    )

    stock_base = articles[["Article", "Shop", "Reparto", "Giacenza", "Venduto", *size_cols]].copy()
    merged = stock_base.merge(sales_rollup, on=["Article", "Shop"], how="outer")
    merged = _coerce_numeric(merged, ["Giacenza", "Venduto", "Periodo_Qty", "Venduto_Qty", *size_cols])
    if "Reparto" not in merged.columns:
        merged["Reparto"] = ""
    merged["Reparto"] = merged["Reparto"].map(lambda value: normalize_reparto(value) or "")
    merged["StockDepth"] = merged[size_cols].sum(axis=1)
    merged["ObservedSalesSignal"] = merged[["Periodo_Qty", "Venduto_Qty"]].max(axis=1)
    merged["ReceiverEligibleBySales"] = merged["ObservedSalesSignal"] > 0.0
    merged["NotebookVendutoSignal"] = merged[["Venduto", "ObservedSalesSignal"]].max(axis=1)

    merged["Fascia"] = merged["Shop"].map(lambda shop: meta.get(shop, {}).get("Fascia", np.nan))
    merged["ShopPriorityRank"] = merged["Fascia"].map(_safe_fascia_rank)
    merged["Role"] = merged["Shop"].map(
        lambda shop: "ONLINE" if shop == ONLINE else "WAREHOUSE" if shop == WAREHOUSE else "STORE"
    )
    merged["IsOutlet"] = merged["Fascia"].map(lambda fascia: _safe_fascia_rank(fascia) in (6, 7))

    available_sizes = [int(col.split("_", 1)[1]) for col in size_cols]

    def core_sizes_for_row(row: pd.Series) -> list[int]:
        return _required_core_sizes(
            row.get("Fascia"),
            reparto=row.get("Reparto"),
            available_sizes=available_sizes,
        )

    def missing_core_sizes(row: pd.Series) -> int:
        core = row.get("CoreSizeList", [])
        if not core:
            return 0
        missing = 0
        for size in core:
            if float(row.get(f"Size_{size}", 0.0) or 0.0) <= 0.0:
                missing += 1
        return missing

    merged["CoreSizeList"] = merged.apply(core_sizes_for_row, axis=1)
    merged["MissingCoreSizes"] = merged.apply(missing_core_sizes, axis=1)
    merged["CoreSizeCoverageRatio"] = merged["CoreSizeList"].map(lambda sizes: 0.0 if not sizes else float(len(sizes)))
    valid_core_counts = merged["CoreSizeCoverageRatio"].replace({0.0: np.nan})
    merged["CoreSizeCoverageRatio"] = (
        valid_core_counts - merged["MissingCoreSizes"]
    ) / valid_core_counts
    merged["CoreSizeCoverageRatio"] = merged["CoreSizeCoverageRatio"].fillna(0.0).clip(lower=0.0, upper=1.0)

    merged["LowStockActiveCandidate"] = (
        merged["NotebookVendutoSignal"].gt(0.0)
        & (~merged["IsOutlet"])
        & (~merged["Role"].isin(["ONLINE", "WAREHOUSE"]))
        & (merged["StockDepth"] <= float(low_stock_threshold))
    )
    merged["ZeroSalesSourceCandidate"] = (
        (merged["StockDepth"] > 0.0)
        & (merged["NotebookVendutoSignal"] <= 0.0)
        & (~merged["Role"].isin(["ONLINE"]))
    )
    merged["DoublePairsAvailable"] = 0.0
    for col in size_cols:
        merged["DoublePairsAvailable"] = merged["DoublePairsAvailable"] + np.maximum(merged[col] - 1.0, 0.0)
    merged["HasDoublePairs"] = merged["DoublePairsAvailable"] > 0.0
    merged["NotebookDestinationCandidate"] = (
        merged["NotebookVendutoSignal"].gt(0.0)
        & (~merged["Role"].isin(["ONLINE", "WAREHOUSE"]))
        & (merged["StockDepth"] <= float(low_stock_threshold))
    )
    merged["NotebookSourceCandidate"] = (
        merged["NotebookVendutoSignal"].le(0.0)
        & merged["StockDepth"].gt(0.0)
        & (~merged["Role"].isin(["ONLINE"]))
    )

    low_stock_gap = np.maximum(float(low_stock_threshold) - merged["StockDepth"], 0.0)
    merged["DestinationPriorityScore"] = (
        merged["NotebookDestinationCandidate"].astype(float) * 300.0
        + merged["NotebookVendutoSignal"] * 100.0
        + merged["MissingCoreSizes"] * 25.0
        + low_stock_gap * 12.0
        + np.maximum(8.0 - merged["ShopPriorityRank"], 0.0) * 10.0
    ).clip(lower=0.0)

    merged["SourcePriorityScore"] = (
        merged["HasDoublePairs"].astype(float) * 350.0
        + merged["DoublePairsAvailable"] * 120.0
        + merged["NotebookSourceCandidate"].astype(float) * 180.0
        + merged["ShopPriorityRank"].clip(lower=0.0) * 15.0
        + np.maximum(merged["StockDepth"] - 1.0, 0.0) * 10.0
        - merged["NotebookVendutoSignal"] * 20.0
    ).clip(lower=0.0)

    return merged[
        [
            "Article",
            "Shop",
            "Reparto",
            "Fascia",
            "ShopPriorityRank",
            "Role",
            "IsOutlet",
            "Periodo_Qty",
            "Venduto_Qty",
            "ObservedSalesSignal",
            "NotebookVendutoSignal",
            "ReceiverEligibleBySales",
            "StockDepth",
            "MissingCoreSizes",
            "CoreSizeCoverageRatio",
            "LowStockActiveCandidate",
            "ZeroSalesSourceCandidate",
            "NotebookDestinationCandidate",
            "NotebookSourceCandidate",
            "DoublePairsAvailable",
            "HasDoublePairs",
            "DestinationPriorityScore",
            "SourcePriorityScore",
        ]
    ].sort_values(["Article", "Shop"])


def suggest_transfers_for_article(
    articles_df: pd.DataFrame,
    sales_df: pd.DataFrame,
    article_name: str,
    *,
    shop_meta: Optional[Any] = None,
    low_stock_threshold: float = 6.0,
) -> pd.DataFrame:
    article_code = str(article_name or "").strip()
    if not article_code:
        return pd.DataFrame(columns=["Article", "From", "To", "Size", "Qty", "Reason"])

    signals = build_article_shop_transfer_signals(
        articles_df,
        sales_df,
        shop_meta=shop_meta,
        low_stock_threshold=low_stock_threshold,
    )
    stock = articles_df.copy()
    stock["Article"] = stock["Article"].astype(str).str.strip()
    stock["Shop"] = _normalize_shop_series(stock["Shop"])
    if "Reparto" not in stock.columns:
        stock["Reparto"] = ""
    stock["Reparto"] = stock["Reparto"].map(lambda value: normalize_reparto(value) or "")
    size_cols = infer_size_columns(stock)
    stock = _coerce_numeric(stock, ["Giacenza", *size_cols])
    available_sizes = [int(col.split("_", 1)[1]) for col in size_cols]

    signal_art = signals[signals["Article"] == article_code].copy()
    stock_art = stock[stock["Article"] == article_code].copy()
    if signal_art.empty:
        return pd.DataFrame(columns=["Article", "From", "To", "Size", "Qty", "Reason"])

    article_reparto = ""
    if "Reparto" in stock_art.columns:
        for value in stock_art["Reparto"].tolist():
            normalized = normalize_reparto(value)
            if normalized:
                article_reparto = normalized
                break

    working = signal_art.merge(
        stock_art[["Article", "Shop", *size_cols]],
        on=["Article", "Shop"],
        how="left",
    )
    working = _coerce_numeric(working, [*size_cols])
    signal_by_shop: dict[str, dict[str, Any]] = {
        str(row["Shop"]): row.to_dict()
        for _, row in working.iterrows()
    }
    stock_by_shop: dict[str, dict[int, float]] = {}
    for _, row in working.iterrows():
        shop = str(row["Shop"])
        stock_by_shop[shop] = {
            int(col.split("_", 1)[1]): float(row.get(col, 0.0) or 0.0)
            for col in size_cols
        }

    def total_stock(shop_code: str) -> float:
        return float(sum(stock_by_shop.get(shop_code, {}).values()))

    def shop_signal(shop_code: str, field: str, default: float = 0.0) -> float:
        row = signal_by_shop.get(shop_code, {})
        try:
            value = row.get(field, default)
            if pd.isna(value):
                return float(default)
            return float(value)
        except Exception:
            return float(default)

    def shop_rank(shop_code: str) -> int:
        row = signal_by_shop.get(shop_code, {})
        return _safe_fascia_rank(row.get("Fascia", np.nan))

    def is_outlet_shop(shop_code: str) -> bool:
        row = signal_by_shop.get(shop_code, {})
        return bool(row.get("IsOutlet", False))

    def donor_candidates(size: int, recv_shop: str) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        warehouse_qty = float(stock_by_shop.get(WAREHOUSE, {}).get(size, 0.0) or 0.0)
        if recv_shop != ONLINE and warehouse_qty >= 1.0:
            candidates.append({"shop": WAREHOUSE, "stage": "warehouse"})

        duplicate_rows: list[dict[str, Any]] = []
        single_rows: list[dict[str, Any]] = []
        for shop in sorted(stock_by_shop.keys()):
            if shop in ("", ONLINE, WAREHOUSE, recv_shop):
                continue
            if is_outlet_shop(shop):
                continue
            qty = float(stock_by_shop.get(shop, {}).get(size, 0.0) or 0.0)
            if qty < 1.0:
                continue
            candidate = {
                "shop": shop,
                "fascia_rank": shop_rank(shop),
                "sales_signal": shop_signal(shop, "NotebookVendutoSignal", 0.0),
                "source_priority_score": shop_signal(shop, "SourcePriorityScore", 0.0),
                "stock_total": total_stock(shop),
            }
            if qty >= 2.0:
                candidate["stage"] = "duplicate"
                candidate["duplicate_units"] = qty - 1.0
                candidate["duplicate_total"] = sum(max(0.0, pair_qty - 1.0) for pair_qty in stock_by_shop.get(shop, {}).values())
                duplicate_rows.append(candidate)
            else:
                candidate["stage"] = "single"
                single_rows.append(candidate)

        duplicate_rows.sort(
            key=lambda item: (
                -item["fascia_rank"],
                -item["duplicate_units"],
                -item["duplicate_total"],
                item["sales_signal"],
                -item["source_priority_score"],
                item["shop"],
            )
        )
        single_rows.sort(
            key=lambda item: (
                -item["fascia_rank"],
                item["sales_signal"],
                -item["source_priority_score"],
                -item["stock_total"],
                item["shop"],
            )
        )
        return candidates + duplicate_rows + single_rows

    def apply_move(donor: str, recv: str, size: int, reason: str, moves: list[dict[str, Any]]) -> None:
        stock_by_shop.setdefault(donor, {}).setdefault(size, 0.0)
        stock_by_shop.setdefault(recv, {}).setdefault(size, 0.0)
        if stock_by_shop[donor][size] < 1.0:
            return
        stock_by_shop[donor][size] -= 1.0
        stock_by_shop[recv][size] += 1.0
        moves.append(
            {
                "Article": article_code,
                "From": donor,
                "To": recv,
                "Size": size,
                "Qty": 1.0,
                "Reason": reason,
            }
        )

    destinations = working[
        working["NotebookDestinationCandidate"]
        & (working["Shop"] != ONLINE)
        & (working["Shop"] != WAREHOUSE)
        & (~working["IsOutlet"])
    ].copy()
    destinations = destinations.sort_values(
        ["ShopPriorityRank", "DestinationPriorityScore", "NotebookVendutoSignal", "MissingCoreSizes", "Shop"],
        ascending=[True, False, False, False, True],
    )

    outlet_candidates = working[working["IsOutlet"]].copy()
    outlet_candidates = outlet_candidates.sort_values(
        ["ShopPriorityRank", "NotebookVendutoSignal", "DestinationPriorityScore", "Shop"],
        ascending=[False, False, False, True],
    )
    outlet_shop = None if outlet_candidates.empty else str(outlet_candidates.iloc[0]["Shop"])

    moves: list[dict[str, Any]] = []
    for _, dest in destinations.iterrows():
        dest_shop = str(dest["Shop"])
        core_sizes = _required_core_sizes(
            dest.get("Fascia"),
            reparto=article_reparto or dest.get("Reparto"),
            available_sizes=available_sizes,
        )
        for size in core_sizes:
            if float(stock_by_shop.get(dest_shop, {}).get(size, 0.0) or 0.0) > 0.0:
                continue
            for donor in donor_candidates(size, dest_shop):
                donor_shop = str(donor["shop"])
                if donor_shop == dest_shop:
                    continue
                if float(stock_by_shop.get(donor_shop, {}).get(size, 0.0) or 0.0) < 1.0:
                    continue
                reason = {
                    "warehouse": "Fill required run from M4",
                    "duplicate": "Fill required run from duplicate stock",
                    "single": "Fill required run from low-fascia single",
                }.get(donor.get("stage"), "Fill required run")
                apply_move(donor_shop, dest_shop, size, reason, moves)
                break

    if outlet_shop:
        for shop in sorted(stock_by_shop.keys()):
            if shop in ("", ONLINE, WAREHOUSE, outlet_shop):
                continue
            if is_outlet_shop(shop):
                continue
            if total_stock(shop) <= 0.0:
                continue
            required_sizes = _required_core_sizes(
                signal_by_shop.get(shop, {}).get("Fascia"),
                reparto=article_reparto or signal_by_shop.get(shop, {}).get("Reparto"),
                available_sizes=available_sizes,
            )
            if all(float(stock_by_shop.get(shop, {}).get(size, 0.0) or 0.0) >= 1.0 for size in required_sizes):
                continue
            for size in SIZES:
                while float(stock_by_shop.get(shop, {}).get(size, 0.0) or 0.0) >= 1.0:
                    apply_move(shop, outlet_shop, size, "Move full line to outlet (size gaps)", moves)

    return pd.DataFrame(moves)


def _default_shops_cfg(root: Path) -> Path:
    cfg = root / "config" / "lista-negozi_integrato.xlsx"
    if cfg.exists():
        return cfg
    return root / "config" / "lista-negozi.xlsx"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analisi diagnostica BARCA: segnali trasferimento e controlli stock, letti dal DB."
    )
    parser.add_argument("--source-run-id", type=str, default=None, help="run_id sorgente DB da analizzare.")
    parser.add_argument("--article", type=str, default=None, help="Articolo specifico per export prototipo trasferimenti.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Cartella di export diagnostico (default: output).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    root = Path(__file__).resolve().parent
    source = load_latest_clean_inputs_from_db(source_run_id=args.source_run_id, verbose=True)

    shop_meta = None
    cfg = _default_shops_cfg(root)
    if cfg.exists():
        shop_meta = load_shop_priority_frame(cfg)

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    integrity = validate_stock_snapshot_integrity(source["stock_df"])
    signals = build_article_shop_transfer_signals(
        source["stock_df"],
        source["sales_df"],
        shop_meta=shop_meta,
    )
    integrity.to_csv(output_dir / "stock_integrity_report.csv", index=False)
    signals.to_csv(output_dir / "article_shop_signals.csv", index=False)

    if args.article:
        prototype = suggest_transfers_for_article(
            source["stock_df"],
            source["sales_df"],
            args.article,
            shop_meta=shop_meta,
        )
        safe_article = str(args.article).replace("/", "_").replace("\\", "_")
        prototype.to_csv(output_dir / f"prototype_transfers_{safe_article}.csv", index=False)

    print(
        f"[ANALYSIS] run_id={source['source_run_id']} signals={len(signals)} "
        f"integrity_rows={len(integrity)} output={output_dir}"
    )


if __name__ == "__main__":
    main()
