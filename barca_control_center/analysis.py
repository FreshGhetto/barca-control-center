from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd

from .db_inputs import load_latest_clean_inputs_from_db
from .reparto_sizes import (
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
    """Suggerisce trasferimenti taglia per taglia per un articolo.

    Logica (dal notebook di analisi):
    - Destinazioni: negozi che hanno venduto (Venduto > 0), non WEB, con giacenza bassa (<=soglia)
    - Sorgenti: negozi con giacenza > 0 ma Venduto == 0, non WEB
    - Per ogni destinazione, cerca le taglie mancanti (stock == 0) e le preleva dalle sorgenti.
    """
    article_code = str(article_name or "").strip()
    if not article_code:
        return pd.DataFrame(columns=["Article", "From", "To", "Size", "Qty", "Reason"])

    # Prepara il dataframe articoli
    stock = articles_df.copy()
    stock["Article"] = stock["Article"].astype(str).str.strip()
    stock["Shop"] = _normalize_shop_series(stock["Shop"])
    size_cols = infer_size_columns(stock)
    stock = _coerce_numeric(stock, ["Giacenza", "Venduto", *size_cols])

    # Merge Periodo_Qty dalle vendite (per il campo Reason)
    sales = sales_df.copy()
    sales["Article"] = sales["Article"].astype(str).str.strip()
    sales["Shop"] = _normalize_shop_series(sales["Shop"])
    sales = _coerce_numeric(sales, ["Periodo_Qty"])
    sales_rollup = (
        sales.groupby(["Article", "Shop"], as_index=False)["Periodo_Qty"].max()
    )

    # Filtra per l'articolo selezionato
    df_art = stock[stock["Article"] == article_code].copy()
    if df_art.empty:
        return pd.DataFrame(columns=["Article", "From", "To", "Size", "Qty", "Reason"])

    df_art = df_art.merge(
        sales_rollup[sales_rollup["Article"] == article_code][["Article", "Shop", "Periodo_Qty"]],
        on=["Article", "Shop"],
        how="left",
    )
    df_art = _coerce_numeric(df_art, ["Periodo_Qty"])

    # 1. Identifica negozi destinazione: hanno venduto, non sono WEB, hanno giacenza bassa
    destinazioni = df_art[
        (df_art["Venduto"] > 0)
        & (df_art["Shop"] != ONLINE)
        & (df_art["Giacenza"] <= float(low_stock_threshold))
    ].copy().sort_values(by="Venduto", ascending=False)

    # 2. Identifica negozi sorgente: hanno giacenza ma non hanno venduto nulla (incluso M4)
    sorgenti = df_art[
        (df_art["Venduto"] == 0)
        & (df_art["Shop"] != ONLINE)
        & (df_art["Giacenza"] > 0)
    ].copy().sort_values(by="Giacenza", ascending=False)

    trasferimenti: list[dict[str, Any]] = []

    # 3. Logica di matching per taglia
    for _, dest in destinazioni.iterrows():
        dest_shop = str(dest["Shop"])
        # Taglie con stock zero nella destinazione
        taglie_mancanti = [s for s in size_cols if float(dest.get(s, 0.0) or 0.0) == 0.0]

        for taglia in taglie_mancanti:
            for idx_sorg, sorg in sorgenti.iterrows():
                if float(sorg.get(taglia, 0.0) or 0.0) > 0.0:
                    size_num: Any = taglia
                    try:
                        size_num = int(taglia.split("_", 1)[1])
                    except Exception:
                        pass
                    periodo_qty = dest.get("Periodo_Qty", dest.get("Venduto", ""))
                    trasferimenti.append(
                        {
                            "Article": article_code,
                            "From": sorg["Shop"],
                            "To": dest_shop,
                            "Size": size_num,
                            "Qty": 1.0,
                            "Reason": (
                                f"Vendite Recenti: {periodo_qty} | Stock Fermo a {sorg['Shop']}"
                            ),
                        }
                    )
                    # Aggiorna il conteggio per evitare doppi trasferimenti
                    sorgenti.at[idx_sorg, taglia] -= 1.0
                    sorgenti.at[idx_sorg, "Giacenza"] -= 1.0
                    break  # Passa alla prossima taglia mancante

    if not trasferimenti:
        return pd.DataFrame(columns=["Article", "From", "To", "Size", "Qty", "Reason"])
    return pd.DataFrame(trasferimenti)


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
    root = Path(__file__).resolve().parent.parent
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
