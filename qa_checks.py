from __future__ import annotations
import json
from pathlib import Path
import sys
import pandas as pd
import numpy as np


def _load_csv(path: Path):
    if not path.exists():
        return None
    return pd.read_csv(path)


def main() -> int:
    root = Path(__file__).resolve().parent
    out = root / "output"

    files = {
        "clean_sales": out / "clean_sales.csv",
        "clean_articles": out / "clean_articles.csv",
        "suggested_transfers": out / "suggested_transfers.csv",
        "suggested_transfers_detailed": out / "suggested_transfers_detailed.csv",
        "features_after": out / "features_after.csv",
        "demand_diagnostics": out / "demand_diagnostics.csv",
        "shipment_plan": out / "shipment_plan.csv",
        "shipment_summary": out / "shipment_summary.csv",
    }

    report = {"errors": [], "warnings": [], "metrics": {}}

    missing = [k for k, p in files.items() if not p.exists()]
    if missing:
        report["errors"].append(f"Missing output files: {missing}")
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 1

    cs = _load_csv(files["clean_sales"])
    ca = _load_csv(files["clean_articles"])
    tr = _load_csv(files["suggested_transfers"])
    trd = _load_csv(files["suggested_transfers_detailed"])
    fa = _load_csv(files["features_after"])
    sp = _load_csv(files["shipment_plan"])
    dd = _load_csv(files["demand_diagnostics"])

    report["metrics"]["shapes"] = {
        "clean_sales": list(cs.shape),
        "clean_articles": list(ca.shape),
        "suggested_transfers": list(tr.shape),
        "suggested_transfers_detailed": list(trd.shape),
        "features_after": list(fa.shape),
        "demand_diagnostics": list(dd.shape),
        "shipment_plan": list(sp.shape),
    }

    # 1) No negative quantities in cleaned operational columns.
    for col in ["Consegnato_Qty", "Venduto_Qty", "Periodo_Qty", "Altro_Venduto_Qty"]:
        neg = int((pd.to_numeric(cs[col], errors="coerce").fillna(0.0) < 0).sum())
        report["metrics"][f"neg_{col}"] = neg
        if neg > 0:
            report["errors"].append(f"Negative values found in clean_sales.{col}: {neg}")

    size_cols = [c for c in ca.columns if c.startswith("Size_")]
    neg_sizes = int((ca[size_cols] < 0).sum().sum()) if size_cols else 0
    report["metrics"]["neg_size_cells_clean_articles"] = neg_sizes
    if neg_sizes > 0:
        report["errors"].append(f"Negative size cells in clean_articles: {neg_sizes}")

    # 2) Uniqueness by (Article, Shop).
    dup_sales = int(cs.duplicated(subset=["Article", "Shop"]).sum())
    dup_articles = int(ca.duplicated(subset=["Article", "Shop"]).sum())
    report["metrics"]["dup_article_shop_sales"] = dup_sales
    report["metrics"]["dup_article_shop_articles"] = dup_articles
    if dup_sales > 0:
        report["errors"].append(f"Duplicate (Article,Shop) in clean_sales: {dup_sales}")
    if dup_articles > 0:
        report["errors"].append(f"Duplicate (Article,Shop) in clean_articles: {dup_articles}")

    # 3) Aggregated transfers must match detailed grouped.
    agg_from_detail = (
        trd.groupby(["Article", "Size", "From", "To", "Reason"], as_index=False)["Qty"]
        .sum()
        .sort_values(["Article", "From", "To", "Size"])
        .reset_index(drop=True)
    )
    agg_file = tr.sort_values(["Article", "From", "To", "Size"]).reset_index(drop=True)
    if not agg_from_detail.equals(agg_file):
        report["errors"].append("suggested_transfers.csv does not match grouped suggested_transfers_detailed.csv")

    # 4) Stock conservation on key-space used by allocator output.
    if size_cols:
        ca2 = ca.copy()
        ca2["Stock_before"] = ca2[size_cols].sum(axis=1)
        keys = set(zip(fa["Article"], fa["Shop"]))
        ca2 = ca2[ca2.apply(lambda r: (r["Article"], r["Shop"]) in keys, axis=1)]
        before = ca2.groupby("Article", as_index=False)["Stock_before"].sum().rename(columns={"Stock_before": "before"})
        after = fa.groupby("Article", as_index=False)["Stock_after"].sum().rename(columns={"Stock_after": "after"})
        chk = before.merge(after, on="Article", how="inner")
        chk["delta"] = chk["after"] - chk["before"]
        max_abs_delta = float(chk["delta"].abs().max()) if len(chk) else 0.0
        report["metrics"]["stock_conservation_max_abs_delta"] = max_abs_delta
        if max_abs_delta > 1e-9:
            report["errors"].append(f"Stock conservation violated. Max abs delta: {max_abs_delta}")

    # 5) Capacity and ops budgets must not be exceeded.
    shop = fa.groupby("Shop", as_index=False).agg(
        stock_after=("Stock_after", "sum"),
        cap_target=("ShopCapacityTarget", "max"),
        in_budget=("ShopInboundBudget", "max"),
        out_budget=("ShopOutboundBudget", "max"),
        in_used=("ShopInboundUsed", "max"),
        out_used=("ShopOutboundUsed", "max"),
    )
    over_cap = int(((shop["stock_after"] - shop["cap_target"]) > 1e-9).sum())
    over_in = int(((shop["in_used"] - shop["in_budget"]) > 1e-9).sum())
    over_out = int(((shop["out_used"] - shop["out_budget"]) > 1e-9).sum())
    report["metrics"]["shops_over_capacity"] = over_cap
    report["metrics"]["shops_over_inbound_budget"] = over_in
    report["metrics"]["shops_over_outbound_budget"] = over_out
    if over_cap > 0:
        report["errors"].append(f"Shops over capacity target: {over_cap}")
    if over_in > 0:
        report["errors"].append(f"Shops over inbound budget: {over_in}")
    if over_out > 0:
        report["errors"].append(f"Shops over outbound budget: {over_out}")

    # 6) Shipment qty must match transfers qty.
    qty_transfers = float(pd.to_numeric(tr["Qty"], errors="coerce").fillna(0.0).sum())
    qty_shipment = float(pd.to_numeric(sp["Qty"], errors="coerce").fillna(0.0).sum())
    report["metrics"]["qty_transfers"] = qty_transfers
    report["metrics"]["qty_shipment"] = qty_shipment
    if abs(qty_transfers - qty_shipment) > 1e-9:
        report["errors"].append(
            f"Qty mismatch between suggested_transfers ({qty_transfers}) and shipment_plan ({qty_shipment})"
        )

    missing_dispatch = int(((sp["ConsolidationStatus"] == "PLANNED") & (sp["DispatchDate"].astype(str).str.strip() == "")).sum())
    missing_eta = int(((sp["ConsolidationStatus"] == "PLANNED") & (sp["EtaDate"].astype(str).str.strip() == "")).sum())
    report["metrics"]["planned_missing_dispatch"] = missing_dispatch
    report["metrics"]["planned_missing_eta"] = missing_eta
    if missing_dispatch > 0 or missing_eta > 0:
        report["errors"].append(
            f"Planned rows missing dispatch/eta dates: dispatch={missing_dispatch}, eta={missing_eta}"
        )

    # Non-blocking informational checks.
    sales_articles = set(cs["Article"])
    stock_articles = set(ca["Article"])
    only_sales = len(sales_articles - stock_articles)
    only_stock = len(stock_articles - sales_articles)
    report["metrics"]["articles_only_in_sales"] = int(only_sales)
    report["metrics"]["articles_only_in_stock"] = int(only_stock)
    if only_sales > 0 or only_stock > 0:
        report["warnings"].append(
            f"Article mismatch between sales and stock (only_sales={only_sales}, only_stock={only_stock})."
        )

    # Optional QA for integrated orders module.
    orders_dir = out / "orders"
    orders_summary_path = orders_dir / "orders_summary.json"
    if orders_summary_path.exists():
        try:
            orders_summary = json.loads(orders_summary_path.read_text(encoding="utf-8"))
            report["metrics"]["orders_enabled"] = bool(orders_summary.get("enabled", False))
            report["metrics"]["orders_bundles_detected"] = len(orders_summary.get("bundles_detected", []))

            if orders_summary.get("enabled", False):
                missing_order_files = []
                for section in ("current", "continuativa"):
                    sec = orders_summary.get(section, {})
                    for fname in sec.get("output_files", []):
                        p = orders_dir / fname
                        if not p.exists():
                            missing_order_files.append(str(p.name))

                    full = sec.get("full", {})
                    if isinstance(full, dict) and full.get("enabled", False):
                        for fname in full.get("output_files", []):
                            p = orders_dir / fname
                            if not p.exists():
                                missing_order_files.append(str(p.name))

                report["metrics"]["orders_missing_output_files"] = len(missing_order_files)
                if missing_order_files:
                    report["errors"].append(f"Orders summary references missing files: {missing_order_files}")

                current_math = orders_dir / "orders_current_previsione_math.csv"
                if current_math.exists():
                    df_cur = pd.read_csv(current_math)
                    if "Da_Acquistare_Totale" in df_cur.columns:
                        neg_cur = int((pd.to_numeric(df_cur["Da_Acquistare_Totale"], errors="coerce").fillna(0.0) < 0).sum())
                        report["metrics"]["orders_current_negative_da_acq"] = neg_cur
                        if neg_cur > 0:
                            report["errors"].append(f"Negative Da_Acquistare_Totale in current orders file: {neg_cur}")

                cont_math = orders_dir / "orders_continuativa_previsione_math.csv"
                if cont_math.exists():
                    df_cont = pd.read_csv(cont_math)
                    if "Da_Acquistare_Totale" in df_cont.columns:
                        neg_cont = int((pd.to_numeric(df_cont["Da_Acquistare_Totale"], errors="coerce").fillna(0.0) < 0).sum())
                        report["metrics"]["orders_cont_negative_da_acq"] = neg_cont
                        if neg_cont > 0:
                            report["errors"].append(f"Negative Da_Acquistare_Totale in continuativa math file: {neg_cont}")

                cont_rf = orders_dir / "orders_continuativa_previsione_rf.csv"
                if cont_rf.exists():
                    df_rf = pd.read_csv(cont_rf)
                    if "Da_Acquistare_Totale" in df_rf.columns:
                        neg_rf = int((pd.to_numeric(df_rf["Da_Acquistare_Totale"], errors="coerce").fillna(0.0) < 0).sum())
                        report["metrics"]["orders_rf_negative_da_acq"] = neg_rf
                        if neg_rf > 0:
                            report["errors"].append(f"Negative Da_Acquistare_Totale in RF orders file: {neg_rf}")
        except Exception as exc:
            report["warnings"].append(f"Orders QA skipped due to parsing error: {exc}")
    else:
        report["metrics"]["orders_enabled"] = False

    # Optional QA for ingest agent.
    ingest_report_path = out / "ingest" / "ingest_report_latest.json"
    if ingest_report_path.exists():
        try:
            ingest = json.loads(ingest_report_path.read_text(encoding="utf-8"))
            report["metrics"]["ingest_processed_total"] = int(ingest.get("processed_total", 0))
            report["metrics"]["ingest_ingested"] = int(ingest.get("ingested", 0))
            report["metrics"]["ingest_quarantine"] = int(ingest.get("quarantine", 0))
            report["metrics"]["ingest_errors"] = int(ingest.get("errors", 0))
            if int(ingest.get("errors", 0)) > 0:
                report["warnings"].append(
                    f"Ingest report contains errors={ingest.get('errors', 0)}. "
                    f"Check output/ingest/ingest_report_latest.json"
                )
        except Exception as exc:
            report["warnings"].append(f"Ingest QA skipped due to parsing error: {exc}")
    else:
        report["metrics"]["ingest_processed_total"] = 0

    qa_path = out / "qa_report.json"
    qa_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))

    return 1 if report["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
