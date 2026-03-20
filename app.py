import argparse
from pathlib import Path
import datetime as dt
import pandas as pd
import numpy as np

from parse_data_v2 import parse_sales, parse_articles
from allocator_v1 import run_allocation
from orders_pipeline import run_orders_pipeline, has_order_inputs
from ingest_agent import ingest_incoming
from db_sync import run_db_sync
from db_inputs import export_latest_clean_inputs_from_db
from db_orders import export_orders_outputs_from_db

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BARCA Control Center: distribuzione giacenze + previsione ordini."
    )
    parser.add_argument(
        "--source-db",
        action="store_true",
        help="Usa come input operativo l'ultimo snapshot sales/stock dal DB (DB-first).",
    )
    parser.add_argument(
        "--source-db-run-id",
        type=str,
        default=None,
        help="run_id sorgente specifico da cui leggere sales/stock quando usi --source-db.",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Salta l'agente ingest automatico (incoming -> input).",
    )
    parser.add_argument(
        "--incoming-root",
        type=Path,
        default=None,
        help="Cartella incoming dei file raw da classificare/normalizzare.",
    )
    parser.add_argument(
        "--keep-incoming",
        action="store_true",
        help="Non spostare i raw processati in incoming/_processed.",
    )
    parser.add_argument(
        "--skip-orders",
        action="store_true",
        help="Salta il modulo ordini e avvia solo distribuzione giacenze.",
    )
    parser.add_argument(
        "--orders-root",
        type=Path,
        default=None,
        help="Cartella root con i CSV ordini (*_sd_1/2/3.csv).",
    )
    parser.add_argument(
        "--orders-source-db",
        action="store_true",
        help="Usa output ordini ricostruiti dal DB invece dei CSV in input/orders.",
    )
    parser.add_argument(
        "--orders-source-db-run-id",
        type=str,
        default=None,
        help="run_id sorgente specifico per ricostruire output ordini dal DB.",
    )
    parser.add_argument(
        "--orders-coverage",
        type=float,
        default=1.20,
        help="Fattore copertura per il modulo ordini (default 1.20).",
    )
    parser.add_argument(
        "--orders-math-only",
        action="store_true",
        help="Esegue solo modello matematico nel modulo ordini.",
    )
    parser.add_argument(
        "--sync-db",
        action="store_true",
        help="Sincronizza output nel database PostgreSQL.",
    )
    parser.add_argument(
        "--db-create-schema",
        action="store_true",
        help="Applica db/schema.sql prima della sync DB.",
    )
    return parser.parse_args()


def pick_orders_root(root: Path, cli_orders_root: Path | None) -> Path | None:
    candidates = []
    if cli_orders_root is not None:
        candidates.append(cli_orders_root)
    else:
        candidates.append(root / "input" / "orders")
        candidates.append(Path(r"C:\Users\bacci\Downloads\Downloads\per_previsioni"))

    for cand in candidates:
        if has_order_inputs(cand):
            return cand
    return None

def main():
    args = parse_args()
    root = Path(__file__).resolve().parent
    inp = root / "input"
    out = root / "output"
    cfg = root / "config"
    out.mkdir(exist_ok=True)

    shops_cfg = cfg / "lista-negozi_integrato.xlsx"
    if not shops_cfg.exists():
        shops_cfg = cfg / "lista-negozi.xlsx"

    clean_sales = out / "clean_sales.csv"
    clean_stock = out / "clean_articles.csv"

    print("=== BARCA Unified Engine ===")
    if args.source_db:
        print("[STEP 0/3] Modalita' DB-first: lettura clean inputs dal database.")
        try:
            db_source = export_latest_clean_inputs_from_db(
                clean_sales_csv=clean_sales,
                clean_stock_csv=clean_stock,
                source_run_id=args.source_db_run_id,
                verbose=True,
            )
            print(
                f"[STEP 0/3] DB source ok: run_id={db_source['source_run_id']}, "
                f"sales={db_source['sales_rows']}, stock={db_source['stock_rows']}"
            )
        except Exception as exc:
            print(f"[STEP 0/3] ERRORE DB source: {exc}")
            raise SystemExit(1)
    else:
        if args.skip_ingest:
            print("[STEP 0/3] Ingest raw saltato (--skip-ingest).")
        else:
            incoming_root = args.incoming_root or (root / "incoming")
            print(f"[STEP 0/3] Avvio ingest raw da: {incoming_root}")
            ingest_summary = ingest_incoming(
                root=root,
                incoming_dir=incoming_root,
                move_processed=not args.keep_incoming,
                verbose=True,
            )
            print(
                f"[STEP 0/3] Ingest completato: "
                f"ingested={ingest_summary['ingested']}, "
                f"quarantine={ingest_summary['quarantine']}, "
                f"errors={ingest_summary['errors']}"
            )

        sales_file = newest_file(inp, "sales")
        stock_file = newest_file(inp, "stock")
        snapshot_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        valid_codes = load_valid_shop_codes(shops_cfg)

        print("1) (Consigliato) metti i raw in .\\incoming\\ (csv/xlsx), ingest automatico.")
        print("2) Oppure metti i file gia' standard in .\\input\\ come:")
        print("   - sales_YYYY-MM.csv  (ANALISI ARTICOLI)")
        print("   - stock_YYYY-MM.csv  (SITUAZIONE ARTICOLI)")
        print("3) (Opzionale) metti i file ordini in .\\input\\orders\\")
        print()
        print(f"Sales file : {sales_file.name}")
        print(f"Stock file : {stock_file.name}")
        print(f"Shop config: {shops_cfg.name}")
        print()

        parse_sales(str(sales_file), str(clean_sales), valid_codes=valid_codes, snapshot_at=snapshot_at)
        parse_articles(str(stock_file), str(clean_stock), valid_codes=valid_codes, snapshot_at=snapshot_at)

    align_report = harmonize_clean_outputs(clean_sales, clean_stock)
    align_report.to_csv(out / "alignment_report.csv", index=False)

    print("[STEP 1/3] Parsing completato. Avvio allocazione...")
    run_allocation(clean_sales, clean_stock, shops_cfg, out)

    if args.skip_orders:
        print("\n[STEP 2/3] Modulo ordini saltato (--skip-orders).")
    else:
        if args.orders_source_db:
            print("\n[STEP 2/3] Modulo ordini DB-first: ricostruzione output da database.")
            try:
                ord_summary = export_orders_outputs_from_db(
                    output_dir=out,
                    source_run_id=args.orders_source_db_run_id,
                    verbose=True,
                )
                if ord_summary.get("enabled", False):
                    print(
                        "[STEP 2/3] Modulo ordini DB-first completato: "
                        f"source_run_id={ord_summary.get('source_run_id')}"
                    )
                else:
                    print(
                        "[STEP 2/3] Modulo ordini DB-first senza dati utili: "
                        f"{ord_summary.get('reason', 'unknown')}"
                    )
            except Exception as exc:
                print(f"[STEP 2/3] ERRORE modulo ordini DB-first: {exc}")
                raise SystemExit(1)
        else:
            orders_root = pick_orders_root(root, args.orders_root)
            if orders_root is None:
                print("\n[STEP 2/3] Modulo ordini: nessun input trovato, skip.")
                print("           Cerca in .\\input\\orders\\ oppure usa --orders-root <path>.")
            else:
                print(f"\n[STEP 2/3] Avvio modulo ordini da: {orders_root}")
                run_orders_pipeline(
                    orders_root=orders_root,
                    output_dir=out,
                    fattore_copertura=float(args.orders_coverage),
                    enable_full=not args.orders_math_only,
                    verbose=True,
                )

    if args.sync_db:
        print("\n[STEP 3/3] Avvio sync PostgreSQL...")
        try:
            db_summary = run_db_sync(
                root=root,
                create_schema=bool(args.db_create_schema),
                run_type="app_pipeline",
                verbose=True,
            )
            print(f"[STEP 3/3] DB sync completata. run_id={db_summary.get('run_id')}")
        except Exception as exc:
            print(f"[STEP 3/3] ERRORE sync DB: {exc}")
            raise SystemExit(1)
    else:
        print("\n[STEP 3/3] Sync DB saltata (--sync-db non impostato).")

    print("\nFatto. Output in .\\output\\")
    print(" - clean_sales.csv")
    print(" - clean_articles.csv")
    print(" - alignment_report.csv")
    print(" - ingest/ingest_report_latest.json")
    print(" - ingest/ingest_report_latest.csv")
    print(" - suggested_transfers.csv")
    print(" - suggested_transfers_detailed.csv")
    print(" - shipment_plan.csv")
    print(" - shipment_summary.csv")
    print(" - features_after.csv")
    print(" - demand_diagnostics.csv")
    print(" - orders/orders_summary.json")
    print(" - orders/orders_run_log.txt")
    print(" - DB: etl_run + fact tables (se --sync-db)")

if __name__ == "__main__":
    main()
