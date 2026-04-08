import argparse
from pathlib import Path

from .allocator_v1 import run_allocation_frames
from .db_sync import run_db_sync
from .db_inputs import load_latest_clean_inputs_from_db
from .db_orders import export_orders_outputs_from_db
from .pipeline_common import harmonize_clean_frames


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

def main():
    args = parse_args()
    root = Path(__file__).resolve().parent.parent
    out = root / "output"
    cfg = root / "config"
    out.mkdir(exist_ok=True)

    shops_cfg = cfg / "lista-negozi_integrato.xlsx"
    if not shops_cfg.exists():
        shops_cfg = cfg / "lista-negozi.xlsx"

    clean_sales = out / "clean_sales.csv"
    clean_stock = out / "clean_articles.csv"

    print("=== BARCA Unified Engine ===")
    if not args.source_db:
        print("[MODE] Pipeline operativa DB-only: input CSV legacy disabilitati, abilito sorgente DB.")
    if not args.skip_orders and not args.orders_source_db:
        print("[MODE] Modulo ordini DB-only: input ordini da file disabilitati, abilito sorgente DB.")

    args.source_db = True
    args.skip_ingest = True
    if not args.skip_orders:
        args.orders_source_db = True

    print("[STEP 0/3] Modalita' DB-only: lettura clean inputs esclusivamente dal database.")
    try:
        db_source = load_latest_clean_inputs_from_db(
            source_run_id=args.source_db_run_id,
            verbose=True,
        )
        sales_df = db_source["sales_df"]
        stock_df = db_source["stock_df"]
        print(
            f"[STEP 0/3] DB source ok: run_id={db_source['source_run_id']}, "
            f"sales={db_source['sales_rows']}, stock={db_source['stock_rows']}"
        )
    except Exception as exc:
        print(f"[STEP 0/3] ERRORE DB source: {exc}")
        raise SystemExit(1)

    sales_df, stock_df, align_report = harmonize_clean_frames(sales_df, stock_df)
    sales_df.to_csv(clean_sales, index=False)
    stock_df.to_csv(clean_stock, index=False)
    align_report.to_csv(out / "alignment_report.csv", index=False)

    print("[STEP 1/3] Inputs DB armonizzati. Avvio allocazione con regole fascia/vendite...")
    alloc_result = run_allocation_frames(sales_df, stock_df, shops_cfg, out, write_outputs=True)

    ord_summary = {"enabled": False, "source": "disabled"}
    orders_source_run_id = None
    orders_sync_enabled = False
    if args.skip_orders:
        print("\n[STEP 2/3] Modulo ordini saltato (--skip-orders).")
    else:
        print("\n[STEP 2/3] Modulo ordini DB-only: ricostruzione output dal database.")
        try:
            ord_summary = export_orders_outputs_from_db(
                output_dir=out,
                source_run_id=args.orders_source_db_run_id,
                verbose=True,
            )
            orders_source_run_id = ord_summary.get("source_run_id")
            orders_sync_enabled = bool(ord_summary.get("enabled", False) or orders_source_run_id)
            if ord_summary.get("enabled", False):
                print(
                    "[STEP 2/3] Modulo ordini DB-first completato: "
                    f"source_run_id={orders_source_run_id}"
                )
            else:
                print(
                    "[STEP 2/3] Modulo ordini DB-first senza dati utili: "
                    f"{ord_summary.get('reason', 'unknown')}"
                )
        except Exception as exc:
            print(f"[STEP 2/3] ERRORE modulo ordini DB-first: {exc}")
            raise SystemExit(1)

    if args.sync_db:
        print("\n[STEP 3/3] Avvio sync PostgreSQL...")
        try:
            db_summary = run_db_sync(
                root=root,
                create_schema=bool(args.db_create_schema),
                run_type="app_pipeline",
                verbose=True,
                clean_sales_df=sales_df,
                clean_stock_df=stock_df,
                transfers_df=alloc_result["transfers"],
                features_df=alloc_result["features"],
                ingest_report={},
                source_sales_stock_run_id=db_source.get("source_run_id"),
                source_orders_run_id=orders_source_run_id if orders_sync_enabled else None,
                include_orders=orders_sync_enabled,
                metadata_extra={
                    "operating_mode": "db_only",
                    "receiver_priority_rule": "fascia_then_local_sales_then_need",
                    "donor_priority_rule": "lower_priority_shop_then_lower_local_sales",
                },
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
    print(" - suggested_transfers.csv")
    print(" - suggested_transfers_detailed.csv")
    print(" - shipment_plan.csv")
    print(" - shipment_summary.csv")
    print(" - features_after.csv")
    print(" - article_shop_signals.csv")
    print(" - stock_integrity_report.csv")
    print(" - demand_diagnostics.csv")
    print(" - orders/orders_summary.json")
    print(" - orders/orders_run_log.txt")
    print(" - DB: etl_run + fact tables (se --sync-db)")

if __name__ == "__main__":
    main()
