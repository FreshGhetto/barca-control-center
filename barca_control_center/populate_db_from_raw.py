from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from .db_sync import run_db_sync
from .ingest_agent import ingest_incoming
from .orders_pipeline import has_order_inputs, run_orders_pipeline
from .parse_data_v2 import parse_articles, parse_sales
from .pipeline_common import harmonize_clean_frames, load_valid_shop_codes, newest_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap DB BARCA: usa raw CSV/Excel solo per popolare il database."
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Salta classificazione raw in incoming/ e usa i file gia' presenti in input/.",
    )
    parser.add_argument(
        "--incoming-root",
        type=Path,
        default=None,
        help="Cartella incoming dei raw da classificare prima del parse.",
    )
    parser.add_argument(
        "--keep-incoming",
        action="store_true",
        help="Non spostare i raw processati in incoming/_processed.",
    )
    parser.add_argument(
        "--sales-file",
        type=Path,
        default=None,
        help="CSV vendite esplicito da parsare. Se omesso prende l'ultimo input/sales_YYYY-MM.csv.",
    )
    parser.add_argument(
        "--stock-file",
        type=Path,
        default=None,
        help="CSV stock esplicito da parsare. Se omesso prende l'ultimo input/stock_YYYY-MM.csv.",
    )
    parser.add_argument(
        "--snapshot-at",
        type=str,
        default=None,
        help="Timestamp snapshot da salvare nel DB (ISO, opzionale).",
    )
    parser.add_argument(
        "--skip-orders",
        action="store_true",
        help="Non eseguire il parse dei bundle ordini da input/orders.",
    )
    parser.add_argument(
        "--orders-root",
        type=Path,
        default=None,
        help="Root bundle ordini. Default: input/orders, poi BARCA_ORDERS_ROOT se impostata.",
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
        help="Esegue solo metodo matematico nel bootstrap ordini.",
    )
    parser.add_argument(
        "--db-create-schema",
        action="store_true",
        help="Applica db/schema.sql prima della sync.",
    )
    return parser.parse_args()


def _pick_orders_root(root: Path, cli_orders_root: Optional[Path]) -> Optional[Path]:
    candidates = []
    if cli_orders_root is not None:
        candidates.append(cli_orders_root)
    else:
        candidates.append(root / "input" / "orders")
        env_orders_root = os.getenv("BARCA_ORDERS_ROOT")
        if env_orders_root:
            candidates.append(Path(env_orders_root).expanduser())

    for cand in candidates:
        if cand and has_order_inputs(cand):
            return cand
    return None


def _resolve_snapshot(explicit_value: Optional[str]) -> Optional[str]:
    if explicit_value:
        return pd.Timestamp(explicit_value).isoformat()
    return pd.Timestamp.now().isoformat()


def main():
    args = parse_args()
    root = Path(__file__).resolve().parent.parent
    input_dir = root / "input"
    output_dir = root / "output"
    config_dir = root / "config"
    output_dir.mkdir(parents=True, exist_ok=True)

    shops_cfg = config_dir / "lista-negozi_integrato.xlsx"
    if not shops_cfg.exists():
        shops_cfg = config_dir / "lista-negozi.xlsx"

    valid_shop_codes = load_valid_shop_codes(shops_cfg)
    clean_sales = output_dir / "clean_sales.csv"
    clean_stock = output_dir / "clean_articles.csv"
    snapshot_at = _resolve_snapshot(args.snapshot_at)

    print("=== BARCA DB Bootstrap ===")
    print("[STEP 0/4] Popolazione DB da raw. Il runtime operativo resta DB-only.")

    ingest_summary = {}
    if args.skip_ingest:
        print("[STEP 0/4] Ingest saltato (--skip-ingest).")
    else:
        print("[STEP 0/4] Ingest raw -> input/ in corso...")
        ingest_summary = ingest_incoming(
            root=root,
            incoming_dir=args.incoming_root,
            move_processed=not args.keep_incoming,
            verbose=True,
        )

    sales_src = args.sales_file or newest_file(input_dir, "sales")
    stock_src = args.stock_file or newest_file(input_dir, "stock")
    print(f"[STEP 1/4] Parse sales da {sales_src}")
    sales_df = parse_sales(sales_src, clean_sales, valid_codes=valid_shop_codes, snapshot_at=snapshot_at)
    print(f"[STEP 1/4] Parse stock da {stock_src}")
    stock_df = parse_articles(stock_src, clean_stock, valid_codes=valid_shop_codes, snapshot_at=snapshot_at)

    sales_df, stock_df, align_report = harmonize_clean_frames(sales_df, stock_df)
    sales_df.to_csv(clean_sales, index=False)
    stock_df.to_csv(clean_stock, index=False)
    align_report.to_csv(output_dir / "alignment_report.csv", index=False)
    print(
        f"[STEP 2/4] Clean inputs pronti: sales={len(sales_df)} righe, stock={len(stock_df)} righe, "
        f"alignment_events={len(align_report)}"
    )

    orders_synced = False
    if args.skip_orders:
        print("[STEP 3/4] Modulo ordini saltato (--skip-orders).")
    else:
        orders_root = _pick_orders_root(root, args.orders_root)
        if orders_root is None:
            print("[STEP 3/4] Nessun bundle ordini trovato. Proseguo con sync sales/stock only.")
        else:
            print(f"[STEP 3/4] Bootstrap ordini da {orders_root}")
            run_orders_pipeline(
                orders_root=orders_root,
                output_dir=output_dir,
                fattore_copertura=args.orders_coverage,
                enable_full=not args.orders_math_only,
                verbose=True,
            )
            orders_synced = True

    print("[STEP 4/4] Sync PostgreSQL raw -> DB...")
    db_summary = run_db_sync(
        root=root,
        create_schema=bool(args.db_create_schema),
        run_type="raw_input_sync",
        verbose=True,
        clean_sales_df=sales_df,
        clean_stock_df=stock_df,
        transfers_df=pd.DataFrame(),
        features_df=pd.DataFrame(),
        ingest_report=ingest_summary,
        include_orders=orders_synced,
        metadata_extra={
            "operating_mode": "raw_to_db",
            "source_path_mode": "files_only_for_db_population",
            "snapshot_at": snapshot_at,
        },
    )
    print(f"[STEP 4/4] DB sync completata. run_id={db_summary.get('run_id')}")
    print("\nFatto. Ora il motore operativo puo' lavorare solo dal DB con `python app.py --sync-db`.")


if __name__ == "__main__":
    main()
