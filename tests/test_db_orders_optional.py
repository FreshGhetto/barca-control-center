from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from tests._support import apply_schema, fetch_scalar, patched_env, resolved_db_env, temporary_database


class DbOrdersOptionalTests(unittest.TestCase):
    db_ctx = None
    db_env: dict[str, str]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        base_env = resolved_db_env()
        cls.db_ctx = temporary_database(base_env, prefix="barca_ord_")
        cls.db_env = cls.db_ctx.__enter__()
        apply_schema(cls.db_env)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls.db_ctx is not None:
                cls.db_ctx.__exit__(None, None, None)
        finally:
            super().tearDownClass()

    def test_export_orders_outputs_from_db_is_non_fatal_when_db_has_no_orders(self) -> None:
        from barca_control_center.db_orders import export_orders_outputs_from_db

        with tempfile.TemporaryDirectory(prefix="barca_orders_out_") as tmp_dir:
            output_dir = Path(tmp_dir)
            with patched_env(self.db_env):
                summary = export_orders_outputs_from_db(output_dir=output_dir, verbose=False)

            self.assertFalse(summary["enabled"])
            self.assertEqual(summary["reason"], "no_orders_available")
            self.assertTrue((output_dir / "orders" / "orders_summary.json").exists())
            self.assertTrue((output_dir / "orders" / "orders_run_log.txt").exists())

    def test_run_db_sync_ignores_stale_local_orders_when_summary_is_disabled(self) -> None:
        from barca_control_center.db_sync import run_db_sync

        with tempfile.TemporaryDirectory(prefix="barca_sync_root_") as tmp_dir:
            root = Path(tmp_dir)
            orders_dir = root / "output" / "orders"
            orders_dir.mkdir(parents=True, exist_ok=True)

            (orders_dir / "orders_summary.json").write_text(
                json.dumps(
                    {
                        "enabled": False,
                        "reason": "no_orders_available",
                        "source": "db",
                        "source_run_id": None,
                        "current": {"season": "25i"},
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            pd.DataFrame(
                [
                    {
                        "Codice_Articolo": "59/STALETEST",
                        "Da_Acquistare_Totale": 7,
                        "Predizione_Vendite": 9,
                        "Prezzo_Acquisto": 10,
                        "Budget_Acquisto": 70,
                        "Acquistare_36": 7,
                    }
                ]
            ).to_csv(orders_dir / "orders_current_previsione_math.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "Codice_Articolo": "59/STALETEST",
                        "Categoria": "BALLERINA",
                        "Tipologia": "TEST",
                        "Marchio": "ACME",
                        "Colore": "NERO",
                        "Materiale": "PELLE",
                        "Descrizione": "ARTICOLO STALE",
                        "Venduto_Totale": 5,
                        "Venduto_Periodo": 3,
                        "Giacenza": 2,
                        "Venduto_Extra": 0,
                        "Fascia_Prezzo": "MEDIO",
                        "Prezzo_Listino": 99,
                        "Prezzo_Acquisto": 40,
                        "Prezzo_Vendita": 79,
                        "Venduto_36": 3,
                    }
                ]
            ).to_csv(orders_dir / "orders_current_dati_originali.csv", index=False)

            with patched_env(self.db_env):
                summary = run_db_sync(
                    root=root,
                    create_schema=False,
                    run_type="manual_sync",
                    verbose=False,
                    clean_sales_df=pd.DataFrame(),
                    clean_stock_df=pd.DataFrame(),
                    transfers_df=pd.DataFrame(),
                    features_df=pd.DataFrame(),
                    include_orders=True,
                )

            run_id = summary["run_id"]
            self.assertEqual(summary["counts"]["fact_order_forecast"], 0)
            self.assertEqual(summary["counts"]["fact_order_forecast_size"], 0)
            self.assertEqual(summary["counts"]["fact_order_source"], 0)
            self.assertEqual(summary["counts"]["fact_order_source_size"], 0)
            self.assertEqual(
                int(
                    fetch_scalar(
                        self.db_env,
                        "SELECT COUNT(*) FROM fact_order_forecast WHERE run_id = %s::uuid",
                        (run_id,),
                    )
                    or 0
                ),
                0,
            )
            self.assertEqual(
                int(
                    fetch_scalar(
                        self.db_env,
                        "SELECT COUNT(*) FROM fact_order_source WHERE run_id = %s::uuid",
                        (run_id,),
                    )
                    or 0
                ),
                0,
            )
