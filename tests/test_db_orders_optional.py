from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests._support import apply_schema, patched_env, resolved_db_env, temporary_database


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
