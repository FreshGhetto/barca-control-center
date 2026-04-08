from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from barca_control_center.populate_db_from_raw import _pick_orders_root


class PopulateDbFromRawPathTests(unittest.TestCase):
    def test_pick_orders_root_prefers_repo_input_orders(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_orders_root_") as tmp_dir:
            root = Path(tmp_dir)
            repo_orders = root / "input" / "orders"
            repo_orders.mkdir(parents=True, exist_ok=True)

            with mock.patch(
                "barca_control_center.populate_db_from_raw.has_order_inputs",
                side_effect=lambda path: Path(path).resolve() == repo_orders.resolve(),
            ):
                selected = _pick_orders_root(root, None)

        self.assertEqual(selected, repo_orders)

    def test_pick_orders_root_uses_env_fallback_instead_of_machine_specific_path(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_orders_env_") as tmp_dir:
            root = Path(tmp_dir)
            env_orders = root / "external_orders"
            env_orders.mkdir(parents=True, exist_ok=True)

            with mock.patch.dict(os.environ, {"BARCA_ORDERS_ROOT": str(env_orders)}, clear=False):
                with mock.patch(
                    "barca_control_center.populate_db_from_raw.has_order_inputs",
                    side_effect=lambda path: Path(path).resolve() == env_orders.resolve(),
                ):
                    selected = _pick_orders_root(root, None)

        self.assertEqual(selected, env_orders)
