from __future__ import annotations

import compileall
import unittest

from tests._support import ROOT


MODULES = [
    "app",
    "analysis",
    "allocator_v1",
    "catalog_excel",
    "catalog_image_provider",
    "catalog_legacy_imports",
    "catalog_local_images",
    "catalog_models",
    "catalog_price",
    "catalog_service",
    "catalog_showcase",
    "catalog_showcase_service",
    "db_inputs",
    "db_orders",
    "db_sync",
    "enterprise_ui",
    "hybrid_demand",
    "ingest_agent",
    "orders_pipeline",
    "parse_data",
    "parse_data_v2",
    "pipeline_common",
    "populate_db_from_raw",
    "populate_db_from_shop_reports",
    "qa_checks",
    "reparto_sizes",
    "sync_order_detail_history",
]


class StaticSmokeTests(unittest.TestCase):
    def test_compileall_passes(self) -> None:
        self.assertTrue(compileall.compile_dir(str(ROOT), quiet=1, force=True))

    def test_core_modules_import(self) -> None:
        for module_name in MODULES:
            with self.subTest(module=module_name):
                __import__(module_name)

