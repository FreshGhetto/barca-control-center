from __future__ import annotations

import compileall
import unittest

from tests._support import ROOT


MODULES = [
    "app",
    "analysis",
    "barca_control_center.allocator_v1",
    "barca_control_center.catalog_excel",
    "barca_control_center.catalog_image_provider",
    "barca_control_center.catalog_legacy_imports",
    "barca_control_center.catalog_local_images",
    "barca_control_center.catalog_models",
    "barca_control_center.catalog_price",
    "barca_control_center.catalog_service",
    "barca_control_center.catalog_showcase",
    "barca_control_center.catalog_showcase_service",
    "barca_control_center.db_inputs",
    "barca_control_center.db_orders",
    "db_sync",
    "enterprise_ui",
    "barca_control_center.hybrid_demand",
    "barca_control_center.ingest_agent",
    "barca_control_center.input_formats",
    "barca_control_center.orders_pipeline",
    "barca_control_center.parse_data",
    "barca_control_center.parse_data_v2",
    "barca_control_center.pipeline_common",
    "populate_db_from_raw",
    "populate_db_from_shop_reports",
    "qa_checks",
    "barca_control_center.reparto_sizes",
    "sync_order_detail_history",
]


class StaticSmokeTests(unittest.TestCase):
    def test_compileall_passes(self) -> None:
        self.assertTrue(compileall.compile_dir(str(ROOT), quiet=1, force=True))

    def test_core_modules_import(self) -> None:
        for module_name in MODULES:
            with self.subTest(module=module_name):
                __import__(module_name)
