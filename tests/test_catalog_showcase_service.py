from __future__ import annotations

import unittest

import pandas as pd

from barca_control_center.catalog_service import (
    _normalize_catalog_store_metadata_from_total,
    _overlay_catalog_kpis_from_order_source,
)
from barca_control_center.catalog_showcase_service import (
    _apply_catalog_article_snapshot_row,
    _catalog_key,
    _finalize_catalog_articles,
)


class CatalogShowcaseServiceTests(unittest.TestCase):
    def test_total_row_overrides_store_metadata_and_kpis(self) -> None:
        articles = {}
        totals = {}

        _apply_catalog_article_snapshot_row(
            articles,
            totals,
            season_code="25Y",
            article_code="49/444TM",
            description="SIM 29/GD031TM CUC CERNIERA T3",
            color="TESTA DI MORO",
            supplier="BASSI SHOES PROJECT",
            reparto="SCARPE DONNA",
            categoria="AN ANFIBIO",
            tipologia="T30 TACCO/30",
            marchio="ANIMA ANDREA",
            store_code="AR",
            giac=-1.0,
            con=91.0,
            ven=140.0,
            perc_ven=11.236,
            source_file="25y_donna.xlsx",
        )
        _apply_catalog_article_snapshot_row(
            articles,
            totals,
            season_code="25Y",
            article_code="49/444TM",
            description="SIM 29/GD031TM CUC CERNIERA T3",
            color="TESTA DI MORO",
            supplier="ANIMA SRL",
            reparto="SCARPE DONNA",
            categoria="ST STIVALE TACCO",
            tipologia="T30 TACCO/30",
            marchio="ANIMA ANDREA",
            store_code="XX",
            giac=162.0,
            con=2279.0,
            ven=2075.0,
            perc_ven=91.0487,
            source_file="25y_donna.xlsx",
        )

        _finalize_catalog_articles(articles, totals)

        article = articles[_catalog_key("25Y", "49/444TM")]
        self.assertEqual(article.categoria, "ST STIVALE TACCO")
        self.assertEqual(article.supplier, "ANIMA SRL")
        self.assertEqual(article.giac, 162.0)
        self.assertEqual(article.con, 2279.0)
        self.assertEqual(article.ven, 2075.0)
        self.assertAlmostEqual(article.perc_ven, 91.0487)
        self.assertIn("AR", article.stores)

    def test_import_normalization_copies_total_metadata_to_store_rows(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "season_code": "25Y",
                    "article_code": "49/444TM",
                    "store_code": "AR",
                    "description": "SIM 29/GD031TM CUC CERNIERA T3",
                    "color": "TESTA DI MORO",
                    "supplier": "BASSI SHOES PROJECT",
                    "reparto": "SCARPE DONNA",
                    "categoria": "AN ANFIBIO",
                    "tipologia": "T30 TACCO/30",
                },
                {
                    "season_code": "25Y",
                    "article_code": "49/444TM",
                    "store_code": "XX",
                    "description": "SIM 29/GD031TM CUC CERNIERA T3",
                    "color": "TESTA DI MORO",
                    "supplier": "ANIMA SRL",
                    "reparto": "SCARPE DONNA",
                    "categoria": "ST STIVALE TACCO",
                    "tipologia": "T30 TACCO/30",
                },
            ]
        )

        normalized = _normalize_catalog_store_metadata_from_total(raw)
        store = normalized[normalized["store_code"] == "AR"].iloc[0]

        self.assertEqual(store["supplier"], "ANIMA SRL")
        self.assertEqual(store["categoria"], "ST STIVALE TACCO")

    def test_import_overlay_uses_order_source_kpis_for_total_rows(self) -> None:
        class FakeCursor:
            def __init__(self, response):
                self.response = response

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params=None):
                self.sql = sql
                self.params = params

            def fetchone(self):
                return self.response

            def fetchall(self):
                return self.response

        class FakeConn:
            def __init__(self):
                self.responses = [
                    [True],
                    [("25Y", "49/444TM", 170, 138, 23)],
                ]

            def cursor(self):
                return FakeCursor(self.responses.pop(0))

        raw = pd.DataFrame(
            [
                {
                    "season_code": "25Y",
                    "article_code": "49/444TM",
                    "store_code": "AR",
                    "con": 10.0,
                    "ven": 8.0,
                    "giac": 2.0,
                    "perc_ven": 80.0,
                },
                {
                    "season_code": "25Y",
                    "article_code": "49/444TM",
                    "store_code": "XX",
                    "con": 2279.0,
                    "ven": 2075.0,
                    "giac": 162.0,
                    "perc_ven": 91.0487,
                },
            ]
        )

        overlaid = _overlay_catalog_kpis_from_order_source(FakeConn(), raw)
        total = overlaid[overlaid["store_code"] == "XX"].iloc[0]
        store = overlaid[overlaid["store_code"] == "AR"].iloc[0]

        self.assertEqual(total["con"], 170.0)
        self.assertEqual(total["ven"], 138.0)
        self.assertEqual(total["giac"], 23.0)
        self.assertAlmostEqual(total["perc_ven"], 81.17647, places=4)
        self.assertEqual(store["con"], 10.0)
        self.assertEqual(store["ven"], 8.0)


if __name__ == "__main__":
    unittest.main()
