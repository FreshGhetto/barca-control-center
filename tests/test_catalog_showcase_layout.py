from __future__ import annotations

import unittest

from barca_control_center.catalog_models import Article
from barca_control_center.catalog_showcase import render_showcase_jpg


class CatalogShowcaseLayoutTests(unittest.TestCase):
    def test_minimal_default_canvas_matches_a6_for_four_up_a4_print(self) -> None:
        article = Article(
            code="59/TESTPNE",
            season="25I",
            description="STIVALE TEST",
            color="NERO",
            categoria="STIVALE",
            marchio="ACME",
            supplier="FORNITORE TEST",
            con=12,
            ven=9,
            giac=3,
            perc_ven=75,
        )

        image = render_showcase_jpg(article, None, layout="minimal")

        self.assertEqual(image.size, (1240, 1748))

    def test_minimal_layout_keeps_canvas_with_supplier_and_brand_text(self) -> None:
        article = Article(
            code="59/TESTPNE",
            season="25I",
            description="STIVALE TEST",
            color="NERO",
            reparto="DONNA",
            categoria="CATEGORIA MOLTO LUNGA DI PROVA",
            marchio="MARCHIO MOLTO LUNGO DI PROVA",
            supplier="FORNITORE MOLTO LUNGO DI PROVA",
            con=12,
            ven=9,
            giac=3,
            perc_ven=75,
        )

        image = render_showcase_jpg(article, None, layout="minimal")

        self.assertEqual(image.size, (1240, 1748))

    def test_minimal_layout_keeps_canvas_with_only_category_supplier_and_brand(self) -> None:
        article = Article(
            code="59/TESTPNE",
            season="25I",
            reparto="TENNIS UNISEX",
            categoria="SNEAKER",
            marchio="ACME",
            supplier="FORNITORE TEST",
            con=12,
            ven=9,
            giac=3,
            perc_ven=75,
        )

        image = render_showcase_jpg(article, None, layout="minimal")

        self.assertEqual(image.size, (1240, 1748))

    def test_detailed_default_canvas_remains_a4(self) -> None:
        article = Article(code="59/TESTPNE", season="25I")

        image = render_showcase_jpg(article, None, layout="detailed")

        self.assertEqual(image.size, (1748, 2480))


if __name__ == "__main__":
    unittest.main()
