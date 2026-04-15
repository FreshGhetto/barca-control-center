from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from barca_control_center.catalog_models import Article
from barca_control_center.catalog_showcase import export_showcase_catalog


class CatalogShowcaseExportTests(unittest.TestCase):
    def test_jpg_export_groups_by_reparto_then_category(self) -> None:
        articles = {
            "25Y||59/UOMOTEST": Article(
                code="59/UOMOTEST",
                season="25Y",
                reparto="SCARPE UOMO",
                categoria="MOCASSINO",
                description="MOCASSINO TEST",
            ),
            "25Y||59/DONNATEST": Article(
                code="59/DONNATEST",
                season="25Y",
                reparto="SCARPE DONNA",
                categoria="BALLERINA",
                description="BALLERINA TEST",
            ),
        }
        codes = ["25Y||59/UOMOTEST", "25Y||59/DONNATEST"]

        with tempfile.TemporaryDirectory(prefix="barca_showcase_export_") as tmp_dir:
            result = export_showcase_catalog(
                output_dir=tmp_dir,
                articles=articles,
                codes=codes,
                export_mode="jpg",
                source_mode="local_only",
                code_to_local_image={},
                fetch_remote_bytes=None,
                title="Catalogo test",
                jpg_layout="minimal",
            )

            self.assertEqual(result["exported_jpg"], 2)
            base = Path(tmp_dir) / "jpg"
            self.assertTrue((base / "UOMO" / "MOCASSINO" / "0001_25Y_59_UOMOTEST.jpg").exists())
            self.assertTrue((base / "DONNA" / "BALLERINA" / "0002_25Y_59_DONNATEST.jpg").exists())


if __name__ == "__main__":
    unittest.main()
