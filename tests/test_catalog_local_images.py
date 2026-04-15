from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from barca_control_center.catalog_local_images import lookup_local_image_path
from barca_control_center.catalog_showcase_service import _build_local_image_index


class CatalogLocalImageIndexTests(unittest.TestCase):
    def test_requested_season_can_fallback_to_full_local_archive(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_local_images_") as tmp_dir:
            root = Path(tmp_dir) / "photos"
            primary = root / "25_I_FOTOGRAFO_MODIFICATE"
            historical = root / "22_I_FOTOGRAFO_MODIFICATE"
            primary.mkdir(parents=True)
            historical.mkdir(parents=True)
            (historical / "29_679NE.jpg").write_bytes(b"not-a-real-jpeg")

            index, summary, _signature = _build_local_image_index(
                root_dir=root,
                requested_seasons=["25Y"],
                position_name="xl",
                allow_position_variants=True,
                cache_root=Path(tmp_dir) / "cache",
            )

            found = lookup_local_image_path("29/679NE", index)
            self.assertEqual(found, historical / "29_679NE.jpg")
            self.assertEqual(summary["primary_seasons"], 1)
            self.assertGreaterEqual(summary["fallback_seasons"], 1)

    def test_requested_season_keeps_priority_over_archive_fallback(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_local_images_priority_") as tmp_dir:
            root = Path(tmp_dir) / "photos"
            primary = root / "25_I_FOTOGRAFO_MODIFICATE"
            historical = root / "22_I_FOTOGRAFO_MODIFICATE"
            primary.mkdir(parents=True)
            historical.mkdir(parents=True)
            (primary / "29_679NE.jpg").write_bytes(b"primary")
            (historical / "29_679NE.jpg").write_bytes(b"historical")

            index, _summary, _signature = _build_local_image_index(
                root_dir=root,
                requested_seasons=["25Y"],
                position_name="xl",
                allow_position_variants=True,
                cache_root=Path(tmp_dir) / "cache",
            )

            found = lookup_local_image_path("29/679NE", index)
            self.assertEqual(found, primary / "29_679NE.jpg")


if __name__ == "__main__":
    unittest.main()
