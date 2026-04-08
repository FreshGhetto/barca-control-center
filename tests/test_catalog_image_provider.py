from __future__ import annotations

import unittest
from unittest import mock

from barca_control_center.catalog_image_provider import fetch_image_bytes


class CatalogImageProviderTests(unittest.TestCase):
    def test_fetch_image_bytes_delegates_to_package_legacy_provider(self) -> None:
        expected = (b"image-bytes", None)
        with mock.patch(
            "barca_control_center.catalog_legacy_imports.legacy_fetch_image_for_code",
            return_value=expected,
        ) as legacy_fetch:
            result = fetch_image_bytes("59/TESTCODE")

        self.assertEqual(result, expected)
        legacy_fetch.assert_called_once_with("59/TESTCODE")
