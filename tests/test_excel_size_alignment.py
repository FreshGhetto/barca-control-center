from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook

from barca_control_center.catalog_excel import parse_situazione_articoli_excel
from barca_control_center.parse_data_v2 import parse_articles
from barca_control_center.populate_db_from_shop_reports import parse_shop_stock_report


class ExcelSizeAlignmentTests(unittest.TestCase):
    def test_parse_articles_skips_inline_percent_before_sizes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_size_align_csv_") as tmp_dir:
            root = Path(tmp_dir)
            source = root / "situazione.csv"
            output = root / "clean_articles.csv"
            with source.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ARTICOLO", "", "DESCRIZIONE", "COLORE", "NEG", "GIAC", "CON", "VEN", "% VEN", "35", "36"])
                writer.writerow(["59/SHOPTEST", "", "BALLERINA TEST", "NERO", "AR", "2", "5", "3", "60", "%", "1", "2"])

            df = parse_articles(source, output, valid_codes=["AR"])

            self.assertEqual(len(df), 1)
            self.assertEqual(float(df.iloc[0]["Size_35"]), 1.0)
            self.assertEqual(float(df.iloc[0]["Size_36"]), 2.0)

    def test_shop_history_parser_skips_inline_percent_before_sizes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_size_align_history_") as tmp_dir:
            root = Path(tmp_dir)
            source = root / "25i_donna.csv"
            with source.open("w", encoding="latin1", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["ARTICOLO", "", "DESCRIZIONE", "COLORE", "NEG", "GIAC", "CON", "VEN", "% VEN", "35", "36"])
                writer.writerow(["59/SHOPTEST", "", "BALLERINA TEST", "NERO", "AR", "2", "5", "3", "60", "%", "1", "2"])

            df = parse_shop_stock_report(source, valid_codes=["AR"])

            self.assertEqual(len(df), 1)
            self.assertEqual(float(df.iloc[0]["Size_35"]), 1.0)
            self.assertEqual(float(df.iloc[0]["Size_36"]), 2.0)

    def test_catalog_excel_skips_inline_percent_before_sizes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_size_align_xlsx_") as tmp_dir:
            root = Path(tmp_dir)
            source = root / "catalogo_test.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.title = "Catalogo"
            rows = [
                ["STAGIONE", "25I", "STAGIONE 25I"],
                ["FORNITORE", "ACME SUPPLIER"],
                ["REPARTO", "SCARPE DONNA"],
                ["CATEGORIA", "BALLERINA"],
                ["MARCHIO", "ACME"],
                ["TIPOLOGIA", "CLASSIC"],
                ["ARTICOLO", "", "DESCRIZIONE", "COLORE", "NEG", "GIAC", "CON", "VEN", "% VEN", "35", "36"],
                ["59/SHOPTEST", "", "BALLERINA TEST", "NERO", "AR", 2, 5, 3, 60, "%", 1, 2],
            ]
            for row in rows:
                sheet.append(row)
            workbook.save(source)
            workbook.close()

            df = parse_situazione_articoli_excel(source)

            self.assertEqual(len(df), 1)
            sizes = json.loads(str(df.iloc[0]["sizes_json"]))
            self.assertEqual(float(sizes["35"]), 1.0)
            self.assertEqual(float(sizes["36"]), 2.0)


if __name__ == "__main__":
    unittest.main()
