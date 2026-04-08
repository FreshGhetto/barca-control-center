from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ingest_agent import _convert_to_csv, _read_preview, ingest_incoming
from tests._support import ROOT


SALES_RAW = ROOT / "data" / "raw_original" / "controlling vendite scarpe donna 25i_periodo1.1_4.2.csv"
STOCK_RAW = ROOT / "data" / "raw_original" / "situaz articoli scarpe donna 25i.csv"


class _FakeSheet:
    nrows = 2
    ncols = 3

    def cell_value(self, ridx: int, cidx: int):
        rows = [
            ["A", "B", "C"],
            ["1", "2", "3"],
        ]
        return rows[ridx][cidx]

    def row_values(self, ridx: int):
        rows = [
            ["A", "B", "C"],
            ["1", "2", "3"],
        ]
        return rows[ridx]


class _FakeBook:
    def sheet_by_index(self, _index: int):
        return _FakeSheet()

    def release_resources(self) -> None:
        return None


class IngestAgentTests(unittest.TestCase):
    def test_ingest_classifies_sales_stock_and_quarantine(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_ingest_") as tmp_dir:
            root = Path(tmp_dir)
            incoming = root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            (incoming / SALES_RAW.name).write_bytes(SALES_RAW.read_bytes())
            (incoming / STOCK_RAW.name).write_bytes(STOCK_RAW.read_bytes())
            (incoming / "mistero.txt").write_text("ignora", encoding="utf-8")

            summary = ingest_incoming(root=root, incoming_dir=incoming, move_processed=False, verbose=False)

            self.assertEqual(summary["ingested"], 2)
            self.assertEqual(summary["quarantine"], 0)
            self.assertEqual(summary["errors"], 0)
            sales_targets = [row["target"] for row in summary["rows"] if row["kind"] == "sales_report"]
            stock_targets = [row["target"] for row in summary["rows"] if row["kind"] == "stock_report"]
            self.assertEqual(len(sales_targets), 1)
            self.assertEqual(len(stock_targets), 1)
            self.assertTrue(Path(sales_targets[0]).exists())
            self.assertTrue(Path(stock_targets[0]).exists())

    def test_ingest_quarantines_unknown_supported_file(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_ingest_unknown_") as tmp_dir:
            root = Path(tmp_dir)
            incoming = root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            unknown = incoming / "mistero.csv"
            unknown.write_text("foo,bar\n1,2\n", encoding="utf-8")

            summary = ingest_incoming(root=root, incoming_dir=incoming, move_processed=False, verbose=False)

            self.assertEqual(summary["ingested"], 0)
            self.assertEqual(summary["quarantine"], 1)
            self.assertEqual(summary["errors"], 0)
            row = summary["rows"][0]
            self.assertEqual(row["status"], "quarantine")
            self.assertTrue(Path(row["target"]).exists())

    def test_xls_preview_and_conversion_use_xlrd(self) -> None:
        fake_book = _FakeBook()
        with tempfile.TemporaryDirectory(prefix="barca_ingest_xls_") as tmp_dir:
            dst = Path(tmp_dir) / "sample.csv"
            with mock.patch("ingest_agent.xlrd.open_workbook", return_value=fake_book) as open_book, mock.patch(
                "ingest_agent.openpyxl.load_workbook",
                side_effect=AssertionError("openpyxl non deve essere usato per .xls"),
            ):
                preview = _read_preview(Path("campione.xls"))
                _convert_to_csv(Path("campione.xls"), dst)

            self.assertIn("A,B,C", preview)
            open_book.assert_called()
            with dst.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.reader(handle))
            self.assertEqual(rows, [["A", "B", "C"], ["1", "2", "3"]])
