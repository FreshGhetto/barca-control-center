from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ingest_agent import _classify_file, _convert_to_csv, _read_preview, ingest_incoming
from input_formats import describe_known_input_formats
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
    def test_known_input_catalog_contains_core_families(self) -> None:
        rows = describe_known_input_formats()
        kinds = {row["kind"] for row in rows}
        self.assertTrue({"sales_report", "stock_report", "orders_sd_1", "orders_sd_2", "orders_sd_3", "orders_prices"} <= kinds)

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

    def test_classifier_uses_logical_headers_not_only_filename(self) -> None:
        sd1_preview = (
            "ANALISI ARTICOLI, Stagione:25I, REPARTO, CATEGORIA, TIPOLOGIA, BRAND, ARTICOLO, CON., VEND., PERIO"
        )
        prices_preview = (
            "Analisi Listini e Ricarichi, REPARTO, CATEGORIA, PREZZO ACQUIS., PREZZO VENDITA, FASCE PRZ., RIC."
        )

        sd1 = _classify_file(Path("stagione_libera_totali.csv"), sd1_preview)
        prices = _classify_file(Path("pricing_export_variant.csv"), prices_preview)

        self.assertEqual(sd1["kind"], "orders_sd_1")
        self.assertEqual(prices["kind"], "orders_prices")
        self.assertGreater(sd1["confidence"], 0.5)
        self.assertGreater(prices["confidence"], 0.5)

    def test_ingest_report_includes_classification_reasons(self) -> None:
        with tempfile.TemporaryDirectory(prefix="barca_ingest_reasons_") as tmp_dir:
            root = Path(tmp_dir)
            incoming = root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            (incoming / SALES_RAW.name).write_bytes(SALES_RAW.read_bytes())

            summary = ingest_incoming(root=root, incoming_dir=incoming, move_processed=False, verbose=False)

            self.assertEqual(summary["ingested"], 1)
            row = summary["rows"][0]
            self.assertIn("header:", row["reasons"])
            report_csv = root / "output" / "ingest" / "ingest_report_latest.csv"
            report_text = report_csv.read_text(encoding="utf-8")
            self.assertIn("confidence", report_text)
            self.assertIn("reasons", report_text)

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
