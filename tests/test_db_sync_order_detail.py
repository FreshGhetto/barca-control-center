import csv
import tempfile
import unittest
from pathlib import Path

from barca_control_center.db_sync import _parse_order_detail_report


class OrderDetailReportParserTests(unittest.TestCase):
    def test_detail_report_preserves_consegnato_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "25y_articoli_venduto_periodo.csv"
            row = [
                "ANALISI ARTICOLI",
                "ARTICOLO",
                "ANIMA ANDREA",
                "49/444TM   SIM 29/GD031TM CUC CER",
                "170",
                "138",
                "50",
                "23",
                "81,18",
                "%",
                "TOTALI :",
            ]
            with path.open("w", encoding="utf-8", newline="") as fh:
                csv.writer(fh).writerow(row)

            parsed = _parse_order_detail_report(path)

        self.assertEqual(len(parsed), 1)
        rec = parsed.iloc[0]
        self.assertEqual(rec["Codice_Articolo"], "49/444TM")
        self.assertEqual(float(rec["Consegnato"]), 170.0)
        self.assertEqual(float(rec["Venduto_Totale"]), 138.0)
        self.assertEqual(float(rec["Venduto_Periodo"]), 50.0)
        self.assertEqual(float(rec["Giacenza"]), 23.0)


if __name__ == "__main__":
    unittest.main()
