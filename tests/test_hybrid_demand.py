from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from barca_control_center.hybrid_demand import compute_hybrid_demand


class HybridDemandTests(unittest.TestCase):
    def test_force_formula_only_disables_ai_blend(self) -> None:
        sales_rows = []
        article_rows = []
        meta = {}
        shops = [f"S{idx:03d}" for idx in range(1, 13)]
        articles = [f"A{idx:03d}" for idx in range(1, 13)]

        for shop_idx, shop in enumerate(shops, start=1):
            meta[shop] = {"Fascia": float((shop_idx % 5) + 1)}
            for article_idx, article in enumerate(articles, start=1):
                periodo = float((article_idx * 2 + shop_idx) % 9)
                venduto = float((article_idx + shop_idx * 2) % 7)
                consegnato = max(periodo + venduto + 2.0, 1.0)
                sellout = min(95.0, 18.0 + article_idx * 3.0 + shop_idx)
                sales_rows.append(
                    {
                        "Article": article,
                        "Shop": shop,
                        "Periodo_Qty": periodo,
                        "Venduto_Qty": venduto,
                        "Consegnato_Qty": consegnato,
                        "Sellout_Clamped": sellout,
                        "Sellout_Percent": sellout,
                    }
                )
                stock_base = float((article_idx + shop_idx) % 6)
                article_rows.append(
                    {
                        "Article": article,
                        "Shop": shop,
                        "Size_37": stock_base + 1.0,
                        "Size_38": stock_base + 2.0,
                        "Size_39": stock_base + 1.0,
                    }
                )

        sales_df = pd.DataFrame(sales_rows)
        articles_df = pd.DataFrame(article_rows)

        demand, diagnostics = compute_hybrid_demand(
            sales_df,
            articles_df,
            meta,
            force_formula_only=True,
        )

        self.assertFalse(diagnostics.empty)
        self.assertTrue((diagnostics["DemandBlendWeight"] == 0.0).all())
        self.assertTrue((diagnostics["DemandModelMode"] == "formula_only").all())
        self.assertTrue((diagnostics["DemandAI"] == 0.0).all())
        self.assertTrue(
            np.allclose(
                diagnostics["DemandHybrid"].to_numpy(dtype=float),
                np.maximum(
                    diagnostics["DemandRule"].to_numpy(dtype=float),
                    np.where(
                        (diagnostics["Periodo_Qty"] + diagnostics["Venduto_Qty"]).to_numpy(dtype=float) > 0.0,
                        1.0,
                        0.0,
                    ),
                ),
            )
        )
        sample = diagnostics.iloc[0]
        self.assertAlmostEqual(
            float(demand[(sample["Article"], sample["Shop"])]),
            float(sample["DemandHybrid"]),
            places=6,
        )


if __name__ == "__main__":
    unittest.main()
