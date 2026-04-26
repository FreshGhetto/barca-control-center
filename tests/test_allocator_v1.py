from __future__ import annotations

import unittest

from barca_control_center.allocator_v1 import (
    ENABLE_OPS_MOVE_BUDGET,
    ENABLE_TOP_UP_TO_TARGET,
    WAREHOUSE,
    ONLINE,
    ops_budget_allows_move,
    prioritized_receivers_for_broken_line,
    prioritized_donors_for_size,
    should_reallocate_broken_line,
)


class AllocatorV1Tests(unittest.TestCase):
    def test_web_receives_from_m4_then_lower_priority_duplicate_donors(self) -> None:
        meta = {
            WAREHOUSE: {"Fascia": None},
            ONLINE: {"Fascia": None},
            "S5": {"Fascia": 5},
            "S4": {"Fascia": 4},
            "S2": {"Fascia": 2},
        }
        article = "ART1"
        shops_for_article = [WAREHOUSE, ONLINE, "S5", "S4", "S2"]
        stock = {
            (article, WAREHOUSE): {38: 5.0},
            (article, ONLINE): {38: 0.0},
            (article, "S5"): {37: 1.0, 38: 2.0, 39: 1.0},
            (article, "S4"): {37: 1.0, 38: 2.0, 39: 1.0},
            (article, "S2"): {37: 0.0, 38: 1.0, 39: 0.0},
        }
        total = {
            (article, WAREHOUSE): 5.0,
            (article, ONLINE): 0.0,
            (article, "S5"): 4.0,
            (article, "S4"): 4.0,
            (article, "S2"): 1.0,
        }
        demand = {}
        sales_signal = {
            (article, "S5"): 0.0,
            (article, "S4"): 2.0,
            (article, "S2"): 1.0,
        }
        signal_lookup = {
            (article, "S5"): {"NotebookVendutoSignal": 0.0, "SourcePriorityScore": 100.0},
            (article, "S4"): {"NotebookVendutoSignal": 2.0, "SourcePriorityScore": 90.0},
            (article, "S2"): {"NotebookVendutoSignal": 1.0, "SourcePriorityScore": 50.0},
        }

        donors = prioritized_donors_for_size(
            meta,
            stock,
            total,
            demand,
            sales_signal,
            signal_lookup,
            article,
            ONLINE,
            38,
            shops_for_article,
            reparto="SCARPE DONNA",
            available_sizes=[37, 38, 39],
        )

        self.assertEqual([item["shop"] for item in donors[:3]], [WAREHOUSE, "S5", "S4"])
        self.assertEqual([item["stage"] for item in donors[:3]], ["warehouse", "duplicate", "duplicate"])

    def test_ops_budget_logic_kept_but_disabled(self) -> None:
        self.assertFalse(ENABLE_OPS_MOVE_BUDGET)
        self.assertTrue(ops_budget_allows_move("S1", {"S1": 999.0}, {"S1": 0.0}))
        self.assertFalse(ENABLE_TOP_UP_TO_TARGET)

    def test_broken_line_feeds_missing_sizes_before_outlet(self) -> None:
        meta = {
            "AR": {"Fascia": 1},
            "EU": {"Fascia": 1},
            "BO": {"Fascia": 2},
            "SD": {"Fascia": 7},
            WAREHOUSE: {"Fascia": None},
            ONLINE: {"Fascia": None},
        }
        article = "ART2"
        shops_for_article = ["AR", "EU", "BO", "SD", WAREHOUSE, ONLINE]
        stock = {
            (article, "AR"): {36: 1.0, 37: 2.0, 38: 4.0, 39: 2.0, 40: 0.0, 41: 1.0},
            (article, "EU"): {36: 0.0, 37: 3.0, 38: 2.0, 39: 2.0, 40: 1.0, 41: 0.0},
            (article, "BO"): {36: 0.0, 37: 2.0, 38: 3.0, 39: 1.0, 40: 1.0, 41: 0.0},
            (article, "SD"): {37: 0.0, 38: 0.0, 39: 0.0, 40: 0.0, 41: 0.0},
            (article, WAREHOUSE): {37: 0.0, 38: 0.0, 39: 0.0, 40: 0.0, 41: 0.0},
            (article, ONLINE): {37: 0.0, 38: 0.0, 39: 0.0, 40: 0.0, 41: 0.0},
        }
        total = {
            (article, "AR"): 10.0,
            (article, "EU"): 8.0,
            (article, "BO"): 7.0,
            (article, "SD"): 0.0,
            (article, WAREHOUSE): 0.0,
            (article, ONLINE): 0.0,
        }
        demand = {
            (article, "EU"): 3.0,
            (article, "BO"): 5.0,
        }
        sales_signal = {
            (article, "EU"): 2.0,
            (article, "BO"): 5.0,
        }
        signal_lookup = {
            (article, "EU"): {"NotebookVendutoSignal": 2.0, "DestinationPriorityScore": 220.0},
            (article, "BO"): {"NotebookVendutoSignal": 5.0, "DestinationPriorityScore": 520.0},
        }
        shop_total_stock = {"AR": 9.0, "EU": 8.0, "BO": 7.0, "SD": 0.0, WAREHOUSE: 0.0, ONLINE: 0.0}
        shop_capacity_target = {"AR": 100.0, "EU": 100.0, "BO": 100.0, "SD": 100.0, WAREHOUSE: float("inf"), ONLINE: 100.0}

        receivers = prioritized_receivers_for_broken_line(
            meta,
            stock,
            total,
            demand,
            sales_signal,
            signal_lookup,
            article,
            "AR",
            shops_for_article,
            reparto="SCARPE DONNA",
            available_sizes=[36, 37, 38, 39, 40, 41],
            shop_total_stock=shop_total_stock,
            shop_capacity_target=shop_capacity_target,
        )

        self.assertEqual(receivers[0]["shop"], "EU")
        self.assertEqual(receivers[0]["missing_sizes"], [36])

    def test_active_broken_store_is_not_used_as_donor(self) -> None:
        meta = {
            "AR": {"Fascia": 1},
            "EU": {"Fascia": 1},
            WAREHOUSE: {"Fascia": None},
            ONLINE: {"Fascia": None},
        }
        article = "ART3"
        shops_for_article = ["AR", "EU", WAREHOUSE, ONLINE]
        stock = {
            (article, "AR"): {36: 0.0, 37: 1.0, 38: 1.0, 39: 2.0, 40: 1.0},
            (article, "EU"): {36: 1.0, 37: 1.0, 38: 1.0, 39: 0.0, 40: 1.0},
            (article, WAREHOUSE): {39: 0.0},
            (article, ONLINE): {39: 0.0},
        }
        total = {
            (article, "AR"): 5.0,
            (article, "EU"): 4.0,
            (article, WAREHOUSE): 0.0,
            (article, ONLINE): 0.0,
        }
        demand = {
            (article, "AR"): 3.0,
            (article, "EU"): 3.0,
        }
        sales_signal = {
            (article, "AR"): 6.0,
            (article, "EU"): 4.0,
        }
        signal_lookup = {
            (article, "AR"): {"NotebookVendutoSignal": 6.0, "SourcePriorityScore": 120.0},
            (article, "EU"): {"NotebookVendutoSignal": 4.0, "SourcePriorityScore": 90.0},
        }

        donors = prioritized_donors_for_size(
            meta,
            stock,
            total,
            demand,
            sales_signal,
            signal_lookup,
            article,
            "EU",
            39,
            shops_for_article,
            reparto="SCARPE DONNA",
            available_sizes=[36, 37, 38, 39, 40],
        )

        self.assertNotIn("AR", [item["shop"] for item in donors])

    def test_only_zero_sales_store_can_be_reallocated_as_broken_line(self) -> None:
        meta = {
            "AR": {"Fascia": 1},
            "TV": {"Fascia": 5},
        }
        article = "ART4"
        sales_signal = {
            (article, "AR"): 5.0,
            (article, "TV"): 0.0,
        }
        signal_lookup = {
            (article, "AR"): {"NotebookVendutoSignal": 5.0},
            (article, "TV"): {"NotebookVendutoSignal": 0.0},
        }

        self.assertFalse(should_reallocate_broken_line(meta, sales_signal, signal_lookup, article, "AR"))
        self.assertTrue(should_reallocate_broken_line(meta, sales_signal, signal_lookup, article, "TV"))


if __name__ == "__main__":
    unittest.main()
