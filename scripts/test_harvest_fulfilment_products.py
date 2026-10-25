#!/usr/bin/env python3
"""Unit tests for the derivation rules in harvest-fulfilment-products.py (offline, stdlib)."""
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("harvest", Path(__file__).with_name("harvest-fulfilment-products.py"))
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)

SMALL = (100, 80, 50)


def cls(name, segs=(), ptype="", dims=SMALL, g=500, temp_path=""):
    return h.classify(name, list(segs), ptype, dims, g, temp_path)


class ClassifyTests(unittest.TestCase):
    def test_oversized_uses_carrier_additional_handling_thresholds(self):
        self.assertIn("Oversized", cls("Sofa", dims=(2000, 900, 850), g=60000)[0])
        self.assertIn("Oversized", cls("Rod", dims=(1300, 20, 20))[0])        # longest > 1219 mm
        self.assertIn("Oversized", cls("Slab", dims=(900, 800, 50))[0])       # second > 762 mm
        self.assertIn("Oversized", cls("Anvil", g=23000)[0])                  # > 22.68 kg
        self.assertNotIn("Oversized", cls("Mug")[0])

    def test_fragile_looks_at_the_category_tail_not_the_whole_path_or_disposables(self):
        self.assertIn("Fragile", cls("Ceramic Vase", ["Home Decor", "Vases"])[0])
        self.assertIn("Fragile", cls("Table Lamp", ["Lighting", "Table Lamps"])[0])
        self.assertNotIn("Fragile", cls("Paper Plates 100-pack", ["Kitchen", "Dinnerware", "Plates"])[0])
        self.assertNotIn("Fragile", cls("Monitor Stand", ["Office", "Monitors"])[0])
        self.assertNotIn("Fragile", cls("Cooking Wine Vinegar", ["Pantry", "Vinegars"])[0])
        self.assertNotIn("Fragile", cls("Sofa", ["Glassware Department", "Living Room", "Sofas"])[0])

    def test_hazmat_comes_from_the_name_only_and_assigns_a_dot_class(self):
        tags, _, dot, ev = cls("AA Lithium Batteries (4-Pack)")
        self.assertEqual((tags, dot), (["Hazmat"], 9))
        self.assertTrue(any("lithium" in e for e in ev))
        self.assertEqual(cls("Hand Sanitizer Gel, 8 fl oz")[2], 3)
        self.assertEqual(cls("Brake Cleaner Aerosol Spray")[2], 2)
        self.assertIsNone(cls("Alcohol-Free Hand Sanitizer")[2])
        self.assertIsNone(cls("Cotton Sheet Set, Bleach Safe")[2])
        self.assertIsNone(cls("Silver Earrings")[2])      # a care note about perfume is in bullets, never read

    def test_temperature_class_from_the_category_path(self):
        self.assertEqual(cls("Nuggets", temp_path="/Categories/Frozen/Meats/Chicken")[1], "Frozen")
        self.assertEqual(cls("Butter", temp_path="/Categories/Dairy, Cheese & Eggs/Butter")[1], "Chilled")
        self.assertEqual(cls("Sundried Tomatoes", temp_path="/Categories/Produce/Dried Fruits & Vegetables")[1], "")
        self.assertEqual(cls("Russet Potatoes", temp_path="/Categories/Produce/Potatoes")[1], "")
        tags = cls("Butter", temp_path="/Categories/Dairy, Cheese & Eggs/Butter")[0]
        self.assertIn("TemperatureSensitive", tags)

    def test_high_value_only_for_the_item_itself_not_accessories(self):
        self.assertIn("HighValue", cls("Laptop", ["Computers", "Laptops"])[0])
        self.assertNotIn("HighValue", cls("Laptop Sleeve", ["Computers", "Laptop Accessories"])[0])
        self.assertIn("HighValue", cls("Gold Ring", ["Jewelry", "Rings"], "FINERING")[0])

    def test_tags_are_unique_and_in_product_master_enum_order(self):
        tags = cls("Glass Vase with Lithium Battery", ["Decor", "Vases"], dims=(1500, 20, 20))[0]
        self.assertEqual(tags, sorted(set(tags), key=["Hazmat", "Fragile", "TemperatureSensitive", "Oversized", "HighValue"].index))


class CleaningTests(unittest.TestCase):
    def test_retailer_name_is_stripped_but_the_product_stays(self):
        self.assertEqual(h.strip_retailer("AmazonBasics Stapler with 1000 Staples - Black"), "Stapler with 1000 Staples - Black")
        self.assertEqual(h.strip_retailer("Amazon Brand - Rivet Sofa, 87\"W"), "Rivet Sofa, 87\"W")
        self.assertNotRegex(h.strip_retailer("Amazon Essentials Men's Polo"), "(?i)amazon")

    def test_density_filter_removes_placeholder_measurements(self):
        self.assertFalse(h.density_ok((50, 50, 25), 454, True))          # a 454 g crab cake in a 5 cm cube
        self.assertTrue(h.density_ok((100, 100, 200), 454, True))        # a pint of milk
        self.assertFalse(h.density_ok((1000, 1000, 1000), 1, False))     # 1 g in a 1 m cube

    def test_limits_follow_product_master_plus_a_weight_plausibility_cap(self):
        self.assertTrue(h.in_limits((300, 200, 100), 1500))
        self.assertFalse(h.in_limits((2, 200, 100), 1500))
        self.assertFalse(h.in_limits((20001, 200, 100), 1500))
        self.assertFalse(h.in_limits((300, 200, 100), 431_094))

    def test_detokenize_wands_names(self):
        self.assertEqual(h.title(h.detokenize("vogan 33 '' single bathroom vanity")), 'Vogan 33" Single Bathroom Vanity')
        self.assertEqual(h.detokenize("all-clad 7 qt . slow cooker"), "all-clad 7 qt. slow cooker")

    def test_stratified_is_deterministic_and_spreads_over_strata(self):
        ps = [{"sku": f"A-{i}", "s": "big"} for i in range(100)] + [{"sku": f"B-{i}", "s": "small"} for i in range(5)]
        a = h.stratified(ps, "s", 20, {})
        b = h.stratified(list(reversed(ps)), "s", 20, {})
        self.assertEqual(sorted(p["sku"] for p in a), sorted(p["sku"] for p in b))
        self.assertEqual(sum(p["s"] == "small" for p in a), 5)       # the small stratum is kept whole
        self.assertGreaterEqual(len(a), 20)


if __name__ == "__main__":
    unittest.main()
