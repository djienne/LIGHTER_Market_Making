import unittest

import market_maker_v2 as mm


class TestPriceChangeBps(unittest.TestCase):
    def test_normal_change(self):
        result = mm.price_change_bps(100.0, 100.5)
        self.assertAlmostEqual(result, 50.0, places=5)

    def test_zero_change(self):
        result = mm.price_change_bps(100.0, 100.0)
        self.assertAlmostEqual(result, 0.0, places=5)

    def test_old_price_none_returns_inf(self):
        result = mm.price_change_bps(None, 100.0)
        self.assertEqual(result, float("inf"))

    def test_old_price_zero_returns_inf(self):
        result = mm.price_change_bps(0.0, 100.0)
        self.assertEqual(result, float("inf"))
