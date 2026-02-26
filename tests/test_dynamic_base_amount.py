import unittest

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestCalculateDynamicBaseAmount(unittest.TestCase):
    def test_calculate_dynamic_base_amount(self):
        with temp_mm_attrs(available_capital=1000.0, _AMOUNT_TICK_FLOAT=0.01):
            result = mm.calculate_dynamic_base_amount(200.0)
            self.assertAlmostEqual(float(result), 0.6, places=8)

            mm.available_capital = None
            fallback = mm.calculate_dynamic_base_amount(200.0)
            self.assertEqual(fallback, mm.BASE_AMOUNT)

    def test_zero_price_returns_none(self):
        with temp_mm_attrs(available_capital=1000.0, _AMOUNT_TICK_FLOAT=0.01):
            result = mm.calculate_dynamic_base_amount(0)
            self.assertIsNone(result)

    def test_large_capital_tick_rounding(self):
        with temp_mm_attrs(available_capital=100000.0, _AMOUNT_TICK_FLOAT=0.001):
            result = mm.calculate_dynamic_base_amount(3000.0)
            self.assertIsNotNone(result)
            # Check that result is a multiple of tick size
            remainder = result / 0.001
            self.assertAlmostEqual(remainder, round(remainder), places=5)
