import unittest

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestGetPositionValueUsd(unittest.TestCase):
    def test_positive_position(self):
        result = mm.get_position_value_usd(2.0, 100.0)
        self.assertAlmostEqual(result, 200.0)

    def test_negative_position_returns_abs(self):
        result = mm.get_position_value_usd(-3.0, 100.0)
        self.assertAlmostEqual(result, 300.0)

    def test_no_mid_price_returns_zero(self):
        result = mm.get_position_value_usd(1.0, None)
        self.assertAlmostEqual(result, 0.0)


class TestPositionLabel(unittest.TestCase):
    def test_long(self):
        self.assertEqual(mm.position_label(1.5), "long")

    def test_short(self):
        self.assertEqual(mm.position_label(-0.1), "short")

    def test_flat(self):
        self.assertEqual(mm.position_label(0.0), "flat")


class TestIsPositionSignificant(unittest.TestCase):
    def test_above_threshold(self):
        with temp_mm_attrs(POSITION_VALUE_THRESHOLD_USD=15.0):
            # 0.1 * 200.0 = $20 > $15
            self.assertTrue(mm.is_position_significant(0.1, 200.0))

    def test_below_threshold(self):
        with temp_mm_attrs(POSITION_VALUE_THRESHOLD_USD=15.0):
            # 0.001 * 100.0 = $0.10 < $15
            self.assertFalse(mm.is_position_significant(0.001, 100.0))

    def test_tiny_position(self):
        with temp_mm_attrs(POSITION_VALUE_THRESHOLD_USD=15.0):
            # abs(1e-12) < 1e-9 guard
            self.assertFalse(mm.is_position_significant(1e-12, 100.0))
