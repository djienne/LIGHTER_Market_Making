"""Tests for _dynamic_max_position_dollar()."""

import unittest
from unittest.mock import patch

import market_maker_v2 as mm


class TestDynamicMaxPositionDollar(unittest.TestCase):
    """Verify dynamic position limit formula: (capital * leverage - 2 * NUM_LEVELS * order_usd) * 0.9"""

    def test_normal_case(self):
        """Standard scenario: $500 capital, leverage 2, 0.0002 BTC @ $80k."""
        result = mm._dynamic_max_position_dollar(80000.0, capital=500.0, base_amount=0.0002)
        # (500*2 - 2*NUM_LEVELS*0.0002*80000) * 0.9
        # NUM_LEVELS=2: (1000 - 64) * 0.9 = 842.4
        self.assertAlmostEqual(result, (500*2 - 2*mm.NUM_LEVELS*0.0002*80000) * 0.9, places=1)

    def test_capital_none_returns_zero(self):
        result = mm._dynamic_max_position_dollar(80000.0, capital=None, base_amount=0.0002)
        self.assertEqual(result, 0.0)

    def test_capital_zero_returns_zero(self):
        result = mm._dynamic_max_position_dollar(80000.0, capital=0.0, base_amount=0.0002)
        self.assertEqual(result, 0.0)

    def test_capital_negative_returns_zero(self):
        result = mm._dynamic_max_position_dollar(80000.0, capital=-100.0, base_amount=0.0002)
        self.assertEqual(result, 0.0)

    def test_mid_price_zero_returns_zero(self):
        result = mm._dynamic_max_position_dollar(0.0, capital=500.0, base_amount=0.0002)
        self.assertEqual(result, 0.0)

    def test_mid_price_none_returns_zero(self):
        result = mm._dynamic_max_position_dollar(None, capital=500.0, base_amount=0.0002)
        self.assertEqual(result, 0.0)

    def test_base_amount_none_no_deduction(self):
        """Without base_amount, no order deduction — just capital * leverage * 0.9."""
        result = mm._dynamic_max_position_dollar(80000.0, capital=500.0, base_amount=None)
        # (500*2) * 0.9 = 900.0
        self.assertAlmostEqual(result, 900.0, places=1)

    def test_base_amount_zero_no_deduction(self):
        result = mm._dynamic_max_position_dollar(80000.0, capital=500.0, base_amount=0.0)
        self.assertAlmostEqual(result, 900.0, places=1)

    def test_order_exceeds_capital_returns_zero(self):
        """If 2*order_value > capital*leverage, result should be 0 (not negative)."""
        # base_amount=1.0 @ $80k = $80k per order, 2*$80k = $160k > $1000
        result = mm._dynamic_max_position_dollar(80000.0, capital=500.0, base_amount=1.0)
        self.assertEqual(result, 0.0)

    def test_large_capital(self):
        """Large capital scales correctly."""
        result = mm._dynamic_max_position_dollar(80000.0, capital=100000.0, base_amount=0.01)
        # (100000*2 - 2*NUM_LEVELS*0.01*80000) * 0.9
        expected = (100000*2 - 2*mm.NUM_LEVELS*0.01*80000) * 0.9
        self.assertAlmostEqual(result, expected, places=0)

    def test_leverage_affects_result(self):
        """Result scales with LEVERAGE constant."""
        with patch.object(mm, 'LEVERAGE', 5):
            result = mm._dynamic_max_position_dollar(80000.0, capital=500.0, base_amount=0.0002)
            # (500*5 - 2*NUM_LEVELS*0.0002*80000) * 0.9
            expected = (500*5 - 2*mm.NUM_LEVELS*0.0002*80000) * 0.9
            self.assertAlmostEqual(result, expected, places=1)


if __name__ == "__main__":
    unittest.main()
