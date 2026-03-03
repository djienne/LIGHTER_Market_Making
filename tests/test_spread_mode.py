import unittest

from sortedcontainers import SortedDict

import market_maker_v2 as mm
from _helpers import temp_mm_attrs
from vol_obi import VolObiCalculator


def _make_warmed_calc(tick_size=0.01, mid=3000.0, spread=0.2):
    """Create and warm-up a VolObiCalculator for testing."""
    calc = VolObiCalculator(
        tick_size=tick_size,
        window_steps=200,
        step_ns=100_000_000,
        vol_to_half_spread=0.8,
        min_half_spread_bps=2.0,
        c1_ticks=160.0,
        c1=0.0,
        skew=1.0,
        looking_depth=0.025,
        min_warmup_samples=50,
        max_position_dollar=500.0,
    )
    for i in range(60):
        offset = spread * (0.5 if i % 2 == 0 else -0.5)
        m = mid + offset
        bids = SortedDict({m - 0.05: 1.0})
        asks = SortedDict({m + 0.05: 1.0})
        calc.on_book_update(m, bids, asks)
    return calc


class TestSpreadModeIntegration(unittest.TestCase):

    def test_vol_obi_mode_uses_calculator(self):
        """Warmed calculator should return valid prices."""
        calc = _make_warmed_calc(tick_size=0.01, mid=3000.0)
        self.assertTrue(calc.warmed_up)

        with temp_mm_attrs(
            vol_obi_calc=calc,
        ):
            levels = mm.calculate_order_prices(3000.0, position_size=0.0)
            buy, sell = levels[0]
            self.assertIsNotNone(buy)
            self.assertIsNotNone(sell)
            self.assertLess(buy, 3000.0)
            self.assertGreater(sell, 3000.0)

    def test_vol_obi_not_warmed_returns_none(self):
        """Un-warmed calculator should return (None, None)."""
        calc = VolObiCalculator(
            tick_size=0.01,
            min_warmup_samples=100,
        )
        self.assertFalse(calc.warmed_up)

        with temp_mm_attrs(
            vol_obi_calc=calc,
        ):
            levels = mm.calculate_order_prices(3000.0, position_size=0.0)
            buy, sell = levels[0]
            self.assertIsNone(buy)
            self.assertIsNone(sell)

    def test_binance_alpha_overrides_lighter_obi(self):
        """When alpha override is active, calculator uses the injected alpha."""
        # Use min_half_spread_bps=0 to avoid the floor clamping both to the same price
        calc = VolObiCalculator(
            tick_size=0.001,
            window_steps=200,
            step_ns=100_000_000,
            vol_to_half_spread=0.8,
            min_half_spread_bps=0.0,
            c1_ticks=160.0,
            c1=0.0,
            skew=1.0,
            looking_depth=0.025,
            min_warmup_samples=50,
            max_position_dollar=500.0,
        )
        for i in range(60):
            offset = 0.5 * (0.5 if i % 2 == 0 else -0.5)
            m = 3000.0 + offset
            bids = SortedDict({m - 0.05: 1.0})
            asks = SortedDict({m + 0.05: 1.0})
            calc.on_book_update(m, bids, asks)
        self.assertTrue(calc.warmed_up)

        # Get baseline quote with Lighter OBI alpha
        with temp_mm_attrs(vol_obi_calc=calc):
            levels_base = mm.calculate_order_prices(3000.0, position_size=0.0)
            buy_base, sell_base = levels_base[0]

        # Now inject a strong positive alpha (bid-heavy -> shift fair price up)
        # Must call on_book_update after setting override so self._alpha updates
        calc.set_alpha_override(3.0)
        bids = SortedDict({2999.95: 1.0})
        asks = SortedDict({3000.05: 1.0})
        calc.on_book_update(3000.0, bids, asks)
        with temp_mm_attrs(vol_obi_calc=calc):
            levels_override = mm.calculate_order_prices(3000.0, position_size=0.0)
            buy_override, sell_override = levels_override[0]

        # With positive alpha the fair price is higher, so bids/asks shift up
        self.assertIsNotNone(buy_override)
        self.assertIsNotNone(sell_override)
        self.assertGreater(buy_override, buy_base)
        calc.set_alpha_override(None)  # clean up

    def test_none_override_uses_lighter_obi(self):
        """When alpha override is None, calculator uses Lighter OBI."""
        calc = _make_warmed_calc(tick_size=0.01, mid=3000.0)
        calc.set_alpha_override(None)
        self.assertTrue(calc.warmed_up)

        with temp_mm_attrs(vol_obi_calc=calc):
            levels = mm.calculate_order_prices(3000.0, position_size=0.0)
            buy, sell = levels[0]
            self.assertIsNotNone(buy)
            self.assertIsNotNone(sell)
            self.assertLess(buy, 3000.0)
            self.assertGreater(sell, 3000.0)
