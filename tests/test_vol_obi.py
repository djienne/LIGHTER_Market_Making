import math
import unittest

from sortedcontainers import SortedDict

from vol_obi import RollingStats, VolObiCalculator

try:
    from _vol_obi_fast import CBookSide
    _HAS_CBOOKSIDE = True
except ImportError:
    _HAS_CBOOKSIDE = False


# ---------------------------------------------------------------------------
# RollingStats tests
# ---------------------------------------------------------------------------

class TestRollingStats(unittest.TestCase):

    def test_empty_stats(self):
        rs = RollingStats(10)
        self.assertEqual(rs.count, 0)
        self.assertAlmostEqual(rs.mean(), 0.0)
        self.assertAlmostEqual(rs.std(), 0.0)
        self.assertAlmostEqual(rs.zscore(5.0), 0.0)

    def test_single_value(self):
        rs = RollingStats(10)
        rs.push(42.0)
        self.assertEqual(rs.count, 1)
        self.assertAlmostEqual(rs.mean(), 42.0)
        self.assertAlmostEqual(rs.std(), 0.0)
        self.assertAlmostEqual(rs.zscore(42.0), 0.0)

    def test_mean_and_std(self):
        rs = RollingStats(100)
        values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        for v in values:
            rs.push(v)
        self.assertEqual(rs.count, len(values))
        expected_mean = sum(values) / len(values)  # 5.0
        self.assertAlmostEqual(rs.mean(), expected_mean, places=8)
        # Population std
        var = sum((x - expected_mean) ** 2 for x in values) / len(values)
        expected_std = var ** 0.5
        self.assertAlmostEqual(rs.std(), expected_std, places=8)

    def test_zscore(self):
        rs = RollingStats(100)
        values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        for v in values:
            rs.push(v)
        mean = rs.mean()
        std = rs.std()
        # zscore(mean) should be 0
        self.assertAlmostEqual(rs.zscore(mean), 0.0, places=8)
        # zscore(mean + std) should be ~1.0
        self.assertAlmostEqual(rs.zscore(mean + std), 1.0, places=8)

    def test_wrap_around(self):
        """Values beyond capacity should evict the oldest."""
        rs = RollingStats(3)
        rs.push(10.0)
        rs.push(20.0)
        rs.push(30.0)
        # Window is [10, 20, 30], mean=20
        self.assertAlmostEqual(rs.mean(), 20.0, places=8)

        rs.push(40.0)
        # Now window is [20, 30, 40], 10 was evicted
        self.assertEqual(rs.count, 3)
        self.assertAlmostEqual(rs.mean(), 30.0, places=8)

    def test_constant_values_zero_std(self):
        rs = RollingStats(10)
        for _ in range(5):
            rs.push(7.0)
        self.assertAlmostEqual(rs.std(), 0.0, places=10)
        self.assertAlmostEqual(rs.zscore(7.0), 0.0)

    def test_clear(self):
        rs = RollingStats(10)
        for v in [1.0, 2.0, 3.0]:
            rs.push(v)
        rs.clear()
        self.assertEqual(rs.count, 0)
        self.assertAlmostEqual(rs.mean(), 0.0)
        self.assertAlmostEqual(rs.std(), 0.0)


# ---------------------------------------------------------------------------
# VolObiCalculator tests
# ---------------------------------------------------------------------------

def _make_book(bid_price, ask_price, bid_size=1.0, ask_size=1.0):
    """Create simple SortedDict bid/ask books for testing."""
    bids = SortedDict({bid_price: bid_size})
    asks = SortedDict({ask_price: ask_size})
    return bids, asks


def _warm_up_calculator(calc, mid=3000.0, spread=0.1, n=None):
    """Feed enough updates to warm up the calculator."""
    if n is None:
        n = calc._min_warmup_samples + 10
    for i in range(n):
        # Small oscillation to produce non-zero volatility
        offset = spread * (0.5 if i % 2 == 0 else -0.5)
        m = mid + offset
        bids, asks = _make_book(m - 0.05, m + 0.05)
        calc.on_book_update(m, bids, asks)


class TestVolObiCalculator(unittest.TestCase):

    def _make_calc(self, tick_size=0.01, **kw):
        defaults = dict(
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
        defaults.update(kw)
        return VolObiCalculator(tick_size=tick_size, **defaults)

    def test_not_warmed_up_returns_none(self):
        calc = self._make_calc()
        bids, asks = _make_book(3000.0, 3000.1)
        for _ in range(10):
            calc.on_book_update(3000.05, bids, asks)
        bid, ask = calc.quote(3000.05, 0.0)
        self.assertIsNone(bid)
        self.assertIsNone(ask)

    def test_warmup_produces_quotes(self):
        calc = self._make_calc()
        _warm_up_calculator(calc, mid=3000.0, spread=0.2)
        self.assertTrue(calc.warmed_up)
        bid, ask = calc.quote(3000.0, 0.0)
        self.assertIsNotNone(bid)
        self.assertIsNotNone(ask)
        self.assertLess(bid, 3000.0)
        self.assertGreater(ask, 3000.0)

    def test_position_skew_long(self):
        """Long position should shift bid lower, ask lower vs neutral."""
        # Use min_half_spread_bps=0 to avoid the floor clamping the skew
        calc = self._make_calc(min_half_spread_bps=0.0, tick_size=0.001)
        _warm_up_calculator(calc, mid=3000.0, spread=0.5)
        bid_neutral, ask_neutral = calc.quote(3000.0, 0.0)
        bid_long, ask_long = calc.quote(3000.0, 0.15)  # 0.15 * 3000 = $450 position
        self.assertLess(bid_long, bid_neutral)
        self.assertLess(ask_long, ask_neutral)

    def test_position_skew_short(self):
        """Short position should shift bid higher, ask higher vs neutral."""
        calc = self._make_calc(min_half_spread_bps=0.0, tick_size=0.001)
        _warm_up_calculator(calc, mid=3000.0, spread=0.5)
        bid_neutral, ask_neutral = calc.quote(3000.0, 0.0)
        bid_short, ask_short = calc.quote(3000.0, -0.15)
        self.assertGreater(bid_short, bid_neutral)
        self.assertGreater(ask_short, ask_neutral)

    def test_min_half_spread_floor(self):
        """With very small volatility, spread should be at least min_half_spread_bps."""
        calc = self._make_calc(min_half_spread_bps=5.0, min_warmup_samples=50)
        # Feed constant mid to get near-zero volatility
        mid = 3000.0
        bids, asks = _make_book(mid - 0.01, mid + 0.01)
        for _ in range(60):
            calc.on_book_update(mid, bids, asks)
        bid, ask = calc.quote(mid, 0.0)
        self.assertIsNotNone(bid)
        self.assertIsNotNone(ask)
        # min_half_spread_bps=5.0 → at least 5/10000 = 0.05% each side
        min_bid = mid * (1.0 - 5.0 / 10000.0)
        min_ask = mid * (1.0 + 5.0 / 10000.0)
        self.assertLessEqual(bid, min_bid + 0.01)  # allow tick rounding
        self.assertGreaterEqual(ask, min_ask - 0.01)

    def test_reset_clears_warmup(self):
        calc = self._make_calc()
        _warm_up_calculator(calc)
        self.assertTrue(calc.warmed_up)
        self.assertGreater(calc.total_samples, 0)
        calc.reset()
        self.assertFalse(calc.warmed_up)
        self.assertEqual(calc.total_samples, 0)
        bid, ask = calc.quote(3000.0, 0.0)
        self.assertIsNone(bid)
        self.assertIsNone(ask)

    def test_tick_alignment(self):
        """Bid should be floored, ask should be ceiled to tick grid."""
        tick = 0.1
        calc = self._make_calc(tick_size=tick)
        _warm_up_calculator(calc, mid=3000.0, spread=0.3)
        bid, ask = calc.quote(3000.0, 0.0)
        self.assertIsNotNone(bid)
        self.assertIsNotNone(ask)
        # bid = floor(raw/tick)*tick → should be on tick grid
        self.assertAlmostEqual(bid % tick, 0.0, places=10)
        # ask = ceil(raw/tick)*tick → should be on tick grid
        self.assertAlmostEqual(ask % tick, 0.0, places=10)

    def test_vol_scale_matches_rust(self):
        """vol_scale = sqrt(1e9 / step_ns) should match expected value."""
        calc = self._make_calc(step_ns=100_000_000)
        expected = math.sqrt(1_000_000_000 / 100_000_000)  # sqrt(10) ~= 3.1623
        self.assertAlmostEqual(calc.vol_scale, expected, places=4)

    def test_alpha_override_injection(self):
        """set_alpha_override() should override computed alpha; None reverts."""
        calc = self._make_calc()
        _warm_up_calculator(calc, mid=3000.0, spread=0.2)
        self.assertTrue(calc.warmed_up)

        # With override, alpha should be the injected value
        calc.set_alpha_override(3.0)
        bids, asks = _make_book(2999.95, 3000.05)
        calc.on_book_update(3000.0, bids, asks)
        self.assertAlmostEqual(calc.alpha, 3.0)

        # Revert to Lighter-computed alpha
        calc.set_alpha_override(None)
        calc.on_book_update(3000.0, bids, asks)
        self.assertNotAlmostEqual(calc.alpha, 3.0)

    def test_reset_clears_alpha_override(self):
        """reset() should clear the alpha override."""
        calc = self._make_calc()
        calc.set_alpha_override(5.0)
        calc.reset()
        # After reset + warm-up, alpha should be Lighter-computed, not 5.0
        _warm_up_calculator(calc, mid=3000.0, spread=0.2)
        self.assertNotAlmostEqual(calc.alpha, 5.0)


# ---------------------------------------------------------------------------
# VolObiCalculator tests with CBookSide (fast path)
# ---------------------------------------------------------------------------

def _make_book_c(bid_price, ask_price, bid_size=1.0, ask_size=1.0):
    """Create CBookSide bid/ask books for testing the C fast path."""
    bids = CBookSide({bid_price: bid_size})
    asks = CBookSide({ask_price: ask_size})
    return bids, asks


def _warm_up_calculator_c(calc, mid=3000.0, spread=0.1, n=None):
    """Feed enough updates to warm up using CBookSide books."""
    if n is None:
        n = calc._min_warmup_samples + 10
    for i in range(n):
        offset = spread * (0.5 if i % 2 == 0 else -0.5)
        m = mid + offset
        bids, asks = _make_book_c(m - 0.05, m + 0.05)
        calc.on_book_update(m, bids, asks)


@unittest.skipUnless(_HAS_CBOOKSIDE, "CBookSide not available")
class TestVolObiCalculatorCBookSide(unittest.TestCase):
    """Re-run key VolObiCalculator tests with CBookSide to verify C fast path."""

    def _make_calc(self, tick_size=0.01, **kw):
        defaults = dict(
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
        defaults.update(kw)
        return VolObiCalculator(tick_size=tick_size, **defaults)

    def test_warmup_produces_quotes(self):
        calc = self._make_calc()
        _warm_up_calculator_c(calc, mid=3000.0, spread=0.2)
        self.assertTrue(calc.warmed_up)
        bid, ask = calc.quote(3000.0, 0.0)
        self.assertIsNotNone(bid)
        self.assertIsNotNone(ask)
        self.assertLess(bid, 3000.0)
        self.assertGreater(ask, 3000.0)

    def test_position_skew_long(self):
        calc = self._make_calc(min_half_spread_bps=0.0, tick_size=0.001)
        _warm_up_calculator_c(calc, mid=3000.0, spread=0.5)
        bid_neutral, ask_neutral = calc.quote(3000.0, 0.0)
        bid_long, ask_long = calc.quote(3000.0, 0.15)
        self.assertLess(bid_long, bid_neutral)
        self.assertLess(ask_long, ask_neutral)

    def test_position_skew_short(self):
        calc = self._make_calc(min_half_spread_bps=0.0, tick_size=0.001)
        _warm_up_calculator_c(calc, mid=3000.0, spread=0.5)
        bid_neutral, ask_neutral = calc.quote(3000.0, 0.0)
        bid_short, ask_short = calc.quote(3000.0, -0.15)
        self.assertGreater(bid_short, bid_neutral)
        self.assertGreater(ask_short, ask_neutral)

    def test_numeric_equivalence_with_sorteddict(self):
        """CBookSide and SortedDict paths must produce identical results."""
        calc_c = self._make_calc(min_half_spread_bps=0.0, tick_size=0.001)
        calc_s = self._make_calc(min_half_spread_bps=0.0, tick_size=0.001)

        mid = 3000.0
        for i in range(70):
            offset = 0.3 * (0.5 if i % 2 == 0 else -0.5)
            m = mid + offset
            bids_c, asks_c = _make_book_c(m - 0.05, m + 0.05)
            bids_s, asks_s = _make_book(m - 0.05, m + 0.05)
            calc_c.on_book_update(m, bids_c, asks_c)
            calc_s.on_book_update(m, bids_s, asks_s)

        self.assertAlmostEqual(calc_c.volatility, calc_s.volatility, places=12)
        self.assertAlmostEqual(calc_c.alpha, calc_s.alpha, places=12)

        bid_c, ask_c = calc_c.quote(mid, 0.01)
        bid_s, ask_s = calc_s.quote(mid, 0.01)
        self.assertAlmostEqual(bid_c, bid_s, places=12)
        self.assertAlmostEqual(ask_c, ask_s, places=12)

    def test_alpha_override_injection(self):
        calc = self._make_calc()
        _warm_up_calculator_c(calc, mid=3000.0, spread=0.2)
        calc.set_alpha_override(3.0)
        bids, asks = _make_book_c(2999.95, 3000.05)
        calc.on_book_update(3000.0, bids, asks)
        self.assertAlmostEqual(calc.alpha, 3.0)
        calc.set_alpha_override(None)
        calc.on_book_update(3000.0, bids, asks)
        self.assertNotAlmostEqual(calc.alpha, 3.0)
