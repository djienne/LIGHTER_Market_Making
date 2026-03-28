"""Volatility + Order Book Imbalance (OBI) spread calculator.

Port of the Rust reference in example_rust/src/strategy/obi.rs and
example_rust/src/strategy/rolling.rs.  All scaling (dollars, ticks,
vol_scale) matches the Rust implementation exactly.

If the Cython extension (_vol_obi_fast) is available, both classes are
imported from C for ~10-20x speedup on the hot path.  Otherwise the
pure-Python fallback below is used transparently.
"""

import math
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RollingStats — O(1) ring-buffer statistics
# ---------------------------------------------------------------------------

class RollingStats:
    """Fixed-capacity ring buffer with O(1) incremental mean / std / zscore.

    Uses Welford's online algorithm for numerically stable variance.
    Caches mean and std on every push() so zscore() is a single division.
    """

    __slots__ = (
        '_buffer', '_capacity', '_write_pos', '_count',
        '_sum', '_m2', '_cached_mean', '_cached_std',
    )

    def __init__(self, capacity: int):
        assert capacity > 0
        self._buffer = [0.0] * capacity
        self._capacity = capacity
        self._write_pos = 0
        self._count = 0
        self._sum = 0.0
        self._m2 = 0.0  # sum of squared differences from mean (Welford)
        self._cached_mean = 0.0
        self._cached_std = 0.0

    # -- mutators --

    def push(self, value: float) -> None:
        idx = self._write_pos % self._capacity

        # Evict oldest value if buffer is full (reverse Welford update)
        if self._count >= self._capacity:
            old = self._buffer[idx]
            n = self._count
            old_mean = self._cached_mean
            new_mean = old_mean + (old_mean - old) / (n - 1) if n > 1 else 0.0
            self._m2 -= (old - old_mean) * (old - new_mean)
            self._m2 = max(0.0, self._m2)
            self._sum -= old
            self._count -= 1
            self._cached_mean = new_mean

        # Add new value (forward Welford update)
        self._buffer[idx] = value
        self._sum += value
        self._count += 1
        self._write_pos += 1
        n = self._count
        old_mean = self._cached_mean
        new_mean = self._sum / n
        self._m2 += (value - old_mean) * (value - new_mean)
        self._m2 = max(0.0, self._m2)
        self._cached_mean = new_mean
        self._cached_std = (self._m2 / n) ** 0.5 if n >= 2 else 0.0

    def clear(self) -> None:
        self._write_pos = 0
        self._count = 0
        self._sum = 0.0
        self._m2 = 0.0
        self._cached_mean = 0.0
        self._cached_std = 0.0

    # -- accessors --

    @property
    def count(self) -> int:
        return self._count

    def mean(self) -> float:
        return self._cached_mean

    def std(self) -> float:
        return self._cached_std

    def zscore(self, value: float) -> float:
        """Z-score of *value* relative to the current window."""
        if self._cached_std < 1e-10:
            return 0.0
        return (value - self._cached_mean) / self._cached_std


# ---------------------------------------------------------------------------
# VolObiCalculator — spread engine
# ---------------------------------------------------------------------------

class VolObiCalculator:
    """Volatility + OBI spread calculator (port of Rust ObiStrategy).

    Call ``on_book_update`` from the orderbook WS callback (hot path),
    then ``quote`` from the trading loop to obtain bid/ask prices.

    Scaling matches ``example_rust/src/strategy/obi.rs`` exactly:
    - mid_price changes are in **dollars**
    - half_spread is computed in dollars, converted to ticks for skew,
      then converted back to dollars for final prices
    - vol_scale = sqrt(1e9 / step_ns)
    - c1 = c1_ticks * tick_size  (if c1 == 0)
    """

    __slots__ = (
        '_mid_stats', '_imb_stats', '_prev_mid',
        '_volatility', '_alpha', '_alpha_override', '_warmed_up', '_total_samples',
        # config
        '_tick_size', '_vol_scale',
        '_vol_to_half_spread', '_min_half_spread_bps',
        '_c1', '_skew', '_looking_depth',
        '_min_warmup_samples', '_max_position_dollar',
    )

    def __init__(
        self,
        *,
        tick_size: float,
        window_steps: int = 6000,
        step_ns: int = 100_000_000,
        vol_to_half_spread: float = 0.8,
        min_half_spread_bps: float = 2.0,
        c1_ticks: float = 160.0,
        c1: float = 0.0,
        skew: float = 1.0,
        looking_depth: float = 0.025,
        min_warmup_samples: int = 100,
        max_position_dollar: float = 500.0,
    ):
        self._mid_stats = RollingStats(window_steps)
        self._imb_stats = RollingStats(window_steps)
        self._prev_mid = None
        self._volatility = 0.0
        self._alpha = 0.0
        self._alpha_override = None
        self._warmed_up = False
        self._total_samples = 0

        if tick_size <= 0:
            raise ValueError(f"tick_size must be positive, got {tick_size}")
        self._tick_size = tick_size
        # vol_scale: converts per-message std to per-second  (Rust config.rs:267)
        self._vol_scale = (1_000_000_000.0 / step_ns) ** 0.5
        self._vol_to_half_spread = vol_to_half_spread
        self._min_half_spread_bps = min_half_spread_bps
        # c1 in dollars  (Rust config.rs:256-263)
        self._c1 = c1 if c1 > 0.0 else c1_ticks * tick_size
        self._skew = skew
        self._looking_depth = looking_depth
        self._min_warmup_samples = min_warmup_samples
        self._max_position_dollar = max_position_dollar

    # ----- hot path: called from on_order_book_update (WS callback) -----

    def on_book_update(self, mid_price: float, bids, asks) -> None:
        """Feed a new mid-price and orderbook sides.

        Args:
            mid_price: Current mid-price in **dollars**.
            bids: SortedDict {price: size} for bids.
            asks: SortedDict {price: size} for asks.
        """
        # 1. Mid-price change  →  volatility   [DOLLARS]
        if self._prev_mid is not None:
            change = mid_price - self._prev_mid          # [DOLLARS]
            self._mid_stats.push(change)
            self._total_samples += 1
        self._prev_mid = mid_price

        # 2. Order book imbalance  →  alpha     [quantity units]
        imbalance = self._compute_imbalance(mid_price, bids, asks)
        self._imb_stats.push(imbalance)

        # 3. Update cached volatility & alpha once warmed up
        if self._total_samples >= self._min_warmup_samples:
            if not self._warmed_up:
                self._warmed_up = True
                logger.info(
                    "Vol+OBI warmed up after %d samples | vol_scale=%.3f",
                    self._total_samples, self._vol_scale,
                )
            vol_raw = self._mid_stats.std()              # [DOLLARS]
            self._volatility = vol_raw * self._vol_scale  # [DOLLARS]  (Rust obi.rs:360)
            if self._alpha_override is not None:
                self._alpha = self._alpha_override
            else:
                self._alpha = self._imb_stats.zscore(imbalance)  # [dimensionless]

    def _compute_imbalance(self, mid_price: float, bids, asks) -> float:
        """Sum bid/ask sizes within looking_depth of mid_price."""
        depth = self._looking_depth
        lower = mid_price * (1.0 - depth)
        upper = mid_price * (1.0 + depth)

        sum_bid = 0.0
        for price, size in reversed(bids.items()):
            if price < lower:
                break
            sum_bid += size

        sum_ask = 0.0
        for price, size in asks.items():
            if price > upper:
                break
            sum_ask += size

        return sum_bid - sum_ask

    # ----- trading loop: called from calculate_order_prices -----

    def quote(self, mid_price: float, position_size: float):
        """Calculate bid/ask prices.

        Returns ``(bid_price, ask_price)`` in dollars, or
        ``(None, None)`` if not warmed up.
        """
        if not self._warmed_up:
            return None, None

        tick = self._tick_size

        # -- STEP 2: half-spread in dollars → ticks  (Rust obi.rs:440) --
        half_spread_price = self._volatility * self._vol_to_half_spread  # [DOLLARS]
        half_spread_tick = half_spread_price / tick                       # [TICKS]

        # -- STEP 4: fair price  (Rust obi.rs:455) --
        fair_price = mid_price + self._c1 * self._alpha                  # [DOLLARS]

        # -- STEP 5: position skew in ticks  (Rust obi.rs:466-502) --
        max_pos = self._max_position_dollar
        if max_pos > 0:
            norm_pos = (position_size * mid_price) / max_pos
            norm_pos = max(-1.0, min(1.0, norm_pos))
        else:
            norm_pos = 0.0

        bid_depth_tick = half_spread_tick * (1.0 + self._skew * norm_pos)  # [TICKS]
        ask_depth_tick = half_spread_tick * (1.0 - self._skew * norm_pos)  # [TICKS]
        bid_depth_tick = max(0.0, bid_depth_tick)
        ask_depth_tick = max(0.0, ask_depth_tick)

        # Convert depths back to dollars
        raw_bid = fair_price - bid_depth_tick * tick   # [DOLLARS]
        raw_ask = fair_price + ask_depth_tick * tick   # [DOLLARS]

        # -- STEP 6: min spread floor in bps  (Rust obi.rs:514-537) --
        if self._min_half_spread_bps > 0.0:
            min_bid = mid_price * (1.0 - self._min_half_spread_bps / 10000.0)
            if raw_bid > min_bid:
                raw_bid = min_bid
            min_ask = mid_price * (1.0 + self._min_half_spread_bps / 10000.0)
            if raw_ask < min_ask:
                raw_ask = min_ask

        # -- Snap to tick grid  (Rust obi.rs:bid_prices[level]) --
        bid_price = math.floor(raw_bid / tick) * tick   # [DOLLARS]
        ask_price = math.ceil(raw_ask / tick) * tick     # [DOLLARS]

        # Guard: never return crossed quotes
        if bid_price >= ask_price:
            return None, None

        return bid_price, ask_price

    # ----- alpha override (Binance OBI injection) -----

    def set_alpha_override(self, alpha) -> None:
        """Set an external alpha value (e.g. from Binance OBI).

        Pass ``None`` to revert to Lighter-computed imbalance.
        """
        self._alpha_override = alpha

    # ----- accessors -----

    @property
    def warmed_up(self) -> bool:
        return self._warmed_up

    @property
    def volatility(self) -> float:
        return self._volatility

    @property
    def alpha(self) -> float:
        return self._alpha

    @property
    def total_samples(self) -> int:
        return self._total_samples

    @property
    def vol_scale(self) -> float:
        return self._vol_scale

    def reset(self) -> None:
        """Clear all state. Called on WS reconnect / snapshot."""
        self._mid_stats.clear()
        self._imb_stats.clear()
        self._prev_mid = None
        self._volatility = 0.0
        self._alpha = 0.0
        self._alpha_override = None
        self._warmed_up = False
        self._total_samples = 0

    def set_max_position_dollar(self, value: float) -> None:
        """Update the max position dollar limit at runtime."""
        self._max_position_dollar = max(0.0, value)


# ---------------------------------------------------------------------------
# Try to replace with Cython-accelerated versions
# ---------------------------------------------------------------------------
try:
    from _vol_obi_fast import RollingStats, VolObiCalculator, CBookSide  # noqa: F811
except ImportError:
    from sortedcontainers import SortedDict as CBookSide  # noqa: F811
    logger.warning(
        "Cython VolObiCalculator not available — using pure-Python fallback. "
        "Build with: python setup_cython.py build_ext --inplace"
    )
