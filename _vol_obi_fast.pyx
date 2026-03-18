# cython: language_level=3, boundscheck=False, wraparound=False
# cython: cdivision=True, initializedcheck=False
"""Cython-accelerated Volatility + OBI spread calculator.

Drop-in replacement for the pure-Python vol_obi module.
Includes CBookSide — a C-level sorted array that replaces SortedDict
for the orderbook, making _compute_imbalance a pure-C loop.
"""

from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.math cimport sqrt, floor, ceil
from libc.string cimport memmove

import logging

logger = logging.getLogger("vol_obi")

DEF INITIAL_CAPACITY = 512


# ---------------------------------------------------------------------------
# CBookSide — sorted (price, size) array, SortedDict-compatible interface
# ---------------------------------------------------------------------------

cdef class CBookSide:
    """C-level sorted array of (price, size) pairs.

    Drop-in replacement for ``SortedDict`` in orderbook usage.
    Prices are kept in ascending order via binary search + memmove.
    """

    cdef double* _prices
    cdef double* _sizes
    cdef int _count
    cdef int _capacity

    def __cinit__(self, initial=None):
        self._capacity = INITIAL_CAPACITY
        self._prices = <double*>PyMem_Malloc(INITIAL_CAPACITY * sizeof(double))
        self._sizes = <double*>PyMem_Malloc(INITIAL_CAPACITY * sizeof(double))
        if self._prices is NULL or self._sizes is NULL:
            raise MemoryError("Failed to allocate CBookSide")
        self._count = 0

    def __init__(self, initial=None):
        if initial is not None:
            for price in sorted(initial):
                self._c_insert(<double>price, <double>initial[price])

    def __dealloc__(self):
        if self._prices is not NULL:
            PyMem_Free(self._prices)
            self._prices = NULL
        if self._sizes is not NULL:
            PyMem_Free(self._sizes)
            self._sizes = NULL

    # -- C-level internals --

    cdef inline int _bisect_left(self, double price) noexcept nogil:
        """Return index of first element with price >= given price."""
        cdef int lo = 0
        cdef int hi = self._count
        cdef int mid
        while lo < hi:
            mid = (lo + hi) >> 1
            if self._prices[mid] < price:
                lo = mid + 1
            else:
                hi = mid
        return lo

    cdef inline int _bisect_right(self, double price) noexcept nogil:
        """Return index of first element with price > given price."""
        cdef int lo = 0
        cdef int hi = self._count
        cdef int mid
        while lo < hi:
            mid = (lo + hi) >> 1
            if self._prices[mid] <= price:
                lo = mid + 1
            else:
                hi = mid
        return lo

    cdef void _ensure_capacity(self):
        """Double capacity if full."""
        cdef int new_cap
        cdef double* tmp
        if self._count >= self._capacity:
            new_cap = self._capacity * 2
            # Realloc prices first — update self._prices immediately so it stays
            # valid even if the second realloc fails and raises MemoryError.
            tmp = <double*>PyMem_Realloc(self._prices, new_cap * sizeof(double))
            if tmp is NULL:
                raise MemoryError("Failed to grow CBookSide")
            self._prices = tmp
            tmp = <double*>PyMem_Realloc(self._sizes, new_cap * sizeof(double))
            if tmp is NULL:
                raise MemoryError("Failed to grow CBookSide")
            self._sizes = tmp
            self._capacity = new_cap

    cdef void _c_insert(self, double price, double size):
        """Insert or update a (price, size) pair, maintaining sorted order."""
        cdef int idx = self._bisect_left(price)

        # Update existing price level
        if idx < self._count and self._prices[idx] == price:
            self._sizes[idx] = size
            return

        # Insert new level — shift right
        self._ensure_capacity()
        if idx < self._count:
            memmove(&self._prices[idx + 1], &self._prices[idx],
                    (self._count - idx) * sizeof(double))
            memmove(&self._sizes[idx + 1], &self._sizes[idx],
                    (self._count - idx) * sizeof(double))
        self._prices[idx] = price
        self._sizes[idx] = size
        self._count += 1

    cdef int _c_remove(self, double price) noexcept nogil:
        """Remove a price level. Returns 1 if found, 0 if not."""
        cdef int idx = self._bisect_left(price)
        if idx >= self._count or self._prices[idx] != price:
            return 0
        # Shift left
        if idx < self._count - 1:
            memmove(&self._prices[idx], &self._prices[idx + 1],
                    (self._count - idx - 1) * sizeof(double))
            memmove(&self._sizes[idx], &self._sizes[idx + 1],
                    (self._count - idx - 1) * sizeof(double))
        self._count -= 1
        return 1

    # -- Python interface (SortedDict-compatible) --

    def __setitem__(self, price, size):
        self._c_insert(<double>price, <double>size)

    def __getitem__(self, price):
        cdef double p = <double>price
        cdef int idx = self._bisect_left(p)
        if idx < self._count and self._prices[idx] == p:
            return self._sizes[idx]
        raise KeyError(price)

    def __contains__(self, price):
        cdef double p = <double>price
        cdef int idx = self._bisect_left(p)
        return idx < self._count and self._prices[idx] == p

    def __len__(self):
        return self._count

    def __bool__(self):
        return self._count > 0

    def pop(self, price, *args):
        """Remove and return the size at *price*. Supports a default argument."""
        cdef double p = <double>price
        cdef int idx = self._bisect_left(p)
        cdef double val
        if idx < self._count and self._prices[idx] == p:
            val = self._sizes[idx]
            # Shift left
            if idx < self._count - 1:
                memmove(&self._prices[idx], &self._prices[idx + 1],
                        (self._count - idx - 1) * sizeof(double))
                memmove(&self._sizes[idx], &self._sizes[idx + 1],
                        (self._count - idx - 1) * sizeof(double))
            self._count -= 1
            return val
        if args:
            return args[0]
        raise KeyError(price)

    def clear(self):
        self._count = 0

    def peekitem(self, int index=0):
        """Return (price, size) at sorted index. Supports negative indices."""
        if self._count == 0:
            raise IndexError("CBookSide is empty")
        cdef int resolved
        if index >= 0:
            resolved = index
        else:
            resolved = self._count + index
        if resolved < 0 or resolved >= self._count:
            raise IndexError(f"index {index} out of range for CBookSide with {self._count} items")
        return (self._prices[resolved], self._sizes[resolved])

    def items(self):
        """Return list of (price, size) tuples in ascending price order."""
        cdef int i
        return [(self._prices[i], self._sizes[i]) for i in range(self._count)]

    def keys(self):
        """Return list of prices in ascending order."""
        cdef int i
        return [self._prices[i] for i in range(self._count)]

    def values(self):
        """Return list of sizes in ascending price order."""
        cdef int i
        return [self._sizes[i] for i in range(self._count)]


# ---------------------------------------------------------------------------
# RollingStats — O(1) ring-buffer statistics (C-level)
# ---------------------------------------------------------------------------

cdef class RollingStats:
    """Fixed-capacity ring buffer with O(1) incremental mean / std / zscore.

    Uses Welford's online algorithm for numerically stable variance.
    Caches mean and std on every push() so zscore() is a single division.
    """

    cdef double* _buffer
    cdef int _capacity
    cdef int _write_pos
    cdef readonly int _count
    cdef double _sum
    cdef double _m2
    cdef double _cached_mean
    cdef double _cached_std

    def __cinit__(self, int capacity):
        assert capacity > 0
        self._buffer = <double*>PyMem_Malloc(capacity * sizeof(double))
        if self._buffer is NULL:
            raise MemoryError("Failed to allocate RollingStats buffer")
        self._capacity = capacity
        cdef int i
        for i in range(capacity):
            self._buffer[i] = 0.0
        self._write_pos = 0
        self._count = 0
        self._sum = 0.0
        self._m2 = 0.0
        self._cached_mean = 0.0
        self._cached_std = 0.0

    def __dealloc__(self):
        if self._buffer is not NULL:
            PyMem_Free(self._buffer)
            self._buffer = NULL

    # -- C-level hot methods (called from VolObiCalculator with zero dispatch) --

    cdef inline void c_push(self, double value) noexcept nogil:
        cdef int idx = self._write_pos
        cdef int n
        cdef double old, old_mean, new_mean

        # Evict oldest value if buffer is full (reverse Welford update)
        if self._count >= self._capacity:
            old = self._buffer[idx]
            n = self._count
            old_mean = self._cached_mean
            if n > 1:
                new_mean = old_mean + (old_mean - old) / (n - 1)
            else:
                new_mean = 0.0
            self._m2 -= (old - old_mean) * (old - new_mean)
            if self._m2 < 0.0:
                self._m2 = 0.0
            self._sum -= old
            self._count -= 1
            self._cached_mean = new_mean

        # Add new value (forward Welford update)
        self._buffer[idx] = value
        self._sum += value
        self._count += 1
        self._write_pos += 1
        if self._write_pos >= self._capacity:
            self._write_pos = 0
        n = self._count
        old_mean = self._cached_mean
        new_mean = self._sum / n
        self._m2 += (value - old_mean) * (value - new_mean)
        if self._m2 < 0.0:
            self._m2 = 0.0
        self._cached_mean = new_mean
        if n >= 2:
            self._cached_std = sqrt(self._m2 / n)
        else:
            self._cached_std = 0.0

    cdef inline void c_clear(self) noexcept nogil:
        self._write_pos = 0
        self._count = 0
        self._sum = 0.0
        self._m2 = 0.0
        self._cached_mean = 0.0
        self._cached_std = 0.0

    cdef inline double c_mean(self) noexcept nogil:
        return self._cached_mean

    cdef inline double c_std(self) noexcept nogil:
        return self._cached_std

    cdef inline double c_zscore(self, double value) noexcept nogil:
        if self._cached_std < 1e-10:
            return 0.0
        return (value - self._cached_mean) / self._cached_std

    # -- Python-accessible wrappers (for tests and external callers) --

    def push(self, double value):
        self.c_push(value)

    def clear(self):
        self.c_clear()

    @property
    def count(self):
        return self._count

    def mean(self):
        return self._cached_mean

    def std(self):
        return self._cached_std

    def zscore(self, double value):
        return self.c_zscore(value)


# ---------------------------------------------------------------------------
# VolObiCalculator — spread engine (C-level internals)
# ---------------------------------------------------------------------------

cdef class VolObiCalculator:
    """Volatility + OBI spread calculator (port of Rust ObiStrategy).

    Call ``on_book_update`` from the orderbook WS callback (hot path),
    then ``quote`` from the trading loop to obtain bid/ask prices.
    """

    cdef RollingStats _mid_stats
    cdef RollingStats _imb_stats
    cdef double _prev_mid
    cdef bint _has_prev_mid
    cdef double _volatility
    cdef double _alpha
    cdef double _alpha_override
    cdef bint _has_alpha_override
    cdef bint _warmed_up
    cdef int _total_samples

    # config
    cdef double _tick_size
    cdef double _vol_scale
    cdef double _vol_to_half_spread
    cdef double _min_half_spread_bps
    cdef double _c1
    cdef double _skew
    cdef double _looking_depth
    cdef readonly int _min_warmup_samples
    cdef double _max_position_dollar

    def __init__(
        self,
        *,
        double tick_size,
        int window_steps = 6000,
        int step_ns = 100_000_000,
        double vol_to_half_spread = 0.8,
        double min_half_spread_bps = 2.0,
        double c1_ticks = 160.0,
        double c1 = 0.0,
        double skew = 1.0,
        double looking_depth = 0.025,
        int min_warmup_samples = 100,
        double max_position_dollar = 500.0,
    ):
        self._mid_stats = RollingStats(window_steps)
        self._imb_stats = RollingStats(window_steps)
        self._prev_mid = 0.0
        self._has_prev_mid = False
        self._volatility = 0.0
        self._alpha = 0.0
        self._alpha_override = 0.0
        self._has_alpha_override = False
        self._warmed_up = False
        self._total_samples = 0

        if tick_size <= 0:
            raise ValueError(f"tick_size must be positive, got {tick_size}")
        self._tick_size = tick_size
        self._vol_scale = sqrt(1_000_000_000.0 / step_ns)
        self._vol_to_half_spread = vol_to_half_spread
        self._min_half_spread_bps = min_half_spread_bps
        self._c1 = c1 if c1 > 0.0 else c1_ticks * tick_size
        self._skew = skew
        self._looking_depth = looking_depth
        self._min_warmup_samples = min_warmup_samples
        self._max_position_dollar = max_position_dollar

    # ----- hot path: called from on_order_book_update (WS callback) -----

    def on_book_update(self, double mid_price, object bids, object asks):
        """Feed a new mid-price and orderbook sides.

        Args:
            mid_price: Current mid-price in **dollars**.
            bids: CBookSide or SortedDict {price: size} for bids.
            asks: CBookSide or SortedDict {price: size} for asks.
        """
        cdef double change, imbalance, vol_raw

        # 1. Mid-price change  →  volatility   [DOLLARS]
        if self._has_prev_mid:
            change = mid_price - self._prev_mid
            self._mid_stats.c_push(change)
            self._total_samples += 1
        self._prev_mid = mid_price
        self._has_prev_mid = True

        # 2. Order book imbalance  →  alpha     [quantity units]
        imbalance = self._compute_imbalance(mid_price, bids, asks)
        self._imb_stats.c_push(imbalance)

        # 3. Update cached volatility & alpha once warmed up
        if self._total_samples >= self._min_warmup_samples:
            if not self._warmed_up:
                self._warmed_up = True
                logger.info(
                    "Vol+OBI warmed up after %d samples | vol_scale=%.3f",
                    self._total_samples, self._vol_scale,
                )
            vol_raw = self._mid_stats.c_std()
            self._volatility = vol_raw * self._vol_scale
            if self._has_alpha_override:
                self._alpha = self._alpha_override
            else:
                self._alpha = self._imb_stats.c_zscore(imbalance)

    cdef double _compute_imbalance(self, double mid_price, object bids, object asks):
        """Dispatch to C-fast or Python-slow path based on book type."""
        if isinstance(bids, CBookSide) and isinstance(asks, CBookSide):
            return self._compute_imbalance_c(mid_price, <CBookSide>bids, <CBookSide>asks)
        return self._compute_imbalance_py(mid_price, bids, asks)

    cdef double _compute_imbalance_c(self, double mid_price, CBookSide bids, CBookSide asks):
        """Pure-C imbalance: binary search + array summation. No Python objects."""
        cdef double lower = mid_price * (1.0 - self._looking_depth)
        cdef double upper = mid_price * (1.0 + self._looking_depth)
        cdef double sum_bid = 0.0
        cdef double sum_ask = 0.0
        cdef int i, start_idx, end_idx

        # Bids: sum all sizes with price >= lower
        start_idx = bids._bisect_left(lower)
        for i in range(start_idx, bids._count):
            sum_bid += bids._sizes[i]

        # Asks: sum all sizes with price <= upper
        end_idx = asks._bisect_right(upper)
        for i in range(end_idx):
            sum_ask += asks._sizes[i]

        return sum_bid - sum_ask

    cdef double _compute_imbalance_py(self, double mid_price, object bids, object asks):
        """Fallback: iterate SortedDict items (Python objects)."""
        cdef double lower = mid_price * (1.0 - self._looking_depth)
        cdef double upper = mid_price * (1.0 + self._looking_depth)
        cdef double sum_bid = 0.0
        cdef double sum_ask = 0.0
        cdef double price, size

        for price, size in reversed(bids.items()):
            if price < lower:
                break
            sum_bid += size

        for price, size in asks.items():
            if price > upper:
                break
            sum_ask += size

        return sum_bid - sum_ask

    # ----- trading loop: called from calculate_order_prices -----

    def quote(self, double mid_price, double position_size):
        """Calculate bid/ask prices.

        Returns ``(bid_price, ask_price)`` in dollars, or
        ``(None, None)`` if not warmed up.
        """
        if not self._warmed_up:
            return (None, None)

        cdef double tick = self._tick_size
        cdef double half_spread_price, half_spread_tick
        cdef double fair_price, max_pos, norm_pos
        cdef double bid_depth_tick, ask_depth_tick
        cdef double raw_bid, raw_ask, min_bid, min_ask
        cdef double bid_price, ask_price

        # -- half-spread in dollars → ticks --
        half_spread_price = self._volatility * self._vol_to_half_spread
        half_spread_tick = half_spread_price / tick

        # -- fair price --
        fair_price = mid_price + self._c1 * self._alpha

        # -- position skew in ticks --
        max_pos = self._max_position_dollar
        if max_pos > 0.0:
            norm_pos = (position_size * mid_price) / max_pos
            if norm_pos < -1.0:
                norm_pos = -1.0
            elif norm_pos > 1.0:
                norm_pos = 1.0
        else:
            norm_pos = 0.0

        bid_depth_tick = half_spread_tick * (1.0 + self._skew * norm_pos)
        ask_depth_tick = half_spread_tick * (1.0 - self._skew * norm_pos)
        if bid_depth_tick < 0.0:
            bid_depth_tick = 0.0
        if ask_depth_tick < 0.0:
            ask_depth_tick = 0.0

        # Convert depths back to dollars
        raw_bid = fair_price - bid_depth_tick * tick
        raw_ask = fair_price + ask_depth_tick * tick

        # -- min spread floor in bps --
        if self._min_half_spread_bps > 0.0:
            min_bid = mid_price * (1.0 - self._min_half_spread_bps / 10000.0)
            if raw_bid > min_bid:
                raw_bid = min_bid
            min_ask = mid_price * (1.0 + self._min_half_spread_bps / 10000.0)
            if raw_ask < min_ask:
                raw_ask = min_ask

        # -- Snap to tick grid --
        bid_price = floor(raw_bid / tick) * tick
        ask_price = ceil(raw_ask / tick) * tick

        # Guard: never return crossed quotes
        if bid_price >= ask_price:
            return (None, None)

        return (bid_price, ask_price)

    # ----- alpha override (Binance OBI injection) -----

    def set_alpha_override(self, alpha):
        """Set an external alpha value (e.g. from Binance OBI).

        Pass ``None`` to revert to Lighter-computed imbalance.
        """
        if alpha is None:
            self._has_alpha_override = False
        else:
            self._has_alpha_override = True
            self._alpha_override = <double>alpha

    # ----- accessors -----

    @property
    def warmed_up(self):
        return self._warmed_up

    @property
    def volatility(self):
        return self._volatility

    @property
    def alpha(self):
        return self._alpha

    @property
    def total_samples(self):
        return self._total_samples

    @property
    def vol_scale(self):
        return self._vol_scale

    def reset(self):
        """Clear all state. Called on WS reconnect / snapshot."""
        self._mid_stats.c_clear()
        self._imb_stats.c_clear()
        self._prev_mid = 0.0
        self._has_prev_mid = False
        self._volatility = 0.0
        self._alpha = 0.0
        self._has_alpha_override = False
        self._warmed_up = False
        self._total_samples = 0

    def set_max_position_dollar(self, double value):
        """Update the max position dollar limit at runtime."""
        self._max_position_dollar = value if value > 0.0 else 0.0
