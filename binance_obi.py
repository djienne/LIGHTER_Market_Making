"""Binance Futures OBI (Order Book Imbalance) alpha signal.

Connects to the Binance Futures partial-depth WebSocket stream
(@depth20@100ms), computes a rolling z-scored imbalance, and publishes
it to a SharedAlpha container that the market maker hot path reads.

Components:
    SharedAlpha              – thread-safe container (written by Binance, read on hot path)
    lighter_to_binance_symbol – maps Lighter symbol → Binance Futures symbol
    BinanceObiClient         – reconnecting async WebSocket client
"""

import asyncio
import logging
import time

try:
    import orjson as _json
    _loads = _json.loads
except ImportError:
    import json as _json
    _loads = _json.loads

from vol_obi import RollingStats

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Symbol mapping
# ---------------------------------------------------------------------------

_LIGHTER_TO_BINANCE = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "PAXG": "paxgusdt",
    "CRV": "crvusdt",
    "SOL": "solusdt",
    "ASTER": None,       # No Binance Futures market
}


def lighter_to_binance_symbol(symbol: str):
    """Map a Lighter symbol to its Binance Futures equivalent.

    Returns ``None`` if the symbol has no Binance counterpart (e.g. ASTER).
    Falls back to ``f"{symbol.lower()}usdt"`` for unknown symbols.
    """
    key = symbol.upper()
    if key in _LIGHTER_TO_BINANCE:
        return _LIGHTER_TO_BINANCE[key]
    # Heuristic fallback for unknown symbols
    return f"{key.lower()}usdt"


# ---------------------------------------------------------------------------
# SharedAlpha — lock-free(ish) container
# ---------------------------------------------------------------------------

class SharedAlpha:
    """Shared container written by the Binance task, read on the hot path.

    In CPython the GIL makes attribute writes atomic for simple types,
    so no explicit locking is needed.
    """

    __slots__ = ('alpha', 'last_update_time', 'sample_count',
                 '_warmed_up', '_min_samples')

    def __init__(self, *, min_samples: int = 150):
        self.alpha: float = 0.0
        self.last_update_time: float = 0.0   # time.monotonic()
        self.sample_count: int = 0
        self._warmed_up: bool = False
        self._min_samples: int = min_samples

    def update(self, alpha: float) -> None:
        """Publish a new alpha value (called by BinanceObiClient)."""
        self.alpha = alpha
        self.last_update_time = time.monotonic()
        self.sample_count += 1
        if not self._warmed_up and self.sample_count >= self._min_samples:
            self._warmed_up = True

    def is_stale(self, threshold_seconds: float = 5.0) -> bool:
        """True if the last update is older than *threshold_seconds*."""
        if self.last_update_time == 0.0:
            return True
        return (time.monotonic() - self.last_update_time) > threshold_seconds

    @property
    def warmed_up(self) -> bool:
        return self._warmed_up

    def reset(self) -> None:
        """Clear all state (called on WS reconnect)."""
        self.alpha = 0.0
        self.last_update_time = 0.0
        self.sample_count = 0
        self._warmed_up = False


# ---------------------------------------------------------------------------
# BinanceObiClient — reconnecting WebSocket client
# ---------------------------------------------------------------------------

BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"


class BinanceObiClient:
    """Streams Binance Futures partial depth and computes OBI alpha.

    Uses the ``@depth20@100ms`` stream (top 20 levels every 100ms).
    No REST snapshot or sequence tracking needed.
    """

    def __init__(
        self,
        binance_symbol: str,
        shared_alpha: SharedAlpha,
        *,
        window_size: int = 300,
        looking_depth: float = 0.025,
        stale_threshold: float = 5.0,
        reconnect_base: float = 2.0,
        reconnect_max: float = 60.0,
    ):
        self._symbol = binance_symbol.lower()
        self._shared_alpha = shared_alpha
        self._imb_stats = RollingStats(window_size)
        self._looking_depth = looking_depth
        self._stale_threshold = stale_threshold
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max

    @property
    def url(self) -> str:
        return f"{BINANCE_FUTURES_WS}/{self._symbol}@depth20@100ms"

    # -- public entry point --

    async def run(self) -> None:
        """Connect, stream, and reconnect forever (until cancelled)."""
        import websockets

        backoff = self._reconnect_base

        while True:
            try:
                self._shared_alpha.reset()
                self._imb_stats.clear()

                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    logger.info("Binance OBI connected: %s", self.url)
                    backoff = self._reconnect_base

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            logger.warning("Binance OBI: no data for 30s, reconnecting…")
                            break

                        self._on_message(raw)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.warning("Binance OBI error: %s — reconnecting in %.0fs", e, backoff)

            self._shared_alpha.reset()
            self._imb_stats.clear()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._reconnect_max)

    # -- message handler (synchronous, called from recv loop) --

    def _on_message(self, raw) -> None:
        """Parse a @depth20 message and update shared alpha."""
        try:
            data = _loads(raw)
        except Exception:
            return

        bids_raw = data.get("bids") or data.get("b")
        asks_raw = data.get("asks") or data.get("a")
        if not bids_raw or not asks_raw:
            return

        bids_raw = sorted(bids_raw, key=lambda x: float(x[0]), reverse=True)
        asks_raw = sorted(asks_raw, key=lambda x: float(x[0]))

        try:
            best_bid = float(bids_raw[0][0])
            best_ask = float(asks_raw[0][0])
        except (IndexError, ValueError, TypeError):
            return

        mid = (best_bid + best_ask) * 0.5
        if mid <= 0.0:
            return

        depth = self._looking_depth
        lower = mid * (1.0 - depth)
        upper = mid * (1.0 + depth)

        sum_bid = 0.0
        for level in bids_raw:
            try:
                price = float(level[0])
                qty = float(level[1])
            except (IndexError, ValueError, TypeError):
                continue
            if price < lower:
                break
            sum_bid += qty

        sum_ask = 0.0
        for level in asks_raw:
            try:
                price = float(level[0])
                qty = float(level[1])
            except (IndexError, ValueError, TypeError):
                continue
            if price > upper:
                break
            sum_ask += qty

        imbalance = sum_bid - sum_ask
        self._imb_stats.push(imbalance)
        alpha = self._imb_stats.zscore(imbalance)

        self._shared_alpha.update(alpha)
