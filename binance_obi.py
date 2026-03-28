"""Binance Futures market data feeds for the market maker.

Two independent feeds:
    BinanceBookTickerClient  – @bookTicker for lowest-latency BBO
    BinanceDiffDepthClient   – @depth@100ms with REST snapshot sync for local book + alpha

Shared state objects:
    SharedBBO    – best bid/ask published by bookTicker feed
    SharedAlpha  – depth-derived imbalance alpha published by diff-depth feed
"""

import asyncio
import logging
import time

import requests as _requests

try:
    import orjson as _json
    _loads = _json.loads
except ImportError:
    import json as _json
    _loads = _json.loads

from vol_obi import RollingStats

try:
    from _vol_obi_fast import CBookSide as _BookSide
    _HAS_CBOOKSIDE = True
except ImportError:
    from sortedcontainers import SortedDict as _BookSide
    _HAS_CBOOKSIDE = False

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
# SharedAlpha — lock-free(ish) container for depth alpha
# ---------------------------------------------------------------------------

class SharedAlpha:
    """Shared container written by the depth task, read on the hot path.

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
        self.alpha = alpha
        self.last_update_time = time.monotonic()
        self.sample_count += 1
        if not self._warmed_up and self.sample_count >= self._min_samples:
            self._warmed_up = True

    def is_stale(self, threshold_seconds: float = 5.0) -> bool:
        if self.last_update_time == 0.0:
            return True
        return (time.monotonic() - self.last_update_time) > threshold_seconds

    @property
    def warmed_up(self) -> bool:
        return self._warmed_up

    def reset(self) -> None:
        self.alpha = 0.0
        self.last_update_time = 0.0
        self.sample_count = 0
        self._warmed_up = False


# ---------------------------------------------------------------------------
# SharedBBO — lock-free container for bookTicker BBO
# ---------------------------------------------------------------------------

class SharedBBO:
    """Shared container for Binance best bid/ask published by @bookTicker.

    Written by BinanceBookTickerClient, read by the trading loop.
    """

    __slots__ = ('best_bid', 'best_ask', 'bid_qty', 'ask_qty', 'mid',
                 'update_id', 'event_time', 'transaction_time',
                 'last_update_time', 'sample_count', '_warmed_up', '_min_samples')

    def __init__(self, *, min_samples: int = 10):
        self.best_bid: float = 0.0
        self.best_ask: float = 0.0
        self.bid_qty: float = 0.0
        self.ask_qty: float = 0.0
        self.mid: float = 0.0
        self.update_id: int = 0
        self.event_time: int = 0
        self.transaction_time: int = 0
        self.last_update_time: float = 0.0   # time.monotonic()
        self.sample_count: int = 0
        self._warmed_up: bool = False
        self._min_samples: int = min_samples

    def update(self, best_bid: float, best_ask: float,
               bid_qty: float, ask_qty: float,
               update_id: int, event_time: int, transaction_time: int) -> None:
        self.best_bid = best_bid
        self.best_ask = best_ask
        self.bid_qty = bid_qty
        self.ask_qty = ask_qty
        self.mid = (best_bid + best_ask) * 0.5
        self.update_id = update_id
        self.event_time = event_time
        self.transaction_time = transaction_time
        self.last_update_time = time.monotonic()
        self.sample_count += 1
        if not self._warmed_up and self.sample_count >= self._min_samples:
            self._warmed_up = True

    def is_stale(self, threshold_seconds: float = 5.0) -> bool:
        if self.last_update_time == 0.0:
            return True
        return (time.monotonic() - self.last_update_time) > threshold_seconds

    @property
    def warmed_up(self) -> bool:
        return self._warmed_up

    def reset(self) -> None:
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.bid_qty = 0.0
        self.ask_qty = 0.0
        self.mid = 0.0
        self.update_id = 0
        self.event_time = 0
        self.transaction_time = 0
        self.last_update_time = 0.0
        self.sample_count = 0
        self._warmed_up = False


# ---------------------------------------------------------------------------
# BinanceBookTickerClient — @bookTicker for lowest-latency BBO
# ---------------------------------------------------------------------------

BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"
BINANCE_FUTURES_REST = "https://fapi.binance.com"


class BinanceBookTickerClient:
    """Streams @bookTicker for real-time best bid/ask.

    Trivially simple: parse b/B/a/A, validate, publish SharedBBO.
    """

    def __init__(
        self,
        binance_symbol: str,
        shared_bbo: SharedBBO,
        *,
        stale_threshold: float = 5.0,
        reconnect_base: float = 2.0,
        reconnect_max: float = 60.0,
    ):
        self._symbol = binance_symbol.lower()
        self._shared_bbo = shared_bbo
        self._stale_threshold = stale_threshold
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max

    @property
    def url(self) -> str:
        return f"{BINANCE_FUTURES_WS}/{self._symbol}@bookTicker"

    async def run(self) -> None:
        import websockets

        backoff = self._reconnect_base
        while True:
            try:
                self._shared_bbo.reset()
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20,
                ) as ws:
                    logger.info("Binance BBO connected: %s", self.url)
                    backoff = self._reconnect_base

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            logger.warning("Binance BBO: no data for 30s, reconnecting...")
                            break
                        self._on_message(raw)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Binance BBO error: %s — reconnecting in %.0fs", e, backoff)

            self._shared_bbo.reset()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._reconnect_max)

    def _on_message(self, raw) -> None:
        try:
            data = _loads(raw)
        except Exception:
            return

        try:
            best_bid = float(data['b'])
            best_ask = float(data['a'])
            bid_qty = float(data['B'])
            ask_qty = float(data['A'])
            update_id = int(data['u'])
            event_time = int(data.get('E', 0))
            transaction_time = int(data.get('T', 0))
        except (KeyError, ValueError, TypeError):
            return

        if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
            return

        self._shared_bbo.update(
            best_bid, best_ask, bid_qty, ask_qty,
            update_id, event_time, transaction_time,
        )


# ---------------------------------------------------------------------------
# BinanceDiffDepthClient — @depth@100ms with REST snapshot sync
# ---------------------------------------------------------------------------

class BinanceDiffDepthClient:
    """Maintains a local Binance order book from diff-depth stream.

    Uses the official snapshot-sync procedure:
    1. Open WS, buffer events
    2. Fetch REST snapshot
    3. Align on lastUpdateId
    4. Validate pu == prev_u on each event
    5. On gap: re-snapshot

    Computes depth-derived imbalance alpha and publishes to SharedAlpha.
    """

    def __init__(
        self,
        binance_symbol: str,
        shared_alpha: SharedAlpha,
        *,
        window_size: int = 6000,
        looking_depth: float = 0.025,
        stale_threshold: float = 5.0,
        reconnect_base: float = 2.0,
        reconnect_max: float = 60.0,
        snapshot_limit: int = 1000,
        snapshot_timeout: float = 10.0,
    ):
        self._symbol = binance_symbol.lower()
        self._shared_alpha = shared_alpha
        self._imb_stats = RollingStats(window_size)
        self._looking_depth = looking_depth
        self._stale_threshold = stale_threshold
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max
        self._snapshot_limit = snapshot_limit
        self._snapshot_timeout = snapshot_timeout

        # Local book
        self._bids = _BookSide()
        self._asks = _BookSide()

        # Sync state
        self._last_update_id: int = 0
        self._prev_u: int = 0
        self._synced: bool = False

    @property
    def url(self) -> str:
        return f"{BINANCE_FUTURES_WS}/{self._symbol}@depth@100ms"

    def _reset(self) -> None:
        self._shared_alpha.reset()
        self._imb_stats.clear()
        self._bids.clear()
        self._asks.clear()
        self._last_update_id = 0
        self._prev_u = 0
        self._synced = False

    async def run(self) -> None:
        import websockets

        backoff = self._reconnect_base
        loop = asyncio.get_running_loop()

        while True:
            try:
                self._reset()
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20,
                ) as ws:
                    logger.info("Binance depth connected: %s", self.url)
                    backoff = self._reconnect_base

                    # Phase 1: Buffer events while fetching REST snapshot
                    buffer = []
                    snapshot_data = None

                    async def _buffer_events():
                        while snapshot_data is None:
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                                data = _loads(raw)
                                if 'U' in data and 'u' in data:
                                    buffer.append(data)
                            except asyncio.TimeoutError:
                                break

                    buffer_task = asyncio.create_task(_buffer_events())
                    try:
                        snapshot_data = await loop.run_in_executor(
                            None, self._fetch_snapshot
                        )
                    except Exception as e:
                        logger.warning("Binance depth snapshot failed: %s", e)
                        buffer_task.cancel()
                        try:
                            await buffer_task
                        except asyncio.CancelledError:
                            pass
                        break  # reconnect
                    finally:
                        # Stop buffering — set sentinel so buffer_task exits
                        # (snapshot_data is now set, so the while loop exits)
                        pass

                    # Give buffer task a moment to catch remaining messages
                    await asyncio.sleep(0.1)
                    buffer_task.cancel()
                    try:
                        await buffer_task
                    except asyncio.CancelledError:
                        pass

                    # Phase 2: Apply snapshot
                    self._apply_snapshot(snapshot_data)
                    logger.info(
                        "Binance depth snapshot applied: lastUpdateId=%d, bids=%d, asks=%d",
                        self._last_update_id, len(self._bids), len(self._asks),
                    )

                    # Phase 3: Drain buffer with sequence alignment
                    first_valid = False
                    for event in buffer:
                        u = event['u']
                        U = event['U']
                        # Drop stale events
                        if u <= self._last_update_id:
                            continue
                        # First valid event check
                        if not first_valid:
                            if U <= self._last_update_id + 1 and u >= self._last_update_id + 1:
                                first_valid = True
                            else:
                                continue
                        self._apply_diff(event)

                    if not first_valid and buffer:
                        logger.warning(
                            "Binance depth: no valid event in buffer (lastUpdateId=%d, buffer=%d events). Re-snapshotting...",
                            self._last_update_id, len(buffer),
                        )
                        continue  # outer while → reconnect

                    self._synced = True
                    logger.info("Binance depth synced, entering live stream")

                    # Phase 4: Normal recv loop
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            logger.warning("Binance depth: no data for 30s, reconnecting...")
                            break

                        try:
                            event = _loads(raw)
                        except Exception:
                            continue

                        if 'U' not in event or 'u' not in event:
                            continue

                        # Sequence check
                        pu = event.get('pu', 0)
                        if self._prev_u != 0 and pu != self._prev_u:
                            logger.warning(
                                "Binance depth sequence gap: pu=%d expected=%d. Re-syncing...",
                                pu, self._prev_u,
                            )
                            break  # reconnect with re-snapshot

                        self._apply_diff(event)
                        self._update_alpha()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Binance depth error: %s — reconnecting in %.0fs", e, backoff)

            self._reset()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._reconnect_max)

    def _fetch_snapshot(self) -> dict:
        """Blocking REST call — run via executor."""
        url = f"{BINANCE_FUTURES_REST}/fapi/v1/depth"
        resp = _requests.get(
            url,
            params={"symbol": self._symbol.upper(), "limit": self._snapshot_limit},
            timeout=self._snapshot_timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def _apply_snapshot(self, data: dict) -> None:
        bids_raw = data.get('bids', [])
        asks_raw = data.get('asks', [])
        if _HAS_CBOOKSIDE:
            self._bids.apply_snapshot_from_binance(bids_raw)
            self._asks.apply_snapshot_from_binance(asks_raw)
        else:
            self._bids.clear()
            self._asks.clear()
            for level in bids_raw:
                price, qty = float(level[0]), float(level[1])
                if qty > 0:
                    self._bids[price] = qty
            for level in asks_raw:
                price, qty = float(level[0]), float(level[1])
                if qty > 0:
                    self._asks[price] = qty
        self._last_update_id = data.get('lastUpdateId', 0)
        self._prev_u = 0  # will be set by first diff event

    def _apply_diff(self, event: dict) -> None:
        bids_raw = event.get('b', [])
        asks_raw = event.get('a', [])
        if _HAS_CBOOKSIDE:
            if bids_raw:
                self._bids.apply_delta_from_binance(bids_raw)
            if asks_raw:
                self._asks.apply_delta_from_binance(asks_raw)
        else:
            for level in bids_raw:
                price, qty = float(level[0]), float(level[1])
                if qty == 0:
                    self._bids.pop(price, None)
                else:
                    self._bids[price] = qty
            for level in asks_raw:
                price, qty = float(level[0]), float(level[1])
                if qty == 0:
                    self._asks.pop(price, None)
                else:
                    self._asks[price] = qty
        self._prev_u = event['u']

    def _update_alpha(self) -> None:
        if not self._bids or not self._asks:
            return
        try:
            best_bid = self._bids.peekitem(-1)[0]
            best_ask = self._asks.peekitem(0)[0]
        except (IndexError, KeyError):
            return
        mid = (best_bid + best_ask) * 0.5
        if mid <= 0.0:
            return

        imbalance = self._compute_imbalance(mid)
        self._imb_stats.push(imbalance)
        alpha = self._imb_stats.zscore(imbalance)
        self._shared_alpha.update(alpha)

    def _compute_imbalance(self, mid: float) -> float:
        lower = mid * (1.0 - self._looking_depth)
        upper = mid * (1.0 + self._looking_depth)

        if _HAS_CBOOKSIDE:
            sum_bid = sum(size for _, size in self._bids.irange(min_price=lower))
            sum_ask = sum(size for _, size in self._asks.irange(max_price=upper))
        else:
            # SortedDict fallback
            sum_bid = 0.0
            for price, size in reversed(self._bids.items()):
                if price < lower:
                    break
                sum_bid += size
            sum_ask = 0.0
            for price, size in self._asks.items():
                if price > upper:
                    break
                sum_ask += size

        return sum_bid - sum_ask
