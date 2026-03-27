"""
Live integration tests for the Lighter DEX orderbook.

These tests hit real public (no-auth) REST and WebSocket endpoints on mainnet
to validate assumptions about orderbook structure, WS protocol, and the
local SortedDict book maintained by ``orderbook.apply_orderbook_update``.

Run with:
    pytest tests_live/ -v --timeout=60
"""

import asyncio
import json

import pytest
import requests
import websockets
from sortedcontainers import SortedDict

from orderbook import apply_orderbook_update

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://mainnet.zklighter.elliot.ai"
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
ETH_MARKET_ID = 0  # most liquid market

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rest_get(path, params=None, timeout=15):
    """Simple GET wrapper with default timeout."""
    resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


async def _ws_collect(market_id, snapshot_timeout=30, delta_count=20, delta_timeout=45):
    """Connect to WS, collect the initial snapshot and a few deltas.

    Returns (snapshot_msg, [delta_msgs]).
    """
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))

        snapshot = None
        deltas = []

        deadline = asyncio.get_event_loop().time() + snapshot_timeout
        while asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=snapshot_timeout)
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "ping":
                await ws.send('{"type":"pong"}')
                continue
            if msg_type == "subscribed":
                continue
            if msg_type == "update/order_book":
                ob = data.get("order_book", {})
                bids = ob.get("bids", [])
                asks = ob.get("asks", [])
                # Heuristic: snapshot has many entries; delta has few
                if snapshot is None and (len(bids) > 20 or len(asks) > 20):
                    snapshot = data
                    break
                elif snapshot is None:
                    # First message might be small on quiet markets; accept it as snapshot
                    snapshot = data
                    break

        if snapshot is None:
            pytest.skip("No WS snapshot received within timeout")

        # Collect a few deltas
        delta_deadline = asyncio.get_event_loop().time() + delta_timeout
        while len(deltas) < delta_count and asyncio.get_event_loop().time() < delta_deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                data = json.loads(raw)
                msg_type = data.get("type")
                if msg_type == "ping":
                    await ws.send('{"type":"pong"}')
                    continue
                if msg_type == "update/order_book":
                    deltas.append(data)
            except asyncio.TimeoutError:
                break

    return snapshot, deltas


# ---------------------------------------------------------------------------
# Module-scoped fixtures (minimize API calls)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def rest_orderbook():
    """Fetch the REST orderbook for ETH (market 0)."""
    data = _rest_get("/api/v1/orderBookOrders", params={"market_id": ETH_MARKET_ID, "limit": 250})
    return data


@pytest.fixture(scope="module")
def rest_order_books():
    """Fetch the market metadata from /api/v1/orderBooks."""
    return _rest_get("/api/v1/orderBooks")


@pytest.fixture(scope="module")
def ws_snapshot_and_deltas():
    """Connect to WS and collect snapshot + deltas for ETH."""
    return asyncio.get_event_loop().run_until_complete(
        _ws_collect(ETH_MARKET_ID)
    )


# ---------------------------------------------------------------------------
# Parsing helpers (REST uses remaining_base_amount; WS uses size)
# ---------------------------------------------------------------------------

def _parse_rest_bids(data):
    """Extract bid (price, size) tuples from REST response, sorted desc by price."""
    raw = data.get("bids", data.get("order_book", {}).get("bids", []))
    entries = []
    for item in raw:
        price = float(item.get("price", 0))
        size = float(item.get("remaining_base_amount", item.get("size", 0)))
        if price > 0 and size > 0:
            entries.append((price, size))
    entries.sort(key=lambda x: x[0], reverse=True)
    return entries


def _parse_rest_asks(data):
    """Extract ask (price, size) tuples from REST response, sorted asc by price."""
    raw = data.get("asks", data.get("order_book", {}).get("asks", []))
    entries = []
    for item in raw:
        price = float(item.get("price", 0))
        size = float(item.get("remaining_base_amount", item.get("size", 0)))
        if price > 0 and size > 0:
            entries.append((price, size))
    entries.sort(key=lambda x: x[0])
    return entries


def _parse_ws_bids(ob):
    """Extract bid entries from a WS order_book payload."""
    entries = []
    for item in ob.get("bids", []):
        price = float(item["price"])
        size = float(item["size"])
        if price > 0 and size > 0:
            entries.append((price, size))
    entries.sort(key=lambda x: x[0], reverse=True)
    return entries


def _parse_ws_asks(ob):
    """Extract ask entries from a WS order_book payload."""
    entries = []
    for item in ob.get("asks", []):
        price = float(item["price"])
        size = float(item["size"])
        if price > 0 and size > 0:
            entries.append((price, size))
    entries.sort(key=lambda x: x[0])
    return entries


# ===========================================================================
# Test Classes
# ===========================================================================

@pytest.mark.live
class TestRESTOrderbook:
    """Validate structural properties of the REST orderbook."""

    def test_has_bids_and_asks(self, rest_orderbook):
        bids = _parse_rest_bids(rest_orderbook)
        asks = _parse_rest_asks(rest_orderbook)
        assert len(bids) > 0, "REST orderbook has no bids"
        assert len(asks) > 0, "REST orderbook has no asks"

    def test_bids_sorted_descending(self, rest_orderbook):
        bids = _parse_rest_bids(rest_orderbook)
        prices = [b[0] for b in bids]
        assert prices == sorted(prices, reverse=True), "Bids not sorted descending"

    def test_asks_sorted_ascending(self, rest_orderbook):
        asks = _parse_rest_asks(rest_orderbook)
        prices = [a[0] for a in asks]
        assert prices == sorted(prices), "Asks not sorted ascending"

    def test_no_crossed_book(self, rest_orderbook):
        bids = _parse_rest_bids(rest_orderbook)
        asks = _parse_rest_asks(rest_orderbook)
        if bids and asks:
            best_bid = bids[0][0]
            best_ask = asks[0][0]
            assert best_bid < best_ask, f"Crossed book: best_bid={best_bid} >= best_ask={best_ask}"

    def test_all_prices_positive(self, rest_orderbook):
        bids = _parse_rest_bids(rest_orderbook)
        asks = _parse_rest_asks(rest_orderbook)
        for price, size in bids + asks:
            assert price > 0, f"Non-positive price: {price}"
            assert size > 0, f"Non-positive size: {size}"


@pytest.mark.live
class TestRESTMarketMetadata:
    """Validate that the ETH market exists in the market list."""

    def test_eth_market_exists(self, rest_order_books):
        order_books = rest_order_books.get("order_books", rest_order_books.get("orderBooks", []))
        # Look for market_id=0 in the list
        found = False
        for market in order_books:
            mid = market.get("market_id", market.get("marketId"))
            if mid is not None and int(mid) == ETH_MARKET_ID:
                found = True
                break
        assert found, f"ETH market (id={ETH_MARKET_ID}) not found in /api/v1/orderBooks"


@pytest.mark.live
class TestWSSnapshot:
    """Validate the initial WS orderbook snapshot."""

    def test_snapshot_received(self, ws_snapshot_and_deltas):
        snapshot, _ = ws_snapshot_and_deltas
        assert snapshot is not None, "No WS snapshot received"

    def test_snapshot_has_at_least_one_side(self, ws_snapshot_and_deltas):
        snapshot, _ = ws_snapshot_and_deltas
        ob = snapshot.get("order_book", {})
        has_bids = len(ob.get("bids", [])) > 0
        has_asks = len(ob.get("asks", [])) > 0
        assert has_bids or has_asks, "WS snapshot has neither bids nor asks"

    def test_stream_reaches_two_sided_book(self, ws_snapshot_and_deltas):
        """The captured snapshot+deltas stream should eventually become two-sided."""
        snapshot, deltas = ws_snapshot_and_deltas
        bids = SortedDict()
        asks = SortedDict()
        initialized = False

        for msg in [snapshot] + deltas:
            ob = msg.get("order_book", {})
            is_snap = apply_orderbook_update(
                bids, asks, initialized, ob.get("bids", []), ob.get("asks", [])
            )
            if is_snap:
                initialized = True
            if bids and asks:
                return

        pytest.skip("Orderbook stream stayed one-sided during collection window")

    def test_snapshot_no_crossed_book(self, ws_snapshot_and_deltas):
        snapshot, _ = ws_snapshot_and_deltas
        ob = snapshot.get("order_book", {})
        bids = _parse_ws_bids(ob)
        asks = _parse_ws_asks(ob)
        if bids and asks:
            best_bid = bids[0][0]
            best_ask = asks[0][0]
            assert best_bid < best_ask, f"WS crossed book: bid={best_bid} >= ask={best_ask}"

    def test_snapshot_has_offset(self, ws_snapshot_and_deltas):
        snapshot, _ = ws_snapshot_and_deltas
        # offset can be at top level or inside order_book
        offset = snapshot.get("offset", snapshot.get("order_book", {}).get("offset"))
        assert offset is not None, "WS snapshot missing offset field"


@pytest.mark.live
class TestWSvsREST:
    """Validate that WS and REST top-of-book are roughly consistent."""

    def test_top_of_book_within_tolerance(self, rest_orderbook, ws_snapshot_and_deltas):
        snapshot, _ = ws_snapshot_and_deltas
        ob = snapshot.get("order_book", {})

        rest_bids = _parse_rest_bids(rest_orderbook)
        rest_asks = _parse_rest_asks(rest_orderbook)
        ws_bids = _parse_ws_bids(ob)
        ws_asks = _parse_ws_asks(ob)

        if not (rest_bids and rest_asks and ws_bids and ws_asks):
            pytest.skip("Insufficient data for WS-vs-REST comparison")

        rest_best_bid = rest_bids[0][0]
        rest_best_ask = rest_asks[0][0]
        ws_best_bid = ws_bids[0][0]
        ws_best_ask = ws_asks[0][0]

        rest_mid = (rest_best_bid + rest_best_ask) / 2
        ws_mid = (ws_best_bid + ws_best_ask) / 2

        # Allow up to 1% divergence (WS and REST snapshots are taken at different times)
        tolerance = 0.01
        if rest_mid > 0:
            diff_pct = abs(ws_mid - rest_mid) / rest_mid
            assert diff_pct < tolerance, (
                f"WS mid ({ws_mid:.4f}) vs REST mid ({rest_mid:.4f}): "
                f"{diff_pct*100:.3f}% divergence exceeds {tolerance*100}% tolerance"
            )


@pytest.mark.live
class TestSpreadReasonableness:
    """Validate that the ETH spread is within reasonable bounds."""

    def test_spread_positive(self, rest_orderbook):
        bids = _parse_rest_bids(rest_orderbook)
        asks = _parse_rest_asks(rest_orderbook)
        if not (bids and asks):
            pytest.skip("No bids/asks for spread check")
        spread = asks[0][0] - bids[0][0]
        assert spread > 0, f"Non-positive spread: {spread}"

    def test_spread_less_than_5_percent(self, rest_orderbook):
        bids = _parse_rest_bids(rest_orderbook)
        asks = _parse_rest_asks(rest_orderbook)
        if not (bids and asks):
            pytest.skip("No bids/asks for spread check")
        mid = (bids[0][0] + asks[0][0]) / 2
        spread = asks[0][0] - bids[0][0]
        spread_pct = spread / mid * 100
        assert spread_pct < 5.0, f"Spread {spread_pct:.2f}% exceeds 5% for ETH"
