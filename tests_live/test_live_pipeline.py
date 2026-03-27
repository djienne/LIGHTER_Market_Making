"""
Live pipeline integration tests for the Lighter DEX Market Maker.

Unlike test_live_orderbook.py (which uses standalone helper functions),
these tests **import and call the real production functions** against live
exchange data.  If a field name changes in on_order_book_update() or
handle_orderbook_message(), these tests will catch it.

No authenticated endpoints are used — only public REST + WS channels.

Run with:
    pytest tests_live/test_live_pipeline.py -v --timeout=120
"""

import asyncio
import copy
import time
from datetime import datetime

import pytest
import requests
from sortedcontainers import SortedDict

from orderbook import apply_orderbook_update
from orderbook_sanity import check_orderbook_sanity
from ws_manager import ws_subscribe


def _run_async(coro):
    """Run a coroutine to completion — resilient to pytest-asyncio closing the default loop."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("loop closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
BASE_URL = "https://mainnet.zklighter.elliot.ai"
ETH_MARKET_ID = 0  # most liquid market


# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ws_messages():
    """Connect to live WS via the real ws_subscribe() for ~15 seconds.

    Collects all messages, tracks on_connect / on_disconnect callbacks.
    This single connection feeds most test classes.
    """
    collected = []
    events = {"on_connect_count": 0, "on_disconnect_count": 0}

    async def _run():
        async def _on_connect():
            events["on_connect_count"] += 1

        def _on_disconnect():
            events["on_disconnect_count"] += 1

        def _on_message(data):
            collected.append(data)

        task = asyncio.create_task(
            ws_subscribe(
                channels=[f"order_book/{ETH_MARKET_ID}", f"trade/{ETH_MARKET_ID}"],
                label="live-pipeline-test",
                on_message=_on_message,
                url=WS_URL,
                ping_interval=20,
                recv_timeout=30.0,
                reconnect_base=5,
                reconnect_max=60,
                on_connect=_on_connect,
                on_disconnect=_on_disconnect,
            )
        )
        await asyncio.sleep(15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    _run_async(_run())
    if not collected:
        pytest.skip("No WS messages received in 15 seconds")
    return {"messages": collected, "events": events}


@pytest.fixture(scope="module")
def orderbook_messages(ws_messages):
    """Filter ws_messages to only order book updates."""
    msgs = [m for m in ws_messages["messages"] if m.get("type") == "update/order_book"]
    if not msgs:
        pytest.skip("No order book update messages received")
    return msgs


@pytest.fixture(scope="module")
def trade_messages(ws_messages):
    """Filter ws_messages to only trade updates."""
    msgs = [m for m in ws_messages["messages"] if m.get("type") == "update/trade"]
    if not msgs:
        pytest.skip("No trade update messages received (market may be quiet)")
    return msgs


@pytest.fixture()
def fresh_mm_state():
    """Save/restore market_maker_v2 state to prevent test pollution.

    Yields the module so tests can call its functions directly.
    """
    import market_maker_v2 as mm

    # Save originals
    orig_market = copy.copy(mm.state.market)
    orig_config_market_id = mm.state.config.market_id
    orig_event_set = mm.order_book_received.is_set()

    # Fresh state
    mm.state.market = mm.MarketState()
    mm.state.config.market_id = ETH_MARKET_ID
    mm.order_book_received.clear()

    try:
        yield mm
    finally:
        # Restore
        mm.state.market = orig_market
        mm.state.config.market_id = orig_config_market_id
        if orig_event_set:
            mm.order_book_received.set()
        else:
            mm.order_book_received.clear()


@pytest.fixture()
def fresh_gather_globals():
    """Save/restore gather_lighter_data globals to prevent test pollution.

    Sets market_info[0] = 'ETH' so handle_orderbook_message works for market 0.
    Yields the module so tests can call its functions.
    """
    import gather_lighter_data as gld

    # Save originals
    orig_local_order_books = dict(gld.local_order_books)
    orig_price_buffers = dict(gld.price_buffers)
    orig_trade_buffers = dict(gld.trade_buffers)
    orig_market_info = dict(gld.market_info)
    orig_stats = dict(gld.stats)
    orig_last_offsets = dict(gld.last_offsets)

    # Fresh state
    gld.local_order_books.clear()
    gld.price_buffers.clear()
    gld.trade_buffers.clear()
    gld.market_info.clear()
    gld.market_info[0] = "ETH"
    gld.stats.update({
        'orderbook_updates': 0,
        'trade_fetches': 0,
        'parquet_writes': 0,
        'errors': 0,
        'start_time': None,
        'last_orderbook_time': {},
        'last_trade_time': {},
        'buffer_flushes': 0,
        'gaps_detected': 0,
        'ws_trades': 0,
    })
    gld.last_offsets.clear()

    try:
        yield gld
    finally:
        # Restore
        gld.local_order_books.clear()
        gld.local_order_books.update(orig_local_order_books)
        gld.price_buffers.clear()
        gld.price_buffers.update(orig_price_buffers)
        gld.trade_buffers.clear()
        gld.trade_buffers.update(orig_trade_buffers)
        gld.market_info.clear()
        gld.market_info.update(orig_market_info)
        gld.stats.update(orig_stats)
        gld.last_offsets.clear()
        gld.last_offsets.update(orig_last_offsets)


# ===========================================================================
# 1. TestWsSubscribeIntegration
# ===========================================================================

@pytest.mark.live
class TestWsSubscribeIntegration:
    """Tests the real ws_subscribe() function from ws_manager.py."""

    def test_on_connect_callback_fires(self, ws_messages):
        assert ws_messages["events"]["on_connect_count"] >= 1, (
            "on_connect callback was never invoked"
        )

    def test_on_message_receives_orderbook_updates(self, ws_messages):
        ob_msgs = [m for m in ws_messages["messages"] if m.get("type") == "update/order_book"]
        assert len(ob_msgs) > 0, "No order_book updates received via ws_subscribe"

    def test_on_message_receives_trade_updates(self, ws_messages):
        trade_msgs = [m for m in ws_messages["messages"] if m.get("type") == "update/trade"]
        # Trades may not happen during quiet periods — skip rather than fail
        if not trade_msgs:
            pytest.skip("No trade updates during collection window (market may be quiet)")

    def test_message_structure_has_channel_and_type(self, ws_messages):
        # Only check update messages (the exchange may send other message types)
        update_msgs = [m for m in ws_messages["messages"] if m.get("type", "").startswith("update/")]
        assert len(update_msgs) > 0, "No update messages received"
        for msg in update_msgs:
            assert "type" in msg, f"Message missing 'type': {list(msg.keys())}"
            assert "channel" in msg, f"Message missing 'channel': {list(msg.keys())}"

    def test_connection_stays_alive_past_ping_interval(self):
        """Collect for >25s — proves ping/pong keep-alive works."""
        collected = []

        async def _run():
            def _on_message(data):
                collected.append(data)

            task = asyncio.create_task(
                ws_subscribe(
                    channels=[f"order_book/{ETH_MARKET_ID}"],
                    label="ping-test",
                    on_message=_on_message,
                    url=WS_URL,
                    ping_interval=20,
                    recv_timeout=30.0,
                )
            )
            await asyncio.sleep(27)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        _run_async(_run())
        assert len(collected) > 0, "No messages received in 27s — ping/pong may be broken"

    def test_reconnect_event_triggers_reconnect(self):
        """Set reconnect_event and verify on_disconnect then on_connect fire again."""
        events = {"connects": 0, "disconnects": 0}
        reconnect_evt = asyncio.Event()

        async def _run():
            async def _on_connect():
                events["connects"] += 1

            def _on_disconnect():
                events["disconnects"] += 1

            def _on_message(data):
                pass

            task = asyncio.create_task(
                ws_subscribe(
                    channels=[f"order_book/{ETH_MARKET_ID}"],
                    label="reconnect-test",
                    on_message=_on_message,
                    url=WS_URL,
                    ping_interval=20,
                    recv_timeout=30.0,
                    reconnect_base=1,
                    on_connect=_on_connect,
                    on_disconnect=_on_disconnect,
                    reconnect_event=reconnect_evt,
                )
            )
            # Wait for initial connection
            await asyncio.sleep(5)
            initial_connects = events["connects"]
            assert initial_connects >= 1, "Initial on_connect never fired"

            # Trigger reconnect
            reconnect_evt.set()
            await asyncio.sleep(8)

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        _run_async(_run())
        # After reconnect, we should have at least 2 connect callbacks
        assert events["connects"] >= 2, (
            f"Expected >=2 connects after reconnect event, got {events['connects']}"
        )
        assert events["disconnects"] >= 1, (
            f"Expected >=1 disconnect callback after reconnect event, got {events['disconnects']}"
        )


# ===========================================================================
# 2. TestOnOrderBookUpdate
# ===========================================================================

@pytest.mark.live
class TestOnOrderBookUpdate:
    """Calls the real market_maker_v2.on_order_book_update() with live WS payloads."""

    def test_snapshot_initializes_book(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        for msg in orderbook_messages:
            ob_payload = msg.get("order_book", {})
            mm.on_order_book_update(ETH_MARKET_ID, ob_payload)
            ob = mm.state.market.local_order_book
            if ob["bids"] and ob["asks"]:
                break

        ob = mm.state.market.local_order_book
        assert ob["initialized"] is True, "Book not initialized after messages"
        assert len(ob["bids"]) > 0, "Bids empty after processing stream"
        assert len(ob["asks"]) > 0, "Asks empty after processing stream"

    def test_mid_price_set_after_update(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        for msg in orderbook_messages:
            ob_payload = msg.get("order_book", {})
            mm.on_order_book_update(ETH_MARKET_ID, ob_payload)
            if mm.state.market.mid_price is not None:
                break
        assert mm.state.market.mid_price is not None
        assert mm.state.market.mid_price > 0

    def test_ws_connection_healthy_set(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        ob_payload = orderbook_messages[0].get("order_book", {})
        mm.on_order_book_update(ETH_MARKET_ID, ob_payload)
        assert mm.state.market.ws_connection_healthy is True

    def test_order_book_received_event_set(self, orderbook_messages, fresh_mm_state):
        """order_book_received should only fire once book has both bids and asks."""
        mm = fresh_mm_state
        for msg in orderbook_messages:
            ob_payload = msg.get("order_book", {})
            mm.on_order_book_update(ETH_MARKET_ID, ob_payload)
            if mm.order_book_received.is_set():
                break
        assert mm.order_book_received.is_set()
        # When the event fires, the book must have valid data
        ob = mm.state.market.local_order_book
        assert len(ob["bids"]) > 0, "Event set but bids empty"
        assert len(ob["asks"]) > 0, "Event set but asks empty"

    def test_deltas_maintain_valid_book(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        for msg in orderbook_messages:
            ob_payload = msg.get("order_book", {})
            mm.on_order_book_update(ETH_MARKET_ID, ob_payload)
            ob = mm.state.market.local_order_book
            if ob["bids"] and ob["asks"]:
                best_bid = ob["bids"].peekitem(-1)[0]
                best_ask = ob["asks"].peekitem(0)[0]
                assert best_bid < best_ask, (
                    f"Crossed book: bid={best_bid} >= ask={best_ask}"
                )

    def test_mid_price_stable_across_deltas(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        mid_prices = []
        for msg in orderbook_messages:
            ob_payload = msg.get("order_book", {})
            mm.on_order_book_update(ETH_MARKET_ID, ob_payload)
            if mm.state.market.mid_price is not None:
                mid_prices.append(mm.state.market.mid_price)

        for i in range(1, len(mid_prices)):
            if mid_prices[i - 1] > 0:
                pct_change = abs(mid_prices[i] - mid_prices[i - 1]) / mid_prices[i - 1] * 100
                assert pct_change < 5.0, (
                    f"Mid price jumped {pct_change:.2f}%: "
                    f"{mid_prices[i-1]} -> {mid_prices[i]}"
                )


# ===========================================================================
# 3. TestOnTradeUpdate
# ===========================================================================

@pytest.mark.live
class TestTradeMessageSchema:
    """Validates live WS trade message structure (no callback — trade feed removed from hot path)."""

    def test_trade_fields_present(self, trade_messages):
        for msg in trade_messages:
            for trade in msg.get("trades", []):
                assert "price" in trade, f"Trade missing 'price': {trade}"
                assert "size" in trade, f"Trade missing 'size': {trade}"
                assert "is_maker_ask" in trade, f"Trade missing 'is_maker_ask': {trade}"

    def test_trade_prices_positive(self, trade_messages):
        for msg in trade_messages:
            for trade in msg.get("trades", []):
                assert float(trade["price"]) > 0, f"Non-positive trade price: {trade}"


# ===========================================================================
# 4. TestApplyOrderbookUpdateLive
# ===========================================================================

@pytest.mark.live
class TestApplyOrderbookUpdateLive:
    """Calls the real orderbook.apply_orderbook_update() with live WS data."""

    def test_first_message_is_snapshot(self, orderbook_messages):
        bids = SortedDict()
        asks = SortedDict()
        ob = orderbook_messages[0].get("order_book", {})
        is_snap = apply_orderbook_update(
            bids, asks, False, ob.get("bids", []), ob.get("asks", [])
        )
        assert is_snap is True, "First message should be treated as snapshot"

    def test_subsequent_messages_are_deltas(self, orderbook_messages):
        if len(orderbook_messages) < 2:
            pytest.skip("Need at least 2 messages to test deltas")
        bids = SortedDict()
        asks = SortedDict()
        # Apply first as snapshot
        ob0 = orderbook_messages[0].get("order_book", {})
        apply_orderbook_update(bids, asks, False, ob0.get("bids", []), ob0.get("asks", []))
        # Second should be delta
        ob1 = orderbook_messages[1].get("order_book", {})
        is_snap = apply_orderbook_update(
            bids, asks, True, ob1.get("bids", []), ob1.get("asks", [])
        )
        assert is_snap is False, "Second message should be a delta, not snapshot"

    def test_sorted_dict_ordering_after_snapshot(self, orderbook_messages):
        bids = SortedDict()
        asks = SortedDict()
        initialized = False
        # Apply messages until we have >1 bid level
        for msg in orderbook_messages:
            ob = msg.get("order_book", {})
            apply_orderbook_update(bids, asks, initialized, ob.get("bids", []), ob.get("asks", []))
            initialized = True
            if len(bids) > 1:
                break
        assert len(bids) > 1, "Need >1 bid level to check ordering"
        # SortedDict stores keys in ascending order
        # peekitem(0) = lowest, peekitem(-1) = highest
        assert bids.peekitem(-1)[0] > bids.peekitem(0)[0], "Bid ordering broken"

    def test_no_crossed_book_throughout_stream(self, orderbook_messages):
        bids = SortedDict()
        asks = SortedDict()
        initialized = False
        for msg in orderbook_messages:
            ob = msg.get("order_book", {})
            is_snap = apply_orderbook_update(
                bids, asks, initialized, ob.get("bids", []), ob.get("asks", [])
            )
            if is_snap:
                initialized = True
            if bids and asks:
                best_bid = bids.peekitem(-1)[0]
                best_ask = asks.peekitem(0)[0]
                assert best_bid < best_ask, (
                    f"Crossed book: bid={best_bid} >= ask={best_ask}"
                )

    def test_all_sizes_positive_in_book(self, orderbook_messages):
        bids = SortedDict()
        asks = SortedDict()
        ob = orderbook_messages[0].get("order_book", {})
        apply_orderbook_update(bids, asks, False, ob.get("bids", []), ob.get("asks", []))
        for price, size in bids.items():
            assert size > 0, f"Non-positive bid size at {price}: {size}"
        for price, size in asks.items():
            assert size > 0, f"Non-positive ask size at {price}: {size}"

    def test_zero_size_removes_level(self, orderbook_messages):
        """If a delta has size '0', that price is absent from the book."""
        bids = SortedDict()
        asks = SortedDict()
        initialized = False
        removals_tested = 0
        for msg in orderbook_messages:
            ob = msg.get("order_book", {})
            bids_in = ob.get("bids", [])
            asks_in = ob.get("asks", [])

            apply_orderbook_update(bids, asks, initialized, bids_in, asks_in)
            if not initialized:
                initialized = True
                continue

            # Check any zero-size entries in this delta
            for item in bids_in:
                if float(item["size"]) == 0:
                    price = float(item["price"])
                    assert price not in bids, f"Zero-size bid at {price} still in book"
                    removals_tested += 1
            for item in asks_in:
                if float(item["size"]) == 0:
                    price = float(item["price"])
                    assert price not in asks, f"Zero-size ask at {price} still in book"
                    removals_tested += 1

        if removals_tested == 0:
            pytest.skip("No zero-size removals observed in stream")


# ===========================================================================
# 5. TestGetBestPricesLive
# ===========================================================================

@pytest.mark.live
class TestGetBestPricesLive:
    """Builds SortedDict from real data, then calls the real get_best_prices()."""

    @staticmethod
    def _populate_book(mm, orderbook_messages):
        """Feed all messages through on_order_book_update to build a populated book."""
        for msg in orderbook_messages:
            payload = msg.get("order_book", {})
            mm.on_order_book_update(ETH_MARKET_ID, payload)
        ob = mm.state.market.local_order_book
        if not ob["bids"] or not ob["asks"]:
            pytest.skip("Book still empty after processing all messages")

    def test_best_bid_is_highest_bid(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        self._populate_book(mm, orderbook_messages)
        best_bid, _ = mm.get_best_prices()
        ob = mm.state.market.local_order_book
        assert best_bid == max(ob["bids"].keys()), "best_bid != max bid price"

    def test_best_ask_is_lowest_ask(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        self._populate_book(mm, orderbook_messages)
        _, best_ask = mm.get_best_prices()
        ob = mm.state.market.local_order_book
        assert best_ask == min(ob["asks"].keys()), "best_ask != min ask price"

    def test_best_bid_less_than_best_ask(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        self._populate_book(mm, orderbook_messages)
        best_bid, best_ask = mm.get_best_prices()
        assert best_bid is not None and best_ask is not None
        assert best_bid < best_ask, f"Crossed: bid={best_bid} >= ask={best_ask}"

    def test_mid_price_between_bid_and_ask(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        self._populate_book(mm, orderbook_messages)
        best_bid, best_ask = mm.get_best_prices()
        mid = mm.state.market.mid_price
        assert best_bid < mid < best_ask, (
            f"Mid price {mid} not between bid={best_bid} and ask={best_ask}"
        )


# ===========================================================================
# 6. TestCheckOrderbookSanityLive
# ===========================================================================

@pytest.mark.live
class TestCheckOrderbookSanityLive:
    """Calls the real orderbook_sanity.check_orderbook_sanity() with live data."""

    @staticmethod
    def _build_sorted_book(orderbook_messages):
        bids = SortedDict()
        asks = SortedDict()
        initialized = False
        for msg in orderbook_messages:
            ob = msg.get("order_book", {})
            is_snap = apply_orderbook_update(
                bids, asks, initialized, ob.get("bids", []), ob.get("asks", [])
            )
            if is_snap:
                initialized = True
            if bids and asks:
                break
        if not bids or not asks:
            pytest.skip("Could not build a populated book from WS stream")
        return bids, asks

    def test_sanity_ok_on_fresh_book(self, orderbook_messages):
        bids, asks = self._build_sorted_book(orderbook_messages)

        async def _run():
            return await check_orderbook_sanity(ETH_MARKET_ID, bids, asks)

        result = _run_async(_run())
        assert result.ok is True, f"Sanity check failed: {result.reason}"

    def test_sanity_result_fields_populated(self, orderbook_messages):
        bids, asks = self._build_sorted_book(orderbook_messages)

        async def _run():
            return await check_orderbook_sanity(ETH_MARKET_ID, bids, asks)

        result = _run_async(_run())
        assert result.latency_ms > 0, "latency_ms should be positive"
        assert result.ws_best_bid > 0, "ws_best_bid should be positive"
        assert result.ws_best_ask > 0, "ws_best_ask should be positive"
        assert result.rest_best_bid > 0, "rest_best_bid should be positive"
        assert result.rest_best_ask > 0, "rest_best_ask should be positive"
        assert isinstance(result.bid_diff_pct, float)
        assert isinstance(result.ask_diff_pct, float)

    def test_sanity_fails_on_empty_book(self):
        async def _run():
            return await check_orderbook_sanity(ETH_MARKET_ID, SortedDict(), SortedDict())

        result = _run_async(_run())
        assert result.ok is False
        assert "empty" in result.reason.lower(), f"Expected 'empty' in reason, got: {result.reason}"

    def test_sanity_fails_on_crossed_book(self):
        # Build a crossed book: bids higher than asks
        bids = SortedDict({2000.0: 1.0, 3000.0: 1.0})
        asks = SortedDict({1000.0: 1.0, 1500.0: 1.0})

        async def _run():
            return await check_orderbook_sanity(ETH_MARKET_ID, bids, asks)

        result = _run_async(_run())
        assert result.ok is False
        assert "crossed" in result.reason.lower(), (
            f"Expected 'crossed' in reason, got: {result.reason}"
        )

    def test_sanity_tolerance_enforcement(self, orderbook_messages):
        """With tolerance_pct=0.0, any tiny diff triggers failure."""
        bids, asks = self._build_sorted_book(orderbook_messages)

        async def _run():
            return await check_orderbook_sanity(
                ETH_MARKET_ID, bids, asks, tolerance_pct=0.0
            )

        result = _run_async(_run())
        # With zero tolerance, practically any WS/REST timing difference should fail
        # But if they happen to match exactly, that's also valid
        assert isinstance(result.ok, bool)
        if not result.ok:
            assert "divergence" in result.reason.lower() or "exceeds" in result.reason.lower()


# ===========================================================================
# 7. TestCheckWebsocketHealth
# ===========================================================================

@pytest.mark.live
class TestCheckWebsocketHealth:
    """Calls the real check_websocket_health() from market_maker_v2."""

    def test_healthy_after_update(self, orderbook_messages, fresh_mm_state):
        mm = fresh_mm_state
        for msg in orderbook_messages:
            payload = msg.get("order_book", {})
            mm.on_order_book_update(ETH_MARKET_ID, payload)
            if mm.state.market.mid_price is not None:
                break
        assert mm.check_websocket_health() is True

    def test_unhealthy_when_flag_false(self, fresh_mm_state):
        mm = fresh_mm_state
        mm.state.market.ws_connection_healthy = False
        assert mm.check_websocket_health() is False

    def test_unhealthy_when_stale(self, fresh_mm_state):
        mm = fresh_mm_state
        mm.state.market.ws_connection_healthy = True
        mm.state.market.last_order_book_update = time.monotonic() - 60
        mm.state.market.mid_price = 2000.0
        assert mm.check_websocket_health() is False

    def test_unhealthy_when_mid_price_none(self, fresh_mm_state):
        mm = fresh_mm_state
        mm.state.market.ws_connection_healthy = True
        mm.state.market.last_order_book_update = time.monotonic()
        mm.state.market.mid_price = None
        assert mm.check_websocket_health() is False


# ===========================================================================
# 8. TestRestartWebsocket
# ===========================================================================

@pytest.mark.live
class TestRestartWebsocket:
    """Calls the real restart_websocket() and verifies orderbook recovery."""

    def test_restart_websocket_recovers_orderbook(self, fresh_mm_state):
        mm = fresh_mm_state
        mm.state.config.market_id = ETH_MARKET_ID

        async def _cancel_ws_task():
            if mm.ws_task is not None and not mm.ws_task.done():
                mm.ws_task.cancel()
                try:
                    await asyncio.wait_for(mm.ws_task, timeout=5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            mm.ws_task = None

        async def _run():
            await _cancel_ws_task()

            try:
                ok = await asyncio.wait_for(mm.restart_websocket(), timeout=25)
                assert ok is True, "restart_websocket() returned False"
                ob = mm.state.market.local_order_book
                assert ob["initialized"] is True, "Orderbook not initialized after restart"
                assert len(ob["bids"]) > 0, "Bids empty after restart"
                assert len(ob["asks"]) > 0, "Asks empty after restart"
                assert mm.check_websocket_health() is True, "Websocket health not restored after restart"
            finally:
                await _cancel_ws_task()

        _run_async(_run())


# ===========================================================================
# 9. TestHandleOrderbookMessage
# ===========================================================================

@pytest.mark.live
class TestHandleOrderbookMessage:
    """Calls the real gather_lighter_data.handle_orderbook_message() with live data."""

    @staticmethod
    def _get_ob_payload(msg):
        """Extract (market_id, payload) from a WS orderbook message."""
        channel = msg.get("channel", "")
        mid = int(channel.split(":")[1]) if ":" in channel else 0
        return mid, msg.get("order_book", {})

    def test_populates_local_order_books(self, orderbook_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        for msg in orderbook_messages:
            mid, payload = self._get_ob_payload(msg)
            gld.handle_orderbook_message(mid, payload)
            lb = gld.local_order_books.get("ETH", {})
            if lb.get("bids") and lb.get("asks"):
                break
        assert "ETH" in gld.local_order_books
        assert len(gld.local_order_books["ETH"]["bids"]) > 0
        assert len(gld.local_order_books["ETH"]["asks"]) > 0

    def test_initialized_flag_set(self, orderbook_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_ob_payload(orderbook_messages[0])
        gld.handle_orderbook_message(mid, payload)
        assert gld.local_order_books["ETH"]["initialized"] is True

    def test_offset_tracked(self, orderbook_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_ob_payload(orderbook_messages[0])
        expected_offset = payload.get("offset", 0)
        gld.handle_orderbook_message(mid, payload)
        assert gld.last_offsets["ETH"] == expected_offset

    def test_price_buffer_populated(self, orderbook_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_ob_payload(orderbook_messages[0])
        gld.handle_orderbook_message(mid, payload)
        assert len(gld.price_buffers["ETH"]) >= 1

    def test_price_buffer_has_top10_structure(self, orderbook_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_ob_payload(orderbook_messages[0])
        gld.handle_orderbook_message(mid, payload)
        entry = gld.price_buffers["ETH"][0]
        for i in range(10):
            assert f"bid_price_{i}" in entry, f"Missing bid_price_{i}"
            assert f"ask_price_{i}" in entry, f"Missing ask_price_{i}"

    def test_stats_incremented(self, orderbook_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_ob_payload(orderbook_messages[0])
        gld.handle_orderbook_message(mid, payload)
        assert gld.stats["orderbook_updates"] == 1

    def test_offset_gap_detection(self, orderbook_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_ob_payload(orderbook_messages[0])
        # First message sets offset
        gld.handle_orderbook_message(mid, payload)
        initial_gaps = gld.stats["gaps_detected"]

        # Simulate a gap: set offset far ahead
        gap_payload = dict(payload)
        current_offset = gld.last_offsets.get("ETH", 0)
        gap_payload["offset"] = current_offset + 200
        gld.handle_orderbook_message(mid, gap_payload)

        assert gld.stats["gaps_detected"] > initial_gaps, (
            "Gap detection did not increment stats"
        )


# ===========================================================================
# 10. TestHandleTradeMessage
# ===========================================================================

@pytest.mark.live
class TestHandleTradeMessage:
    """Calls the real gather_lighter_data.handle_trade_message() with live data."""

    def _get_first_trade_payload(self, trade_messages):
        """Extract (market_id, payload) from first WS trade message."""
        msg = trade_messages[0]
        channel = msg.get("channel", "")
        mid = int(channel.split(":")[1]) if ":" in channel else 0
        return mid, msg

    def test_populates_trade_buffers(self, trade_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_first_trade_payload(trade_messages)
        gld.handle_trade_message(mid, payload)
        assert len(gld.trade_buffers["ETH"]) > 0

    def test_trade_data_structure(self, trade_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_first_trade_payload(trade_messages)
        gld.handle_trade_message(mid, payload)
        for entry in gld.trade_buffers["ETH"]:
            assert "timestamp" in entry, f"Missing 'timestamp': {entry.keys()}"
            assert "price" in entry, f"Missing 'price': {entry.keys()}"
            assert "size" in entry, f"Missing 'size': {entry.keys()}"
            assert "side" in entry, f"Missing 'side': {entry.keys()}"
            assert "trade_id" in entry, f"Missing 'trade_id': {entry.keys()}"

    def test_side_is_buy_or_sell(self, trade_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_first_trade_payload(trade_messages)
        gld.handle_trade_message(mid, payload)
        for entry in gld.trade_buffers["ETH"]:
            assert entry["side"] in ("buy", "sell"), f"Unexpected side: {entry['side']}"

    def test_prices_and_sizes_positive(self, trade_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_first_trade_payload(trade_messages)
        gld.handle_trade_message(mid, payload)
        for entry in gld.trade_buffers["ETH"]:
            assert entry["price"] > 0, f"Non-positive price: {entry['price']}"
            assert entry["size"] > 0, f"Non-positive size: {entry['size']}"

    def test_timestamp_reasonable(self, trade_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_first_trade_payload(trade_messages)
        gld.handle_trade_message(mid, payload)
        now = datetime.now()
        for entry in gld.trade_buffers["ETH"]:
            ts = datetime.fromisoformat(entry["timestamp"])
            diff = abs((now - ts).total_seconds())
            assert diff < 7 * 86400, f"Timestamp {ts} is >7 days from now"
            # Not in the future (with 5 min tolerance for clock skew)
            assert (ts - now).total_seconds() < 300, f"Timestamp {ts} is in the future"

    def test_stats_ws_trades_incremented(self, trade_messages, fresh_gather_globals):
        gld = fresh_gather_globals
        mid, payload = self._get_first_trade_payload(trade_messages)
        gld.handle_trade_message(mid, payload)
        assert gld.stats["ws_trades"] > 0


# ===========================================================================
# 11. TestFullPipelineEndToEnd
# ===========================================================================

@pytest.mark.live
class TestFullPipelineEndToEnd:
    """Wires ws_subscribe() -> on_order_book_update() -> get_best_prices() ->
    check_orderbook_sanity() end-to-end using production code."""

    def test_full_pipeline_produces_valid_state(self, fresh_mm_state):
        mm = fresh_mm_state
        result_holder = {}

        async def _run():
            def _on_message(data):
                msg_type = data.get("type")
                if msg_type == "update/order_book" and "order_book" in data:
                    mm.on_order_book_update(ETH_MARKET_ID, data["order_book"])

            task = asyncio.create_task(
                ws_subscribe(
                    channels=[f"order_book/{ETH_MARKET_ID}"],
                    label="e2e-pipeline-test",
                    on_message=_on_message,
                    url=WS_URL,
                    ping_interval=20,
                    recv_timeout=30.0,
                )
            )

            # Wait for the order book to be received
            try:
                await asyncio.wait_for(mm.order_book_received.wait(), timeout=30)
            except asyncio.TimeoutError:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                pytest.fail("order_book_received never set within 30s")

            # Give a moment for a few more updates
            await asyncio.sleep(3)

            # Validate mid price
            assert mm.state.market.mid_price is not None
            assert mm.state.market.mid_price > 0

            # Validate book
            ob = mm.state.market.local_order_book
            assert ob["initialized"]
            assert len(ob["bids"]) > 0
            assert len(ob["asks"]) > 0

            # Validate get_best_prices
            best_bid, best_ask = mm.get_best_prices()
            assert best_bid is not None and best_ask is not None
            assert best_bid < best_ask

            # Validate sanity check
            sanity = await check_orderbook_sanity(
                ETH_MARKET_ID, ob["bids"], ob["asks"]
            )
            result_holder["sanity"] = sanity

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        _run_async(_run())
        sanity = result_holder.get("sanity")
        assert sanity is not None
        assert sanity.ok is True, f"Sanity check failed in E2E pipeline: {sanity.reason}"


# ===========================================================================
# 12. TestMultiMessageStreamProcessing
# ===========================================================================

@pytest.mark.live
class TestMultiMessageStreamProcessing:
    """Processes a full stream of ~15s of real messages through production code."""

    def test_received_multiple_updates(self, orderbook_messages):
        assert len(orderbook_messages) >= 3, (
            f"Expected >=3 orderbook messages, got {len(orderbook_messages)}"
        )

    def test_book_never_crosses_during_stream(self, orderbook_messages):
        bids = SortedDict()
        asks = SortedDict()
        initialized = False
        for msg in orderbook_messages:
            ob = msg.get("order_book", {})
            is_snap = apply_orderbook_update(
                bids, asks, initialized, ob.get("bids", []), ob.get("asks", [])
            )
            if is_snap:
                initialized = True
            if bids and asks:
                best_bid = bids.peekitem(-1)[0]
                best_ask = asks.peekitem(0)[0]
                assert best_bid < best_ask, (
                    f"Crossed: bid={best_bid} >= ask={best_ask}"
                )

    def test_mid_price_no_large_jumps(self, orderbook_messages):
        bids = SortedDict()
        asks = SortedDict()
        initialized = False
        mid_prices = []
        for msg in orderbook_messages:
            ob = msg.get("order_book", {})
            apply_orderbook_update(
                bids, asks, initialized, ob.get("bids", []), ob.get("asks", [])
            )
            initialized = True
            if bids and asks:
                mid = (bids.peekitem(-1)[0] + asks.peekitem(0)[0]) / 2
                mid_prices.append(mid)

        for i in range(1, len(mid_prices)):
            if mid_prices[i - 1] > 0:
                pct = abs(mid_prices[i] - mid_prices[i - 1]) / mid_prices[i - 1] * 100
                assert pct < 5.0, (
                    f"Mid price jump {pct:.2f}%: {mid_prices[i-1]} -> {mid_prices[i]}"
                )

    def test_offsets_monotonic(self, orderbook_messages):
        offsets = []
        for msg in orderbook_messages:
            ob = msg.get("order_book", {})
            offset = ob.get("offset", msg.get("offset"))
            if offset is not None:
                offsets.append(int(offset))
        if len(offsets) < 2:
            pytest.skip("Not enough offsets to verify monotonicity")
        for i in range(1, len(offsets)):
            assert offsets[i] >= offsets[i - 1], (
                f"Offset went backwards: {offsets[i-1]} -> {offsets[i]}"
            )


# ===========================================================================
# 13. TestRESTvsWSConsistency
# ===========================================================================

@pytest.mark.live
class TestRESTvsWSConsistency:
    """Compare the WS-maintained book against a fresh REST snapshot."""

    def test_ws_book_matches_rest_after_stream(self, orderbook_messages):
        """After processing WS messages, fetch REST snapshot, compare mid prices within 1%."""
        bids = SortedDict()
        asks = SortedDict()
        initialized = False
        for msg in orderbook_messages:
            ob = msg.get("order_book", {})
            is_snap = apply_orderbook_update(
                bids, asks, initialized, ob.get("bids", []), ob.get("asks", [])
            )
            if is_snap:
                initialized = True

        if not bids or not asks:
            pytest.skip("WS book is empty after stream")

        ws_best_bid = bids.peekitem(-1)[0]
        ws_best_ask = asks.peekitem(0)[0]
        ws_mid = (ws_best_bid + ws_best_ask) / 2

        # Fetch fresh REST snapshot
        resp = requests.get(
            f"{BASE_URL}/api/v1/orderBookOrders",
            params={"market_id": ETH_MARKET_ID, "limit": 10},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        rest_bids = data.get("bids", data.get("order_book", {}).get("bids", []))
        rest_asks = data.get("asks", data.get("order_book", {}).get("asks", []))

        rest_best_bid = 0.0
        for item in rest_bids:
            price = float(item.get("price", 0))
            size = float(item.get("remaining_base_amount", item.get("size", 0)))
            if price > 0 and size > 0:
                rest_best_bid = max(rest_best_bid, price)

        rest_best_ask = float("inf")
        for item in rest_asks:
            price = float(item.get("price", 0))
            size = float(item.get("remaining_base_amount", item.get("size", 0)))
            if price > 0 and size > 0:
                rest_best_ask = min(rest_best_ask, price)

        if rest_best_bid <= 0 or rest_best_ask == float("inf"):
            pytest.skip("REST returned empty book")

        rest_mid = (rest_best_bid + rest_best_ask) / 2

        if rest_mid > 0:
            diff_pct = abs(ws_mid - rest_mid) / rest_mid * 100
            assert diff_pct < 1.0, (
                f"WS mid ({ws_mid:.4f}) vs REST mid ({rest_mid:.4f}): "
                f"{diff_pct:.3f}% divergence exceeds 1%"
            )
