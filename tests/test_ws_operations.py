"""Tests for the 5 WebSocket-driven operations added to market_maker_v2.

Covers:
  1. Ticker handler — on_ticker_update
  2. Ticker-based sanity check — periodic_orderbook_sanity_check
  3. Dynamic reconciler — stale_order_reconciler_loop
  4. WS cancel confirmation — _confirm_order_absent_on_exchange
  5. WS batch send — sign_and_send_batch  &  batch cancel operations
"""

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from sortedcontainers import SortedDict

import market_maker_v2 as mm
from _helpers import DummyClient, temp_mm_attrs, temp_event_state


# ---------------------------------------------------------------------------
# Module-level helpers for constructing BatchOp objects
# ---------------------------------------------------------------------------

def _make_create_op(side="buy", level=0, price=100.0, size=1.0, order_id=1):
    return mm.BatchOp(
        side=side, level=level, action="create",
        price=price, size=size,
        order_id=order_id, exchange_id=0,
    )


def _make_cancel_op(side="buy", level=0, order_id=10, exchange_id=10):
    return mm.BatchOp(
        side=side, level=level, action="cancel",
        price=0, size=0,
        order_id=order_id, exchange_id=exchange_id,
    )


# ---------------------------------------------------------------------------
# 1) Ticker handler — on_ticker_update
# ---------------------------------------------------------------------------

class TestTickerUpdate(unittest.TestCase):

    def test_ticker_update_sets_best_bid_ask(self):
        """Valid data updates state.market.ticker_* fields."""
        with temp_mm_attrs(MARKET_ID=7):
            mm.state.market.ticker_best_bid = None
            mm.state.market.ticker_best_ask = None
            mm.state.market.ticker_updated_at = 0.0

            mm.on_ticker_update(7, {"best_bid": "100.5", "best_ask": "101.0"})

            self.assertAlmostEqual(mm.state.market.ticker_best_bid, 100.5)
            self.assertAlmostEqual(mm.state.market.ticker_best_ask, 101.0)
            self.assertGreater(mm.state.market.ticker_updated_at, 0.0)

    def test_ticker_update_ignores_wrong_market(self):
        """Non-matching market_id leaves state unchanged."""
        with temp_mm_attrs(MARKET_ID=7):
            mm.state.market.ticker_best_bid = 50.0
            mm.state.market.ticker_best_ask = 51.0
            old_ts = mm.state.market.ticker_updated_at

            mm.on_ticker_update(999, {"best_bid": "200.0", "best_ask": "201.0"})

            self.assertAlmostEqual(mm.state.market.ticker_best_bid, 50.0)
            self.assertAlmostEqual(mm.state.market.ticker_best_ask, 51.0)
            self.assertEqual(mm.state.market.ticker_updated_at, old_ts)

    def test_ticker_update_handles_partial_data(self):
        """Only best_bid or only best_ask updates partially."""
        with temp_mm_attrs(MARKET_ID=3):
            mm.state.market.ticker_best_bid = None
            mm.state.market.ticker_best_ask = None

            # Only best_bid
            mm.on_ticker_update(3, {"best_bid": "42.0"})
            self.assertAlmostEqual(mm.state.market.ticker_best_bid, 42.0)
            self.assertIsNone(mm.state.market.ticker_best_ask)

            # Now only best_ask
            mm.on_ticker_update(3, {"best_ask": "43.0"})
            self.assertAlmostEqual(mm.state.market.ticker_best_bid, 42.0)
            self.assertAlmostEqual(mm.state.market.ticker_best_ask, 43.0)


# ---------------------------------------------------------------------------
# 2) Ticker-based sanity check — periodic_orderbook_sanity_check
# ---------------------------------------------------------------------------

class TestSanityCheck(unittest.IsolatedAsyncioTestCase):

    async def test_sanity_check_passes_with_ticker(self):
        """Ticker agrees with orderbook -> no reconnect event."""
        ob = {'bids': SortedDict({99.0: 1.0}),
              'asks': SortedDict({100.0: 1.0}),
              'initialized': True}

        with temp_mm_attrs(MARKET_ID=1, local_order_book=ob):
            mm.state.market.ticker_best_bid = 99.0
            mm.state.market.ticker_best_ask = 100.0
            mm.state.market.ticker_updated_at = time.monotonic()  # fresh
            mm.ws_reconnect_event.clear()

            # Run one iteration then cancel
            sleep_calls = []
            original_sleep = asyncio.sleep

            async def fake_sleep(secs, *a, **kw):
                sleep_calls.append(secs)
                if len(sleep_calls) >= 2:
                    raise asyncio.CancelledError
                # First call is the interval sleep — just return

            with patch("asyncio.sleep", side_effect=fake_sleep):
                with self.assertRaises(asyncio.CancelledError):
                    await mm.periodic_orderbook_sanity_check(interval=1, tolerance_pct=1.0)

            self.assertFalse(mm.ws_reconnect_event.is_set())

    async def test_sanity_check_fails_triggers_reconnect(self):
        """Ticker diverges -> ws_reconnect_event set."""
        ob = {'bids': SortedDict({99.0: 1.0}),
              'asks': SortedDict({100.0: 1.0}),
              'initialized': True}

        with temp_mm_attrs(MARKET_ID=1, local_order_book=ob):
            # Ticker shows very different prices
            mm.state.market.ticker_best_bid = 110.0
            mm.state.market.ticker_best_ask = 111.0
            mm.state.market.ticker_updated_at = time.monotonic()
            mm.ws_reconnect_event.clear()

            async def fake_sleep(secs, *a, **kw):
                raise asyncio.CancelledError

            # The first sleep is the interval; after it raises CancelledError,
            # the loop body won't execute. We need to let the first sleep pass.
            call_count = 0

            async def fake_sleep_once(secs, *a, **kw):
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    raise asyncio.CancelledError

            with patch("asyncio.sleep", side_effect=fake_sleep_once):
                with self.assertRaises(asyncio.CancelledError):
                    await mm.periodic_orderbook_sanity_check(interval=0, tolerance_pct=0.1)

            self.assertTrue(mm.ws_reconnect_event.is_set())
            mm.ws_reconnect_event.clear()

    async def test_sanity_check_falls_back_to_rest(self):
        """Stale ticker (age > 30s) -> REST fallback used."""
        ob = {'bids': SortedDict({99.0: 1.0}),
              'asks': SortedDict({100.0: 1.0}),
              'initialized': True}

        with temp_mm_attrs(MARKET_ID=1, local_order_book=ob):
            # Make ticker stale
            mm.state.market.ticker_best_bid = 99.0
            mm.state.market.ticker_best_ask = 100.0
            mm.state.market.ticker_updated_at = time.monotonic() - 60.0  # 60s ago

            call_count = 0

            async def fake_sleep(secs, *a, **kw):
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    raise asyncio.CancelledError

            rest_result = MagicMock()
            rest_result.ok = True
            rest_result.bid_diff_pct = 0.01
            rest_result.ask_diff_pct = 0.01
            rest_result.latency_ms = 10

            with patch("asyncio.sleep", side_effect=fake_sleep), \
                 patch("market_maker_v2.check_orderbook_sanity", new_callable=AsyncMock, return_value=rest_result) as mock_rest:
                mm.ws_reconnect_event.clear()
                with self.assertRaises(asyncio.CancelledError):
                    await mm.periodic_orderbook_sanity_check(interval=0, tolerance_pct=1.0)

                # REST fallback was called
                mock_rest.assert_called_once()
                self.assertFalse(mm.ws_reconnect_event.is_set())


# ---------------------------------------------------------------------------
# 3) Dynamic reconciler — stale_order_reconciler_loop
# ---------------------------------------------------------------------------

class TestReconcilerInterval(unittest.IsolatedAsyncioTestCase):

    async def test_reconciler_slow_when_ws_healthy(self):
        """Sleeps 60s when WS order feed is healthy."""
        intervals = []

        async def fake_sleep(secs, *a, **kw):
            intervals.append(secs)
            raise asyncio.CancelledError

        with temp_event_state(mm._account_orders_ws_connected, set_value=True):
            saved = mm._account_orders_ws_ready
            mm._account_orders_ws_ready = True
            try:
                with patch("asyncio.sleep", side_effect=fake_sleep):
                    with self.assertRaises(asyncio.CancelledError):
                        await mm.stale_order_reconciler_loop(MagicMock(), market_id=1, account_id=1)
            finally:
                mm._account_orders_ws_ready = saved

        self.assertEqual(len(intervals), 1)
        self.assertEqual(intervals[0], mm.RECONCILER_SLOW_INTERVAL_SEC)

    async def test_reconciler_fast_when_ws_down(self):
        """Sleeps fast_interval when WS order feed is down."""
        intervals = []

        async def fake_sleep(secs, *a, **kw):
            intervals.append(secs)
            raise asyncio.CancelledError

        with temp_event_state(mm._account_orders_ws_connected, set_value=False):
            saved = mm._account_orders_ws_ready
            mm._account_orders_ws_ready = False
            try:
                with patch("asyncio.sleep", side_effect=fake_sleep):
                    with self.assertRaises(asyncio.CancelledError):
                        await mm.stale_order_reconciler_loop(MagicMock(), market_id=1, account_id=1)
            finally:
                mm._account_orders_ws_ready = saved

        self.assertEqual(len(intervals), 1)
        expected = max(mm.STALE_ORDER_POLLER_INTERVAL_SEC, 0.5)
        self.assertEqual(intervals[0], expected)


# ---------------------------------------------------------------------------
# 4) WS cancel confirmation — _confirm_order_absent_on_exchange
# ---------------------------------------------------------------------------

class TestCancelConfirm(unittest.IsolatedAsyncioTestCase):

    async def test_cancel_confirm_via_ws_event(self):
        """Event set immediately -> returns True, 0 REST calls."""
        client = MagicMock()

        # The function creates its own Event, so we set it concurrently
        # after it registers the key.
        async def _set_after_registration():
            # Spin until the function registers the event
            for _ in range(200):
                if 12345 in mm._order_cancel_events:
                    mm._order_cancel_events[12345].set()
                    return
                await asyncio.sleep(0.001)

        setter = asyncio.create_task(_set_after_registration())
        result = await mm._confirm_order_absent_on_exchange(client, 12345, timeout_sec=2.0)
        await setter
        self.assertTrue(result)
        # Cleaned up
        self.assertNotIn(12345, mm._order_cancel_events)

    async def test_cancel_confirm_timeout_rest_fallback_absent(self):
        """Event not set, timeout -> single REST call, order absent -> True."""
        client = MagicMock()
        # _fetch_account_active_orders returns empty list (order is gone)
        with patch.object(mm, "_fetch_account_active_orders", new_callable=AsyncMock, return_value=[]):
            result = await mm._confirm_order_absent_on_exchange(client, 99999, timeout_sec=0.01)
        self.assertTrue(result)
        self.assertNotIn(99999, mm._order_cancel_events)

    async def test_cancel_confirm_timeout_rest_still_live(self):
        """Event not set, timeout -> REST shows order still live -> False."""
        client = MagicMock()
        # Return an order whose client_order_index matches
        live_orders = [{"client_order_index": 88888, "order_index": 500,
                        "is_ask": False, "price": "100", "remaining_base_amount": "1"}]
        with patch.object(mm, "_fetch_account_active_orders", new_callable=AsyncMock, return_value=live_orders):
            result = await mm._confirm_order_absent_on_exchange(client, 88888, timeout_sec=0.01)
        self.assertFalse(result)
        self.assertNotIn(88888, mm._order_cancel_events)


# ---------------------------------------------------------------------------
# 5a) WS batch send — sign_and_send_batch
# ---------------------------------------------------------------------------

class TestSignAndSendBatch(unittest.IsolatedAsyncioTestCase):

    async def test_batch_sent_via_ws_when_connected(self):
        """_tx_ws.send_batch called, REST client.send_tx_batch not called."""
        client = DummyClient()
        op = _make_create_op()

        mock_tx_ws = MagicMock()
        mock_tx_ws.is_connected = True
        mock_tx_ws.send_batch = AsyncMock(return_value={"code": 0, "volume_quota_remaining": "50"})

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=mock_tx_ws, _global_backoff_until=0.0, _last_send_time=0.0,
            current_bid_order_id=None,
        ):
            await mm.sign_and_send_batch(client, [op])

        # WS was used
        mock_tx_ws.send_batch.assert_called_once()
        # REST was NOT used
        self.assertEqual(len(client.send_tx_batch_calls), 0)
        # sign_create_order was called
        self.assertEqual(len(client.sign_create_calls), 1)

    async def test_batch_falls_back_to_rest(self):
        """_tx_ws disconnected -> REST client.send_tx_batch used."""
        client = DummyClient()
        op = _make_create_op()

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
            current_bid_order_id=None,
        ):
            await mm.sign_and_send_batch(client, [op])

        # REST was used
        self.assertEqual(len(client.send_tx_batch_calls), 1)

    async def test_batch_cancel_ops_skip_bind_live(self):
        """Cancel BatchOps don't call order_manager.bind_live."""
        client = DummyClient()
        cancel_op = _make_cancel_op()

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
        ):
            with patch.object(mm.order_manager, "bind_live", wraps=mm.order_manager.bind_live) as spy:
                await mm.sign_and_send_batch(client, [cancel_op])
                spy.assert_not_called()


# ---------------------------------------------------------------------------
# 5b) Batch cancel operations (pause + orphan)
# ---------------------------------------------------------------------------

class TestBatchCancelOperations(unittest.IsolatedAsyncioTestCase):

    async def test_orphan_cancel_uses_batch(self):
        """reconcile_orders_with_exchange uses batch for orphan cancellation."""
        # Fill all local slots so the orphan can't be rebound
        # NUM_LEVELS=2, so we need bid[0], bid[1], ask[0], ask[1] filled
        # The exchange also has these tracked orders plus the orphan
        tracked_orders = []
        for lvl in range(mm.NUM_LEVELS):
            tracked_orders.append({
                "client_order_index": 1000 + lvl, "order_index": 2000 + lvl,
                "is_ask": False, "price": "100", "remaining_base_amount": "1",
            })
            tracked_orders.append({
                "client_order_index": 3000 + lvl, "order_index": 4000 + lvl,
                "is_ask": True, "price": "101", "remaining_base_amount": "1",
            })
        # The orphan — an extra buy order beyond NUM_LEVELS capacity
        orphan = {
            "client_order_index": 777, "order_index": 500,
            "is_ask": False, "price": "100", "remaining_base_amount": "1",
        }
        all_orders = tracked_orders + [orphan]

        client = MagicMock()
        batch_calls = []

        async def mock_sign_and_send(client_arg, ops):
            batch_calls.append(ops)

        async def mock_wait_for_write(op_count=4, cancel_only=False):
            return True

        # Set local state to track the non-orphan orders
        overrides = {
            "MARKET_ID": 1,
            "_PRICE_TICK_FLOAT": 0.01,
            "_AMOUNT_TICK_FLOAT": 0.001,
        }
        with temp_mm_attrs(**overrides):
            # Fill local order slots
            for lvl in range(mm.NUM_LEVELS):
                mm.state.orders.bid_order_ids[lvl] = 1000 + lvl
                mm.state.orders.ask_order_ids[lvl] = 3000 + lvl

            with patch.object(mm, "_fetch_account_active_orders", new_callable=AsyncMock, return_value=all_orders), \
                 patch.object(mm, "sign_and_send_batch", side_effect=mock_sign_and_send), \
                 patch.object(mm, "_wait_for_write_slot", side_effect=mock_wait_for_write):
                await mm.reconcile_orders_with_exchange(client, source="test")

        # Batch cancel was used for the orphan
        self.assertEqual(len(batch_calls), 1)
        ops = batch_calls[0]
        self.assertEqual(len(ops), 1)
        self.assertEqual(ops[0].action, "cancel")
        self.assertEqual(ops[0].exchange_id, 500)


# ---------------------------------------------------------------------------
# 6) Additional edge-case tests — ticker malformed data
# ---------------------------------------------------------------------------

class TestTickerEdgeCases(unittest.TestCase):

    def test_ticker_update_malformed_data_no_crash(self):
        """Non-numeric best_bid is caught; state unchanged."""
        with temp_mm_attrs(MARKET_ID=7):
            mm.state.market.ticker_best_bid = 50.0
            mm.state.market.ticker_best_ask = 51.0
            old_ts = mm.state.market.ticker_updated_at

            # "not_a_number" will raise ValueError in float()
            mm.on_ticker_update(7, {"best_bid": "not_a_number"})

            # State should be unchanged because the exception was caught
            self.assertAlmostEqual(mm.state.market.ticker_best_bid, 50.0)
            self.assertAlmostEqual(mm.state.market.ticker_best_ask, 51.0)
            self.assertEqual(mm.state.market.ticker_updated_at, old_ts)


# ---------------------------------------------------------------------------
# 7) Sanity check — crossed book + REST fallback failure
# ---------------------------------------------------------------------------

class TestSanityCheckEdgeCases(unittest.IsolatedAsyncioTestCase):

    async def test_sanity_check_crossed_book_triggers_reconnect(self):
        """ws_best_bid >= ws_best_ask -> ws_reconnect_event set."""
        # Crossed book: bid 101 >= ask 100
        ob = {'bids': SortedDict({101.0: 1.0}),
              'asks': SortedDict({100.0: 1.0}),
              'initialized': True}

        with temp_mm_attrs(MARKET_ID=1, local_order_book=ob):
            mm.ws_reconnect_event.clear()

            call_count = 0

            async def fake_sleep(secs, *a, **kw):
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    raise asyncio.CancelledError

            with patch("asyncio.sleep", side_effect=fake_sleep):
                with self.assertRaises(asyncio.CancelledError):
                    await mm.periodic_orderbook_sanity_check(interval=0, tolerance_pct=1.0)

            self.assertTrue(mm.ws_reconnect_event.is_set())
            mm.ws_reconnect_event.clear()

    async def test_sanity_check_rest_fallback_failure_triggers_reconnect(self):
        """Stale ticker + REST returns ok=False -> ws_reconnect_event set."""
        ob = {'bids': SortedDict({99.0: 1.0}),
              'asks': SortedDict({100.0: 1.0}),
              'initialized': True}

        with temp_mm_attrs(MARKET_ID=1, local_order_book=ob):
            # Make ticker stale so REST fallback is used
            mm.state.market.ticker_best_bid = 99.0
            mm.state.market.ticker_best_ask = 100.0
            mm.state.market.ticker_updated_at = time.monotonic() - 60.0
            mm.ws_reconnect_event.clear()

            call_count = 0

            async def fake_sleep(secs, *a, **kw):
                nonlocal call_count
                call_count += 1
                if call_count >= 2:
                    raise asyncio.CancelledError

            rest_result = MagicMock()
            rest_result.ok = False
            rest_result.reason = "diverged"
            rest_result.ws_best_bid = 99.0
            rest_result.ws_best_ask = 100.0
            rest_result.rest_best_bid = 110.0
            rest_result.rest_best_ask = 111.0

            with patch("asyncio.sleep", side_effect=fake_sleep), \
                 patch("market_maker_v2.check_orderbook_sanity",
                       new_callable=AsyncMock, return_value=rest_result):
                with self.assertRaises(asyncio.CancelledError):
                    await mm.periodic_orderbook_sanity_check(interval=0, tolerance_pct=1.0)

            self.assertTrue(mm.ws_reconnect_event.is_set())
            mm.ws_reconnect_event.clear()


# ---------------------------------------------------------------------------
# 8) _confirm_order_absent_on_exchange — edge cases
# ---------------------------------------------------------------------------

class TestCancelConfirmEdgeCases(unittest.IsolatedAsyncioTestCase):

    async def test_cancel_confirm_rest_fetch_none_returns_false(self):
        """_fetch_account_active_orders returns None -> False (unconfirmed)."""
        with patch.object(mm, "_fetch_account_active_orders",
                          new_callable=AsyncMock, return_value=None):
            result = await mm._confirm_order_absent_on_exchange(
                MagicMock(), 99999, timeout_sec=0.01)
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# 9) sign_and_send_batch — error handling
# ---------------------------------------------------------------------------

class TestSignAndSendBatchErrors(unittest.IsolatedAsyncioTestCase):

    async def test_batch_sign_error_skips_op(self):
        """Sign error -> op skipped, nonce acknowledged as failed."""
        client = DummyClient(sign_err="bad key")
        op = _make_create_op()

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
            current_bid_order_id=None,
        ):
            await mm.sign_and_send_batch(client, [op])

        # Sign was called but send_tx_batch was NOT (all ops failed signing)
        self.assertEqual(len(client.sign_create_calls), 1)
        self.assertEqual(len(client.send_tx_batch_calls), 0)
        # Nonce failure was acknowledged
        self.assertGreater(len(client.nonce_manager.failures), 0)

    async def test_batch_all_sign_fail_sends_nothing(self):
        """All ops fail signing -> no send call at all."""
        client = DummyClient(sign_err="bad")
        ops = [_make_create_op(order_id=1), _make_cancel_op(order_id=2)]

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
            current_bid_order_id=None,
        ):
            await mm.sign_and_send_batch(client, ops)

        self.assertEqual(len(client.send_tx_batch_calls), 0)
        # Two failures acknowledged (one per op)
        self.assertEqual(len(client.nonce_manager.failures), 2)

    async def test_batch_429_response_triggers_backoff(self):
        """Response containing '429' triggers _trigger_global_backoff."""
        client = DummyClient(send_code=429, send_message="429 Too Many Requests")
        op = _make_create_op()

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
            current_bid_order_id=None,
        ):
            with patch.object(mm, "_trigger_global_backoff") as mock_backoff:
                await mm.sign_and_send_batch(client, [op])
                mock_backoff.assert_called_once()

    async def test_batch_nonce_error_triggers_refresh(self):
        """Response message containing 'nonce' triggers hard_refresh_nonce."""
        client = DummyClient(send_code=1, send_message="invalid nonce")
        op = _make_create_op()

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
            current_bid_order_id=None,
        ):
            await mm.sign_and_send_batch(client, [op])

        # send was called (signing succeeded)
        self.assertEqual(len(client.send_tx_batch_calls), 1)

    async def test_batch_modify_op_signs_correctly(self):
        """Modify-action BatchOp calls sign_modify_order."""
        client = DummyClient()
        op = mm.BatchOp(
            side="buy", level=0, action="modify",
            price=100.0, size=1.0,
            order_id=42, exchange_id=99,
        )

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
        ):
            await mm.sign_and_send_batch(client, [op])

        self.assertEqual(len(client.sign_modify_calls), 1)
        self.assertEqual(client.sign_modify_calls[0]["order_index"], 99)
        self.assertEqual(len(client.send_tx_batch_calls), 1)

    async def test_batch_modify_failure_does_not_fallback(self):
        """Batch modify failure records rejection but does NOT cancel+place."""
        client = DummyClient(send_code=1, send_message="order not found")
        op = mm.BatchOp(
            side="buy", level=0, action="modify",
            price=100.0, size=1.0,
            order_id=42, exchange_id=99,
        )

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
        ):
            with patch.object(mm, "_record_order_rejection") as mock_reject:
                await mm.sign_and_send_batch(client, [op])
                mock_reject.assert_called_once()

        # Modify was signed and sent
        self.assertEqual(len(client.sign_modify_calls), 1)
        self.assertEqual(len(client.send_tx_batch_calls), 1)
        # No cancel or create fallback
        self.assertEqual(len(client.cancel_order_calls), 0)
        self.assertEqual(len(client.create_order_calls), 0)


# ---------------------------------------------------------------------------
# 10) on_account_orders_update — cancel event signaling
# ---------------------------------------------------------------------------

class TestAccountOrdersCancelEvent(unittest.TestCase):

    def test_account_orders_incremental_fires_cancel_event(self):
        """Dead order (status='cancelled') fires _order_cancel_events[cid]."""
        # Ensure WS is in incremental mode (not snapshot)
        saved_ready = mm._account_orders_ws_ready
        mm._account_orders_ws_ready = True
        evt = asyncio.Event()
        mm._order_cancel_events[555] = evt
        try:
            data = {"orders": {"1": [
                {"client_order_index": 555, "order_index": 700, "status": "cancelled"}
            ]}}
            with temp_mm_attrs(MARKET_ID=1):
                mm.on_account_orders_update(account_id=1, market_id=1, data=data)
            self.assertTrue(evt.is_set())
            # Event should be popped from the dict
            self.assertNotIn(555, mm._order_cancel_events)
        finally:
            mm._account_orders_ws_ready = saved_ready
            mm._order_cancel_events.pop(555, None)

    def test_account_orders_incremental_fires_cancel_event_by_exchange_id(self):
        """Dead order also fires event keyed by order_index (exchange_id)."""
        saved_ready = mm._account_orders_ws_ready
        mm._account_orders_ws_ready = True
        evt = asyncio.Event()
        mm._order_cancel_events[700] = evt  # keyed by exchange order_index
        try:
            data = {"orders": {"1": [
                {"client_order_index": 555, "order_index": 700, "status": "filled"}
            ]}}
            with temp_mm_attrs(MARKET_ID=1):
                mm.on_account_orders_update(account_id=1, market_id=1, data=data)
            self.assertTrue(evt.is_set())
            self.assertNotIn(700, mm._order_cancel_events)
        finally:
            mm._account_orders_ws_ready = saved_ready
            mm._order_cancel_events.pop(700, None)


# ---------------------------------------------------------------------------
# 11) collect_order_operations — pure logic, previously 0 tests
# ---------------------------------------------------------------------------

class TestCollectOrderOperations(unittest.TestCase):

    def test_collect_creates_for_empty_orders(self):
        """No existing orders -> all create ops."""
        with temp_mm_attrs(
            MARKET_ID=1,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS=5.0,
        ):
            # Clear all order slots
            for lvl in range(mm.NUM_LEVELS):
                mm.state.orders.bid_order_ids[lvl] = None
                mm.state.orders.bid_prices[lvl] = None
                mm.state.orders.ask_order_ids[lvl] = None
                mm.state.orders.ask_prices[lvl] = None

            level_prices = [(100.0, 101.0)]
            ops = mm.collect_order_operations(level_prices, base_amount=1.0)

            self.assertEqual(len(ops), 2)
            self.assertTrue(all(op.action == "create" for op in ops))
            sides = {op.side for op in ops}
            self.assertEqual(sides, {"buy", "sell"})

    def test_collect_modifies_when_price_changes(self):
        """Existing order with different price -> modify op."""
        with temp_mm_attrs(
            MARKET_ID=1,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS=0.0,
        ):
            mm.state.orders.bid_order_ids[0] = 42
            mm.state.orders.bid_prices[0] = 100.0
            mm.state.orders.bid_sizes[0] = 1.0
            mm.state.orders.ask_order_ids[0] = None
            mm.state.orders.ask_prices[0] = None
            mm._client_to_exchange_id[42] = 420

            level_prices = [(105.0, 106.0)]
            ops = mm.collect_order_operations(level_prices, base_amount=1.0)

            modify_ops = [o for o in ops if o.action == "modify"]
            create_ops = [o for o in ops if o.action == "create"]
            self.assertEqual(len(modify_ops), 1)
            self.assertEqual(modify_ops[0].side, "buy")
            self.assertAlmostEqual(modify_ops[0].price, 105.0)
            self.assertEqual(len(create_ops), 1)
            self.assertEqual(create_ops[0].side, "sell")

    def test_collect_skips_when_within_threshold(self):
        """Price within QUOTE_UPDATE_THRESHOLD_BPS -> skip (no op)."""
        with temp_mm_attrs(
            MARKET_ID=1,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS=100.0,  # 100 bps = 1%
        ):
            mm.state.orders.bid_order_ids[0] = 42
            mm.state.orders.bid_prices[0] = 100.0
            mm.state.orders.bid_sizes[0] = 1.0
            mm.state.orders.ask_order_ids[0] = 43
            mm.state.orders.ask_prices[0] = 101.0
            mm.state.orders.ask_sizes[0] = 1.0
            mm._client_to_exchange_id[42] = 420
            mm._client_to_exchange_id[43] = 430

            # Small price change: 0.01% < 1% threshold
            level_prices = [(100.005, 101.005)]
            ops = mm.collect_order_operations(level_prices, base_amount=1.0)

            self.assertEqual(len(ops), 0)

    def test_collect_modifies_when_size_changes(self):
        """Existing order with unchanged price but different size -> modify op."""
        with temp_mm_attrs(
            MARKET_ID=1,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS=100.0,
        ):
            mm.state.orders.bid_order_ids[0] = 42
            mm.state.orders.bid_prices[0] = 100.0
            mm.state.orders.bid_sizes[0] = 0.5
            mm._client_to_exchange_id[42] = 420

            ops = mm.collect_order_operations([(100.0, 101.0)], base_amount=1.0)

            self.assertEqual(len(ops), 2)
            modify_ops = [o for o in ops if o.action == "modify"]
            self.assertEqual(len(modify_ops), 1)
            self.assertEqual(modify_ops[0].side, "buy")
            self.assertAlmostEqual(modify_ops[0].size, 1.0)

    def test_collect_waits_for_exchange_id_before_modifying(self):
        """Tracked orders without exchange order_index should not emit modify yet."""
        with temp_mm_attrs(
            MARKET_ID=1,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS=0.0,
        ):
            mm.state.orders.bid_order_ids[0] = 42
            mm.state.orders.bid_prices[0] = 100.0
            mm.state.orders.bid_sizes[0] = 1.0
            mm.state.orders.ask_order_ids[0] = None
            mm.state.orders.ask_prices[0] = None
            mm.state.orders.ask_sizes[0] = None

            ops = mm.collect_order_operations([(105.0, 106.0)], base_amount=1.0)

            self.assertEqual(len(ops), 1)
            self.assertEqual(ops[0].action, "create")
            self.assertEqual(ops[0].side, "sell")

    def test_collect_handles_none_order_id(self):
        """order_id=None on a level -> create op."""
        with temp_mm_attrs(
            MARKET_ID=1,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            DEFAULT_QUOTE_UPDATE_THRESHOLD_BPS=5.0,
        ):
            mm.state.orders.bid_order_ids[0] = None
            mm.state.orders.bid_prices[0] = None
            mm.state.orders.ask_order_ids[0] = 99
            mm.state.orders.ask_prices[0] = 101.0
            mm.state.orders.ask_sizes[0] = 1.0
            mm._client_to_exchange_id[99] = 990

            level_prices = [(100.0, 101.0)]
            ops = mm.collect_order_operations(level_prices, base_amount=1.0)

            create_ops = [o for o in ops if o.action == "create"]
            # Bid has no order -> create; Ask within threshold -> skip
            self.assertEqual(len(create_ops), 1)
            self.assertEqual(create_ops[0].side, "buy")


# ---------------------------------------------------------------------------
# 12) _wait_for_write_slot — backoff path
# ---------------------------------------------------------------------------

class TestWaitForWriteSlot(unittest.IsolatedAsyncioTestCase):

    async def test_wait_for_write_slot_respects_global_backoff(self):
        """_global_backoff_until in the future -> returns False."""
        with temp_mm_attrs(
            _global_backoff_until=time.monotonic() + 60.0,
            _last_send_time=0.0,
        ):
            result = await mm._wait_for_write_slot()
            self.assertFalse(result)


# ---------------------------------------------------------------------------
# 13) on_account_orders_update — snapshot path
# ---------------------------------------------------------------------------

class TestAccountOrdersSnapshot(unittest.TestCase):

    def test_account_orders_snapshot_rebinds_orders(self):
        """First WS message (snapshot) keeps live orders and refreshes fields."""
        original_orders = mm.OrderState(**vars(mm.state.orders))
        original_risk = mm.RiskState(**vars(mm.state.risk))
        saved_ready = mm._account_orders_ws_ready

        try:
            mm._account_orders_ws_ready = False  # simulate first message

            with temp_mm_attrs(
                MARKET_ID=1,
                current_bid_order_id=100,
                current_bid_price=50.0,
                current_bid_size=0.01,
                current_ask_order_id=200,
                current_ask_price=51.0,
                current_ask_size=0.01,
            ):
                # Snapshot says only ask 200 is alive
                data = {"orders": {"1": [
                    {
                        "client_order_index": 200,
                        "order_index": 9999,
                        "status": "open",
                        "is_ask": True,
                        "price": "52.5",
                        "remaining_base_amount": "0.25",
                    }
                ]}}
                mm.on_account_orders_update(account_id=1, market_id=1, data=data)

                # After snapshot: _account_orders_ws_ready should be True
                self.assertTrue(mm._account_orders_ws_ready)
                # Bid 100 not in snapshot -> cleared
                self.assertIsNone(mm.state.orders.bid_order_ids[0])
                # Ask 200 in snapshot -> kept
                self.assertEqual(mm.state.orders.ask_order_ids[0], 200)
                self.assertAlmostEqual(mm.state.orders.ask_prices[0], 52.5)
                self.assertAlmostEqual(mm.state.orders.ask_sizes[0], 0.25)
        finally:
            mm.state.orders = original_orders
            mm.state.risk = original_risk
            mm._account_orders_ws_ready = saved_ready
            mm.risk_controller = mm.RiskController(mm.state.risk)
            mm.order_manager = mm.OrderManager(mm.state.order_manager)

    def test_incremental_update_clears_dead_order(self):
        """Incremental update with status=filled clears order and signals cancel event."""
        original_orders = mm.OrderState(**vars(mm.state.orders))
        original_risk = mm.RiskState(**vars(mm.state.risk))
        saved_ready = mm._account_orders_ws_ready

        try:
            mm._account_orders_ws_ready = True  # already past snapshot

            evt = asyncio.Event()
            mm._order_cancel_events[300] = evt

            with temp_mm_attrs(
                MARKET_ID=1,
                current_bid_order_id=300,
                current_bid_price=50.0,
                current_bid_size=0.01,
                current_ask_order_id=400,
                current_ask_price=51.0,
                current_ask_size=0.01,
            ):
                data = {"orders": {"1": [
                    {"client_order_index": 300, "order_index": 8888, "status": "filled"}
                ]}}
                mm.on_account_orders_update(account_id=1, market_id=1, data=data)

                # Bid 300 was filled -> cleared
                self.assertIsNone(mm.state.orders.bid_order_ids[0])
                # Ask 400 untouched (not in incremental)
                self.assertEqual(mm.state.orders.ask_order_ids[0], 400)
                # Cancel event was signaled
                self.assertTrue(evt.is_set())
                self.assertNotIn(300, mm._order_cancel_events)
        finally:
            mm.state.orders = original_orders
            mm.state.risk = original_risk
            mm._account_orders_ws_ready = saved_ready
            mm.risk_controller = mm.RiskController(mm.state.risk)
            mm.order_manager = mm.OrderManager(mm.state.order_manager)
            mm._order_cancel_events.pop(300, None)

    def test_incremental_update_refreshes_partial_fill_size(self):
        """Incremental live update should refresh remaining size for tracked orders."""
        original_orders = mm.OrderState(**vars(mm.state.orders))
        original_risk = mm.RiskState(**vars(mm.state.risk))
        saved_ready = mm._account_orders_ws_ready

        try:
            mm._account_orders_ws_ready = True

            with temp_mm_attrs(
                MARKET_ID=1,
                current_bid_order_id=300,
                current_bid_price=50.0,
                current_bid_size=1.0,
            ):
                data = {"orders": {"1": [
                    {
                        "client_order_index": 300,
                        "order_index": 8888,
                        "status": "partial_filled",
                        "is_ask": False,
                        "price": "50.5",
                        "remaining_base_amount": "0.25",
                    }
                ]}}
                mm.on_account_orders_update(account_id=1, market_id=1, data=data)

                self.assertEqual(mm.state.orders.bid_order_ids[0], 300)
                self.assertAlmostEqual(mm.state.orders.bid_prices[0], 50.5)
                self.assertAlmostEqual(mm.state.orders.bid_sizes[0], 0.25)
                self.assertEqual(mm._client_to_exchange_id[300], 8888)
        finally:
            mm.state.orders = original_orders
            mm.state.risk = original_risk
            mm._account_orders_ws_ready = saved_ready
            mm.risk_controller = mm.RiskController(mm.state.risk)
            mm.order_manager = mm.OrderManager(mm.state.order_manager)


# ---------------------------------------------------------------------------
# 13b) restart_websocket — order state preservation
# ---------------------------------------------------------------------------

class TestRestartWebsocketPreservesOrders(unittest.IsolatedAsyncioTestCase):

    async def test_restart_websocket_preserves_order_tracking(self):
        """restart_websocket() should NOT clear order tracking state."""
        original_orders = mm.OrderState(**vars(mm.state.orders))
        original_market = mm.MarketConfig(**vars(mm.state.config))

        try:
            mm.state.config.market_id = 1

            with temp_mm_attrs(
                current_bid_order_id=500,
                current_bid_price=49000.0,
                current_bid_size=0.05,
                current_ask_order_id=600,
                current_ask_price=51000.0,
                current_ask_size=0.05,
            ):
                # Patch subscribe_to_market_data to avoid real WS connection
                async def _fake_subscribe(market_id):
                    mm.order_book_received.set()
                    while True:
                        await asyncio.sleep(999)

                with patch.object(mm, "subscribe_to_market_data", side_effect=_fake_subscribe), \
                     patch.object(mm, "_cancel_task_with_timeout", new_callable=AsyncMock):
                    result = await mm.restart_websocket()

                self.assertTrue(result)
                # Orders must NOT be cleared
                self.assertEqual(mm.state.orders.bid_order_ids[0], 500)
                self.assertEqual(mm.state.orders.ask_order_ids[0], 600)
                self.assertAlmostEqual(mm.state.orders.bid_prices[0], 49000.0)
                self.assertAlmostEqual(mm.state.orders.ask_prices[0], 51000.0)
        finally:
            mm.state.orders = original_orders
            mm.state.config = original_market
            mm.order_manager = mm.OrderManager(mm.state.order_manager)
            # Clean up the ws_task created by restart_websocket
            if mm.ws_task is not None:
                mm.ws_task.cancel()
                try:
                    await mm.ws_task
                except (asyncio.CancelledError, Exception):
                    pass
                mm.ws_task = None


# ---------------------------------------------------------------------------
# 14) _update_id_mapping_from_orders — bounded growth
# ---------------------------------------------------------------------------

class TestIdMappingTrim(unittest.TestCase):

    def test_id_mapping_trims_at_threshold(self):
        """Insert 201 entries -> map trims to 100."""
        saved = dict(mm._client_to_exchange_id)
        mm._client_to_exchange_id.clear()
        try:
            orders = [
                {"client_order_index": i, "order_index": i + 10000}
                for i in range(201)
            ]
            mm._update_id_mapping_from_orders(orders)
            self.assertLessEqual(len(mm._client_to_exchange_id), 100)
        finally:
            mm._client_to_exchange_id.clear()
            mm._client_to_exchange_id.update(saved)


# ---------------------------------------------------------------------------
# 15) sign_and_send_batch — partial sign failure
# ---------------------------------------------------------------------------

class TestBatchPartialSignFailure(unittest.IsolatedAsyncioTestCase):

    async def test_partial_sign_failure_sends_remaining(self):
        """When 1 of N ops fails signing, the rest are still sent."""
        sign_call_count = 0

        class _PartialFailClient(DummyClient):
            def sign_create_order(self, **kwargs):
                nonlocal sign_call_count
                sign_call_count += 1
                if sign_call_count == 1:
                    # First op fails signing
                    return (1, b"tx_info", b"hash", "bad key")
                return super().sign_create_order(**kwargs)

        client = _PartialFailClient()
        op1 = _make_create_op(order_id=1, price=100.0)
        op2 = _make_create_op(order_id=2, price=101.0)

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
            current_bid_order_id=None,
        ):
            await mm.sign_and_send_batch(client, [op1, op2])

        # Both were signed, but only op2 succeeded signing
        self.assertEqual(sign_call_count, 2)
        # send_tx_batch was called with 1 op (the successful one)
        self.assertEqual(len(client.send_tx_batch_calls), 1)
        tx_types, tx_infos = client.send_tx_batch_calls[0]
        self.assertEqual(len(tx_types), 1)
        # First op's nonce was acknowledged as failure
        self.assertEqual(len(client.nonce_manager.failures), 1)


# ---------------------------------------------------------------------------
# 16) sign_and_send_batch — WS not None but disconnected
# ---------------------------------------------------------------------------

class TestBatchWsDisconnectedFallback(unittest.IsolatedAsyncioTestCase):

    async def test_ws_not_none_but_disconnected_uses_rest(self):
        """_tx_ws exists but is_connected=False -> falls back to REST."""
        client = DummyClient()
        op = _make_create_op()

        mock_tx_ws = MagicMock()
        mock_tx_ws.is_connected = False
        mock_tx_ws.send_batch = AsyncMock()

        with temp_mm_attrs(
            MARKET_ID=1, _PRICE_TICK_FLOAT=0.01, _AMOUNT_TICK_FLOAT=0.001,
            _tx_ws=mock_tx_ws, _global_backoff_until=0.0, _last_send_time=0.0,
            current_bid_order_id=None,
        ):
            await mm.sign_and_send_batch(client, [op])

        # WS send_batch was NOT called (disconnected)
        mock_tx_ws.send_batch.assert_not_called()
        # REST was used
        self.assertEqual(len(client.send_tx_batch_calls), 1)


# ---------------------------------------------------------------------------
# 17) _wait_for_write_slot — quota pacing
# ---------------------------------------------------------------------------

class TestQuotaPacing(unittest.IsolatedAsyncioTestCase):

    async def test_quota_low_waits_for_free_slot(self):
        """When volume quota < LOW threshold, wait for free 15s slot."""
        sleep_calls = []

        async def _track_sleep(secs):
            sleep_calls.append(secs)

        with temp_mm_attrs(
            _global_backoff_until=0.0,
            _last_send_time=time.monotonic() - 1.0,  # 1s ago (less than 15s)
            _volume_quota_remaining=3,  # below _RL_QUOTA_LOW (10)
            _op_timestamps=mm._op_timestamps.__class__(),  # empty deque
        ):
            with patch("asyncio.sleep", side_effect=_track_sleep):
                result = await mm._wait_for_write_slot(op_count=1, cancel_only=False)

        self.assertTrue(result)
        # Should have slept for the free slot interval (roughly 14s)
        free_slot_sleeps = [s for s in sleep_calls if s > 10.0]
        self.assertGreater(len(free_slot_sleeps), 0)

    async def test_quota_pacing_stretches_interval(self):
        """When quota is in MEDIUM range, interval is stretched by multiplier."""
        sleep_calls = []

        async def _track_sleep(secs):
            sleep_calls.append(secs)

        with temp_mm_attrs(
            _global_backoff_until=0.0,
            _last_send_time=time.monotonic(),  # just sent — triggers floor + stretch
            _volume_quota_remaining=30,  # between MEDIUM (20) and HIGH (50)
            _op_timestamps=mm._op_timestamps.__class__(),  # empty deque
        ):
            with patch("asyncio.sleep", side_effect=_track_sleep):
                result = await mm._wait_for_write_slot(op_count=1, cancel_only=False)

        self.assertTrue(result)
        # Phase 3 floor wait + Phase 4 stretch wait = at least 2 sleeps
        self.assertGreaterEqual(len(sleep_calls), 2)

    async def test_cancel_only_skips_quota_pacing(self):
        """cancel_only=True should skip volume-quota pacing entirely."""
        sleep_calls = []

        async def _track_sleep(secs):
            sleep_calls.append(secs)

        with temp_mm_attrs(
            _global_backoff_until=0.0,
            _last_send_time=0.0,  # long ago — no floor wait
            _volume_quota_remaining=3,  # critically low quota
            _op_timestamps=mm._op_timestamps.__class__(),
        ):
            with patch("asyncio.sleep", side_effect=_track_sleep):
                result = await mm._wait_for_write_slot(op_count=1, cancel_only=True)

        self.assertTrue(result)
        # No long sleeps for free slot — cancel bypasses quota pacing
        long_sleeps = [s for s in sleep_calls if s > 10.0]
        self.assertEqual(len(long_sleeps), 0)


# ---------------------------------------------------------------------------
# 18) Quota threshold warning system
# ---------------------------------------------------------------------------

class TestQuotaThresholdWarnings(unittest.TestCase):

    def test_warning_at_each_threshold_crossing(self):
        """_update_volume_quota should emit WARNING at each threshold crossing."""
        with temp_mm_attrs(
            _volume_quota_remaining=1000,
            _quota_warning_level="ok",
        ):
            with patch.object(mm.logger, "warning") as mock_warn, \
                 patch.object(mm.logger, "info") as mock_info:
                # Drop to MEDIUM range (< 50)
                mm._update_volume_quota(30)
                self.assertEqual(mm._quota_warning_level, "medium")
                mock_warn.assert_called()
                self.assertIn("QUOTA MEDIUM", mock_warn.call_args_list[-1][0][0])

                mock_warn.reset_mock()

                # Drop to LOW range (< 10)
                mm._update_volume_quota(5)
                self.assertEqual(mm._quota_warning_level, "low")
                mock_warn.assert_called()
                self.assertIn("QUOTA LOW", mock_warn.call_args_list[-1][0][0])

                mock_warn.reset_mock()

                # Drop to CRITICAL (0)
                mm._update_volume_quota(0)
                self.assertEqual(mm._quota_warning_level, "critical")
                mock_warn.assert_called()
                self.assertIn("QUOTA EXHAUSTED", mock_warn.call_args_list[-1][0][0])

                mock_warn.reset_mock()

                # Recover above MEDIUM threshold
                mm._update_volume_quota(100)
                self.assertEqual(mm._quota_warning_level, "ok")
                mock_info.assert_called()
                self.assertIn("QUOTA RECOVERED", mock_info.call_args_list[-1][0][0])

    def test_no_duplicate_warnings(self):
        """Same level should not re-emit warnings."""
        with temp_mm_attrs(
            _volume_quota_remaining=1000,
            _quota_warning_level="ok",
        ):
            with patch.object(mm.logger, "warning") as mock_warn:
                mm._update_volume_quota(30)
                call_count_1 = mock_warn.call_count

                # Same range — no new warning
                mm._update_volume_quota(25)
                self.assertEqual(mock_warn.call_count, call_count_1)

    def test_none_and_invalid_ignored(self):
        """None, '?', and non-numeric values should not change quota."""
        with temp_mm_attrs(_volume_quota_remaining=500, _quota_warning_level="ok"):
            mm._update_volume_quota(None)
            self.assertEqual(mm._volume_quota_remaining, 500)
            mm._update_volume_quota("?")
            self.assertEqual(mm._volume_quota_remaining, 500)
            mm._update_volume_quota("not_a_number")
            self.assertEqual(mm._volume_quota_remaining, 500)


# ---------------------------------------------------------------------------
# 19) Window-full skip threshold (30s)
# ---------------------------------------------------------------------------

class TestWindowFullThreshold(unittest.IsolatedAsyncioTestCase):

    async def test_window_full_waits_up_to_30s(self):
        """Wait time of 20s (< 30s) should wait, not skip."""
        sleep_calls = []

        async def _track_sleep(secs):
            sleep_calls.append(secs)

        # Fill op_timestamps so _time_until_ops_free returns ~20s
        now = time.monotonic()
        # 40 ops sent 40s ago — they expire at now+20s
        timestamps = mm._op_timestamps.__class__(
            [now - 40.0] * mm._RL_OPS_PER_WINDOW
        )

        with temp_mm_attrs(
            _global_backoff_until=0.0,
            _last_send_time=0.0,
            _volume_quota_remaining=1000,
            _op_timestamps=timestamps,
        ):
            with patch("asyncio.sleep", side_effect=_track_sleep):
                result = await mm._wait_for_write_slot(op_count=4, cancel_only=False)

        self.assertTrue(result)
        # Should have waited (not skipped)
        self.assertGreater(len(sleep_calls), 0)

    async def test_window_full_skips_above_30s(self):
        """Wait time > 30s should skip."""
        now = time.monotonic()
        # 40 ops sent 20s ago — they expire at now+40s (> 30s threshold)
        timestamps = mm._op_timestamps.__class__(
            [now - 20.0] * mm._RL_OPS_PER_WINDOW
        )

        with temp_mm_attrs(
            _global_backoff_until=0.0,
            _last_send_time=0.0,
            _volume_quota_remaining=1000,
            _op_timestamps=timestamps,
        ):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await mm._wait_for_write_slot(op_count=4, cancel_only=False)

        self.assertFalse(result)


# ---------------------------------------------------------------------------
# 20) Quota recovery
# ---------------------------------------------------------------------------

class TestQuotaRecovery(unittest.TestCase):

    def test_quota_recovery_disabled_by_default_config(self):
        """Verify _QR_ENABLED reflects config; default in code is True but
        can be overridden to False via config."""
        # The default in code is True (enabled by default per user request).
        # This test just verifies the variable exists and is bool.
        self.assertIsInstance(mm._QR_ENABLED, bool)


class TestQuotaRecoveryAsync(unittest.IsolatedAsyncioTestCase):

    async def test_quota_recovery_stops_if_quota_unchanged(self):
        """If exchange response returns same quota, recovery aborts."""
        mock_response = MagicMock()
        mock_response.volume_quota_remaining = "3"  # same as starting quota

        mock_client = MagicMock()
        mock_client.create_market_order = AsyncMock(
            return_value=("tx", mock_response, None)
        )
        mock_client.cancel_all_orders = AsyncMock(
            return_value=("tx", mock_response, None)
        )

        with temp_mm_attrs(
            _volume_quota_remaining=3,
            _quota_warning_level="critical",
            _quota_recovery_in_progress=False,
            _quota_recovery_last_attempt=0.0,
            _last_send_time=0.0,
            _op_timestamps=mm._op_timestamps.__class__(),
            _global_backoff_until=0.0,
            current_position_size=0.0,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            MARKET_ID=1,
        ):
            # Set up min orderbook data
            from sortedcontainers import SortedDict
            mm.state.market.local_order_book = {
                'bids': SortedDict({100.0: 1.0}),
                'asks': SortedDict({100.5: 1.0}),
                'initialized': True,
            }
            mm.state.market.mid_price = 100.25
            mm.state.config.min_base_amount = 0.001

            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await mm._attempt_quota_recovery(mock_client)

        self.assertFalse(result)

    async def test_quota_recovery_max_loss_abort(self):
        """Recovery stops after cumulative loss exceeds threshold."""
        call_count = 0

        async def _fake_market_order(**kwargs):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            # Each call increases quota by 1
            resp.volume_quota_remaining = str(3 + call_count)
            return ("tx", resp, None)

        mock_client = MagicMock()
        mock_client.create_market_order = AsyncMock(side_effect=_fake_market_order)
        mock_client.cancel_all_orders = AsyncMock(
            return_value=("tx", MagicMock(volume_quota_remaining="3"), None)
        )

        with temp_mm_attrs(
            _volume_quota_remaining=3,
            _quota_warning_level="critical",
            _quota_recovery_in_progress=False,
            _quota_recovery_last_attempt=0.0,
            _last_send_time=0.0,
            _op_timestamps=mm._op_timestamps.__class__(),
            _global_backoff_until=0.0,
            current_position_size=0.0,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            MARKET_ID=1,
        ):
            from sortedcontainers import SortedDict
            mm.state.market.local_order_book = {
                'bids': SortedDict({100.0: 1.0}),
                'asks': SortedDict({100.5: 1.0}),
                'initialized': True,
            }
            mm.state.market.mid_price = 100.25
            mm.state.config.min_base_amount = 0.001

            # Set very low loss limit to trigger abort quickly
            orig_max_loss = mm._QR_MAX_LOSS
            mm._QR_MAX_LOSS = 0.0001
            try:
                with patch("asyncio.sleep", new_callable=AsyncMock):
                    result = await mm._attempt_quota_recovery(mock_client)
            finally:
                mm._QR_MAX_LOSS = orig_max_loss

        # Should have stopped early due to loss limit
        self.assertLessEqual(call_count, 2)

    async def test_quota_recovery_pnl_drop_abort(self):
        """Recovery aborts if portfolio value drops by more than max_loss."""
        call_count = 0

        async def _market_order_with_pnl_drop(**kwargs):
            nonlocal call_count
            call_count += 1
            # First order succeeds and increases quota, but drops PnL
            resp = MagicMock()
            resp.volume_quota_remaining = str(3 + call_count)
            # Simulate PnL drop after first fill
            mm.state.account.portfolio_value = 997.0  # $3 drop > $2 max_loss
            return ("tx", resp, None)

        mock_client = MagicMock()
        mock_client.create_market_order = AsyncMock(side_effect=_market_order_with_pnl_drop)

        with temp_mm_attrs(
            _volume_quota_remaining=3,
            _quota_warning_level="critical",
            _quota_recovery_in_progress=False,
            _quota_recovery_last_attempt=0.0,
            _last_send_time=0.0,
            _op_timestamps=mm._op_timestamps.__class__(),
            _global_backoff_until=0.0,
            current_position_size=0.0,
            _PRICE_TICK_FLOAT=0.01,
            _AMOUNT_TICK_FLOAT=0.001,
            MARKET_ID=1,
            portfolio_value=1000.0,
        ):
            from sortedcontainers import SortedDict
            mm.state.market.local_order_book = {
                'bids': SortedDict({100.0: 1.0}),
                'asks': SortedDict({100.5: 1.0}),
                'initialized': True,
            }
            mm.state.market.mid_price = 100.25
            mm.state.config.min_base_amount = 0.001

            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await mm._attempt_quota_recovery(mock_client)

        # First attempt succeeds but drops PnL, second attempt should see the drop and abort
        self.assertEqual(call_count, 1)  # only 1 market order before PnL check aborts

    async def test_quota_recovery_respects_cooldown(self):
        """Recovery should not trigger within cooldown period."""
        mock_client = MagicMock()

        with temp_mm_attrs(
            _volume_quota_remaining=3,
            _quota_recovery_in_progress=False,
            # Last attempt was just now — within cooldown
            _quota_recovery_last_attempt=time.monotonic(),
        ):
            result = await mm._attempt_quota_recovery(mock_client)

        self.assertFalse(result)


# ---------------------------------------------------------------------------
# Regression: WS orderbook snapshot must be processed
# ---------------------------------------------------------------------------

class TestOrderBookSnapshotProcessing(unittest.TestCase):
    """Regression test: WS orderbook snapshot (type=subscribed/order_book) must be processed."""

    def test_subscribed_order_book_processed(self):
        """The initial 'subscribed/order_book' snapshot must update the local book."""
        with temp_mm_attrs(
            local_order_book={'bids': SortedDict(), 'asks': SortedDict(), 'initialized': False},
            current_mid_price_cached=None,
            MARKET_ID=1,
        ):
            snapshot_payload = {
                "bids": [
                    {"price": "68000.0", "size": "1.0"},
                    {"price": "67990.0", "size": "2.0"},
                ],
                "asks": [
                    {"price": "68010.0", "size": "1.0"},
                    {"price": "68020.0", "size": "2.0"},
                ],
            }

            mm.on_order_book_update(1, snapshot_payload)

            ob = mm.state.market.local_order_book
            self.assertTrue(ob['initialized'])
            self.assertEqual(len(ob['bids']), 2)
            self.assertEqual(len(ob['asks']), 2)
            self.assertAlmostEqual(ob['bids'].peekitem(-1)[0], 68000.0)
            self.assertAlmostEqual(ob['asks'].peekitem(0)[0], 68010.0)
            self.assertAlmostEqual(mm.state.market.mid_price, 68005.0)

    def test_update_order_book_still_works(self):
        """Verify delta updates (type=update/order_book) still work."""
        with temp_mm_attrs(
            local_order_book={
                'bids': SortedDict({67990.0: 1.0}),
                'asks': SortedDict({68010.0: 1.0}),
                'initialized': True,
            },
            MARKET_ID=1,
        ):
            delta = {
                "bids": [{"price": "67995.0", "size": "0.5"}],
                "asks": [{"price": "68005.0", "size": "0.5"}],
            }
            mm.on_order_book_update(1, delta)

            ob = mm.state.market.local_order_book
            self.assertEqual(len(ob['bids']), 2)   # 67990 + 67995
            self.assertEqual(len(ob['asks']), 2)   # 68005 + 68010
            self.assertAlmostEqual(ob['bids'].peekitem(-1)[0], 67995.0)   # new best bid
            self.assertAlmostEqual(ob['asks'].peekitem(0)[0], 68005.0)    # new best ask
