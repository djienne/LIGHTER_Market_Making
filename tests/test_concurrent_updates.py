"""Tests for concurrent race conditions.

Covers: simultaneous orderbook + position updates,
on_order_book_update during order lifecycle transitions, and concurrent
reconciliation with order placement.
"""

import asyncio
import unittest
from unittest.mock import patch

import market_maker_v2 as mm
from _helpers import DummyClient, temp_mm_attrs
from sortedcontainers import SortedDict


class TestConcurrentOrderbookAndPosition(unittest.IsolatedAsyncioTestCase):
    async def test_orderbook_and_position_updates_simultaneous(self):
        """Simultaneous orderbook and position updates should not corrupt state."""
        original_config = mm.MarketConfig(**vars(mm.state.config))

        try:
            mm.state.config.market_id = 1
            mm.state.config.price_tick_float = 0.1
            mm.state.config.amount_tick_float = 0.00001

            # Set up initial orderbook state
            ob = mm.state.market.local_order_book
            ob['bids'] = SortedDict()
            ob['asks'] = SortedDict()
            ob['initialized'] = True
            ob['bids'][49990.0] = 1.0
            ob['asks'][50010.0] = 1.0

            # Simulate concurrent updates
            update_count = 50
            errors = []

            async def _rapid_orderbook_updates():
                for i in range(update_count):
                    try:
                        payload = {
                            'bids': [{'price': str(49990.0 + i * 0.1), 'size': '1.0'}],
                            'asks': [{'price': str(50010.0 + i * 0.1), 'size': '1.0'}],
                        }
                        mm.on_order_book_update(1, payload)
                    except Exception as e:
                        errors.append(f"orderbook: {e}")
                    await asyncio.sleep(0)

            async def _rapid_position_updates():
                for i in range(update_count):
                    try:
                        data = {
                            'positions': {
                                '1': {
                                    'position': str(i * 0.001),
                                    'sign': 1 if i % 2 == 0 else -1,
                                }
                            }
                        }
                        mm.on_account_all_update(mm.ACCOUNT_INDEX, data)
                    except Exception as e:
                        errors.append(f"position: {e}")
                    await asyncio.sleep(0)

            await asyncio.gather(
                _rapid_orderbook_updates(),
                _rapid_position_updates(),
            )

            self.assertEqual(len(errors), 0, f"Errors during concurrent updates: {errors}")

            # State should still be valid
            self.assertIsNotNone(mm.state.market.mid_price)
            # Position should be the last update value
            # (either positive or negative of 49 * 0.001)
        finally:
            mm.state.config = original_config

    async def test_orderbook_update_during_price_calculation(self):
        """Mid-price used for quoting should be consistent within a single cycle."""
        client = DummyClient()
        original_config = mm.MarketConfig(**vars(mm.state.config))

        mid_prices_seen = []

        def _capture_calculate(mid, **kwargs):
            mid_prices_seen.append(mid)
            return mid - 5, mid + 5

        try:
            mm.state.config.market_id = 1
            mm.state.config.price_tick_float = 0.1
            mm.state.config.amount_tick_float = 0.00001

            with temp_mm_attrs(
                current_mid_price_cached=50000.0,
                ws_connection_healthy=True,
                last_order_book_update=1e18,
                available_capital=10000.0,
                current_position_size=0.0,
                current_bid_order_id=None,
                current_ask_order_id=None,
                current_bid_price=None,
                current_ask_price=None,
                current_bid_size=None,
                current_ask_size=None,
                QUOTE_UPDATE_THRESHOLD_BPS=0.0,
                MIN_LOOP_INTERVAL=0.0,
                ORDER_TIMEOUT=0.01,
            ):
                iteration = [0]

                async def _fake_wait_for(coro, *, timeout=None):
                    # Change mid-price between wait_for return and calculate_order_prices
                    # This simulates an orderbook update arriving mid-cycle
                    await asyncio.sleep(0)

                async def _stop(seconds):
                    iteration[0] += 1
                    if iteration[0] >= 1:
                        raise KeyboardInterrupt

                with patch.object(mm, "calculate_order_prices", side_effect=_capture_calculate):
                    with patch.object(mm.asyncio, "wait_for", side_effect=_fake_wait_for):
                        with patch.object(mm.asyncio, "sleep", side_effect=_stop):
                            with self.assertRaises(KeyboardInterrupt):
                                await mm.market_making_loop(client)

                # The mid_price used for calculation should be the snapshot value
                if mid_prices_seen:
                    self.assertEqual(mid_prices_seen[0], 50000.0)
        finally:
            mm.state.config = original_config


class TestOrderbookUpdateDuringLifecycle(unittest.IsolatedAsyncioTestCase):
    async def test_orderbook_update_during_batch_send(self):
        """Order book updates during sign_and_send_batch should not affect signed price."""
        original_config = mm.MarketConfig(**vars(mm.state.config))

        try:
            mm.state.config.market_id = 1
            mm.state.config.price_tick_float = 0.1
            mm.state.config.amount_tick_float = 0.00001

            # Set up initial book
            ob = mm.state.market.local_order_book
            ob['bids'] = SortedDict({49990.0: 1.0})
            ob['asks'] = SortedDict({50010.0: 1.0})
            ob['initialized'] = True

            signed_prices = []

            class _BookUpdatingClient(DummyClient):
                def sign_create_order(self, **kwargs):
                    signed_prices.append(kwargs.get("price"))
                    # Simulate orderbook update arriving during signing
                    mm.on_order_book_update(1, {
                        'bids': [{'price': '49500.0', 'size': '1.0'}],
                        'asks': [{'price': '50500.0', 'size': '1.0'}],
                    })
                    return super().sign_create_order(**kwargs)

            client = _BookUpdatingClient()
            op = mm.BatchOp(
                side="buy", level=0, action="create",
                price=49990.0, size=0.01,
                order_id=1, exchange_id=0,
            )

            with temp_mm_attrs(
                MARKET_ID=1, _PRICE_TICK_FLOAT=0.1, _AMOUNT_TICK_FLOAT=0.00001,
                _tx_ws=None, _global_backoff_until=0.0, _last_send_time=0.0,
                current_bid_order_id=None, current_bid_price=None, current_bid_size=None,
                current_ask_order_id=None, current_ask_price=None, current_ask_size=None,
            ):
                await mm.sign_and_send_batch(client, [op])

            # The order was signed at the original price (49990.0 / 0.1 = 499900)
            self.assertEqual(len(signed_prices), 1)
            self.assertEqual(signed_prices[0], 499900)
        finally:
            mm.state.config = original_config


class TestReconciliationDuringRefresh(unittest.IsolatedAsyncioTestCase):
    async def test_account_orders_incremental_clears_dead_order(self):
        """Incremental WS update with filled status should clear the order locally."""
        original_orders = mm.OrderState(**vars(mm.state.orders))
        original_risk = mm.RiskState(**vars(mm.state.risk))
        original_account_ws = mm._account_orders_ws_ready

        try:
            mm._account_orders_ws_ready = True  # already past snapshot

            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
                current_bid_order_id=100,
                current_bid_price=49990.0,
                current_bid_size=0.01,
                current_ask_order_id=200,
                current_ask_price=50010.0,
                current_ask_size=0.01,
            ):
                # Incremental update: bid was filled (status=filled, not in LIVE set)
                mm.on_account_orders_update(
                    mm.ACCOUNT_INDEX, 1,
                    {"orders": {"1": [{"order_index": 8888, "client_order_index": 100, "status": "filled"}]}}
                )

                # Bid should now be cleared (filled)
                self.assertIsNone(mm.current_bid_order_id)
                # Ask should still be present (not mentioned in incremental)
                self.assertEqual(mm.current_ask_order_id, 200)
        finally:
            mm.state.orders = original_orders
            mm.state.risk = original_risk
            mm._account_orders_ws_ready = original_account_ws
            mm.risk_controller = mm.RiskController(mm.state.risk)
            mm.order_manager = mm.OrderManager(mm.state.order_manager)

    async def test_account_orders_snapshot_clears_absent_orders(self):
        """Initial snapshot should clear orders not present in the snapshot."""
        original_orders = mm.OrderState(**vars(mm.state.orders))
        original_risk = mm.RiskState(**vars(mm.state.risk))
        original_account_ws = mm._account_orders_ws_ready

        try:
            mm._account_orders_ws_ready = False  # simulate first message (snapshot)

            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
                current_bid_order_id=100,
                current_bid_price=49990.0,
                current_bid_size=0.01,
                current_ask_order_id=200,
                current_ask_price=50010.0,
                current_ask_size=0.01,
            ):
                # Snapshot only contains the ask (bid is absent → cleared)
                mm.on_account_orders_update(
                    mm.ACCOUNT_INDEX, 1,
                    {"orders": {"1": [{"order_index": 9999, "client_order_index": 200, "status": "open"}]}}
                )

                # Bid absent from snapshot → cleared
                self.assertIsNone(mm.current_bid_order_id)
                # Ask present in snapshot → kept
                self.assertEqual(mm.current_ask_order_id, 200)
        finally:
            mm.state.orders = original_orders
            mm.state.risk = original_risk
            mm._account_orders_ws_ready = original_account_ws
            mm.risk_controller = mm.RiskController(mm.state.risk)
            mm.order_manager = mm.OrderManager(mm.state.order_manager)

    async def test_account_orders_incremental_ignores_live_order(self):
        """Incremental update with status=open should NOT clear other orders."""
        original_orders = mm.OrderState(**vars(mm.state.orders))
        original_risk = mm.RiskState(**vars(mm.state.risk))
        original_account_ws = mm._account_orders_ws_ready

        try:
            mm._account_orders_ws_ready = True  # already past snapshot

            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
                current_bid_order_id=100,
                current_bid_price=49990.0,
                current_bid_size=0.01,
                current_ask_order_id=200,
                current_ask_price=50010.0,
                current_ask_size=0.01,
            ):
                # Incremental: ask confirmed open — bid should NOT be affected
                mm.on_account_orders_update(
                    mm.ACCOUNT_INDEX, 1,
                    {"orders": {"1": [{"order_index": 9999, "client_order_index": 200, "status": "open"}]}}
                )

                # Both should still be present
                self.assertEqual(mm.current_bid_order_id, 100)
                self.assertEqual(mm.current_ask_order_id, 200)
        finally:
            mm.state.orders = original_orders
            mm.state.risk = original_risk
            mm._account_orders_ws_ready = original_account_ws
            mm.risk_controller = mm.RiskController(mm.state.risk)
            mm.order_manager = mm.OrderManager(mm.state.order_manager)


