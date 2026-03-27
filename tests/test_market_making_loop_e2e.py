"""End-to-end tests for market_making_loop() with a fully mocked exchange.

Covers multiple quoting cycles, WS health transitions, calculator warm-up
gating, mid-price None handling, and base-amount validation.
"""

import asyncio
import unittest
from unittest.mock import patch, MagicMock

import market_maker_v2 as mm
from _helpers import DummyClient, temp_mm_attrs


class _VolObiStub:
    """Minimal stub for VolObiCalculator that returns deterministic quotes."""

    def __init__(self, warmed=True, spread_bps=10.0):
        self._warmed = warmed
        self._spread_bps = spread_bps
        self.quote_calls = []

    @property
    def warmed_up(self):
        return self._warmed

    def quote(self, mid_price, position_size):
        self.quote_calls.append((mid_price, position_size))
        half = mid_price * self._spread_bps / 10000
        return mid_price - half, mid_price + half

    def reset(self):
        pass

    def on_book_update(self, mid, bids, asks):
        pass

    def set_alpha_override(self, _):
        pass

    def set_max_position_dollar(self, _):
        pass


class TestMarketMakingLoopE2E(unittest.IsolatedAsyncioTestCase):
    """Full mocked loop: WS healthy -> quote -> refresh both sides."""

    async def test_happy_path_two_quoting_cycles(self):
        """Loop runs two cycles, placing orders on both sides each time."""
        client = DummyClient()
        calc = _VolObiStub(warmed=True, spread_bps=10.0)
        iteration = 0
        _real_sleep = asyncio.sleep  # save before patching

        original_risk = mm.RiskState(**vars(mm.state.risk))

        async def _fake_wait_for(coro, *, timeout=None):
            nonlocal iteration
            await _real_sleep(0)  # yield so background send tasks can run
            iteration += 1
            if iteration > 2:
                raise KeyboardInterrupt

        async def _noop_sleep(seconds):
            pass  # bg tasks won't sleep with _RL_MIN_SEND_INTERVAL=0

        try:
            mm.state.risk = mm.RiskState()
            mm.risk_controller = mm.RiskController(mm.state.risk)
            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
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
                vol_obi_calc=calc,
                QUOTE_UPDATE_THRESHOLD_BPS=0.0,
                MIN_LOOP_INTERVAL=0.0,
                ORDER_TIMEOUT=0.01,
                WARMUP_SECONDS=0,
                _last_send_time=0.0,
                _global_backoff_until=0.0,
                _RL_MIN_SEND_INTERVAL=0.0,
                _send_task=None,
            ):
                with patch.object(mm.asyncio, "wait_for", side_effect=_fake_wait_for):
                    with patch.object(mm.asyncio, "sleep", side_effect=_noop_sleep):
                        with self.assertRaises(KeyboardInterrupt):
                            await mm.market_making_loop(client)
                    # Let pending background send complete (sleep mock removed)
                    if mm._send_task is not None and not mm._send_task.done():
                        await mm._send_task
                    mm._send_task = None

            # Calculator should have been called at least once
            self.assertGreater(len(calc.quote_calls), 0)
            # Orders placed via batch (sign_create_order for bid + ask)
            self.assertGreaterEqual(len(client.sign_create_calls), 2)

            # Verify both buy and sell were placed
            sides_placed = [c["is_ask"] for c in client.sign_create_calls]
            self.assertIn(False, sides_placed)  # buy
            self.assertIn(True, sides_placed)   # sell
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    async def test_mid_price_none_skips_quoting(self):
        """When mid_price is None, the loop should skip quoting and sleep."""
        client = DummyClient()
        slept = []

        original_risk = mm.RiskState(**vars(mm.state.risk))

        async def _fake_wait_for(coro, *, timeout=None):
            await asyncio.sleep(0)

        async def _track_sleep(seconds):
            slept.append(seconds)
            if len(slept) >= 2:
                raise KeyboardInterrupt

        try:
            mm.state.risk = mm.RiskState()
            mm.risk_controller = mm.RiskController(mm.state.risk)
            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
                current_mid_price_cached=None,
                ws_connection_healthy=True,
                last_order_book_update=1e18,
                available_capital=10000.0,
                MIN_LOOP_INTERVAL=0.01,
                ORDER_TIMEOUT=0.01,
                WARMUP_SECONDS=0,
            ):
                with patch.object(mm.asyncio, "wait_for", side_effect=_fake_wait_for):
                    with patch.object(mm.asyncio, "sleep", side_effect=_track_sleep):
                        with self.assertRaises(KeyboardInterrupt):
                            await mm.market_making_loop(client)

            # No orders should have been placed
            self.assertEqual(len(client.create_order_calls), 0)
            self.assertEqual(len(client.modify_order_calls), 0)
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    async def test_calculator_not_warmed_skips_quoting(self):
        """When vol_obi calculator is not warmed up, no orders should be placed."""
        client = DummyClient()
        calc = _VolObiStub(warmed=False)
        iteration = 0

        original_risk = mm.RiskState(**vars(mm.state.risk))

        async def _fake_wait_for(coro, *, timeout=None):
            await asyncio.sleep(0)

        async def _controlled_sleep(seconds):
            nonlocal iteration
            iteration += 1
            if iteration >= 2:
                raise KeyboardInterrupt

        try:
            mm.state.risk = mm.RiskState()
            mm.risk_controller = mm.RiskController(mm.state.risk)
            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
                current_mid_price_cached=50000.0,
                ws_connection_healthy=True,
                last_order_book_update=1e18,
                available_capital=10000.0,
                current_position_size=0.0,
                vol_obi_calc=calc,
                MIN_LOOP_INTERVAL=0.0,
                ORDER_TIMEOUT=0.01,
                WARMUP_SECONDS=0,
            ):
                with patch.object(mm.asyncio, "wait_for", side_effect=_fake_wait_for):
                    with patch.object(mm.asyncio, "sleep", side_effect=_controlled_sleep):
                        with self.assertRaises(KeyboardInterrupt):
                            await mm.market_making_loop(client)

            # Calculator was not consulted because it's not warmed up
            self.assertEqual(len(calc.quote_calls), 0)
            # No orders placed
            self.assertEqual(len(client.create_order_calls), 0)
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    async def test_ws_unhealthy_triggers_reconnect_event(self):
        """When WS is unhealthy the hot loop should trigger the reconnect event."""
        client = DummyClient()
        slept = []

        original_risk = mm.RiskState(**vars(mm.state.risk))

        async def _controlled_sleep(seconds):
            slept.append(seconds)
            if len(slept) >= 1:
                raise KeyboardInterrupt

        try:
            mm.state.risk = mm.RiskState()
            mm.risk_controller = mm.RiskController(mm.state.risk)
            mm.ws_reconnect_event.clear()
            with temp_mm_attrs(
                ws_connection_healthy=False,
                current_mid_price_cached=50000.0,
                last_order_book_update=0.0,
                MIN_LOOP_INTERVAL=0.0,
                WARMUP_SECONDS=0,
            ):
                with patch.object(mm.asyncio, "sleep", side_effect=_controlled_sleep):
                    with self.assertRaises(KeyboardInterrupt):
                        await mm.market_making_loop(client)

            self.assertTrue(mm.ws_reconnect_event.is_set())
            self.assertEqual(len(client.create_order_calls), 0)
        finally:
            mm.ws_reconnect_event.clear()
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    async def test_zero_base_amount_skips_quoting(self):
        """When calculate_dynamic_base_amount returns 0, no orders should be placed."""
        client = DummyClient()
        calc = _VolObiStub(warmed=True, spread_bps=10.0)
        slept = []

        original_risk = mm.RiskState(**vars(mm.state.risk))

        async def _fake_wait_for(coro, *, timeout=None):
            await asyncio.sleep(0)

        async def _track_sleep(seconds):
            slept.append(seconds)
            if len(slept) >= 2:
                raise KeyboardInterrupt

        try:
            mm.state.risk = mm.RiskState()
            mm.risk_controller = mm.RiskController(mm.state.risk)
            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
                current_mid_price_cached=50000.0,
                ws_connection_healthy=True,
                last_order_book_update=1e18,
                available_capital=0.0,
                current_position_size=0.0,
                vol_obi_calc=calc,
                MIN_LOOP_INTERVAL=0.01,
                ORDER_TIMEOUT=0.01,
                CAPITAL_USAGE_PERCENT=0.12,
                WARMUP_SECONDS=0,
            ):
                with patch.object(mm, "calculate_dynamic_base_amount", return_value=0):
                    with patch.object(mm.asyncio, "wait_for", side_effect=_fake_wait_for):
                        with patch.object(mm.asyncio, "sleep", side_effect=_track_sleep):
                            with self.assertRaises(KeyboardInterrupt):
                                await mm.market_making_loop(client)

            self.assertEqual(len(client.create_order_calls), 0)
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    async def test_loop_modifies_existing_orders_on_second_cycle(self):
        """On the second cycle, existing orders should be modified (not placed fresh)."""
        client = DummyClient(modify_err=None)
        calc = _VolObiStub(warmed=True, spread_bps=10.0)
        cycle = 0
        _real_sleep = asyncio.sleep  # save before patching

        original_risk = mm.RiskState(**vars(mm.state.risk))
        original_orders = mm.OrderState(**vars(mm.state.orders))

        try:
            mm.state.risk = mm.RiskState()
            mm.risk_controller = mm.RiskController(mm.state.risk)
            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
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
                vol_obi_calc=calc,
                QUOTE_UPDATE_THRESHOLD_BPS=0.0,
                MIN_LOOP_INTERVAL=0.0,
                ORDER_TIMEOUT=0.01,
                WARMUP_SECONDS=0,
                _last_send_time=0.0,
                _global_backoff_until=0.0,
                _RL_MIN_SEND_INTERVAL=0.0,
                _send_task=None,
            ):
                # Change mid_price between cycles to force modify
                orig_mid = mm.state.market.mid_price
                call_count = [0]

                async def _cycle_wait_for(coro, *, timeout=None):
                    nonlocal call_count, cycle
                    await _real_sleep(0)  # yield so background send tasks can run
                    call_count[0] += 1
                    cycle += 1
                    if cycle >= 8:
                        raise KeyboardInterrupt
                    # Shift mid_price each cycle so price changes exceed threshold
                    mm.state.market.mid_price = 50000.0 + call_count[0] * 100
                    bid_id = mm.state.orders.bid_order_ids[0]
                    ask_id = mm.state.orders.ask_order_ids[0]
                    if bid_id is not None and bid_id not in mm._client_to_exchange_id:
                        mm._client_to_exchange_id[bid_id] = 1000
                    if ask_id is not None and ask_id not in mm._client_to_exchange_id:
                        mm._client_to_exchange_id[ask_id] = 2000

                async def _noop_sleep(seconds):
                    pass  # bg tasks won't sleep with _RL_MIN_SEND_INTERVAL=0

                with patch.object(mm.asyncio, "wait_for", side_effect=_cycle_wait_for):
                    with patch.object(mm.asyncio, "sleep", side_effect=_noop_sleep):
                        with self.assertRaises(KeyboardInterrupt):
                            await mm.market_making_loop(client)
                    # Let pending background send complete (sleep mock removed)
                    if mm._send_task is not None and not mm._send_task.done():
                        await mm._send_task
                    mm._send_task = None

            # First cycle: sign_create_order (batch) for bid+ask
            # Second+ cycles: sign_modify_order (since orders now exist)
            self.assertGreaterEqual(len(client.sign_create_calls), 2)
            self.assertGreater(len(client.sign_modify_calls), 0)
        finally:
            mm.state.orders = original_orders
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)
            mm.order_manager = mm.OrderManager(mm.state.order_manager)

    async def test_unhandled_exception_doesnt_crash_loop(self):
        """An exception in calculate_order_prices should be caught and loop continues."""
        client = DummyClient()
        error_count = [0]

        original_risk = mm.RiskState(**vars(mm.state.risk))

        async def _fake_wait_for(coro, *, timeout=None):
            await asyncio.sleep(0)

        async def _controlled_sleep(seconds):
            nonlocal error_count
            error_count[0] += 1
            if error_count[0] >= 2:
                raise KeyboardInterrupt

        def _exploding_calculate(*args, **kwargs):
            raise RuntimeError("boom")

        try:
            mm.state.risk = mm.RiskState()
            mm.risk_controller = mm.RiskController(mm.state.risk)
            with temp_mm_attrs(
                MARKET_ID=1,
                _PRICE_TICK_FLOAT=0.1,
                _AMOUNT_TICK_FLOAT=0.00001,
                current_mid_price_cached=50000.0,
                ws_connection_healthy=True,
                last_order_book_update=1e18,
                available_capital=10000.0,
                current_position_size=0.0,
                MIN_LOOP_INTERVAL=0.0,
                ORDER_TIMEOUT=0.01,
                WARMUP_SECONDS=0,
            ):
                with patch.object(mm, "calculate_order_prices", side_effect=_exploding_calculate):
                    with patch.object(mm.asyncio, "wait_for", side_effect=_fake_wait_for):
                        with patch.object(mm.asyncio, "sleep", side_effect=_controlled_sleep):
                            with self.assertRaises(KeyboardInterrupt):
                                await mm.market_making_loop(client)

            # Loop survived the exception (iterated again before KeyboardInterrupt)
            self.assertGreaterEqual(error_count[0], 2)
            # No orders placed because of the exception
            self.assertEqual(len(client.create_order_calls), 0)
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)
