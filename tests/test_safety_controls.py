import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import market_maker_v2 as mm
from _helpers import DummyClient, temp_mm_attrs


class TestSafetyControls(unittest.IsolatedAsyncioTestCase):
    def test_reconcile_clears_missing_local_orders(self):
        original_risk = mm.RiskState(**vars(mm.state.risk))
        try:
            with temp_mm_attrs(
                current_bid_order_id=111,
                current_bid_price=100.0,
                current_bid_size=0.1,
                current_ask_order_id=222,
                current_ask_price=101.0,
                current_ask_size=0.1,
            ):
                ok, unknown_ids = mm._reconcile_local_orders_with_remote_orders([], source="unit")
                self.assertFalse(ok)
                self.assertIsNone(mm.current_bid_order_id)
                self.assertIsNone(mm.current_ask_order_id)
                self.assertFalse(mm.state.risk.last_reconcile_ok)
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    def test_reconcile_cancels_untracked_remote_orders(self):
        """Untracked remote orders are flagged as unknown (not rebound)."""
        original_risk = mm.RiskState(**vars(mm.state.risk))
        try:
            with temp_mm_attrs(
                current_bid_order_id=None,
                current_bid_price=None,
                current_bid_size=None,
                current_ask_order_id=None,
                current_ask_price=None,
                current_ask_size=None,
            ):
                remote = [
                    {
                        "order_index": 501,
                        "client_order_index": 501,
                        "is_ask": False,
                        "price": "100.5",
                        "remaining_base_amount": "0.25",
                    },
                    {
                        "order_index": 601,
                        "client_order_index": 601,
                        "is_ask": True,
                        "price": "101.5",
                        "remaining_base_amount": "0.25",
                    },
                ]
                ok, unknown_ids = mm._reconcile_local_orders_with_remote_orders(remote, source="unit")
                self.assertFalse(ok)
                # Orders should NOT be rebound — they should be unknown
                self.assertIsNone(mm.state.orders.bid_order_ids[0])
                self.assertIsNone(mm.state.orders.ask_order_ids[0])
                # Both exchange order_indexes should be in unknown_ids for cancellation
                self.assertEqual(unknown_ids, {501, 601})
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    def test_circuit_breaker_triggers_after_threshold(self):
        rc = mm.RiskController(mm.RiskState())
        with temp_mm_attrs(MAX_CONSECUTIVE_ORDER_REJECTIONS=2, CIRCUIT_BREAKER_COOLDOWN_SEC=10.0):
            rc.record_rejection("reject-1")
            self.assertFalse(rc.is_paused())
            rc.record_rejection("reject-2")
            self.assertTrue(rc.is_paused())

    async def test_confirm_absent_timeout_zero_short_circuits(self):
        result = await mm._confirm_order_absent_on_exchange(None, 123, 0)
        self.assertTrue(result)

    async def test_reconcile_fetch_failure_marks_risk(self):
        original_risk = mm.RiskState(**vars(mm.state.risk))
        try:
            with patch.object(mm, "_fetch_account_active_orders", return_value=None):
                ok = await mm.reconcile_orders_with_exchange(
                    client=None,
                    market_id=1,
                    account_id=2,
                    source="unit",
                )
            self.assertFalse(ok)
            self.assertFalse(mm.state.risk.last_reconcile_ok)
            self.assertEqual(mm.state.risk.last_reconcile_reason, "unit:fetch_failed")
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    async def test_stale_reconciler_triggers_pause_after_debounce(self):
        original_risk = mm.RiskState(**vars(mm.state.risk))
        try:
            async def _always_mismatch(*args, **kwargs):
                mm.risk_controller.mark_reconcile(ok=False, reason="unit_mismatch")
                return False

            with temp_mm_attrs(
                STALE_ORDER_POLLER_INTERVAL_SEC=0.01,
                STALE_ORDER_DEBOUNCE_COUNT=2,
                CIRCUIT_BREAKER_COOLDOWN_SEC=5.0,
            ):
                with patch.object(mm, "reconcile_orders_with_exchange", side_effect=_always_mismatch):
                    task = asyncio.create_task(mm.stale_order_reconciler_loop(None, 1, 1))
                    await asyncio.sleep(1.3)
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task

            self.assertTrue(mm.risk_controller.is_paused())
            self.assertIn("order reconciliation mismatch", mm.state.risk.pause_reason)
        finally:
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    async def test_subscribe_to_account_orders_returns_when_auth_unavailable(self):
        with patch.object(mm, "_generate_ws_auth_token", return_value=None):
            with patch.object(mm, "ws_subscribe") as ws_subscribe_mock:
                await mm.subscribe_to_account_orders(object(), market_id=1, account_id=123)
        ws_subscribe_mock.assert_not_called()

    async def test_market_making_loop_paused_schedules_cleanup_once(self):
        original_pause_cleanup_running = mm._pause_cleanup_running
        scheduled = []

        async def _interrupt_sleep(_seconds):
            raise KeyboardInterrupt

        def _record_task(coro):
            scheduled.append(coro)
            coro.close()
            return SimpleNamespace(cancel=lambda: None, done=lambda: True)

        client = DummyClient()

        try:
            with temp_mm_attrs(
                MIN_LOOP_INTERVAL=0.0,
                WARMUP_SECONDS=0,
            ):
                mm.state.risk.pause_cancel_done = False
                mm._pause_cleanup_running = False

                with patch.object(mm, "_pause_cleanup_task") as cleanup_task_mock, \
                     patch.object(mm.asyncio, "create_task", side_effect=_record_task) as create_task_mock, \
                     patch.object(mm, "check_websocket_health", return_value=True), \
                     patch.object(mm.risk_controller, "maybe_recover", return_value=False), \
                     patch.object(mm.risk_controller, "is_paused", return_value=True), \
                     patch.object(mm.asyncio, "sleep", side_effect=_interrupt_sleep):
                    with self.assertRaises(KeyboardInterrupt):
                        await mm.market_making_loop(client)

            cleanup_task_mock.assert_called_once_with(client)
            self.assertEqual(create_task_mock.call_count, 1)
            self.assertEqual(len(scheduled), 1)
            self.assertFalse(mm.state.risk.pause_cancel_done)
        finally:
            mm._pause_cleanup_running = original_pause_cleanup_running

    async def test_pause_cleanup_task_cancels_orders_once(self):
        original_risk = mm.RiskState(**vars(mm.state.risk))
        original_pause_cleanup_running = mm._pause_cleanup_running
        batch_calls = []

        async def _record_batch(_client, ops):
            batch_calls.append(ops)

        async def _clear_on_reconcile(*a, **kw):
            mm.order_manager.clear_all()
            return True

        async def _mock_wait_for_write(op_count=4, cancel_only=False):
            return True

        client = DummyClient()

        try:
            with temp_mm_attrs(
                current_bid_order_id=101,
                current_ask_order_id=202,
                current_bid_price=100.0,
                current_ask_price=101.0,
                current_bid_size=0.1,
                current_ask_size=0.1,
                _PRICE_TICK_FLOAT=0.01,
                _AMOUNT_TICK_FLOAT=0.001,
                MARKET_ID=1,
                MIN_LOOP_INTERVAL=0.0,
                WARMUP_SECONDS=0,
            ):
                mm.state.risk.pause_cancel_done = False
                mm._pause_cleanup_running = True
                mm._client_to_exchange_id[101] = 1001
                mm._client_to_exchange_id[202] = 2002

                with patch.object(mm, "sign_and_send_batch", side_effect=_record_batch), \
                     patch.object(mm, "reconcile_orders_with_exchange", side_effect=_clear_on_reconcile), \
                     patch.object(mm, "_wait_for_write_slot", side_effect=_mock_wait_for_write), \
                     patch.object(mm, "check_websocket_health", return_value=True):
                    await mm._pause_cleanup_task(client)

            # sign_and_send_batch was called with cancel ops for both sides
            self.assertEqual(len(batch_calls), 1)
            ops = batch_calls[0]
            self.assertTrue(all(op.action == "cancel" for op in ops))
            cancel_ids = {op.order_id for op in ops}
            self.assertIn(101, cancel_ids)
            self.assertIn(202, cancel_ids)
            self.assertTrue(mm.state.risk.pause_cancel_done)
        finally:
            mm._pause_cleanup_running = original_pause_cleanup_running
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)

    async def test_pause_cleanup_task_retries_when_reconcile_fails(self):
        original_risk = mm.RiskState(**vars(mm.state.risk))
        original_pause_cleanup_running = mm._pause_cleanup_running
        batch_calls = []

        async def _record_batch(_client, ops):
            batch_calls.append(ops)

        async def _failed_reconcile(*a, **kw):
            return False

        async def _mock_wait_for_write(op_count=4, cancel_only=False):
            return True

        client = DummyClient()

        try:
            with temp_mm_attrs(
                current_bid_order_id=101,
                current_ask_order_id=202,
                current_bid_price=100.0,
                current_ask_price=101.0,
                current_bid_size=0.1,
                current_ask_size=0.1,
                _PRICE_TICK_FLOAT=0.01,
                _AMOUNT_TICK_FLOAT=0.001,
                MARKET_ID=1,
                MIN_LOOP_INTERVAL=0.0,
                WARMUP_SECONDS=0,
            ):
                mm.state.risk.pause_cancel_done = False
                mm._pause_cleanup_running = True
                mm._client_to_exchange_id[101] = 1001
                mm._client_to_exchange_id[202] = 2002

                with patch.object(mm, "sign_and_send_batch", side_effect=_record_batch), \
                     patch.object(mm, "reconcile_orders_with_exchange", side_effect=_failed_reconcile), \
                     patch.object(mm, "_wait_for_write_slot", side_effect=_mock_wait_for_write), \
                     patch.object(mm, "check_websocket_health", return_value=True):
                    await mm._pause_cleanup_task(client)
                self.assertEqual(len(batch_calls), 1)
                self.assertFalse(mm.state.risk.pause_cancel_done)
                self.assertEqual(mm.current_bid_order_id, 101)
                self.assertEqual(mm.current_ask_order_id, 202)
        finally:
            mm._pause_cleanup_running = original_pause_cleanup_running
            mm.state.risk = original_risk
            mm.risk_controller = mm.RiskController(mm.state.risk)
