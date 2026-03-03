"""Tests for order rejection recording and circuit breaker integration.

Tests the real production path: _record_order_rejection() → risk_controller.record_rejection()
→ circuit breaker threshold, and _record_order_success() → counter reset.
"""

import unittest

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestRecordOrderRejection(unittest.TestCase):
    def _fresh_risk(self):
        risk_state = mm.RiskState()
        rc = mm.RiskController(risk_state)
        return risk_state, rc

    def test_rejection_increments_counter(self):
        """Each _record_order_rejection call increments consecutive_rejections."""
        risk_state, rc = self._fresh_risk()
        orig_rc = mm.risk_controller
        orig_risk = mm.state.risk
        try:
            mm.risk_controller = rc
            mm.state.risk = risk_state
            mm._record_order_rejection("test:error1")
            self.assertEqual(risk_state.consecutive_rejections, 1)
            mm._record_order_rejection("test:error2")
            self.assertEqual(risk_state.consecutive_rejections, 2)
        finally:
            mm.risk_controller = orig_rc
            mm.state.risk = orig_risk

    def test_success_resets_rejection_counter(self):
        """_record_order_success resets consecutive_rejections to 0."""
        risk_state, rc = self._fresh_risk()
        orig_rc = mm.risk_controller
        orig_risk = mm.state.risk
        orig_backoff = mm._global_backoff_consecutive
        orig_successes = mm._consecutive_successes
        try:
            mm.risk_controller = rc
            mm.state.risk = risk_state
            # Accumulate some rejections
            for _ in range(3):
                mm._record_order_rejection("test:err")
            self.assertEqual(risk_state.consecutive_rejections, 3)
            # Success resets
            mm._record_order_success()
            self.assertEqual(risk_state.consecutive_rejections, 0)
            self.assertFalse(rc.is_paused())
        finally:
            mm.risk_controller = orig_rc
            mm.state.risk = orig_risk
            mm._global_backoff_consecutive = orig_backoff
            mm._consecutive_successes = orig_successes

    def test_repeated_rejections_trigger_circuit_breaker(self):
        """N consecutive rejections via _record_order_rejection trigger the circuit breaker."""
        threshold = 3
        risk_state, rc = self._fresh_risk()
        orig_rc = mm.risk_controller
        orig_risk = mm.state.risk
        try:
            mm.risk_controller = rc
            mm.state.risk = risk_state
            with temp_mm_attrs(
                MAX_CONSECUTIVE_ORDER_REJECTIONS=threshold,
                CIRCUIT_BREAKER_COOLDOWN_SEC=30.0,
            ):
                for i in range(threshold - 1):
                    mm._record_order_rejection(f"test:err{i}")
                    self.assertFalse(rc.is_paused())
                # The Nth rejection triggers
                mm._record_order_rejection("test:final")
                self.assertTrue(rc.is_paused())
        finally:
            mm.risk_controller = orig_rc
            mm.state.risk = orig_risk

    def test_mixed_rejection_reasons_accumulate(self):
        """Different rejection reasons all contribute to the same counter."""
        risk_state, rc = self._fresh_risk()
        orig_rc = mm.risk_controller
        orig_risk = mm.state.risk
        try:
            mm.risk_controller = rc
            mm.state.risk = risk_state
            with temp_mm_attrs(
                MAX_CONSECUTIVE_ORDER_REJECTIONS=3,
                CIRCUIT_BREAKER_COOLDOWN_SEC=30.0,
            ):
                mm._record_order_rejection("batch:insufficient_balance")
                self.assertEqual(risk_state.consecutive_rejections, 1)
                mm._record_order_rejection("batch:order_not_found")
                self.assertEqual(risk_state.consecutive_rejections, 2)
                mm._record_order_rejection("batch:exception")
                self.assertTrue(rc.is_paused())
        finally:
            mm.risk_controller = orig_rc
            mm.state.risk = orig_risk
