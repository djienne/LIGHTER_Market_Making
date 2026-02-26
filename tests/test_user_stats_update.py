import unittest

import market_maker_v2 as mm
from _helpers import temp_event_state, temp_mm_attrs


class TestUserStatsUpdate(unittest.TestCase):
    def test_on_user_stats_update_sets_balance(self):
        with temp_mm_attrs(available_capital=None, portfolio_value=None):
            with temp_event_state(mm.account_state_received, set_value=False):
                mm.on_user_stats_update(mm.ACCOUNT_INDEX, {
                    "available_balance": "100",
                    "portfolio_value": "200",
                })

                self.assertEqual(mm.available_capital, 100.0)
                self.assertEqual(mm.portfolio_value, 200.0)
                self.assertTrue(mm.account_state_received.is_set())

    def test_wrong_account_ignored(self):
        with temp_mm_attrs(available_capital=None, portfolio_value=None):
            with temp_event_state(mm.account_state_received, set_value=False):
                mm.on_user_stats_update(999999, {
                    "available_balance": "100",
                    "portfolio_value": "200",
                })

                self.assertIsNone(mm.available_capital)
                self.assertIsNone(mm.portfolio_value)
                self.assertFalse(mm.account_state_received.is_set())

    def test_missing_fields_no_crash(self):
        with temp_mm_attrs(available_capital=None, portfolio_value=None):
            with temp_event_state(mm.account_state_received, set_value=False):
                # Only partial data — missing portfolio_value
                mm.on_user_stats_update(mm.ACCOUNT_INDEX, {
                    "available_balance": "100",
                })

                # Should not crash; state should remain unchanged
                self.assertIsNone(mm.available_capital)
                self.assertFalse(mm.account_state_received.is_set())
