import unittest
from collections import deque

import market_maker_v2 as mm
from _helpers import temp_event_state, temp_mm_attrs


class TestAccountAllUpdatePosition(unittest.TestCase):
    def test_on_account_all_update_position_sign(self):
        with temp_mm_attrs(MARKET_ID=1, current_position_size=0.0):
            with temp_event_state(mm.account_all_received, set_value=False):
                mm.on_account_all_update(mm.ACCOUNT_INDEX, {
                    "positions": {
                        "1": {"position": "2.5", "sign": -1},
                    }
                })

                self.assertEqual(mm.current_position_size, -2.5)
                self.assertTrue(mm.account_all_received.is_set())

    def test_wrong_account_ignored(self):
        with temp_mm_attrs(MARKET_ID=1, current_position_size=0.0):
            with temp_event_state(mm.account_all_received, set_value=False):
                mm.on_account_all_update(999999, {
                    "positions": {
                        "1": {"position": "5.0", "sign": 1},
                    }
                })

                self.assertEqual(mm.current_position_size, 0.0)
                self.assertFalse(mm.account_all_received.is_set())

    def test_positive_sign_position(self):
        with temp_mm_attrs(MARKET_ID=1, current_position_size=0.0):
            with temp_event_state(mm.account_all_received, set_value=False):
                mm.on_account_all_update(mm.ACCOUNT_INDEX, {
                    "positions": {
                        "1": {"position": "3.0", "sign": 1},
                    }
                })

                self.assertEqual(mm.current_position_size, 3.0)

    def test_market_id_int_key_supported(self):
        with temp_mm_attrs(MARKET_ID=1, current_position_size=0.0):
            with temp_event_state(mm.account_all_received, set_value=False):
                mm.on_account_all_update(mm.ACCOUNT_INDEX, {
                    "positions": {
                        1: {"position": "2.0", "sign": -1},
                    }
                })

                self.assertEqual(mm.current_position_size, -2.0)
                self.assertTrue(mm.account_all_received.is_set())

    def test_missing_market_in_positions(self):
        """When positions dict doesn't contain current market, position_size should be 0."""
        with temp_mm_attrs(MARKET_ID=1, current_position_size=5.0):
            with temp_event_state(mm.account_all_received, set_value=False):
                mm.on_account_all_update(mm.ACCOUNT_INDEX, {
                    "positions": {
                        "99": {"position": "10.0", "sign": 1},
                    }
                })

                self.assertEqual(mm.current_position_size, 0.0)

    def test_trades_appended_to_recent_trades(self):
        with temp_mm_attrs(MARKET_ID=1, current_position_size=0.0, recent_trades=deque(maxlen=20)):
            mm.on_account_all_update(mm.ACCOUNT_INDEX, {
                "positions": {
                    "1": {"position": "1.0", "sign": 1},
                },
                "trades": {
                    "1": [
                        {"market_id": 1, "type": "buy", "size": "0.5", "price": "100", "timestamp": 1},
                    ]
                },
            })

            self.assertEqual(len(mm.recent_trades), 1)
            self.assertEqual(mm.recent_trades[0]["price"], "100")

    def test_no_positions_key_does_not_set_event(self):
        """Data without 'positions' key should not set account_all_received."""
        with temp_mm_attrs(MARKET_ID=1, current_position_size=0.0):
            with temp_event_state(mm.account_all_received, set_value=False):
                mm.on_account_all_update(mm.ACCOUNT_INDEX, {
                    "trades": {}
                })

                self.assertFalse(mm.account_all_received.is_set())

    def test_malformed_positions_payload_does_not_corrupt_state(self):
        with temp_mm_attrs(MARKET_ID=1, current_position_size=1.25, account_positions={"1": {"position": "1.25", "sign": 1}}):
            with temp_event_state(mm.account_all_received, set_value=False):
                mm.on_account_all_update(mm.ACCOUNT_INDEX, {
                    "positions": ["not", "a", "dict"]
                })

                self.assertEqual(mm.current_position_size, 1.25)
                self.assertEqual(mm.account_positions, {"1": {"position": "1.25", "sign": 1}})
                self.assertFalse(mm.account_all_received.is_set())

    def test_invalid_position_value_keeps_previous_size(self):
        with temp_mm_attrs(MARKET_ID=1, current_position_size=0.75):
            with temp_event_state(mm.account_all_received, set_value=False):
                mm.on_account_all_update(mm.ACCOUNT_INDEX, {
                    "positions": {
                        "1": {"position": "nan-not-number", "sign": 1},
                    }
                })

                self.assertEqual(mm.current_position_size, 0.75)
                self.assertFalse(mm.account_all_received.is_set())
