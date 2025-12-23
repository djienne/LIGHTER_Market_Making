import unittest

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
