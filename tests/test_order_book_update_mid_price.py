import unittest

import market_maker_v2 as mm
from _helpers import temp_event_state, temp_mm_attrs


class TestOrderBookUpdateMidPrice(unittest.TestCase):
    def test_on_order_book_update_sets_mid_price(self):
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book={"bids": {}, "asks": {}, "initialized": False},
            current_mid_price_cached=None,
            ws_connection_healthy=False,
        ):
            with temp_event_state(mm.order_book_received, set_value=False):
                mm.on_order_book_update(1, {
                    "bids": [{"price": "100", "size": "1"}],
                    "asks": [{"price": "101", "size": "1"}],
                })

                self.assertTrue(mm.order_book_received.is_set())
                self.assertEqual(mm.current_mid_price_cached, 100.5)
                self.assertTrue(mm.ws_connection_healthy)
