import unittest
from sortedcontainers import SortedDict

import market_maker_v2 as mm
from _helpers import temp_event_state, temp_mm_attrs


def _fresh_book():
    return {"bids": SortedDict(), "asks": SortedDict(), "initialized": False}


class TestOrderBookUpdateMidPrice(unittest.TestCase):
    def test_on_order_book_update_sets_mid_price(self):
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
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

    def test_wrong_market_id_ignored(self):
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=None,
            ws_connection_healthy=False,
        ):
            mm.on_order_book_update(999, {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "1"}],
            })

            self.assertIsNone(mm.current_mid_price_cached)
            self.assertFalse(mm.ws_connection_healthy)

    def test_empty_bids_asks_no_crash(self):
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=None,
            ws_connection_healthy=False,
        ):
            mm.on_order_book_update(1, {"bids": [], "asks": []})

            # Should not crash; mid price stays None since book is empty
            self.assertIsNone(mm.current_mid_price_cached)

    def test_one_sided_book_clears_mid_price(self):
        """Bids-only update should clear mid_price to None."""
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=50.0,
            ws_connection_healthy=False,
        ):
            mm.on_order_book_update(1, {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [],
            })

            self.assertIsNone(mm.current_mid_price_cached)

    def test_empty_book_does_not_set_event(self):
        """Empty bids and asks should not set order_book_received."""
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=None,
            ws_connection_healthy=False,
        ):
            with temp_event_state(mm.order_book_received, set_value=False):
                mm.on_order_book_update(1, {"bids": [], "asks": []})

                self.assertFalse(mm.order_book_received.is_set())

    def test_exception_sets_ws_unhealthy(self):
        """A malformed payload that triggers an exception should mark ws unhealthy."""
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=None,
            ws_connection_healthy=True,
        ):
            # Pass a payload that will cause apply_orderbook_update to fail
            # by giving non-dict bids entries
            mm.on_order_book_update(1, {"bids": "not_a_list", "asks": "not_a_list"})

            self.assertFalse(mm.ws_connection_healthy)
