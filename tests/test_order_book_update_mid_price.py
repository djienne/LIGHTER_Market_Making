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


class TestOffsetTracking(unittest.TestCase):
    """Offset sanity via the Lighter `offset` sequence field.

    Live-feed behavior (verified 2026-06-10): offsets advance by ARBITRARY
    positive steps between coalesced deltas, so forward jumps are normal.
    Only a non-advancing offset (stale/out-of-order) is anomalous.
    """

    def _snapshot(self, offset=100):
        return {
            "bids": [{"price": "100", "size": "1"}],
            "asks": [{"price": "101", "size": "1"}],
            "offset": offset,
        }

    def test_non_contiguous_forward_offsets_are_normal(self):
        """Jumps like 100 -> 107 -> 119 must be applied without reconnect."""
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=None,
            ws_connection_healthy=False,
        ):
            with temp_event_state(mm.ws_reconnect_event, set_value=False):
                mm.on_order_book_update(1, self._snapshot(100), is_snapshot_hint=True)
                mm.on_order_book_update(1, {
                    "bids": [{"price": "100", "size": "2"}], "asks": [],
                    "offset": 107,
                })
                mm.on_order_book_update(1, {
                    "bids": [{"price": "99.5", "size": "1"}], "asks": [],
                    "offset": 119,
                })

                self.assertFalse(mm.ws_reconnect_event.is_set())
                self.assertEqual(mm.state.market.local_order_book["last_offset"], 119)
                # Deltas were applied
                self.assertEqual(mm.state.market.local_order_book["bids"][100.0], 2.0)
                self.assertEqual(mm.state.market.local_order_book["bids"][99.5], 1.0)

    def test_stale_offset_delta_skipped(self):
        """A delta whose offset does not advance must be ignored."""
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=None,
            ws_connection_healthy=False,
        ):
            with temp_event_state(mm.ws_reconnect_event, set_value=False):
                mm.on_order_book_update(1, self._snapshot(100), is_snapshot_hint=True)
                mm.on_order_book_update(1, {
                    "bids": [{"price": "100", "size": "3"}], "asks": [],
                    "offset": 110,
                })

                # Replay with an older offset and a different size — must be skipped
                mm.on_order_book_update(1, {
                    "bids": [{"price": "100", "size": "9"}], "asks": [],
                    "offset": 105,
                })
                # Duplicate of the latest offset — also skipped
                mm.on_order_book_update(1, {
                    "bids": [{"price": "100", "size": "7"}], "asks": [],
                    "offset": 110,
                })

                self.assertFalse(mm.ws_reconnect_event.is_set())
                self.assertEqual(mm.state.market.local_order_book["bids"][100.0], 3.0)
                self.assertEqual(mm.state.market.local_order_book["last_offset"], 110)

    def test_snapshot_resets_offset_baseline(self):
        """An in-connection snapshot must not be gap-checked against old offsets."""
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=None,
            ws_connection_healthy=False,
        ):
            with temp_event_state(mm.ws_reconnect_event, set_value=False):
                mm.on_order_book_update(1, self._snapshot(100), is_snapshot_hint=True)
                # Server refresh: snapshot with a far-ahead offset is fine
                mm.on_order_book_update(1, self._snapshot(500), is_snapshot_hint=True)

                self.assertFalse(mm.ws_reconnect_event.is_set())
                self.assertEqual(mm.state.market.local_order_book["last_offset"], 500)

    def test_missing_offset_skips_gap_checks(self):
        """Payloads without offset must keep working (no tracking, no gaps)."""
        with temp_mm_attrs(
            MARKET_ID=1,
            local_order_book=_fresh_book(),
            current_mid_price_cached=None,
            ws_connection_healthy=False,
        ):
            with temp_event_state(mm.ws_reconnect_event, set_value=False):
                mm.on_order_book_update(1, {
                    "bids": [{"price": "100", "size": "1"}],
                    "asks": [{"price": "101", "size": "1"}],
                })
                mm.on_order_book_update(1, {
                    "bids": [{"price": "100", "size": "2"}], "asks": [],
                })

                self.assertFalse(mm.ws_reconnect_event.is_set())
                self.assertEqual(mm.current_mid_price_cached, 100.5)
