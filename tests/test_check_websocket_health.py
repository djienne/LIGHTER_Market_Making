import time
import unittest

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestCheckWebsocketHealth(unittest.TestCase):
    def test_healthy_returns_true(self):
        with temp_mm_attrs(
            ws_connection_healthy=True,
            last_order_book_update=time.monotonic(),
            current_mid_price_cached=100.0,
        ):
            self.assertTrue(mm.check_websocket_health())

    def test_unhealthy_when_flag_false(self):
        with temp_mm_attrs(
            ws_connection_healthy=False,
            last_order_book_update=time.monotonic(),
            current_mid_price_cached=100.0,
        ):
            self.assertFalse(mm.check_websocket_health())

    def test_unhealthy_when_stale(self):
        with temp_mm_attrs(
            ws_connection_healthy=True,
            last_order_book_update=time.monotonic() - 60,
            current_mid_price_cached=100.0,
        ):
            self.assertFalse(mm.check_websocket_health())

    def test_unhealthy_when_mid_price_none(self):
        with temp_mm_attrs(
            ws_connection_healthy=True,
            last_order_book_update=time.monotonic(),
            current_mid_price_cached=None,
        ):
            self.assertFalse(mm.check_websocket_health())
