import unittest

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestOnTradeUpdate(unittest.TestCase):
    def test_logs_trade_data(self):
        """Calling with valid trade data and matching market_id should not raise."""
        with temp_mm_attrs(MARKET_ID=1):
            mm.on_trade_update(1, [
                {"price": "100.5", "size": "0.5", "is_maker_ask": True},
                {"price": "101.0", "size": "1.0", "is_maker_ask": False},
            ])

    def test_wrong_market_id_ignored(self):
        """Trade data for a different market should be silently skipped."""
        with temp_mm_attrs(MARKET_ID=1):
            mm.on_trade_update(999, [
                {"price": "100.5", "size": "0.5", "is_maker_ask": True},
            ])

    def test_exception_caught(self):
        """Malformed trade data should not crash — exception is caught internally."""
        with temp_mm_attrs(MARKET_ID=1):
            # Pass a non-iterable instead of a list of trades
            mm.on_trade_update(1, None)
