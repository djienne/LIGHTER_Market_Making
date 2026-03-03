import unittest
from contextlib import contextmanager

import gather_lighter_data as gld


@contextmanager
def _saved_globals():
    """Save and restore gather_lighter_data module globals to avoid test pollution."""
    orig_market_info = gld.market_info.copy()
    orig_local_books = dict(gld.local_order_books)
    orig_last_offsets = gld.last_offsets.copy()
    orig_price_buffers = dict(gld.price_buffers)
    orig_trade_buffers = dict(gld.trade_buffers)
    orig_stats = gld.stats.copy()
    orig_stats['last_orderbook_time'] = gld.stats['last_orderbook_time'].copy()
    orig_stats['last_trade_time'] = gld.stats['last_trade_time'].copy()
    try:
        yield
    finally:
        gld.market_info.clear()
        gld.market_info.update(orig_market_info)
        gld.local_order_books.clear()
        gld.local_order_books.update(orig_local_books)
        gld.last_offsets.clear()
        gld.last_offsets.update(orig_last_offsets)
        gld.price_buffers.clear()
        gld.price_buffers.update(orig_price_buffers)
        gld.trade_buffers.clear()
        gld.trade_buffers.update(orig_trade_buffers)
        gld.stats.clear()
        gld.stats.update(orig_stats)


class TestHandleOrderbookMessage(unittest.TestCase):
    def test_unknown_market_returns_early(self):
        with _saved_globals():
            gld.market_info.clear()
            gld.stats['orderbook_updates'] = 0
            gld.handle_orderbook_message(999, {"bids": [], "asks": []})
            self.assertEqual(gld.stats['orderbook_updates'], 0)

    def test_populates_local_order_books(self):
        with _saved_globals():
            gld.market_info.clear()
            gld.market_info[1] = "ETH"
            gld.local_order_books["ETH"] = {'bids': {}, 'asks': {}, 'initialized': False}

            gld.handle_orderbook_message(1, {
                "bids": [{"price": "100", "size": "1.5"}],
                "asks": [{"price": "101", "size": "2.0"}],
                "offset": 1,
            })

            self.assertIn(100.0, gld.local_order_books["ETH"]["bids"])
            self.assertIn(101.0, gld.local_order_books["ETH"]["asks"])
            self.assertEqual(len(gld.price_buffers["ETH"]), 1)

    def test_gap_detection(self):
        with _saved_globals():
            gld.market_info.clear()
            gld.market_info[1] = "ETH"
            gld.local_order_books["ETH"] = {'bids': {}, 'asks': {}, 'initialized': False}
            gld.last_offsets["ETH"] = 100
            gld.stats['gaps_detected'] = 0

            gld.handle_orderbook_message(1, {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "1"}],
                "offset": 200,
            })

            self.assertGreater(gld.stats['gaps_detected'], 0)


class TestHandleTradeMessage(unittest.TestCase):
    def test_unknown_market_returns_early(self):
        with _saved_globals():
            gld.market_info.clear()
            gld.stats['ws_trades'] = 0
            gld.handle_trade_message(999, {"trades": [{"price": "100", "size": "1", "timestamp": "1700000000"}]})
            self.assertEqual(gld.stats['ws_trades'], 0)

    def test_populates_trade_buffers(self):
        with _saved_globals():
            gld.market_info.clear()
            gld.market_info[1] = "ETH"
            gld.trade_buffers["ETH"] = []
            gld.stats['ws_trades'] = 0

            gld.handle_trade_message(1, {
                "trades": [{"price": "100.5", "size": "0.5", "timestamp": "1700000000", "trade_id": "1"}],
            })

            self.assertEqual(len(gld.trade_buffers["ETH"]), 1)
            self.assertEqual(gld.trade_buffers["ETH"][0]["price"], 100.5)
            self.assertEqual(gld.stats['ws_trades'], 1)

    def test_millisecond_timestamp(self):
        with _saved_globals():
            gld.market_info.clear()
            gld.market_info[1] = "ETH"
            gld.trade_buffers["ETH"] = []

            # Timestamp > 30_000_000_000 → treated as milliseconds
            gld.handle_trade_message(1, {
                "trades": [{"price": "100", "size": "1", "timestamp": "1700000000000", "trade_id": "2"}],
            })

            self.assertEqual(len(gld.trade_buffers["ETH"]), 1)
            # The timestamp should be converted from ms to s before formatting
            ts_str = gld.trade_buffers["ETH"][0]["timestamp"]
            self.assertIn("2023", ts_str)  # 1700000000 is Nov 2023
