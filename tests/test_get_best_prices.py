import unittest
from sortedcontainers import SortedDict

import market_maker_v2 as mm
from _helpers import temp_mm_attrs


class TestGetBestPrices(unittest.TestCase):
    def test_returns_best_bid_ask(self):
        bids = SortedDict({99.0: 1.0, 100.0: 2.0})
        asks = SortedDict({101.0: 1.5, 102.0: 3.0})
        with temp_mm_attrs(local_order_book={"bids": bids, "asks": asks}):
            best_bid, best_ask = mm.get_best_prices()
            self.assertEqual(best_bid, 100.0)
            self.assertEqual(best_ask, 101.0)

    def test_empty_book_returns_none(self):
        with temp_mm_attrs(local_order_book={"bids": SortedDict(), "asks": SortedDict()}):
            best_bid, best_ask = mm.get_best_prices()
            self.assertIsNone(best_bid)
            self.assertIsNone(best_ask)

    def test_one_side_empty(self):
        bids = SortedDict({100.0: 1.0})
        with temp_mm_attrs(local_order_book={"bids": bids, "asks": SortedDict()}):
            best_bid, best_ask = mm.get_best_prices()
            self.assertIsNone(best_bid)
            self.assertIsNone(best_ask)
