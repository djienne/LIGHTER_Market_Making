import unittest
from unittest.mock import patch

from sortedcontainers import SortedDict

import orderbook_sanity


class TestCheckOrderbookSanity(unittest.IsolatedAsyncioTestCase):
    async def test_empty_bids_returns_not_ok(self):
        result = await orderbook_sanity.check_orderbook_sanity(
            market_id=1,
            ws_bids=SortedDict(),
            ws_asks=SortedDict({100.0: 1.0}),
        )
        self.assertFalse(result.ok)
        self.assertIn("empty", result.reason.lower())

    async def test_empty_asks_returns_not_ok(self):
        result = await orderbook_sanity.check_orderbook_sanity(
            market_id=1,
            ws_bids=SortedDict({99.0: 1.0}),
            ws_asks=SortedDict(),
        )
        self.assertFalse(result.ok)
        self.assertIn("empty", result.reason.lower())

    async def test_crossed_book_returns_not_ok(self):
        result = await orderbook_sanity.check_orderbook_sanity(
            market_id=1,
            ws_bids=SortedDict({101.0: 1.0}),
            ws_asks=SortedDict({100.0: 1.0}),
        )
        self.assertFalse(result.ok)
        self.assertIn("crossed", result.reason.lower())

    @patch("orderbook_sanity._fetch_rest_top_of_book", side_effect=Exception("timeout"))
    async def test_rest_failure_returns_not_ok(self, _mock):
        result = await orderbook_sanity.check_orderbook_sanity(
            market_id=1,
            ws_bids=SortedDict({99.0: 1.0}),
            ws_asks=SortedDict({100.0: 1.0}),
        )
        self.assertFalse(result.ok)
        self.assertIn("REST fetch failed", result.reason)

    @patch("orderbook_sanity._fetch_rest_top_of_book", return_value=(0.0, 0.0))
    async def test_rest_invalid_prices_returns_not_ok(self, _mock):
        result = await orderbook_sanity.check_orderbook_sanity(
            market_id=1,
            ws_bids=SortedDict({99.0: 1.0}),
            ws_asks=SortedDict({100.0: 1.0}),
        )
        self.assertFalse(result.ok)
        self.assertIn("invalid", result.reason.lower())

    @patch("orderbook_sanity._fetch_rest_top_of_book", return_value=(99.05, 100.05))
    async def test_within_tolerance_returns_ok(self, _mock):
        result = await orderbook_sanity.check_orderbook_sanity(
            market_id=1,
            ws_bids=SortedDict({99.0: 1.0}),
            ws_asks=SortedDict({100.0: 1.0}),
            tolerance_pct=0.5,
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.reason, "OK")

    @patch("orderbook_sanity._fetch_rest_top_of_book", return_value=(101.0, 102.0))
    async def test_exceeds_tolerance_returns_not_ok(self, _mock):
        result = await orderbook_sanity.check_orderbook_sanity(
            market_id=1,
            ws_bids=SortedDict({99.0: 1.0}),
            ws_asks=SortedDict({100.0: 1.0}),
            tolerance_pct=0.5,
        )
        self.assertFalse(result.ok)
        self.assertGreater(result.bid_diff_pct, 0.5)
