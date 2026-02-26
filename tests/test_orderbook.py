import unittest
from orderbook import apply_orderbook_update


class TestApplyOrderbookUpdate(unittest.TestCase):

    def test_snapshot_on_uninitialized(self):
        bids, asks = {}, {}
        bids_in = [{"price": "100", "size": "1"}, {"price": "99", "size": "2"}]
        asks_in = [{"price": "101", "size": "3"}]

        is_snapshot = apply_orderbook_update(bids, asks, False, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        self.assertEqual(bids, {100.0: 1.0, 99.0: 2.0})
        self.assertEqual(asks, {101.0: 3.0})

    def test_snapshot_on_large_update(self):
        bids, asks = {999.0: 1.0}, {150.0: 1.0}
        # More than 100 items triggers snapshot
        bids_in = [{"price": str(i), "size": "1"} for i in range(101)]
        asks_in = [{"price": "200", "size": "5"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        # Old entries should be gone (cleared on snapshot)
        self.assertNotIn(999.0, bids)
        self.assertNotIn(150.0, asks)
        self.assertEqual(len(bids), 101)  # 0..100 all have size 1
        self.assertEqual(asks, {200.0: 5.0})

    def test_delta_update(self):
        bids = {100.0: 1.0, 99.0: 2.0}
        asks = {101.0: 3.0}
        bids_in = [{"price": "100", "size": "5"}, {"price": "98", "size": "1"}]
        asks_in = [{"price": "102", "size": "2"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertFalse(is_snapshot)
        self.assertEqual(bids[100.0], 5.0)
        self.assertEqual(bids[99.0], 2.0)  # untouched
        self.assertEqual(bids[98.0], 1.0)  # new level
        self.assertEqual(asks[101.0], 3.0)  # untouched
        self.assertEqual(asks[102.0], 2.0)  # new level

    def test_zero_size_removes_level(self):
        bids = {100.0: 1.0, 99.0: 2.0}
        asks = {101.0: 3.0, 102.0: 4.0}
        bids_in = [{"price": "99", "size": "0"}]
        asks_in = [{"price": "102", "size": "0"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertFalse(is_snapshot)
        self.assertNotIn(99.0, bids)
        self.assertEqual(bids[100.0], 1.0)
        self.assertNotIn(102.0, asks)
        self.assertEqual(asks[101.0], 3.0)

    def test_snapshot_ignores_zero_size(self):
        bids, asks = {}, {}
        bids_in = [{"price": "100", "size": "1"}, {"price": "99", "size": "0"}]
        asks_in = [{"price": "101", "size": "0"}]

        is_snapshot = apply_orderbook_update(bids, asks, False, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        self.assertEqual(bids, {100.0: 1.0})
        self.assertEqual(asks, {})

    def test_empty_update(self):
        bids = {100.0: 1.0}
        asks = {101.0: 2.0}

        is_snapshot = apply_orderbook_update(bids, asks, True, [], [])

        self.assertFalse(is_snapshot)
        self.assertEqual(bids, {100.0: 1.0})
        self.assertEqual(asks, {101.0: 2.0})

    def test_works_with_sorteddict(self):
        from sortedcontainers import SortedDict
        bids, asks = SortedDict(), SortedDict()
        bids_in = [{"price": "100", "size": "1"}, {"price": "99", "size": "2"}]
        asks_in = [{"price": "101", "size": "3"}]

        is_snapshot = apply_orderbook_update(bids, asks, False, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        self.assertEqual(bids.peekitem(-1), (100.0, 1.0))
        self.assertEqual(asks.peekitem(0), (101.0, 3.0))


if __name__ == "__main__":
    unittest.main()
