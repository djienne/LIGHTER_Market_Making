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
        bids_in = [{"price": str(i), "size": "1"} for i in range(1, 102)]
        asks_in = [{"price": "200", "size": "5"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        # Old entries should be gone (cleared on snapshot)
        self.assertNotIn(999.0, bids)
        self.assertNotIn(150.0, asks)
        self.assertEqual(len(bids), 101)  # 1..101 all have size 1
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

    def test_snapshot_hint_forces_snapshot_on_small_book(self):
        """A protocol-marked snapshot must replace the book even when small."""
        bids = {999.0: 1.0, 100.0: 1.0}
        asks = {150.0: 1.0}
        bids_in = [{"price": "100", "size": "2"}]
        asks_in = [{"price": "101", "size": "3"}]

        is_snapshot = apply_orderbook_update(
            bids, asks, True, bids_in, asks_in, is_snapshot_hint=True,
        )

        self.assertTrue(is_snapshot)
        # Stale levels must be gone — without the hint this would have been
        # applied as a delta, leaving 999.0/150.0 behind.
        self.assertEqual(bids, {100.0: 2.0})
        self.assertEqual(asks, {101.0: 3.0})

    def test_snapshot_hint_false_keeps_delta_semantics(self):
        """hint=False on a small update is still a delta."""
        bids = {100.0: 1.0}
        asks = {101.0: 3.0}

        is_snapshot = apply_orderbook_update(
            bids, asks, True,
            [{"price": "99", "size": "2"}], [], is_snapshot_hint=False,
        )

        self.assertFalse(is_snapshot)
        self.assertEqual(bids, {100.0: 1.0, 99.0: 2.0})

    def test_snapshot_hint_false_overrides_size_heuristic(self):
        """A >100-level update/order_book message is still a DELTA.

        Live feed shows busy deltas can exceed the legacy 100-level
        heuristic; treating them as snapshots would wipe the rest of the
        book.  The protocol msg type is authoritative when provided.
        """
        bids = {50.0: 1.0}   # far level that a wrongly-applied snapshot would wipe
        asks = {200.0: 1.0}
        bids_in = [{"price": str(100 + i), "size": "1"} for i in range(124)]

        is_snapshot = apply_orderbook_update(
            bids, asks, True, bids_in, [], is_snapshot_hint=False,
        )

        self.assertFalse(is_snapshot)
        self.assertIn(50.0, bids)        # pre-existing level survived
        self.assertIn(200.0, asks)
        self.assertEqual(len(bids), 125)  # 1 old + 124 delta levels

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


class TestApplyOrderbookUpdateCBookSide(unittest.TestCase):
    """Same tests as above but with CBookSide containers.

    Exercises the real production code path: apply_orderbook_update()
    called with CBookSide instances (Cython sorted arrays).
    """

    @classmethod
    def setUpClass(cls):
        try:
            from _vol_obi_fast import CBookSide
        except ImportError:
            raise unittest.SkipTest(
                "Cython extension not built (python setup_cython.py build_ext --inplace)"
            )
        cls.CBookSide = CBookSide

    def _make(self):
        return self.CBookSide(), self.CBookSide()

    def test_snapshot_on_uninitialized(self):
        bids, asks = self._make()
        bids_in = [{"price": "100", "size": "1"}, {"price": "99", "size": "2"}]
        asks_in = [{"price": "101", "size": "3"}]

        is_snapshot = apply_orderbook_update(bids, asks, False, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        self.assertEqual(bids[100.0], 1.0)
        self.assertEqual(bids[99.0], 2.0)
        self.assertEqual(asks[101.0], 3.0)
        self.assertEqual(bids.peekitem(-1), (100.0, 1.0))

    def test_delta_update(self):
        bids, asks = self.CBookSide({100.0: 1.0, 99.0: 2.0}), self.CBookSide({101.0: 3.0})
        bids_in = [{"price": "100", "size": "5"}, {"price": "98", "size": "1"}]
        asks_in = [{"price": "102", "size": "2"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertFalse(is_snapshot)
        self.assertEqual(bids[100.0], 5.0)
        self.assertEqual(bids[99.0], 2.0)
        self.assertEqual(bids[98.0], 1.0)
        self.assertEqual(asks[101.0], 3.0)
        self.assertEqual(asks[102.0], 2.0)

    def test_zero_size_removes_level(self):
        bids = self.CBookSide({100.0: 1.0, 99.0: 2.0})
        asks = self.CBookSide({101.0: 3.0, 102.0: 4.0})
        bids_in = [{"price": "99", "size": "0"}]
        asks_in = [{"price": "102", "size": "0"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertFalse(is_snapshot)
        self.assertNotIn(99.0, bids)
        self.assertEqual(bids[100.0], 1.0)
        self.assertNotIn(102.0, asks)
        self.assertEqual(asks[101.0], 3.0)

    def test_snapshot_ignores_zero_size(self):
        bids, asks = self._make()
        bids_in = [{"price": "100", "size": "1"}, {"price": "99", "size": "0"}]
        asks_in = [{"price": "101", "size": "0"}]

        is_snapshot = apply_orderbook_update(bids, asks, False, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        self.assertEqual(len(bids), 1)
        self.assertEqual(bids[100.0], 1.0)
        self.assertEqual(len(asks), 0)

    def test_snapshot_clears_old_entries(self):
        bids = self.CBookSide({999.0: 1.0})
        asks = self.CBookSide({150.0: 1.0})
        bids_in = [{"price": str(i), "size": "1"} for i in range(1, 102)]
        asks_in = [{"price": "200", "size": "5"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        self.assertNotIn(999.0, bids)
        self.assertNotIn(150.0, asks)
        self.assertEqual(len(bids), 101)
        self.assertEqual(asks[200.0], 5.0)


if __name__ == "__main__":
    unittest.main()
