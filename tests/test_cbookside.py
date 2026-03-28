"""Tests for CBookSide — C-level sorted orderbook side."""

import unittest
from _vol_obi_fast import CBookSide


class TestCBookSideBasic(unittest.TestCase):

    def test_empty(self):
        b = CBookSide()
        self.assertEqual(len(b), 0)
        self.assertFalse(b)
        self.assertEqual(b.items(), [])
        self.assertEqual(b.keys(), [])
        self.assertEqual(b.values(), [])

    def test_insert_and_retrieve(self):
        b = CBookSide()
        b[100.0] = 5.0
        self.assertEqual(len(b), 1)
        self.assertTrue(b)
        self.assertEqual(b[100.0], 5.0)
        self.assertIn(100.0, b)

    def test_sorted_order(self):
        b = CBookSide()
        b[300.0] = 1.0
        b[100.0] = 2.0
        b[200.0] = 3.0
        self.assertEqual(b.keys(), [100.0, 200.0, 300.0])
        self.assertEqual(b.values(), [2.0, 3.0, 1.0])
        self.assertEqual(b.items(), [(100.0, 2.0), (200.0, 3.0), (300.0, 1.0)])

    def test_update_existing(self):
        b = CBookSide()
        b[100.0] = 5.0
        b[100.0] = 10.0
        self.assertEqual(len(b), 1)
        self.assertEqual(b[100.0], 10.0)

    def test_init_from_dict(self):
        b = CBookSide({300.0: 1.0, 100.0: 2.0, 200.0: 3.0})
        self.assertEqual(len(b), 3)
        self.assertEqual(b.keys(), [100.0, 200.0, 300.0])
        self.assertEqual(b[100.0], 2.0)
        self.assertEqual(b[200.0], 3.0)
        self.assertEqual(b[300.0], 1.0)

    def test_contains_missing(self):
        b = CBookSide()
        b[100.0] = 1.0
        self.assertNotIn(200.0, b)
        self.assertNotIn(50.0, b)

    def test_getitem_missing_raises(self):
        b = CBookSide()
        b[100.0] = 1.0
        with self.assertRaises(KeyError):
            _ = b[200.0]


class TestCBookSidePop(unittest.TestCase):

    def test_pop_existing(self):
        b = CBookSide()
        b[100.0] = 5.0
        b[200.0] = 10.0
        val = b.pop(100.0)
        self.assertEqual(val, 5.0)
        self.assertEqual(len(b), 1)
        self.assertNotIn(100.0, b)
        self.assertEqual(b[200.0], 10.0)

    def test_pop_with_default(self):
        b = CBookSide()
        b[100.0] = 5.0
        val = b.pop(999.0, None)
        self.assertIsNone(val)
        self.assertEqual(len(b), 1)

    def test_pop_missing_no_default_raises(self):
        b = CBookSide()
        with self.assertRaises(KeyError):
            b.pop(999.0)

    def test_pop_middle(self):
        b = CBookSide()
        b[100.0] = 1.0
        b[200.0] = 2.0
        b[300.0] = 3.0
        b.pop(200.0)
        self.assertEqual(b.keys(), [100.0, 300.0])


class TestCBookSideClear(unittest.TestCase):

    def test_clear(self):
        b = CBookSide()
        b[100.0] = 1.0
        b[200.0] = 2.0
        b.clear()
        self.assertEqual(len(b), 0)
        self.assertFalse(b)
        self.assertEqual(b.items(), [])

    def test_reuse_after_clear(self):
        b = CBookSide()
        b[100.0] = 1.0
        b.clear()
        b[200.0] = 2.0
        self.assertEqual(len(b), 1)
        self.assertEqual(b[200.0], 2.0)


class TestCBookSidePeekitem(unittest.TestCase):

    def test_peekitem_first(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0})
        self.assertEqual(b.peekitem(0), (100.0, 1.0))

    def test_peekitem_last(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0})
        self.assertEqual(b.peekitem(-1), (300.0, 3.0))

    def test_peekitem_empty_raises(self):
        b = CBookSide()
        with self.assertRaises(IndexError):
            b.peekitem(0)

    def test_peekitem_out_of_range_raises(self):
        b = CBookSide({100.0: 1.0})
        with self.assertRaises(IndexError):
            b.peekitem(5)
        with self.assertRaises(IndexError):
            b.peekitem(-2)


class TestCBookSideIteration(unittest.TestCase):

    def test_items_reversed(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0})
        rev = list(reversed(b.items()))
        self.assertEqual(rev, [(300.0, 3.0), (200.0, 2.0), (100.0, 1.0)])

    def test_list_and_reversed_raise_keyerror(self):
        """CBookSide __getitem__ expects price keys, not int indices.
        list(bookside) and reversed(bookside) fail because Python falls
        back to __getitem__(0), __getitem__(1), etc.
        See: dry_run.py CBookSide iteration fix."""
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0})
        with self.assertRaises(KeyError):
            list(b)
        with self.assertRaises((KeyError, TypeError)):
            list(reversed(b))


class TestCBookSideCapacityGrowth(unittest.TestCase):

    def test_grow_beyond_initial_capacity(self):
        b = CBookSide()
        n = 600  # > 512 initial capacity
        for i in range(n):
            b[float(i)] = float(i * 10)
        self.assertEqual(len(b), n)
        # Verify sorted order and values
        keys = b.keys()
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(b[0.0], 0.0)
        self.assertEqual(b[599.0], 5990.0)
        self.assertEqual(b.peekitem(0), (0.0, 0.0))
        self.assertEqual(b.peekitem(-1), (599.0, 5990.0))


class TestCBookSideWithApplyOrderbookUpdate(unittest.TestCase):

    def test_snapshot(self):
        from orderbook import apply_orderbook_update
        bids, asks = CBookSide(), CBookSide()
        bids_in = [{"price": "100", "size": "1"}, {"price": "99", "size": "2"}]
        asks_in = [{"price": "101", "size": "3"}]

        is_snapshot = apply_orderbook_update(bids, asks, False, bids_in, asks_in)

        self.assertTrue(is_snapshot)
        self.assertEqual(len(bids), 2)
        self.assertEqual(bids[100.0], 1.0)
        self.assertEqual(bids[99.0], 2.0)
        self.assertEqual(asks[101.0], 3.0)
        # Verify peekitem works (SortedDict compatibility)
        self.assertEqual(bids.peekitem(-1), (100.0, 1.0))
        self.assertEqual(asks.peekitem(0), (101.0, 3.0))

    def test_delta(self):
        from orderbook import apply_orderbook_update
        bids = CBookSide({100.0: 1.0, 99.0: 2.0})
        asks = CBookSide({101.0: 3.0})
        bids_in = [{"price": "100", "size": "5"}, {"price": "98", "size": "1"}]
        asks_in = [{"price": "102", "size": "2"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertFalse(is_snapshot)
        self.assertEqual(bids[100.0], 5.0)
        self.assertEqual(bids[99.0], 2.0)
        self.assertEqual(bids[98.0], 1.0)
        self.assertEqual(asks[101.0], 3.0)
        self.assertEqual(asks[102.0], 2.0)

    def test_zero_size_removes(self):
        from orderbook import apply_orderbook_update
        bids = CBookSide({100.0: 1.0, 99.0: 2.0})
        asks = CBookSide({101.0: 3.0, 102.0: 4.0})
        bids_in = [{"price": "99", "size": "0"}]
        asks_in = [{"price": "102", "size": "0"}]

        is_snapshot = apply_orderbook_update(bids, asks, True, bids_in, asks_in)

        self.assertFalse(is_snapshot)
        self.assertNotIn(99.0, bids)
        self.assertEqual(bids[100.0], 1.0)
        self.assertNotIn(102.0, asks)
        self.assertEqual(asks[101.0], 3.0)


class TestCBookSideIrange(unittest.TestCase):
    """Tests for the irange() range-scan method."""

    def test_full_range(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0})
        self.assertEqual(b.irange(), [(100.0, 1.0), (200.0, 2.0), (300.0, 3.0)])

    def test_min_price(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0})
        self.assertEqual(b.irange(min_price=200.0), [(200.0, 2.0), (300.0, 3.0)])

    def test_max_price(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0})
        self.assertEqual(b.irange(max_price=200.0), [(100.0, 1.0), (200.0, 2.0)])

    def test_min_and_max(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0, 400.0: 4.0})
        self.assertEqual(b.irange(min_price=200.0, max_price=300.0),
                         [(200.0, 2.0), (300.0, 3.0)])

    def test_reverse(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0})
        self.assertEqual(b.irange(reverse=True),
                         [(300.0, 3.0), (200.0, 2.0), (100.0, 1.0)])

    def test_reverse_with_bounds(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0, 300.0: 3.0, 400.0: 4.0})
        self.assertEqual(b.irange(min_price=200.0, max_price=300.0, reverse=True),
                         [(300.0, 3.0), (200.0, 2.0)])

    def test_empty_range(self):
        b = CBookSide({100.0: 1.0, 300.0: 3.0})
        self.assertEqual(b.irange(min_price=150.0, max_price=250.0), [])

    def test_empty_book(self):
        b = CBookSide()
        self.assertEqual(b.irange(), [])
        self.assertEqual(b.irange(min_price=100.0), [])

    def test_single_element(self):
        b = CBookSide({200.0: 5.0})
        self.assertEqual(b.irange(min_price=200.0, max_price=200.0), [(200.0, 5.0)])
        self.assertEqual(b.irange(min_price=201.0), [])


class TestCBookSideBulkWire(unittest.TestCase):
    """Tests for apply_snapshot_from_wire and apply_delta_from_wire."""

    def test_snapshot_from_wire(self):
        b = CBookSide({999.0: 9.0})  # existing data should be cleared
        levels = [
            {"price": "300", "size": "3"},
            {"price": "100", "size": "1"},
            {"price": "200", "size": "2"},
        ]
        b.apply_snapshot_from_wire(levels)
        self.assertEqual(len(b), 3)
        self.assertEqual(b.keys(), [100.0, 200.0, 300.0])
        self.assertEqual(b[100.0], 1.0)
        self.assertEqual(b[200.0], 2.0)
        self.assertEqual(b[300.0], 3.0)

    def test_snapshot_filters_zeros(self):
        b = CBookSide()
        levels = [
            {"price": "100", "size": "1"},
            {"price": "200", "size": "0"},
            {"price": "300", "size": "3"},
        ]
        b.apply_snapshot_from_wire(levels)
        self.assertEqual(len(b), 2)
        self.assertNotIn(200.0, b)

    def test_snapshot_empty(self):
        b = CBookSide({100.0: 1.0})
        b.apply_snapshot_from_wire([])
        self.assertEqual(len(b), 0)

    def test_delta_from_wire_insert(self):
        b = CBookSide({100.0: 1.0})
        levels = [
            {"price": "200", "size": "2"},
            {"price": "100", "size": "5"},  # update
        ]
        b.apply_delta_from_wire(levels)
        self.assertEqual(b[100.0], 5.0)
        self.assertEqual(b[200.0], 2.0)

    def test_delta_from_wire_remove(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0})
        levels = [{"price": "100", "size": "0"}]
        b.apply_delta_from_wire(levels)
        self.assertEqual(len(b), 1)
        self.assertNotIn(100.0, b)
        self.assertEqual(b[200.0], 2.0)

    def test_snapshot_large(self):
        """Snapshot with >512 entries (triggers one capacity growth)."""
        levels = [{"price": str(i), "size": str(i * 10)} for i in range(1, 601)]
        b = CBookSide()
        b.apply_snapshot_from_wire(levels)
        self.assertEqual(len(b), 600)
        self.assertEqual(b.peekitem(0), (1.0, 10.0))
        self.assertEqual(b.peekitem(-1), (600.0, 6000.0))

    def test_snapshot_very_large(self):
        """Snapshot with >1024 entries (triggers multiple capacity growths).

        Regression: when n > 2*initial_capacity, the growth loop in
        apply_snapshot_from_wire must keep _count in sync with _capacity
        after each _ensure_capacity() call, otherwise the resize guard
        (_count >= _capacity) becomes false and capacity stops growing.
        """
        levels = [{"price": str(i), "size": str(i)} for i in range(1, 2001)]
        b = CBookSide()
        b.apply_snapshot_from_wire(levels)
        self.assertEqual(len(b), 2000)
        self.assertEqual(b.peekitem(0), (1.0, 1.0))
        self.assertEqual(b.peekitem(-1), (2000.0, 2000.0))


class TestCBookSideBinanceFormat(unittest.TestCase):
    """Tests for Binance-format bulk methods ([[price_str, qty_str], ...])."""

    def test_snapshot_from_binance(self):
        b = CBookSide()
        levels = [["300", "3"], ["100", "1"], ["200", "2"]]
        b.apply_snapshot_from_binance(levels)
        self.assertEqual(len(b), 3)
        self.assertEqual(b.keys(), [100.0, 200.0, 300.0])
        self.assertEqual(b[100.0], 1.0)

    def test_snapshot_filters_zeros(self):
        b = CBookSide()
        levels = [["100", "1"], ["200", "0"], ["300", "3"]]
        b.apply_snapshot_from_binance(levels)
        self.assertEqual(len(b), 2)
        self.assertNotIn(200.0, b)

    def test_snapshot_empty(self):
        b = CBookSide({100.0: 1.0})
        b.apply_snapshot_from_binance([])
        self.assertEqual(len(b), 0)

    def test_delta_from_binance_insert(self):
        b = CBookSide({100.0: 1.0})
        levels = [["200", "2"], ["100", "5"]]
        b.apply_delta_from_binance(levels)
        self.assertEqual(b[100.0], 5.0)
        self.assertEqual(b[200.0], 2.0)

    def test_delta_from_binance_remove(self):
        b = CBookSide({100.0: 1.0, 200.0: 2.0})
        levels = [["100", "0"]]
        b.apply_delta_from_binance(levels)
        self.assertEqual(len(b), 1)
        self.assertNotIn(100.0, b)
        self.assertEqual(b[200.0], 2.0)

    def test_snapshot_large(self):
        """Snapshot with >1024 entries (triggers multiple capacity growths)."""
        levels = [[str(i), str(i * 10)] for i in range(1, 2001)]
        b = CBookSide()
        b.apply_snapshot_from_binance(levels)
        self.assertEqual(len(b), 2000)
        self.assertEqual(b.peekitem(0), (1.0, 10.0))
        self.assertEqual(b.peekitem(-1), (2000.0, 20000.0))


if __name__ == "__main__":
    unittest.main()
