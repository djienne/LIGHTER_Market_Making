import json
import unittest

from binance_obi import (
    SharedAlpha, SharedBBO,
    BinanceBookTickerClient, BinanceDiffDepthClient,
    lighter_to_binance_symbol,
)


# ---------------------------------------------------------------------------
# SharedAlpha tests
# ---------------------------------------------------------------------------

class TestSharedAlpha(unittest.TestCase):

    def test_initial_state(self):
        sa = SharedAlpha(min_samples=10)
        self.assertAlmostEqual(sa.alpha, 0.0)
        self.assertEqual(sa.sample_count, 0)
        self.assertFalse(sa.warmed_up)
        self.assertTrue(sa.is_stale())

    def test_update_and_warmup(self):
        sa = SharedAlpha(min_samples=10)
        for i in range(10):
            sa.update(0.5)
        self.assertTrue(sa.warmed_up)
        self.assertEqual(sa.sample_count, 10)
        self.assertAlmostEqual(sa.alpha, 0.5)

    def test_not_warmed_below_min(self):
        sa = SharedAlpha(min_samples=10)
        for i in range(9):
            sa.update(0.1)
        self.assertFalse(sa.warmed_up)
        self.assertEqual(sa.sample_count, 9)

    def test_staleness(self):
        sa = SharedAlpha(min_samples=1)
        sa.update(1.0)
        self.assertFalse(sa.is_stale(threshold_seconds=5.0))
        self.assertTrue(sa.is_stale(threshold_seconds=0.0))

    def test_reset(self):
        sa = SharedAlpha(min_samples=5)
        for _ in range(10):
            sa.update(0.5)
        self.assertTrue(sa.warmed_up)
        sa.reset()
        self.assertAlmostEqual(sa.alpha, 0.0)
        self.assertEqual(sa.sample_count, 0)
        self.assertFalse(sa.warmed_up)
        self.assertTrue(sa.is_stale())


# ---------------------------------------------------------------------------
# SharedBBO tests
# ---------------------------------------------------------------------------

class TestSharedBBO(unittest.TestCase):

    def test_initial_state(self):
        bbo = SharedBBO(min_samples=5)
        self.assertAlmostEqual(bbo.best_bid, 0.0)
        self.assertAlmostEqual(bbo.best_ask, 0.0)
        self.assertAlmostEqual(bbo.mid, 0.0)
        self.assertEqual(bbo.sample_count, 0)
        self.assertFalse(bbo.warmed_up)
        self.assertTrue(bbo.is_stale())

    def test_update_and_warmup(self):
        bbo = SharedBBO(min_samples=3)
        for _ in range(3):
            bbo.update(100.0, 101.0, 5.0, 3.0, 42, 1000, 1001)
        self.assertTrue(bbo.warmed_up)
        self.assertEqual(bbo.sample_count, 3)
        self.assertAlmostEqual(bbo.best_bid, 100.0)
        self.assertAlmostEqual(bbo.best_ask, 101.0)
        self.assertAlmostEqual(bbo.mid, 100.5)
        self.assertAlmostEqual(bbo.bid_qty, 5.0)
        self.assertAlmostEqual(bbo.ask_qty, 3.0)
        self.assertEqual(bbo.update_id, 42)
        self.assertEqual(bbo.event_time, 1000)
        self.assertEqual(bbo.transaction_time, 1001)

    def test_not_warmed_below_min(self):
        bbo = SharedBBO(min_samples=5)
        for _ in range(4):
            bbo.update(100.0, 101.0, 1.0, 1.0, 1, 0, 0)
        self.assertFalse(bbo.warmed_up)

    def test_staleness(self):
        bbo = SharedBBO(min_samples=1)
        bbo.update(100.0, 101.0, 1.0, 1.0, 1, 0, 0)
        self.assertFalse(bbo.is_stale(threshold_seconds=5.0))
        self.assertTrue(bbo.is_stale(threshold_seconds=0.0))

    def test_mid_computation(self):
        bbo = SharedBBO()
        bbo.update(50000.0, 50002.0, 1.0, 1.0, 1, 0, 0)
        self.assertAlmostEqual(bbo.mid, 50001.0)

    def test_reset(self):
        bbo = SharedBBO(min_samples=1)
        bbo.update(100.0, 101.0, 5.0, 3.0, 42, 1000, 1001)
        self.assertTrue(bbo.warmed_up)
        bbo.reset()
        self.assertAlmostEqual(bbo.best_bid, 0.0)
        self.assertAlmostEqual(bbo.best_ask, 0.0)
        self.assertAlmostEqual(bbo.mid, 0.0)
        self.assertEqual(bbo.sample_count, 0)
        self.assertFalse(bbo.warmed_up)
        self.assertTrue(bbo.is_stale())


# ---------------------------------------------------------------------------
# Symbol mapping tests
# ---------------------------------------------------------------------------

class TestSymbolMapping(unittest.TestCase):

    def test_known_symbols(self):
        self.assertEqual(lighter_to_binance_symbol("BTC"), "btcusdt")
        self.assertEqual(lighter_to_binance_symbol("ETH"), "ethusdt")
        self.assertEqual(lighter_to_binance_symbol("PAXG"), "paxgusdt")

    def test_unknown_fallback(self):
        self.assertEqual(lighter_to_binance_symbol("DOGE"), "dogeusdt")

    def test_case_insensitive(self):
        self.assertEqual(lighter_to_binance_symbol("btc"), "btcusdt")
        self.assertEqual(lighter_to_binance_symbol("Eth"), "ethusdt")

    def test_unmapped_returns_none(self):
        self.assertIsNone(lighter_to_binance_symbol("ASTER"))


# ---------------------------------------------------------------------------
# BinanceBookTickerClient message parsing tests
# ---------------------------------------------------------------------------

class TestBinanceBookTickerClientParsing(unittest.TestCase):

    def _make_client(self, **kwargs):
        bbo = SharedBBO(min_samples=2)
        defaults = dict(binance_symbol="btcusdt", shared_bbo=bbo)
        defaults.update(kwargs)
        return BinanceBookTickerClient(**defaults), bbo

    def _make_msg(self, b="50000.0", a="50001.0", B="1.5", A="2.0",
                  u=100, E=1000, T=1001, **extra):
        d = {"e": "bookTicker", "s": "BTCUSDT",
             "b": b, "a": a, "B": B, "A": A,
             "u": u, "E": E, "T": T}
        d.update(extra)
        return json.dumps(d)

    def test_valid_message_updates_bbo(self):
        client, bbo = self._make_client()
        client._on_message(self._make_msg())
        self.assertEqual(bbo.sample_count, 1)
        self.assertAlmostEqual(bbo.best_bid, 50000.0)
        self.assertAlmostEqual(bbo.best_ask, 50001.0)
        self.assertAlmostEqual(bbo.bid_qty, 1.5)
        self.assertAlmostEqual(bbo.ask_qty, 2.0)
        self.assertAlmostEqual(bbo.mid, 50000.5)
        self.assertEqual(bbo.update_id, 100)

    def test_crossed_book_ignored(self):
        client, bbo = self._make_client()
        # ask <= bid
        client._on_message(self._make_msg(b="50001.0", a="50000.0"))
        self.assertEqual(bbo.sample_count, 0)

    def test_zero_price_ignored(self):
        client, bbo = self._make_client()
        client._on_message(self._make_msg(b="0", a="50001.0"))
        self.assertEqual(bbo.sample_count, 0)

    def test_bad_json_ignored(self):
        client, bbo = self._make_client()
        client._on_message(b"not json!!!")
        self.assertEqual(bbo.sample_count, 0)

    def test_missing_fields_ignored(self):
        client, bbo = self._make_client()
        client._on_message(json.dumps({"foo": "bar"}))
        self.assertEqual(bbo.sample_count, 0)

    def test_multiple_updates_count(self):
        client, bbo = self._make_client()
        for i in range(5):
            client._on_message(self._make_msg(u=100 + i))
        self.assertEqual(bbo.sample_count, 5)
        self.assertTrue(bbo.warmed_up)  # min_samples=2


# ---------------------------------------------------------------------------
# BinanceDiffDepthClient tests (unit tests, no real WS)
# ---------------------------------------------------------------------------

class TestBinanceDiffDepthClientSnapshot(unittest.TestCase):
    """Test snapshot/delta/alpha logic without real WebSocket."""

    def _make_client(self, **kwargs):
        sa = SharedAlpha(min_samples=2)
        defaults = dict(
            binance_symbol="btcusdt",
            shared_alpha=sa,
            window_size=100,
            looking_depth=0.025,
        )
        defaults.update(kwargs)
        return BinanceDiffDepthClient(**defaults), sa

    def test_apply_snapshot(self):
        client, _ = self._make_client()
        snapshot = {
            'lastUpdateId': 500,
            'bids': [["50000.0", "1.0"], ["49999.0", "2.0"]],
            'asks': [["50001.0", "1.5"], ["50002.0", "3.0"]],
        }
        client._apply_snapshot(snapshot)
        self.assertEqual(client._last_update_id, 500)
        self.assertEqual(len(client._bids), 2)
        self.assertEqual(len(client._asks), 2)
        self.assertAlmostEqual(client._bids.peekitem(-1)[0], 50000.0)
        self.assertAlmostEqual(client._asks.peekitem(0)[0], 50001.0)

    def test_apply_diff_insert_and_update(self):
        client, _ = self._make_client()
        client._apply_snapshot({
            'lastUpdateId': 500,
            'bids': [["50000.0", "1.0"]],
            'asks': [["50001.0", "1.5"]],
        })
        event = {'U': 501, 'u': 501, 'pu': 0,
                 'b': [["50000.0", "5.0"], ["49998.0", "2.0"]],
                 'a': [["50001.0", "3.0"]]}
        client._apply_diff(event)
        self.assertAlmostEqual(client._bids[50000.0], 5.0)
        self.assertAlmostEqual(client._bids[49998.0], 2.0)
        self.assertAlmostEqual(client._asks[50001.0], 3.0)

    def test_apply_diff_removes_zero_qty(self):
        client, _ = self._make_client()
        client._apply_snapshot({
            'lastUpdateId': 500,
            'bids': [["50000.0", "1.0"], ["49999.0", "2.0"]],
            'asks': [["50001.0", "1.5"]],
        })
        event = {'U': 501, 'u': 501, 'pu': 0,
                 'b': [["49999.0", "0"]],
                 'a': []}
        client._apply_diff(event)
        self.assertEqual(len(client._bids), 1)
        self.assertNotIn(49999.0, client._bids)

    def test_update_alpha_computes_imbalance(self):
        client, sa = self._make_client()
        client._apply_snapshot({
            'lastUpdateId': 500,
            'bids': [["50000.0", "10.0"], ["49999.0", "5.0"]],
            'asks': [["50001.0", "1.0"], ["50002.0", "1.0"]],
        })
        # Bid-heavy book → positive imbalance → positive alpha after warmup
        for _ in range(10):
            client._update_alpha()
        self.assertGreater(sa.sample_count, 0)

    def test_imbalance_balanced_book(self):
        client, _ = self._make_client()
        client._apply_snapshot({
            'lastUpdateId': 500,
            'bids': [["50000.0", "5.0"]],
            'asks': [["50001.0", "5.0"]],
        })
        mid = 50000.5
        imb = client._compute_imbalance(mid)
        # Symmetric → near zero
        self.assertAlmostEqual(imb, 0.0, places=1)

    def test_imbalance_bid_heavy(self):
        client, _ = self._make_client()
        client._apply_snapshot({
            'lastUpdateId': 500,
            'bids': [["50000.0", "100.0"]],
            'asks': [["50001.0", "1.0"]],
        })
        mid = 50000.5
        imb = client._compute_imbalance(mid)
        self.assertGreater(imb, 0.0)


if __name__ == "__main__":
    unittest.main()
