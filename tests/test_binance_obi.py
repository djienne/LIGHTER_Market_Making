import unittest

from binance_obi import SharedAlpha, BinanceObiClient, lighter_to_binance_symbol


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
        # Fresh update should not be stale
        sa.update(1.0)
        self.assertFalse(sa.is_stale(threshold_seconds=5.0))
        # With a very small threshold, it should be stale (time has passed at least a little)
        # Use a 0 threshold
        self.assertTrue(sa.is_stale(threshold_seconds=0.0))

    def test_reset(self):
        sa = SharedAlpha(min_samples=5)
        for _ in range(10):
            sa.update(0.5)
        self.assertTrue(sa.warmed_up)
        self.assertEqual(sa.sample_count, 10)

        sa.reset()
        self.assertAlmostEqual(sa.alpha, 0.0)
        self.assertEqual(sa.sample_count, 0)
        self.assertFalse(sa.warmed_up)
        self.assertTrue(sa.is_stale())


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
# BinanceObiClient message parsing tests
# ---------------------------------------------------------------------------

class TestBinanceObiClientMessageParsing(unittest.TestCase):

    def _make_client(self, **kwargs):
        sa = SharedAlpha(min_samples=2)
        defaults = dict(
            binance_symbol="btcusdt",
            shared_alpha=sa,
            window_size=100,
            looking_depth=0.025,
        )
        defaults.update(kwargs)
        return BinanceObiClient(**defaults), sa

    def _make_depth_message(self, bids, asks):
        """Build a Binance @depth20 JSON string."""
        import json
        return json.dumps({"bids": bids, "asks": asks})

    def test_balanced_book_alpha_near_zero(self):
        client, sa = self._make_client()
        # Symmetric book: 5 levels each side with equal quantity
        bids = [[str(100.0 - i * 0.1), "1.0"] for i in range(5)]
        asks = [[str(100.1 + i * 0.1), "1.0"] for i in range(5)]
        # Feed enough to warm up
        for _ in range(10):
            msg = self._make_depth_message(bids, asks)
            client._on_message(msg)
        # Alpha should be near zero with a balanced book
        self.assertAlmostEqual(sa.alpha, 0.0, places=5)

    def test_bid_heavy_positive_alpha(self):
        client, sa = self._make_client()
        # First warm up with balanced book
        bids_balanced = [[str(100.0 - i * 0.1), "1.0"] for i in range(5)]
        asks_balanced = [[str(100.1 + i * 0.1), "1.0"] for i in range(5)]
        for _ in range(50):
            client._on_message(self._make_depth_message(bids_balanced, asks_balanced))

        # Now bid-heavy book
        bids_heavy = [[str(100.0 - i * 0.1), "10.0"] for i in range(5)]
        asks_light = [[str(100.1 + i * 0.1), "1.0"] for i in range(5)]
        client._on_message(self._make_depth_message(bids_heavy, asks_light))

        self.assertGreater(sa.alpha, 0.0)

    def test_invalid_message_ignored(self):
        client, sa = self._make_client()
        initial_count = sa.sample_count
        # Bad JSON
        client._on_message(b"not json at all!!!")
        self.assertEqual(sa.sample_count, initial_count)
        # Valid JSON but no bids/asks
        client._on_message(b'{"foo": "bar"}')
        self.assertEqual(sa.sample_count, initial_count)
