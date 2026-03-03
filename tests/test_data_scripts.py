"""Tests for gather_lighter_data helper functions and find_trend_lighter backtest logic.

Covers: parquet path/part logic, write_buffers_to_parquet dedup, top-10 buffer
structure, and find_trend_lighter run_backtest with synthetic data.
"""

import os
import tempfile
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import numpy as np
import pandas as pd

import gather_lighter_data as gld


@contextmanager
def _saved_globals():
    """Save and restore gather_lighter_data module globals."""
    orig_market_info = gld.market_info.copy()
    orig_local_books = dict(gld.local_order_books)
    orig_last_offsets = gld.last_offsets.copy()
    orig_price_buffers = dict(gld.price_buffers)
    orig_trade_buffers = dict(gld.trade_buffers)
    orig_stats = gld.stats.copy()
    orig_stats['last_orderbook_time'] = gld.stats['last_orderbook_time'].copy()
    orig_stats['last_trade_time'] = gld.stats['last_trade_time'].copy()
    orig_part_indices = {k: dict(v) for k, v in gld.parquet_part_indices.items()}
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
        gld.parquet_part_indices.clear()
        gld.parquet_part_indices.update(orig_part_indices)


class TestParsePartIndex(unittest.TestCase):
    def test_valid_part_filename(self):
        self.assertEqual(gld._parse_part_index("prices_BTC_part000001.parquet"), 1)
        self.assertEqual(gld._parse_part_index("trades_ETH_part000042.parquet"), 42)

    def test_no_match_returns_none(self):
        self.assertIsNone(gld._parse_part_index("prices_BTC.parquet"))
        self.assertIsNone(gld._parse_part_index("random_file.txt"))
        self.assertIsNone(gld._parse_part_index(""))

    def test_zero_index(self):
        self.assertEqual(gld._parse_part_index("prices_SOL_part000000.parquet"), 0)


class TestParquetPartPath(unittest.TestCase):
    def test_path_format(self):
        path = gld._parquet_part_path("prices", "BTC", 0)
        self.assertTrue(path.endswith("prices_BTC_part000000.parquet"))

    def test_path_with_large_index(self):
        path = gld._parquet_part_path("trades", "ETH", 999999)
        self.assertTrue(path.endswith("trades_ETH_part999999.parquet"))

    def test_path_includes_data_folder(self):
        path = gld._parquet_part_path("prices", "SOL", 1)
        self.assertTrue(path.startswith(gld.DATA_FOLDER))


class TestOrderbookTop10Structure(unittest.TestCase):
    def test_top10_bid_ask_fields_present(self):
        """handle_orderbook_message should produce top-10 bid/ask fields."""
        with _saved_globals():
            gld.market_info[1] = "BTC"
            gld.local_order_books["BTC"] = {'bids': {}, 'asks': {}, 'initialized': False}

            bids = [{"price": str(100 - i), "size": "1.0"} for i in range(12)]
            asks = [{"price": str(101 + i), "size": "1.0"} for i in range(12)]

            gld.handle_orderbook_message(1, {"bids": bids, "asks": asks, "offset": 1})

            self.assertEqual(len(gld.price_buffers["BTC"]), 1)
            entry = gld.price_buffers["BTC"][0]

            # Should have bid_price_0..9 and ask_price_0..9
            for i in range(10):
                self.assertIn(f"bid_price_{i}", entry)
                self.assertIn(f"bid_size_{i}", entry)
                self.assertIn(f"ask_price_{i}", entry)
                self.assertIn(f"ask_size_{i}", entry)

            # bid_price_0 should be the highest bid
            self.assertEqual(entry["bid_price_0"], 100.0)
            # ask_price_0 should be the lowest ask
            self.assertEqual(entry["ask_price_0"], 101.0)

    def test_sparse_book_pads_none(self):
        """When the book has fewer than 10 levels, remaining fields are None."""
        with _saved_globals():
            gld.market_info[1] = "BTC"
            gld.local_order_books["BTC"] = {'bids': {}, 'asks': {}, 'initialized': False}

            gld.handle_orderbook_message(1, {
                "bids": [{"price": "100", "size": "1.0"}],
                "asks": [{"price": "101", "size": "1.0"}],
                "offset": 1,
            })

            entry = gld.price_buffers["BTC"][0]
            self.assertIsNotNone(entry["bid_price_0"])
            self.assertIsNone(entry["bid_price_1"])
            self.assertIsNone(entry["ask_price_5"])


class TestTradeMessageHandling(unittest.TestCase):
    def test_trade_id_field_recorded(self):
        """Trade messages should record trade_id."""
        with _saved_globals():
            gld.market_info[1] = "ETH"
            gld.trade_buffers["ETH"] = []

            gld.handle_trade_message(1, {
                "trades": [{"price": "200.5", "size": "1.5", "timestamp": "1700000000", "trade_id": "42"}],
            })

            self.assertEqual(gld.trade_buffers["ETH"][0]["trade_id"], 42)

    def test_explicit_side_field_override(self):
        """If trade has explicit 'type' field, it should override is_maker_ask logic."""
        with _saved_globals():
            gld.market_info[1] = "ETH"
            gld.trade_buffers["ETH"] = []

            gld.handle_trade_message(1, {
                "trades": [{
                    "price": "100",
                    "size": "1",
                    "timestamp": "1700000000",
                    "trade_id": "1",
                    "is_maker_ask": False,
                    "type": "buy",
                }],
            })

            self.assertEqual(gld.trade_buffers["ETH"][0]["side"], "buy")

    def test_multiple_trades_in_single_message(self):
        """Multiple trades in one message should all be buffered."""
        with _saved_globals():
            gld.market_info[1] = "ETH"
            gld.trade_buffers["ETH"] = []

            trades = [
                {"price": "100", "size": "1", "timestamp": "1700000000", "trade_id": str(i)}
                for i in range(5)
            ]
            gld.handle_trade_message(1, {"trades": trades})

            self.assertEqual(len(gld.trade_buffers["ETH"]), 5)


class TestWriteBuffersDedup(unittest.IsolatedAsyncioTestCase):
    async def test_consecutive_duplicate_prices_deduped(self):
        """write_buffers_to_parquet should dedup consecutive identical price records."""
        with _saved_globals():
            gld.stats['start_time'] = 1.0
            gld.stats['buffer_flushes'] = 0

            # Add 5 identical price records (only timestamp differs)
            for i in range(5):
                gld.price_buffers["BTC"].append({
                    "timestamp": f"2025-01-01T00:00:0{i}",
                    "offset": 1,
                    "bid_price_0": 100.0,
                    "ask_price_0": 101.0,
                })

            written_dfs = []
            original_write = gld._write_parquet_dataframe

            def _capture_write(df, file_path, label, symbol):
                written_dfs.append(df.copy())

            with patch.object(gld, "_write_parquet_dataframe", side_effect=_capture_write):
                with patch.object(gld, "_get_parquet_write_path", return_value="/tmp/test.parquet"):
                    # Run one iteration of write_buffers_to_parquet
                    import asyncio
                    task = asyncio.create_task(gld.write_buffers_to_parquet())
                    await asyncio.sleep(gld.BUFFER_SECONDS + 0.5)
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            # Should have written only 1 record (all duplicates deduped)
            if written_dfs:
                self.assertEqual(len(written_dfs[0]), 1)


class TestFindTrendLighterBacktest(unittest.TestCase):
    """Tests for find_trend_lighter.py backtest functions."""

    def test_run_backtest_with_sufficient_data(self):
        """run_backtest should return a result dict with expected keys."""
        import find_trend_lighter as ftl

        n = 500
        rng = np.random.default_rng(42)
        prices = 100.0 + np.cumsum(rng.standard_normal(n) * 0.5)
        prices = np.maximum(prices, 1.0)  # keep positive

        price_data = {
            'open': prices,
            'high': prices + rng.uniform(0, 1, n),
            'low': prices - rng.uniform(0, 1, n),
            'close': prices + rng.uniform(-0.5, 0.5, n),
        }

        result = ftl.run_backtest(price_data, atr_period=14, atr_multiplier=3.0)
        self.assertIsNotNone(result)
        self.assertIn('period', result)
        self.assertIn('multiplier', result)
        self.assertIn('flips', result)
        self.assertIn('sharpe', result)
        self.assertIn('return', result)
        self.assertIn('last_signal', result)

    def test_run_backtest_insufficient_data_returns_none(self):
        """run_backtest should return None when data is too short for the ATR period."""
        import find_trend_lighter as ftl

        price_data = {
            'open': np.array([100.0, 101.0]),
            'high': np.array([101.0, 102.0]),
            'low': np.array([99.0, 100.0]),
            'close': np.array([100.5, 101.5]),
        }

        result = ftl.run_backtest(price_data, atr_period=14, atr_multiplier=3.0)
        self.assertIsNone(result)

    def test_supertrend_direction_output_shape(self):
        """_supertrend_direction_numba should return array matching input length."""
        import find_trend_lighter as ftl

        n = 200
        rng = np.random.default_rng(42)
        close = 100.0 + np.cumsum(rng.standard_normal(n) * 0.5)
        high = close + rng.uniform(0, 1, n)
        low = close - rng.uniform(0, 1, n)

        direction, start = ftl._supertrend_direction_numba(high, low, close, 14, 3.0)
        self.assertEqual(len(direction), n)
        self.assertGreaterEqual(start, 0)

        # Direction values should be in {-1, 0, 1}
        unique_vals = set(direction)
        self.assertTrue(unique_vals.issubset({-1, 0, 1}))

    def test_supertrend_very_short_input(self):
        """Supertrend on input shorter than period should return zeros."""
        import find_trend_lighter as ftl

        direction, start = ftl._supertrend_direction_numba(
            np.array([1.0, 2.0]),
            np.array([0.5, 1.5]),
            np.array([0.8, 1.8]),
            14, 3.0,
        )
        self.assertEqual(start, -1)  # insufficient data
        np.testing.assert_array_equal(direction, np.zeros(2, dtype=np.int8))

    def test_run_backtest_flat_prices_valid(self):
        """run_backtest with constant prices should not crash (no signals)."""
        import find_trend_lighter as ftl

        n = 100
        flat = np.full(n, 100.0)
        price_data = {
            'open': flat.copy(),
            'high': flat.copy() + 0.01,
            'low': flat.copy() - 0.01,
            'close': flat.copy(),
        }

        result = ftl.run_backtest(price_data, atr_period=14, atr_multiplier=3.0)
        # May return None or a result with 0 flips — either is valid
        if result is not None:
            self.assertEqual(result['flips'], 0)


class TestGetMaxParquetBytes(unittest.TestCase):
    def test_default_value(self):
        """Without env var, should default to 5MB."""
        with patch.dict(os.environ, {}, clear=False):
            if "PARQUET_MAX_MB" in os.environ:
                del os.environ["PARQUET_MAX_MB"]
            result = gld._get_max_parquet_bytes()
        self.assertEqual(result, 5 * 1024 * 1024)

    def test_custom_value(self):
        """Custom PARQUET_MAX_MB env var should be honored."""
        with patch.dict(os.environ, {"PARQUET_MAX_MB": "10"}):
            result = gld._get_max_parquet_bytes()
        self.assertEqual(result, 10 * 1024 * 1024)

    def test_invalid_value_falls_back(self):
        """Invalid PARQUET_MAX_MB falls back to 5MB."""
        with patch.dict(os.environ, {"PARQUET_MAX_MB": "abc"}):
            result = gld._get_max_parquet_bytes()
        self.assertEqual(result, 5 * 1024 * 1024)

    def test_zero_value_falls_back(self):
        """Zero PARQUET_MAX_MB falls back to 5MB."""
        with patch.dict(os.environ, {"PARQUET_MAX_MB": "0"}):
            result = gld._get_max_parquet_bytes()
        self.assertEqual(result, 5 * 1024 * 1024)
