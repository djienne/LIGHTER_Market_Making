"""Tests for config.json loading and validation.

Covers valid config loading, missing file, invalid JSON, missing keys,
and gather_lighter_data config loading (CRYPTO_TICKERS).
"""

import json
import os
import unittest
from unittest.mock import patch

from utils import load_config_params


class TestLoadConfigParams(unittest.TestCase):
    def test_valid_config_loads_correctly(self):
        """A well-formed config.json should load all fields."""
        cfg = {
            "CRYPTO_TICKERS": ["BTC", "ETH"],
            "trading": {"leverage": 3, "base_amount": 0.1},
            "safety": {"max_consecutive_order_rejections": 10},
        }
        config_data = json.dumps(cfg)
        with patch("builtins.open", unittest.mock.mock_open(read_data=config_data)):
            result = load_config_params()
        self.assertEqual(result.get("CRYPTO_TICKERS"), ["BTC", "ETH"])
        self.assertEqual(result["trading"]["leverage"], 3)
        self.assertEqual(result["safety"]["max_consecutive_order_rejections"], 10)

    def test_missing_config_returns_empty_dict(self):
        """When config.json doesn't exist, should return empty dict."""
        with patch("builtins.open", side_effect=FileNotFoundError):
            result = load_config_params()
        self.assertEqual(result, {})

    def test_invalid_json_returns_empty_dict(self):
        """Malformed JSON should return empty dict."""
        with patch("builtins.open", unittest.mock.mock_open(read_data="{invalid json!!}")):
            result = load_config_params()
        self.assertEqual(result, {})

    def test_empty_config_returns_empty_dict(self):
        """Empty JSON object is valid but has no keys."""
        with patch("builtins.open", unittest.mock.mock_open(read_data="{}")):
            result = load_config_params()
        self.assertEqual(result, {})
        self.assertIsNone(result.get("trading"))

    def test_nested_keys_with_defaults(self):
        """Accessing missing nested keys should work with .get() defaults."""
        cfg = {"trading": {}}
        with patch("builtins.open", unittest.mock.mock_open(read_data=json.dumps(cfg))):
            result = load_config_params()

        trading = result.get("trading", {})
        self.assertEqual(trading.get("leverage", 1), 1)
        self.assertEqual(trading.get("base_amount", 0.047), 0.047)

        safety = result.get("safety", {})
        self.assertEqual(safety.get("max_consecutive_order_rejections", 5), 5)


class TestConfigKeyTypes(unittest.TestCase):
    """Verify that config values are used with correct types by market_maker_v2."""

    def test_trading_config_types(self):
        """Trading config values should be numeric."""
        import market_maker_v2 as mm

        self.assertIsInstance(mm.BASE_AMOUNT, float)
        self.assertIsInstance(mm.CAPITAL_USAGE_PERCENT, float)
        self.assertIsInstance(mm.ORDER_TIMEOUT, float)
        self.assertIsInstance(mm.QUOTE_UPDATE_THRESHOLD_BPS, float)
        self.assertIsInstance(mm.LEVERAGE, int)

    def test_safety_config_types(self):
        """Safety config values should be numeric."""
        import market_maker_v2 as mm

        self.assertIsInstance(mm.MAX_CONSECUTIVE_ORDER_REJECTIONS, int)
        self.assertIsInstance(mm.CIRCUIT_BREAKER_COOLDOWN_SEC, float)
        self.assertIsInstance(mm.MAX_LIVE_ORDERS_PER_MARKET, int)
        self.assertIsInstance(mm.STALE_ORDER_POLLER_INTERVAL_SEC, float)
        self.assertIsInstance(mm.STALE_ORDER_DEBOUNCE_COUNT, int)
        self.assertIsInstance(mm.ORDER_RECONCILE_TIMEOUT_SEC, float)

    def test_websocket_config_types(self):
        """WebSocket config values should be numeric."""
        import market_maker_v2 as mm

        self.assertIsInstance(mm.WS_PING_INTERVAL, int)
        self.assertIsInstance(mm.WS_RECV_TIMEOUT, float)
        self.assertIsInstance(mm.WS_RECONNECT_BASE_DELAY, int)
        self.assertIsInstance(mm.WS_RECONNECT_MAX_DELAY, int)

    def test_trading_config_positive(self):
        """Trading config values should be positive."""
        import market_maker_v2 as mm

        self.assertGreater(mm.BASE_AMOUNT, 0)
        self.assertGreater(mm.CAPITAL_USAGE_PERCENT, 0)

    def test_safety_config_positive(self):
        """Safety config values should be positive."""
        import market_maker_v2 as mm

        self.assertGreater(mm.MAX_CONSECUTIVE_ORDER_REJECTIONS, 0)
        self.assertGreater(mm.CIRCUIT_BREAKER_COOLDOWN_SEC, 0)
        self.assertGreater(mm.MAX_LIVE_ORDERS_PER_MARKET, 0)
