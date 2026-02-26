import unittest

import market_maker_v2 as mm
from _helpers import DummyModifyClient, temp_mm_attrs


class TestModifyOrderPath(unittest.IsolatedAsyncioTestCase):
    """Tests for the atomic modify_order path and cancel+place fallback."""

    async def test_modify_order_success(self):
        """When modify_order succeeds, no cancel or create should be called."""
        client = DummyModifyClient(modify_err=None)

        with temp_mm_attrs(
            MARKET_ID=1,
            current_bid_order_id=100,
            current_bid_price=50.0,
            current_bid_size=1.0,
            QUOTE_UPDATE_THRESHOLD_BPS=0.0,
        ):
            await mm.refresh_order_if_needed(client, True, 51.0, 1.0)

            self.assertEqual(len(client.modify_order_calls), 1)
            self.assertEqual(len(client.create_order_calls), 0)
            self.assertEqual(len(client.cancel_order_calls), 0)

            call = client.modify_order_calls[0]["kwargs"]
            self.assertEqual(call["market_index"], 1)
            self.assertEqual(call["order_index"], 100)
            self.assertEqual(call["base_amount"], 1.0)
            self.assertEqual(call["price"], 51.0)

            # State should be updated with new price
            self.assertEqual(mm.current_bid_price, 51.0)
            # Order ID unchanged (modify keeps existing ID)
            self.assertEqual(mm.current_bid_order_id, 100)

    async def test_modify_order_fallback_to_cancel_place(self):
        """When modify_order fails, should fall back to cancel + place."""
        client = DummyModifyClient(modify_err="order already filled")

        with temp_mm_attrs(
            MARKET_ID=1,
            current_ask_order_id=200,
            current_ask_price=55.0,
            current_ask_size=1.0,
            QUOTE_UPDATE_THRESHOLD_BPS=0.0,
        ):
            await mm.refresh_order_if_needed(client, False, 56.0, 1.0)

            # modify was attempted
            self.assertEqual(len(client.modify_order_calls), 1)
            # fallback: cancel then create
            self.assertEqual(len(client.cancel_order_calls), 1)
            self.assertEqual(len(client.create_order_calls), 1)

            # New order should have a new ID (not 200)
            self.assertIsNotNone(mm.current_ask_order_id)
            self.assertNotEqual(mm.current_ask_order_id, 200)
            self.assertEqual(mm.current_ask_price, 56.0)

    async def test_no_modify_when_no_existing_order(self):
        """Without an existing order, should place directly without modify."""
        client = DummyModifyClient(modify_err=None)

        with temp_mm_attrs(
            MARKET_ID=1,
            current_bid_order_id=None,
            current_bid_price=None,
            current_bid_size=None,
            QUOTE_UPDATE_THRESHOLD_BPS=0.0,
        ):
            await mm.refresh_order_if_needed(client, True, 50.0, 1.0)

            self.assertEqual(len(client.modify_order_calls), 0)
            self.assertEqual(len(client.cancel_order_calls), 0)
            self.assertEqual(len(client.create_order_calls), 1)

    async def test_skip_when_below_threshold(self):
        """Should skip update when price change is below threshold."""
        client = DummyModifyClient(modify_err=None)

        with temp_mm_attrs(
            MARKET_ID=1,
            current_bid_order_id=100,
            current_bid_price=50.0,
            current_bid_size=1.0,
            QUOTE_UPDATE_THRESHOLD_BPS=10.0,
        ):
            # 0.001 / 50.0 * 10000 = 0.2 bps, below threshold of 10
            await mm.refresh_order_if_needed(client, True, 50.001, 1.0)

            self.assertEqual(len(client.modify_order_calls), 0)
            self.assertEqual(len(client.create_order_calls), 0)
            self.assertEqual(mm.current_bid_price, 50.0)


class TestCalculateOrderPrices(unittest.TestCase):
    """Tests for the combined calculate_order_prices function."""

    def test_returns_both_prices_with_cache(self):
        cache = mm.AvellanedaCache(
            gamma=0.5, sigma=0.01, k_bid=1.0, k_ask=1.0, time_remaining=0.5
        )
        with temp_mm_attrs(
            avellaneda_cache=cache,
            _PRICE_TICK_FLOAT=0.01,
            REQUIRE_PARAMS=False,
        ):
            buy, sell = mm.calculate_order_prices(100.0, 0.0)
            self.assertIsNotNone(buy)
            self.assertIsNotNone(sell)
            self.assertLess(buy, 100.0)
            self.assertGreater(sell, 100.0)

    def test_fallback_without_cache(self):
        with temp_mm_attrs(
            avellaneda_cache=None,
            REQUIRE_PARAMS=False,
        ):
            buy, sell = mm.calculate_order_prices(100.0, 0.0)
            self.assertIsNotNone(buy)
            self.assertIsNotNone(sell)
            self.assertLess(buy, 100.0)
            self.assertGreater(sell, 100.0)

    def test_require_params_returns_none(self):
        with temp_mm_attrs(
            avellaneda_cache=None,
            REQUIRE_PARAMS=True,
        ):
            buy, sell = mm.calculate_order_prices(100.0, 0.0)
            self.assertIsNone(buy)
            self.assertIsNone(sell)

    def test_inventory_skews_prices(self):
        cache = mm.AvellanedaCache(
            gamma=0.5, sigma=0.1, k_bid=1.0, k_ask=1.0, time_remaining=0.5
        )
        with temp_mm_attrs(
            avellaneda_cache=cache,
            _PRICE_TICK_FLOAT=0.0001,  # fine tick so rounding doesn't erase the skew
        ):
            buy_neutral, sell_neutral = mm.calculate_order_prices(100.0, 0.0)
            buy_long, sell_long = mm.calculate_order_prices(100.0, 2.0)

            # Long inventory should shift both prices down
            self.assertLess(buy_long, buy_neutral)
            self.assertLess(sell_long, sell_neutral)
