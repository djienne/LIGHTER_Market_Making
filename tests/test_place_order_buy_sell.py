import unittest
from decimal import Decimal

import market_maker_v2 as mm
from _helpers import DummyOrderClient, temp_mm_attrs


class TestPlaceOrderBuySell(unittest.IsolatedAsyncioTestCase):
    async def test_place_order_buy_and_sell(self):
        with temp_mm_attrs(MARKET_ID=1):
            client = DummyOrderClient()

            await mm.place_order(client, "buy", 100.0, 111, Decimal("0.5"))
            await mm.place_order(client, "sell", 101.0, 112, Decimal("0.5"))

            self.assertEqual(len(client.create_order_calls), 2)

            buy_kwargs = client.create_order_calls[0]["kwargs"]
            self.assertEqual(buy_kwargs["market_index"], 1)
            self.assertEqual(buy_kwargs["client_order_index"], 111)
            self.assertEqual(buy_kwargs["base_amount"], 0.5)
            self.assertEqual(buy_kwargs["price"], 100.0)
            self.assertFalse(buy_kwargs["is_ask"])
            self.assertEqual(
                buy_kwargs["order_type"],
                mm.lighter.SignerClient.ORDER_TYPE_LIMIT,
            )
            self.assertEqual(
                buy_kwargs["time_in_force"],
                mm.lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            )

            sell_kwargs = client.create_order_calls[1]["kwargs"]
            self.assertEqual(sell_kwargs["client_order_index"], 112)
            self.assertEqual(sell_kwargs["price"], 101.0)
            self.assertTrue(sell_kwargs["is_ask"])
