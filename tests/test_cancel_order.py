import unittest

import market_maker_v2 as mm
from _helpers import DummyCancelSingleClient, temp_mm_attrs


class TestCancelOrder(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_order_success(self):
        client = DummyCancelSingleClient(cancel_err=None)
        with temp_mm_attrs(MARKET_ID=1):
            result = await mm.cancel_order(client, 42)
            self.assertTrue(result)
            self.assertEqual(len(client.cancel_order_calls), 1)
            kwargs = client.cancel_order_calls[0]["kwargs"]
            self.assertEqual(kwargs["market_index"], 1)
            self.assertEqual(kwargs["order_index"], 42)

    async def test_cancel_order_none_id_returns_true(self):
        client = DummyCancelSingleClient()
        result = await mm.cancel_order(client, None)
        self.assertTrue(result)
        self.assertEqual(len(client.cancel_order_calls), 0)

    async def test_cancel_order_api_error(self):
        client = DummyCancelSingleClient(cancel_err="order not found")
        with temp_mm_attrs(MARKET_ID=1):
            result = await mm.cancel_order(client, 99)
            self.assertFalse(result)

    async def test_cancel_order_exception(self):
        client = DummyCancelSingleClient(raise_exc=RuntimeError("connection lost"))
        with temp_mm_attrs(MARKET_ID=1):
            result = await mm.cancel_order(client, 99)
            self.assertFalse(result)
