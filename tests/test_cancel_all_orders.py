import unittest

import market_maker_v2 as mm
from _helpers import DummyCancelClient


class TestCancelAllOrders(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_all_orders(self):
        client = DummyCancelClient()
        await mm.cancel_all_orders(client)

        self.assertEqual(len(client.cancel_all_calls), 1)
        kwargs = client.cancel_all_calls[0]["kwargs"]
        self.assertEqual(kwargs["time"], 0)
        self.assertEqual(
            kwargs["time_in_force"],
            mm.lighter.SignerClient.CANCEL_ALL_TIF_IMMEDIATE,
        )
