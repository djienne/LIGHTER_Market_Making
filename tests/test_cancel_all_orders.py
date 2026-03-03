import unittest
from unittest.mock import patch

import market_maker_v2 as mm
from _helpers import DummyCancelClient


class TestCancelAllOrders(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_all_orders(self):
        client = DummyCancelClient()
        await mm.cancel_all_orders(client)

        self.assertEqual(len(client.cancel_all_calls), 1)
        kwargs = client.cancel_all_calls[0]["kwargs"]
        self.assertIn("timestamp_ms", kwargs)
        self.assertIsInstance(kwargs["timestamp_ms"], int)
        self.assertEqual(kwargs["timestamp_ms"], 0)
        self.assertEqual(
            kwargs["time_in_force"],
            mm.lighter.SignerClient.CANCEL_ALL_TIF_IMMEDIATE,
        )

    async def test_retry_on_transient_error(self):
        """429 error string triggers retry with backoff."""
        call_count = 0

        class _RetryClient:
            def __init__(self):
                self.cancel_all_calls = []

            async def cancel_all_orders(self, *args, **kwargs):
                nonlocal call_count
                call_count += 1
                self.cancel_all_calls.append({"args": args, "kwargs": kwargs})
                if call_count == 1:
                    return "tx", "resp", "429 Too Many Requests"
                return "tx", "resp", None

        client = _RetryClient()
        with patch("asyncio.sleep"):  # skip actual backoff delay
            await mm.cancel_all_orders(client)

        self.assertEqual(call_count, 2)  # first call + 1 retry
        self.assertEqual(len(client.cancel_all_calls), 2)

    async def test_retry_exhaustion(self):
        """3 consecutive 429s exhaust retries (no infinite loop)."""
        call_count = 0

        class _AlwaysRateLimitClient:
            async def cancel_all_orders(self, *args, **kwargs):
                nonlocal call_count
                call_count += 1
                return "tx", "resp", "429 Too Many Requests"

        client = _AlwaysRateLimitClient()
        with patch("asyncio.sleep"):
            await mm.cancel_all_orders(client)

        # 1 initial + 3 retries = 4 total calls
        self.assertEqual(call_count, 4)

    async def test_retry_on_transient_exception(self):
        """Transient exception (not error string) also triggers retry."""
        call_count = 0

        class _ExceptionThenOkClient:
            async def cancel_all_orders(self, *args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("429 rate limit")
                return "tx", "resp", None

        client = _ExceptionThenOkClient()
        with patch("asyncio.sleep"):
            await mm.cancel_all_orders(client)

        self.assertEqual(call_count, 2)

    async def test_typeerror_fallback(self):
        """TypeError on timestamp_ms= falls back to time= parameter."""
        class _TypeErrorClient:
            def __init__(self):
                self.cancel_all_calls = []
                self._first_call = True

            async def cancel_all_orders(self, *args, **kwargs):
                self.cancel_all_calls.append({"args": args, "kwargs": kwargs})
                if self._first_call and "timestamp_ms" in kwargs:
                    self._first_call = False
                    raise TypeError("unexpected keyword argument 'timestamp_ms'")
                return "tx", "resp", None

        client = _TypeErrorClient()
        await mm.cancel_all_orders(client)

        # Should have 2 calls: first with timestamp_ms (fails), second with time
        self.assertEqual(len(client.cancel_all_calls), 2)
        self.assertIn("timestamp_ms", client.cancel_all_calls[0]["kwargs"])
        self.assertIn("time", client.cancel_all_calls[1]["kwargs"])
        self.assertEqual(client.cancel_all_calls[1]["kwargs"]["time"], 0)
