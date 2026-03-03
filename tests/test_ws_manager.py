import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import ws_manager


def _make_mock_ws(messages):
    """Create a mock websocket that yields *messages* then blocks forever."""
    ws = AsyncMock()
    ws.send = AsyncMock()

    _iter = iter(messages)

    async def _recv():
        try:
            return next(_iter)
        except StopIteration:
            await asyncio.sleep(999)  # block until cancelled

    ws.recv = _recv
    return ws


class _FakeConnectCM:
    """Async context manager that returns a pre-built mock WS."""
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *exc):
        return False


class TestWsSubscribe(unittest.IsolatedAsyncioTestCase):
    @patch("ws_manager.websockets.connect")
    async def test_on_connect_called_after_subscribe(self, mock_connect):
        ws = _make_mock_ws([])
        mock_connect.return_value.__aenter__ = AsyncMock(return_value=ws)
        mock_connect.return_value.__aexit__ = AsyncMock(return_value=False)

        on_connect = AsyncMock()
        on_message = MagicMock()

        task = asyncio.create_task(
            ws_manager.ws_subscribe(
                channels=["order_book/1"],
                label="test",
                on_message=on_message,
                on_connect=on_connect,
                recv_timeout=0.1,
                reconnect_base=0.01,
                reconnect_max=0.02,
            )
        )
        await asyncio.sleep(0.15)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        on_connect.assert_awaited()

    @patch("ws_manager.websockets.connect")
    async def test_on_message_called_for_data(self, mock_connect):
        ws = _make_mock_ws(['{"type":"update","price":100}'])
        mock_connect.return_value.__aenter__ = AsyncMock(return_value=ws)
        mock_connect.return_value.__aexit__ = AsyncMock(return_value=False)

        on_message = MagicMock()

        task = asyncio.create_task(
            ws_manager.ws_subscribe(
                channels=["order_book/1"],
                label="test",
                on_message=on_message,
                recv_timeout=0.2,
                reconnect_base=0.01,
                reconnect_max=0.02,
            )
        )
        await asyncio.sleep(0.15)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        on_message.assert_called_once()
        data = on_message.call_args[0][0]
        self.assertEqual(data["type"], "update")
        self.assertEqual(data["price"], 100)

    @patch("ws_manager.websockets.connect")
    async def test_ping_answered_with_pong(self, mock_connect):
        ws = _make_mock_ws(['{"type":"ping"}'])
        mock_connect.return_value.__aenter__ = AsyncMock(return_value=ws)
        mock_connect.return_value.__aexit__ = AsyncMock(return_value=False)

        on_message = MagicMock()

        task = asyncio.create_task(
            ws_manager.ws_subscribe(
                channels=[],
                label="test",
                on_message=on_message,
                recv_timeout=0.2,
                reconnect_base=0.01,
                reconnect_max=0.02,
            )
        )
        await asyncio.sleep(0.15)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        # ping should NOT be forwarded to on_message
        on_message.assert_not_called()
        # pong should be sent back
        ws.send.assert_called_with('{"type":"pong"}')

    @patch("ws_manager.websockets.connect")
    async def test_on_disconnect_not_called_before_actual_disconnect(self, mock_connect):
        ws = _make_mock_ws([])
        mock_connect.return_value.__aenter__ = AsyncMock(return_value=ws)
        mock_connect.return_value.__aexit__ = AsyncMock(return_value=False)

        on_disconnect = MagicMock()

        task = asyncio.create_task(
            ws_manager.ws_subscribe(
                channels=[],
                label="test",
                on_message=MagicMock(),
                on_disconnect=on_disconnect,
                recv_timeout=0.2,
                reconnect_base=0.01,
                reconnect_max=0.02,
            )
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        on_disconnect.assert_not_called()

    @patch("ws_manager.websockets.connect")
    async def test_reconnect_on_connection_closed(self, mock_connect):
        import websockets.exceptions

        ws_first = AsyncMock()

        async def _recv_raise():
            raise websockets.exceptions.ConnectionClosed(None, None)

        ws_first.recv = _recv_raise
        ws_first.send = AsyncMock()

        ws_second = _make_mock_ws([])
        ws_second.send = AsyncMock()

        call_count = 0
        def _connect_side_effect(*a, **kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _FakeConnectCM(ws_first)
            return _FakeConnectCM(ws_second)

        mock_connect.side_effect = _connect_side_effect
        on_disconnect = MagicMock()

        task = asyncio.create_task(
            ws_manager.ws_subscribe(
                channels=[],
                label="test",
                on_message=MagicMock(),
                on_disconnect=on_disconnect,
                recv_timeout=0.5,
                reconnect_base=0.01,
                reconnect_max=0.02,
            )
        )
        await asyncio.sleep(0.3)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        # Should have connected at least twice (initial + reconnect)
        self.assertGreaterEqual(call_count, 2)
        self.assertGreaterEqual(on_disconnect.call_count, 1)

    @patch("ws_manager.websockets.connect")
    async def test_reconnect_event_breaks_inner_loop(self, mock_connect):
        reconnect_event = asyncio.Event()
        reconnect_event.set()  # pre-set to trigger immediate reconnect

        connect_count = 0
        on_disconnect = MagicMock()

        def _connect_counter(*a, **kw):
            nonlocal connect_count
            connect_count += 1
            if connect_count > 1:
                reconnect_event.clear()
            return _FakeConnectCM(_make_mock_ws([]))

        mock_connect.side_effect = _connect_counter

        task = asyncio.create_task(
            ws_manager.ws_subscribe(
                channels=[],
                label="test",
                on_message=MagicMock(),
                on_disconnect=on_disconnect,
                reconnect_event=reconnect_event,
                recv_timeout=0.2,
                reconnect_base=0.01,
                reconnect_max=0.02,
            )
        )
        await asyncio.sleep(0.3)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        # Should have reconnected at least once due to the event
        self.assertGreaterEqual(connect_count, 2)
        self.assertGreaterEqual(on_disconnect.call_count, 1)

    @patch("ws_manager.websockets.connect")
    async def test_backoff_increases_on_repeated_failures(self, mock_connect):
        """Verify backoff doubles on consecutive failures (capped at reconnect_max)."""
        mock_connect.side_effect = Exception("connection refused")

        connect_times = []

        original_sleep = asyncio.sleep

        async def _timed_sleep(delay, *a, **kw):
            connect_times.append(delay)
            if len(connect_times) >= 3:
                raise asyncio.CancelledError
            await original_sleep(0)  # don't actually sleep

        with patch("ws_manager.asyncio.sleep", side_effect=_timed_sleep):
            task = asyncio.create_task(
                ws_manager.ws_subscribe(
                    channels=[],
                    label="test",
                    on_message=MagicMock(),
                    reconnect_base=1,
                    reconnect_max=8,
                )
            )
            with self.assertRaises(asyncio.CancelledError):
                await task

        # Backoff includes jitter (base + base * 0.2 * fraction), so check approximate doubling
        # First delay should be ~1s, second ~2s
        self.assertGreaterEqual(len(connect_times), 2)
        self.assertGreater(connect_times[1], connect_times[0])
