"""Shared WebSocket subscription manager used by market_maker_v2 and gather_lighter_data."""

import asyncio
import logging
import time

import websockets

try:
    import orjson as _json

    def _dumps(obj):
        return _json.dumps(obj).decode()

    _loads = _json.loads
except ImportError:
    import json as _json

    _dumps = _json.dumps
    _loads = _json.loads

_PONG_MSG = '{"type":"pong"}'

_logger = logging.getLogger(__name__)


async def ws_subscribe(
    channels,
    label,
    on_message,
    *,
    url="wss://mainnet.zklighter.elliot.ai/stream",
    ping_interval=20,
    recv_timeout=30.0,
    reconnect_base=5,
    reconnect_max=60,
    on_connect=None,
    on_disconnect=None,
    logger=None,
):
    """Generic WebSocket subscription loop with exponential backoff reconnect.

    Args:
        channels: List of channel strings to subscribe to.
        label: Human-readable label for log messages.
        on_message: Callback ``(data: dict)`` called for each decoded message
            (excluding pings and subscription confirmations).
        url: WebSocket endpoint URL.
        ping_interval: Seconds between protocol-level pings.
        recv_timeout: Seconds of silence before watchdog triggers reconnect.
        reconnect_base: Initial backoff delay in seconds.
        reconnect_max: Maximum backoff delay in seconds.
        on_connect: Optional async callback invoked after subscribing.
        on_disconnect: Optional callback invoked on disconnect.
        logger: Optional logger instance; defaults to module-level logger.
    """
    if logger is None:
        logger = _logger

    backoff = reconnect_base

    while True:
        try:
            if on_disconnect:
                on_disconnect()

            async with websockets.connect(
                url,
                ping_interval=ping_interval,
                ping_timeout=ping_interval,
            ) as ws:
                logger.info(f"Connected to {url} for {label}")

                for ch in channels:
                    await ws.send(_dumps({"type": "subscribe", "channel": ch}))
                logger.info(f"Subscribed to {', '.join(channels)}")

                if on_connect:
                    await on_connect()

                backoff = reconnect_base  # reset on successful connect

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                        try:
                            data = _loads(message)
                            msg_type = data.get("type")

                            if msg_type == "ping":
                                await ws.send(_PONG_MSG)
                            elif msg_type == "subscribed":
                                logger.info(f"Subscribed to channel: {data.get('channel')}")
                            else:
                                on_message(data)
                        except (ValueError, TypeError):
                            logger.warning(f"Failed to decode JSON from {label}: {message}")

                    except asyncio.TimeoutError:
                        logger.warning(
                            f"{label} WebSocket watchdog triggered "
                            f"(no data for {recv_timeout}s). Reconnecting..."
                        )
                        break

        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
            logger.info(f"{label} WebSocket disconnected ({e}), reconnecting in {backoff:.0f}s...")
            if on_disconnect:
                on_disconnect()
            await asyncio.sleep(backoff + backoff * 0.2 * (time.monotonic() % 1))
            backoff = min(backoff * 2, reconnect_max)

        except asyncio.CancelledError:
            raise

        except Exception as e:
            logger.error(f"Unexpected error in {label} socket: {e}. Reconnecting in {backoff:.0f}s...")
            if on_disconnect:
                on_disconnect()
            await asyncio.sleep(backoff + backoff * 0.2 * (time.monotonic() % 1))
            backoff = min(backoff * 2, reconnect_max)
