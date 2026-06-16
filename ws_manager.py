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


async def _cancel_and_drain(task: asyncio.Task | None) -> None:
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await task
    except (asyncio.CancelledError, websockets.exceptions.ConnectionClosedOK):
        pass


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
    reconnect_event=None,
    channel_auths: dict | None = None,
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
        reconnect_event: Optional ``asyncio.Event``.  When set by an external
            caller (e.g. the orderbook sanity checker), the inner recv loop
            breaks and triggers a reconnect with a fresh snapshot.  The event
            is cleared automatically after being consumed.
    """
    if logger is None:
        logger = _logger

    backoff = reconnect_base

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=ping_interval,
                ping_timeout=ping_interval,
                close_timeout=5,
            ) as ws:
                logger.info(f"Connected to {url} for {label}")

                for ch in channels:
                    sub = {"type": "subscribe", "channel": ch}
                    if channel_auths and ch in channel_auths:
                        sub["auth"] = channel_auths[ch]
                    await ws.send(_dumps(sub))
                logger.info(f"Subscribed to {', '.join(channels)}")

                if on_connect:
                    await on_connect()

                backoff = reconnect_base  # reset on successful connect

                # Create reconnect-event watcher ONCE per connection
                # (not per message — saves a create_task + cancel each iteration)
                event_task = None
                if reconnect_event is not None:
                    event_task = asyncio.create_task(reconnect_event.wait())

                while True:
                    recv_task = asyncio.create_task(ws.recv())
                    wait_set = {recv_task}
                    if event_task is not None:
                        wait_set.add(event_task)

                    try:
                        done, pending = await asyncio.wait(
                            wait_set,
                            timeout=recv_timeout,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except asyncio.CancelledError:
                        await _cancel_and_drain(recv_task)
                        await _cancel_and_drain(event_task)
                        raise
                    for task in pending:
                        await _cancel_and_drain(task)

                    if not done:
                        logger.warning(
                            f"{label} WebSocket watchdog triggered "
                            f"(no data for {recv_timeout}s). Reconnecting..."
                        )
                        if on_disconnect:
                            on_disconnect()
                        break

                    if event_task is not None and event_task in done:
                        if reconnect_event.is_set():
                            reconnect_event.clear()
                            logger.info(f"{label} reconnect requested via event; dropping connection for fresh snapshot...")
                            if not recv_task.done():
                                await _cancel_and_drain(recv_task)
                            if on_disconnect:
                                on_disconnect()
                            break
                        # Spurious wake (event cleared before we checked) — recreate watcher
                        event_task = asyncio.create_task(reconnect_event.wait())
                        if recv_task not in done:
                            continue

                    message = recv_task.result()
                    try:
                        data = _loads(message)
                        msg_type = data.get("type")
                    except (ValueError, TypeError, AttributeError):
                        logger.warning(f"Failed to decode JSON from {label}: {message!r}")
                        continue

                    if msg_type == "ping":
                        await ws.send(_PONG_MSG)
                    elif msg_type == "subscribed":
                        logger.info(f"Subscribed to channel: {data.get('channel')}")
                    else:
                        # A buggy callback must not tear down the connection —
                        # log and keep consuming the stream.
                        try:
                            on_message(data)
                        except Exception:
                            logger.error(
                                f"Error in {label} on_message callback; continuing",
                                exc_info=True,
                            )

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


async def ws_subscribe_fast(
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
    reconnect_event=None,
    channel_auths: dict | None = None,
):
    """Low-overhead WebSocket subscription loop for latency-sensitive feeds.

    Drop-in replacement for ``ws_subscribe()`` that eliminates per-message
    ``create_task``/``asyncio.wait``/``cancel`` overhead by using a tight
    ``asyncio.wait_for(ws.recv(), timeout)`` loop instead.

    Use for hot-path market data (orderbook, ticker).  Cold-path channels
    (account, positions) can keep using ``ws_subscribe()``.
    """
    if logger is None:
        logger = _logger

    backoff = reconnect_base

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=ping_interval,
                ping_timeout=ping_interval,
                close_timeout=5,
            ) as ws:
                logger.info(f"Connected to {url} for {label}")

                for ch in channels:
                    sub = {"type": "subscribe", "channel": ch}
                    if channel_auths and ch in channel_auths:
                        sub["auth"] = channel_auths[ch]
                    await ws.send(_dumps(sub))
                logger.info(f"Subscribed to {', '.join(channels)}")

                if on_connect:
                    await on_connect()

                backoff = reconnect_base  # reset on successful connect

                # Tight recv loop — no per-message task creation
                while True:
                    # Non-blocking reconnect-event check (replaces event_task racing)
                    if reconnect_event is not None and reconnect_event.is_set():
                        reconnect_event.clear()
                        logger.info(f"{label} reconnect requested via event; dropping connection for fresh snapshot...")
                        if on_disconnect:
                            on_disconnect()
                        break

                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"{label} WebSocket watchdog triggered "
                            f"(no data for {recv_timeout}s). Reconnecting..."
                        )
                        if on_disconnect:
                            on_disconnect()
                        break

                    try:
                        data = _loads(message)
                        msg_type = data.get("type")
                    except (ValueError, TypeError, AttributeError):
                        logger.warning(f"Failed to decode JSON from {label}: {message!r}")
                        continue

                    if msg_type == "ping":
                        await ws.send(_PONG_MSG)
                    elif msg_type == "subscribed":
                        logger.info(f"Subscribed to channel: {data.get('channel')}")
                    else:
                        # A buggy callback must not tear down the connection —
                        # a reconnect here clears the book and resets the
                        # volatility state (quoting downtime).  Log and keep
                        # consuming the stream instead.
                        try:
                            on_message(data)
                        except Exception:
                            logger.error(
                                f"Error in {label} on_message callback; continuing",
                                exc_info=True,
                            )

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
