"""Runtime orderbook sanity checker.

Periodically compares the local WebSocket-maintained orderbook against a fresh
REST snapshot.  On divergence beyond a configurable tolerance the caller can
force a WS reconnect to obtain a fresh snapshot.
"""

import asyncio
import logging
import time
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://mainnet.zklighter.elliot.ai"


@dataclass(frozen=True, slots=True)
class SanityResult:
    ok: bool
    reason: str
    ws_best_bid: float
    ws_best_ask: float
    rest_best_bid: float
    rest_best_ask: float
    bid_diff_pct: float
    ask_diff_pct: float
    latency_ms: float


def _fetch_rest_top_of_book(market_id: int, timeout: float = 5.0):
    """Fetch best bid/ask from REST (runs in executor — blocking)."""
    resp = requests.get(
        f"{BASE_URL}/api/v1/orderBookOrders",
        params={"market_id": market_id, "limit": 10},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()

    best_bid = 0.0
    best_ask = 0.0

    bids = data.get("bids", data.get("order_book", {}).get("bids", []))
    asks = data.get("asks", data.get("order_book", {}).get("asks", []))

    for item in bids:
        price = float(item.get("price", 0))
        size = float(item.get("remaining_base_amount", item.get("size", 0)))
        if price > 0 and size > 0:
            if price > best_bid:
                best_bid = price

    for item in asks:
        price = float(item.get("price", 0))
        size = float(item.get("remaining_base_amount", item.get("size", 0)))
        if price > 0 and size > 0:
            if best_ask == 0.0 or price < best_ask:
                best_ask = price

    return best_bid, best_ask


async def check_orderbook_sanity(
    market_id: int,
    ws_bids,
    ws_asks,
    tolerance_pct: float = 0.5,
    timeout: float = 5.0,
) -> SanityResult:
    """Compare local WS book against REST and return a SanityResult.

    Args:
        market_id: Lighter market index.
        ws_bids: SortedDict of bids (price -> size).
        ws_asks: SortedDict of asks (price -> size).
        tolerance_pct: Maximum acceptable divergence (percent) between
            WS and REST best bid/ask.
        timeout: HTTP request timeout in seconds.

    Returns:
        SanityResult with ``ok=True`` when the books agree.
    """
    _zero = SanityResult(
        ok=False, reason="", ws_best_bid=0, ws_best_ask=0,
        rest_best_bid=0, rest_best_ask=0,
        bid_diff_pct=0, ask_diff_pct=0, latency_ms=0,
    )

    # --- Check WS book validity first (no network needed) ---
    if not ws_bids or not ws_asks:
        return SanityResult(
            ok=False, reason="WS book empty",
            ws_best_bid=0, ws_best_ask=0,
            rest_best_bid=0, rest_best_ask=0,
            bid_diff_pct=0, ask_diff_pct=0, latency_ms=0,
        )

    try:
        ws_best_bid = ws_bids.peekitem(-1)[0]
        ws_best_ask = ws_asks.peekitem(0)[0]
    except (IndexError, ValueError):
        return SanityResult(
            ok=False, reason="WS book peek failed",
            ws_best_bid=0, ws_best_ask=0,
            rest_best_bid=0, rest_best_ask=0,
            bid_diff_pct=0, ask_diff_pct=0, latency_ms=0,
        )

    if ws_best_bid >= ws_best_ask:
        return SanityResult(
            ok=False,
            reason=f"WS crossed book: bid={ws_best_bid} >= ask={ws_best_ask}",
            ws_best_bid=ws_best_bid, ws_best_ask=ws_best_ask,
            rest_best_bid=0, rest_best_ask=0,
            bid_diff_pct=0, ask_diff_pct=0, latency_ms=0,
        )

    # --- Fetch REST top-of-book ---
    loop = asyncio.get_running_loop()
    t0 = time.monotonic()
    try:
        rest_best_bid, rest_best_ask = await loop.run_in_executor(
            None, _fetch_rest_top_of_book, market_id, timeout,
        )
    except Exception as e:
        return SanityResult(
            ok=False, reason=f"REST fetch failed: {e}",
            ws_best_bid=ws_best_bid, ws_best_ask=ws_best_ask,
            rest_best_bid=0, rest_best_ask=0,
            bid_diff_pct=0, ask_diff_pct=0, latency_ms=0,
        )
    latency_ms = (time.monotonic() - t0) * 1000

    if rest_best_bid <= 0 or rest_best_ask <= 0:
        return SanityResult(
            ok=False, reason="REST returned invalid prices",
            ws_best_bid=ws_best_bid, ws_best_ask=ws_best_ask,
            rest_best_bid=rest_best_bid, rest_best_ask=rest_best_ask,
            bid_diff_pct=0, ask_diff_pct=0, latency_ms=latency_ms,
        )

    # --- Compare ---
    bid_diff_pct = abs(ws_best_bid - rest_best_bid) / rest_best_bid * 100
    ask_diff_pct = abs(ws_best_ask - rest_best_ask) / rest_best_ask * 100

    if bid_diff_pct > tolerance_pct or ask_diff_pct > tolerance_pct:
        return SanityResult(
            ok=False,
            reason=(
                f"Divergence exceeds {tolerance_pct}%: "
                f"bid_diff={bid_diff_pct:.4f}%, ask_diff={ask_diff_pct:.4f}%"
            ),
            ws_best_bid=ws_best_bid, ws_best_ask=ws_best_ask,
            rest_best_bid=rest_best_bid, rest_best_ask=rest_best_ask,
            bid_diff_pct=bid_diff_pct, ask_diff_pct=ask_diff_pct,
            latency_ms=latency_ms,
        )

    return SanityResult(
        ok=True, reason="OK",
        ws_best_bid=ws_best_bid, ws_best_ask=ws_best_ask,
        rest_best_bid=rest_best_bid, rest_best_ask=rest_best_ask,
        bid_diff_pct=bid_diff_pct, ask_diff_pct=ask_diff_pct,
        latency_ms=latency_ms,
    )
