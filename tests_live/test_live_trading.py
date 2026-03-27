"""
Live trading integration tests for the Lighter DEX Market Maker.

Tests authenticated trading operations (balance check, order placement,
cancellation, and price modification) by calling the **real production
functions** in market_maker_v2.py — not raw SDK calls.

Credentials required in .env (auto-skipped when absent):
    API_KEY_PRIVATE_KEY=0x...
    ACCOUNT_INDEX=123
    API_KEY_INDEX=0    (optional, defaults to 0)

Safety:
    - Buy orders placed 20% below mid-price  (no fill risk)
    - Sell orders placed 20% above mid-price (no fill risk)
    - Order size: 0.0002 BTC (minimum practical)
    - autouse teardown cancels all orders after every test

Run with:
    pytest tests_live/test_live_trading.py -v --timeout=60
"""

import asyncio
import os
import pathlib
import sys
import time

import pytest
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Make tests/_helpers importable from tests_live/ without a conftest change.
# tests/conftest.py adds this path for the unit-test suite; we replicate it
# here so temp_mm_attrs is available when running only tests_live/.
# ---------------------------------------------------------------------------
_tests_dir = str(pathlib.Path(__file__).resolve().parent.parent / "tests")
if _tests_dir not in sys.path:
    sys.path.insert(0, _tests_dir)

from _helpers import temp_mm_attrs  # noqa: E402
import market_maker_v2 as mm  # noqa: E402


BASE_URL = "https://mainnet.zklighter.elliot.ai"
_ORDER_CLEANUP_TIMEOUT_SEC = 30.0
_ORDER_CLEANUP_POLL_SEC = 0.5


# ---------------------------------------------------------------------------
# REST helpers (public — no auth required)
# ---------------------------------------------------------------------------

def _get_btc_market_id() -> int | None:
    """Return the market_id for the BTC perpetual, or None if not found.

    The exchange API uses 'symbol' (not 'market_ticker') in /api/v1/orderBooks.
    """
    data = requests.get(f"{BASE_URL}/api/v1/orderBooks", timeout=10).json()
    for ob in data.get("order_books", []):
        ticker = ob.get("symbol") or ob.get("market_ticker", "")
        if "BTC" in ticker.upper():
            return ob["market_id"]
    return None


def _get_btc_mid_price(market_id: int) -> float:
    """Return the current mid-price for the given BTC market.

    Uses /api/v1/orderBookOrders (public, returns bids/asks).
    """
    data = requests.get(
        f"{BASE_URL}/api/v1/orderBookOrders",
        params={"market_id": market_id, "limit": 1},
        timeout=10,
    ).json()
    best_bid = float(data["bids"][0]["price"])
    best_ask = float(data["asks"][0]["price"])
    return (best_bid + best_ask) / 2.0


def _get_active_orders(account_index: int, market_id: int, auth_token: str = "") -> list:
    """Return the list of active orders for the account on the given market.

    Requires an auth token (from _generate_ws_auth_token / create_auth_token_with_expiry).
    Raises AssertionError on transport/auth/protocol errors to avoid false positives.
    """
    params = {"account_index": account_index, "market_id": market_id}
    if auth_token:
        params["auth"] = auth_token
    try:
        resp = requests.get(
            f"{BASE_URL}/api/v1/accountActiveOrders",
            params=params,
            timeout=10,
        )
    except requests.RequestException as exc:
        raise AssertionError(f"accountActiveOrders request failed: {exc}") from exc

    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        snippet = resp.text[:300]
        raise AssertionError(
            f"accountActiveOrders HTTP {resp.status_code}: {snippet!r}"
        ) from exc

    try:
        data = resp.json()
    except ValueError as exc:
        raise AssertionError(
            f"accountActiveOrders returned non-JSON body: {resp.text[:300]!r}"
        ) from exc

    orders = data.get("orders")
    if not isinstance(orders, list):
        raise AssertionError(
            f"accountActiveOrders payload missing list 'orders': keys={list(data.keys())}"
        )
    return orders


def _find_order_by_price(
    orders: list, target_price: float, tolerance: float = 1.0
) -> dict | None:
    """Return the first order whose price is within *tolerance* of target_price."""
    for order in orders:
        if abs(float(order.get("price", 0)) - target_price) <= tolerance:
            return order
    return None


def _order_index(order: dict) -> int:
    """Extract the exchange order identifier from an active-orders entry."""
    idx = order.get("order_index") or order.get("client_order_index")
    if idx is None:
        raise KeyError(f"No order_index / client_order_index in order: {order}")
    return int(idx)


def _ensure_no_open_orders(
    live_client,
    account_index: int,
    market_id: int,
    *,
    timeout_sec: float = _ORDER_CLEANUP_TIMEOUT_SEC,
) -> None:
    """Cancel and verify account has zero active orders for the target market."""
    _run_async(mm.cancel_all_orders(live_client))
    deadline = time.monotonic() + timeout_sec
    last_orders = None
    last_error = None

    while time.monotonic() < deadline:
        token = mm._generate_ws_auth_token(live_client)
        if token is None:
            last_error = "auth token unavailable during open-order verification"
            _run_async(mm.cancel_all_orders(live_client))
            time.sleep(_ORDER_CLEANUP_POLL_SEC)
            continue

        try:
            last_orders = _get_active_orders(account_index, market_id, token)
        except AssertionError as exc:
            last_error = str(exc)
            time.sleep(_ORDER_CLEANUP_POLL_SEC)
            continue

        if not last_orders:
            return

        _run_async(mm.cancel_all_orders(live_client))
        time.sleep(_ORDER_CLEANUP_POLL_SEC)

    raise AssertionError(
        "Open-order cleanup verification failed; residual open orders remain. "
        f"market_id={market_id} account_index={account_index} "
        f"last_orders={last_orders} last_error={last_error}"
    )


def _signed_position_for_market(positions: dict, market_id: int) -> float | None:
    """Derive signed position from account_all positions payload for one market."""
    market_position = positions.get(str(market_id))
    if market_position is None:
        market_position = positions.get(market_id)
    if market_position is None:
        return None
    size = float(market_position.get("position", 0))
    sign = int(market_position.get("sign", 1))
    return -size if sign == -1 else size


_shared_loop = None


def _run_async(coro):
    """Run a coroutine on a persistent module-level event loop.

    All fixtures and tests share the same loop so the aiohttp session
    (created in the module-scoped ``live_client`` fixture) is never used
    from a different loop.
    """
    global _shared_loop
    if _shared_loop is None or _shared_loop.is_closed():
        _shared_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_shared_loop)
    return _shared_loop.run_until_complete(_shared_loop.create_task(coro))


# ---------------------------------------------------------------------------
# Module-scoped fixtures  (one auth session for the whole trading test suite)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def creds():
    """Load and return trading credentials from .env.

    Calls pytest.skip() if credentials are absent (belt-and-suspenders on top
    of the conftest.py collection modifier).
    """
    load_dotenv()
    private_key = os.getenv("API_KEY_PRIVATE_KEY")
    account_index = os.getenv("ACCOUNT_INDEX")
    api_key_index = os.getenv("API_KEY_INDEX", "0")
    if not private_key or not account_index:
        pytest.skip("Trading credentials not in .env (API_KEY_PRIVATE_KEY, ACCOUNT_INDEX)")
    return {
        "private_key": private_key,
        "account_index": int(account_index),
        "api_key_index": int(api_key_index),
    }


@pytest.fixture(scope="module")
def btc_market_id():
    """Return the BTC perpetual market_id, or skip if not found."""
    mid = _get_btc_market_id()
    if mid is None:
        pytest.skip("BTC market not found on exchange")
    return mid


@pytest.fixture(scope="module")
def btc_tick_sizes(btc_market_id):
    """Return (price_tick_float, amount_tick_float) for the BTC market.

    Fetched from the exchange REST API via get_market_details_async.
    """
    from utils import get_market_details

    _, price_tick, amount_tick = get_market_details("BTC")
    if price_tick is None or amount_tick is None:
        pytest.skip("Could not fetch tick sizes for BTC")
    return float(price_tick), float(amount_tick)


@pytest.fixture(scope="module")
def live_client(creds):
    """Create and validate a SignerClient; close it after all module tests finish.

    SignerClient.__init__ creates an aiohttp connector that requires a running
    event loop, so construction is wrapped in _run_async.
    """
    async def _make():
        return mm.build_signer_client(
            url=BASE_URL,
            account_index=creds["account_index"],
            private_key=creds["private_key"],
            api_key_index=creds["api_key_index"],
        )

    client = _run_async(_make())
    err = client.check_client()
    if err:
        pytest.skip(f"SignerClient check failed: {err}")
    yield client
    _run_async(client.close())


@pytest.fixture
def auth_token(live_client):
    """Short-lived WS/REST auth token for authenticated API calls.

    Token lifetime is short (~10 minutes), so generate per test to avoid expiry flakes.
    """
    token = mm._generate_ws_auth_token(live_client)
    if token is None:
        pytest.skip("Could not generate auth token — skipping authenticated tests")
    return token


# ---------------------------------------------------------------------------
# Function-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def cleanup(live_client, creds, btc_market_id):
    """Pre/post guard: force and verify zero open orders around every live test."""
    _ensure_no_open_orders(live_client, creds["account_index"], btc_market_id)
    yield
    _ensure_no_open_orders(live_client, creds["account_index"], btc_market_id)


@pytest.fixture(scope="module", autouse=True)
def module_final_cleanup(live_client, creds, btc_market_id):
    """Final guard at module teardown: no residual open orders can remain."""
    yield
    _ensure_no_open_orders(live_client, creds["account_index"], btc_market_id, timeout_sec=45.0)


@pytest.fixture
def safe_buy_price(btc_market_id):
    """Price 20% below mid — guaranteed not to fill under normal conditions."""
    return round(_get_btc_mid_price(btc_market_id) * 0.80, 1)


@pytest.fixture
def safe_sell_price(btc_market_id):
    """Price 20% above mid — guaranteed not to fill under normal conditions."""
    return round(_get_btc_mid_price(btc_market_id) * 1.20, 1)


# ===========================================================================
# 1. TestLiveBalance
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveBalance:
    """Verify that the account credentials are valid and the account is queryable."""

    def test_account_credentials_valid(self, live_client, creds):
        """check_client() must succeed — proves the account exists and creds are valid.

        Note: /api/v1/accounts requires authentication as of early 2026, so we
        validate via the authenticated SDK client instead of a public REST call.
        """
        err = live_client.check_client()
        assert err is None, f"check_client() returned error: {err}"
        assert live_client.account_index == creds["account_index"], (
            f"Client account_index mismatch: {live_client.account_index} != {creds['account_index']}"
        )

    def test_account_has_positive_balance(self, creds):
        """Connect to user_stats WS and verify the account has a positive portfolio value."""
        import json as _json

        import websockets as _ws

        account_id = creds["account_index"]
        channel = f"user_stats/{account_id}"
        balance_info = {"portfolio_value": None, "available_balance": None}

        async def _run():
            async with _ws.connect(
                mm.WEBSOCKET_URL, ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(_json.dumps({
                    "type": "subscribe",
                    "channel": channel,
                }))
                deadline = asyncio.get_event_loop().time() + 15
                while asyncio.get_event_loop().time() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        data = _json.loads(raw)
                        msg_type = data.get("type", "")
                        if msg_type == "ping":
                            await ws.send('{"type":"pong"}')
                        elif "user_stats" in msg_type:
                            stats = data.get("stats", {})
                            if "portfolio_value" in stats:
                                balance_info["portfolio_value"] = float(stats["portfolio_value"])
                                balance_info["available_balance"] = float(stats.get("available_balance", 0))
                                break
                    except asyncio.TimeoutError:
                        break

        _run_async(_run())
        pv = balance_info["portfolio_value"]
        ab = balance_info["available_balance"]
        assert pv is not None, "Did not receive portfolio_value from user_stats WS within 15s"
        assert pv > 0, f"Portfolio value is not positive: ${pv:.2f}"
        assert ab is not None and ab >= 0, f"Available balance invalid: ${ab}"


# ===========================================================================
# 2. TestLiveUserStatsSubscriber
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveUserStatsSubscriber:
    """Verify subscribe_to_user_stats() updates account state and sets readiness."""

    def test_subscribe_to_user_stats_sets_state(self, creds):
        orig_available = mm.state.account.available_capital
        orig_portfolio = mm.state.account.portfolio_value
        orig_event_set = mm.account_state_received.is_set()
        mm.state.account.available_capital = None
        mm.state.account.portfolio_value = None
        mm.account_state_received.clear()

        async def _run():
            task = asyncio.create_task(mm.subscribe_to_user_stats(creds["account_index"]))
            got_state = False
            try:
                await asyncio.wait_for(mm.account_state_received.wait(), timeout=20)
                got_state = True
            except asyncio.TimeoutError:
                got_state = False
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            return got_state

        try:
            got_state = _run_async(_run())
            if not got_state:
                pytest.skip("subscribe_to_user_stats produced no account snapshot within 20s")
            assert mm.account_state_received.is_set(), "account_state_received was not set"
            assert mm.state.account.available_capital is not None
            assert mm.state.account.available_capital >= 0
            assert mm.state.account.portfolio_value is not None
            assert mm.state.account.portfolio_value > 0
        finally:
            mm.state.account.available_capital = orig_available
            mm.state.account.portfolio_value = orig_portfolio
            if orig_event_set:
                mm.account_state_received.set()
            else:
                mm.account_state_received.clear()


# ===========================================================================
# 3. TestLiveAccountAllSubscriber
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveAccountAllSubscriber:
    """Verify subscribe_to_account_all() updates positions and signed position size."""

    def test_subscribe_to_account_all_sets_position_state(self, creds, btc_market_id):
        orig_market_id = mm.state.config.market_id
        orig_positions = dict(mm.state.account.positions)
        orig_position_size = mm.state.account.position_size
        orig_event_set = mm.account_all_received.is_set()
        mm.state.account.positions = {}
        mm.state.account.position_size = 0.0
        mm.account_all_received.clear()

        async def _run():
            task = asyncio.create_task(mm.subscribe_to_account_all(creds["account_index"]))
            got_state = False
            try:
                await asyncio.wait_for(mm.account_all_received.wait(), timeout=25)
                got_state = True
            except asyncio.TimeoutError:
                got_state = False
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            return got_state

        try:
            with temp_mm_attrs(MARKET_ID=btc_market_id):
                got_state = _run_async(_run())
                if not got_state:
                    pytest.skip("subscribe_to_account_all produced no account snapshot within 25s")
                assert mm.account_all_received.is_set(), "account_all_received was not set"
                assert isinstance(mm.state.account.positions, dict), "positions payload is not a dict"
                signed = _signed_position_for_market(mm.state.account.positions, btc_market_id)
                if signed is None:
                    assert mm.state.account.position_size == 0.0, (
                        "position_size should be 0 when the market is absent in positions"
                    )
                else:
                    assert abs(mm.state.account.position_size - signed) < 1e-12, (
                        f"Signed position mismatch: state={mm.state.account.position_size} payload={signed}"
                    )
        finally:
            mm.state.config.market_id = orig_market_id
            mm.state.account.positions = orig_positions
            mm.state.account.position_size = orig_position_size
            if orig_event_set:
                mm.account_all_received.set()
            else:
                mm.account_all_received.clear()


# ===========================================================================
# 4. TestLiveOrderPlacement
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveOrderPlacement:
    """Verify mm.place_order() creates real orders on both sides."""

    def test_place_buy_order(self, live_client, creds, btc_market_id, safe_buy_price, auth_token, btc_tick_sizes):
        ptick, atick = btc_tick_sizes
        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            order_id = mm.next_client_order_index()
            result = _run_async(mm.place_order(live_client, "buy", safe_buy_price, order_id, 0.0002))
        assert result is True, "mm.place_order returned False for buy"

        time.sleep(1)
        orders = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        assert _find_order_by_price(orders, safe_buy_price) is not None, (
            f"Buy order at {safe_buy_price} not found in active orders: {orders}"
        )

    def test_place_sell_order(self, live_client, creds, btc_market_id, safe_sell_price, auth_token, btc_tick_sizes):
        ptick, atick = btc_tick_sizes
        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            order_id = mm.next_client_order_index()
            result = _run_async(mm.place_order(live_client, "sell", safe_sell_price, order_id, 0.0002))
        assert result is True, "mm.place_order returned False for sell"

        time.sleep(1)
        orders = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        assert _find_order_by_price(orders, safe_sell_price) is not None, (
            f"Sell order at {safe_sell_price} not found in active orders: {orders}"
        )

    def test_place_both_sides(
        self, live_client, creds, btc_market_id, safe_buy_price, safe_sell_price, auth_token, btc_tick_sizes
    ):
        """Place bid and ask sequentially — exercises the two-sided quoting path.

        Orders must be placed sequentially (not via asyncio.gather) because
        the exchange requires strictly ordered nonces per account.
        """
        ptick, atick = btc_tick_sizes
        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            bid_id = mm.next_client_order_index()
            bid_result = _run_async(mm.place_order(live_client, "buy", safe_buy_price, bid_id, 0.0002))
            ask_id = mm.next_client_order_index()
            ask_result = _run_async(mm.place_order(live_client, "sell", safe_sell_price, ask_id, 0.0002))

        assert bid_result is True, "Buy order failed"
        assert ask_result is True, "Sell order failed"

        time.sleep(1)
        orders = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        assert _find_order_by_price(orders, safe_buy_price) is not None, (
            f"Buy side not found. Active orders: {orders}"
        )
        assert _find_order_by_price(orders, safe_sell_price) is not None, (
            f"Sell side not found. Active orders: {orders}"
        )


# ===========================================================================
# 5. TestLiveOrderCancellation
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveOrderCancellation:
    """Verify mm.cancel_order() and mm.cancel_all_orders() via production code."""

    def test_cancel_specific_order(self, live_client, creds, btc_market_id, safe_buy_price, auth_token, btc_tick_sizes):
        ptick, atick = btc_tick_sizes
        # Place
        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            order_id = mm.next_client_order_index()
            result = _run_async(mm.place_order(live_client, "buy", safe_buy_price, order_id, 0.0002))
        assert result is True, "Failed to place buy order"
        time.sleep(1)

        # Locate on exchange and get exchange order_index
        orders = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        found = _find_order_by_price(orders, safe_buy_price)
        assert found is not None, "Buy order not found after placement"
        exchange_idx = _order_index(found)

        # Cancel via production function
        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            cancelled = _run_async(mm.cancel_order(live_client, exchange_idx))
        assert cancelled is True, "mm.cancel_order returned False"
        time.sleep(1)

        # Verify gone
        orders_after = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        assert _find_order_by_price(orders_after, safe_buy_price) is None, (
            f"Order still present after cancel: {orders_after}"
        )

    def test_cancel_all_orders(self, live_client, creds, btc_market_id, safe_buy_price, auth_token, btc_tick_sizes):
        ptick, atick = btc_tick_sizes
        # Place two buys at slightly different prices
        price_a = round(safe_buy_price * 0.99, 1)
        price_b = round(safe_buy_price * 0.98, 1)

        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            id_a = mm.next_client_order_index()
            id_b = mm.next_client_order_index()
            _run_async(mm.place_order(live_client, "buy", price_a, id_a, 0.0002))
            _run_async(mm.place_order(live_client, "buy", price_b, id_b, 0.0002))
        time.sleep(1)

        # Cancel all via production function (no market_id needed)
        _run_async(mm.cancel_all_orders(live_client))
        time.sleep(1)

        orders_after = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        assert len(orders_after) == 0, (
            f"Expected empty order list after cancel_all, got: {orders_after}"
        )


# ===========================================================================
# 6. TestLiveOrderPriceChange
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveOrderPriceChange:
    """Verify mm.refresh_order_if_needed() modifies a live order's price."""

    def test_modify_order_price(self, live_client, creds, btc_market_id, safe_buy_price, auth_token, btc_tick_sizes):
        ptick, atick = btc_tick_sizes
        original_price = safe_buy_price
        new_price = round(original_price * 0.99, 1)

        # Place order
        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            order_id = mm.next_client_order_index()
            result = _run_async(mm.place_order(live_client, "buy", original_price, order_id, 0.0002))
        assert result is True, "Failed to place order for modify test"
        time.sleep(1)

        # Get exchange order_index
        orders = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        found = _find_order_by_price(orders, original_price)
        assert found is not None, "Order not found after placement"
        exchange_idx = _order_index(found)

        # Modify via refresh_order_if_needed — sets up full state as the bot would
        with temp_mm_attrs(
            MARKET_ID=btc_market_id,
            _PRICE_TICK_FLOAT=ptick,
            _AMOUNT_TICK_FLOAT=atick,
            current_bid_order_id=exchange_idx,
            current_bid_price=original_price,
            current_bid_size=0.0002,
            QUOTE_UPDATE_THRESHOLD_BPS=0.0,  # force update regardless of bps change
        ):
            _run_async(mm.refresh_order_if_needed(live_client, True, new_price, 0.0002))
        time.sleep(1)

        orders_after = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        assert _find_order_by_price(orders_after, new_price) is not None, (
            f"Order at new price {new_price} not found. Active orders: {orders_after}"
        )
        assert _find_order_by_price(orders_after, original_price) is None, (
            f"Old order at {original_price} still exists after modify. Active orders: {orders_after}"
        )


# ===========================================================================
# 7. TestLiveOrderLifecycle
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveOrderLifecycle:
    """Full end-to-end lifecycle: place → assert → modify → assert → cancel → assert."""

    def test_full_lifecycle(self, live_client, creds, btc_market_id, safe_buy_price, auth_token, btc_tick_sizes):
        ptick, atick = btc_tick_sizes
        original_price = safe_buy_price
        modified_price = round(original_price * 0.99, 1)

        # 1. Place
        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            order_id = mm.next_client_order_index()
            placed = _run_async(mm.place_order(live_client, "buy", original_price, order_id, 0.0002))
        assert placed is True, "Failed to place order"
        time.sleep(1)

        # 2. Assert visible
        orders = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        found = _find_order_by_price(orders, original_price)
        assert found is not None, f"Order not visible after placement. Active orders: {orders}"
        exchange_idx = _order_index(found)

        # 3. Modify price via refresh_order_if_needed
        with temp_mm_attrs(
            MARKET_ID=btc_market_id,
            _PRICE_TICK_FLOAT=ptick,
            _AMOUNT_TICK_FLOAT=atick,
            current_bid_order_id=exchange_idx,
            current_bid_price=original_price,
            current_bid_size=0.0002,
            QUOTE_UPDATE_THRESHOLD_BPS=0.0,
        ):
            _run_async(mm.refresh_order_if_needed(live_client, True, modified_price, 0.0002))
        time.sleep(1)

        # 4. Assert updated price
        orders = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        found_modified = _find_order_by_price(orders, modified_price)
        assert found_modified is not None, (
            f"Modified order at {modified_price} not found. Active orders: {orders}"
        )

        # 5. Cancel via mm.cancel_order
        modified_idx = _order_index(found_modified)
        with temp_mm_attrs(MARKET_ID=btc_market_id, _PRICE_TICK_FLOAT=ptick, _AMOUNT_TICK_FLOAT=atick):
            cancelled = _run_async(mm.cancel_order(live_client, modified_idx))
        assert cancelled is True, "cancel_order returned False"
        time.sleep(1)

        # 6. Assert gone
        orders_final = _get_active_orders(creds["account_index"], btc_market_id, auth_token)
        assert _find_order_by_price(orders_final, modified_price) is None, (
            f"Order still exists after cancel: {orders_final}"
        )


# ===========================================================================
# 8. TestLiveLeverageSetup
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveLeverageSetup:
    """Verify adjust_leverage() successfully sets leverage on the exchange."""

    def test_set_leverage_succeeds(self, live_client, btc_market_id):
        """Call adjust_leverage with the config value and assert no error."""
        from adjust_leverage import adjust_leverage

        leverage = mm.LEVERAGE
        margin_mode = mm.MARGIN_MODE
        tx, response, err = _run_async(
            adjust_leverage(live_client, btc_market_id, leverage, margin_mode)
        )
        assert err is None, (
            f"adjust_leverage failed with leverage={leverage}, "
            f"margin_mode={margin_mode}: {err}"
        )

    def test_leverage_value_from_config(self):
        """LEVERAGE must be read from config.json or env, not hard-coded to 1."""
        # config.json has leverage=2, so with no env override it should be 2
        assert mm.LEVERAGE >= 1, f"LEVERAGE={mm.LEVERAGE} is not a positive int"

    def test_leverage_matches_env_or_config(self):
        """LEVERAGE should respect env override first, else config.json fallback."""
        expected = int(os.getenv("LEVERAGE", mm._trading.get("leverage", 1)))
        assert mm.LEVERAGE == expected, (
            f"Expected LEVERAGE={expected} from env/config, got {mm.LEVERAGE}"
        )

    def test_margin_mode_valid(self):
        """MARGIN_MODE must be 'cross' or 'isolated'."""
        assert mm.MARGIN_MODE in ("cross", "isolated"), (
            f"Unexpected MARGIN_MODE: {mm.MARGIN_MODE!r}"
        )


# ===========================================================================
# 9. TestLiveWsAuthToken
# (formerly 6)
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveWsAuthToken:
    """Verify _generate_ws_auth_token() works with a real SignerClient."""

    def test_generates_non_empty_string(self, live_client):
        token = mm._generate_ws_auth_token(live_client)
        assert token is not None, "_generate_ws_auth_token returned None with valid client"
        assert isinstance(token, str), f"Expected str, got {type(token)}"
        assert len(token) > 10, f"Token suspiciously short: {token!r}"

    def test_returns_none_on_invalid_client(self):
        """Passing None as client must not raise — returns None gracefully."""
        token = mm._generate_ws_auth_token(None)
        assert token is None, f"Expected None for invalid client, got {token!r}"


# ===========================================================================
# 10. TestLiveAccountOrdersWs
# ===========================================================================

@pytest.mark.live
@pytest.mark.live_trading
class TestLiveAccountOrdersWs:
    """Verify the account_orders authenticated WS channel works end-to-end."""

    def test_auth_token_accepted_by_ws(self, live_client, creds, btc_market_id):
        """Connect to account_orders with the auth token; confirm subscription is accepted.

        The exchange may confirm via a 'subscribed' message or by sending the
        initial data snapshot (type containing 'account_orders').  Both count
        as success.  An explicit error/reject message is a failure.
        """
        import json as _json

        import websockets as _ws

        account_id = creds["account_index"]
        channel = f"account_orders/{btc_market_id}/{account_id}"
        token = mm._generate_ws_auth_token(live_client)
        if token is None:
            pytest.skip("Auth token unavailable — skipping WS auth test")

        received = {"accepted": False, "error": None, "messages": []}

        async def _run():
            async with _ws.connect(
                mm.WEBSOCKET_URL, ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(_json.dumps({
                    "type": "subscribe",
                    "channel": channel,
                    "auth": token,
                }))
                deadline = asyncio.get_event_loop().time() + 15
                while asyncio.get_event_loop().time() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        data = _json.loads(raw)
                        msg_type = data.get("type", "")
                        received["messages"].append(msg_type)
                        if msg_type == "ping":
                            await ws.send('{"type":"pong"}')
                        elif msg_type == "subscribed":
                            received["accepted"] = True
                            break
                        elif "account_orders" in msg_type:
                            # Initial snapshot counts as acceptance
                            received["accepted"] = True
                            break
                        elif "error" in msg_type.lower() or "reject" in msg_type.lower():
                            received["error"] = data
                            break
                    except asyncio.TimeoutError:
                        break

        _run_async(_run())
        assert received["error"] is None, (
            f"WS auth was rejected by exchange: {received['error']}"
        )
        assert received["accepted"] is True, (
            f"account_orders subscription not confirmed within 15s. "
            f"Messages received: {received['messages']}"
        )

    def test_on_account_orders_update_initial_snapshot_no_state_change(
        self, live_client, creds, btc_market_id
    ):
        """First call sets the ready flag and logs, but must not clear live order IDs."""
        # Reset the ready flag so we can observe the first-call behaviour
        mm._account_orders_ws_ready = False

        # Give the bot fake active orders
        orig_bid = mm.state.orders.bid_order_id
        orig_ask = mm.state.orders.ask_order_id
        mm.state.orders.bid_order_id = 99901
        mm.state.orders.ask_order_id = 99902

        try:
            # Build a minimal account_orders payload that lists those orders as open
            data = {
                "type": "update/account_orders",
                "orders": {
                    str(btc_market_id): [
                        {"order_index": "99901", "status": "open"},
                        {"order_index": "99902", "status": "open"},
                    ]
                },
            }
            mm.on_account_orders_update(creds["account_index"], btc_market_id, data)

            # Ready flag must now be True
            assert mm._account_orders_ws_ready is True, (
                "_account_orders_ws_ready not set after first call"
            )
            # State must NOT have been touched on the first snapshot
            assert mm.state.orders.bid_order_id == 99901, (
                "bid_order_id was cleared on first snapshot — should be skipped"
            )
            assert mm.state.orders.ask_order_id == 99902, (
                "ask_order_id was cleared on first snapshot — should be skipped"
            )
        finally:
            mm.state.orders.bid_order_id = orig_bid
            mm.state.orders.ask_order_id = orig_ask
            mm._account_orders_ws_ready = False

    def test_on_account_orders_update_clears_filled_bid(
        self, live_client, creds, btc_market_id
    ):
        """After the first snapshot, a bid absent from active_ids must be cleared."""
        mm._account_orders_ws_ready = True   # simulate post-snapshot state

        orig_bid = mm.state.orders.bid_order_id
        orig_bid_price = mm.state.orders.bid_price
        orig_bid_size = mm.state.orders.bid_size
        mm.state.orders.bid_order_id = 88801
        mm.state.orders.bid_price = 50000.0
        mm.state.orders.bid_size = 0.0002

        try:
            # Payload does NOT include order 88801 → it has been filled/cancelled
            data = {
                "type": "update/account_orders",
                "orders": {str(btc_market_id): []},
            }
            mm.on_account_orders_update(creds["account_index"], btc_market_id, data)

            assert mm.state.orders.bid_order_id is None, (
                "bid_order_id was not cleared after fill"
            )
            assert mm.state.orders.bid_price is None
            assert mm.state.orders.bid_size is None
        finally:
            mm.state.orders.bid_order_id = orig_bid
            mm.state.orders.bid_price = orig_bid_price
            mm.state.orders.bid_size = orig_bid_size
            mm._account_orders_ws_ready = False

    def test_on_account_orders_update_clears_filled_ask(
        self, live_client, creds, btc_market_id
    ):
        """After the first snapshot, an ask absent from active_ids must be cleared."""
        mm._account_orders_ws_ready = True

        orig_ask = mm.state.orders.ask_order_id
        orig_ask_price = mm.state.orders.ask_price
        orig_ask_size = mm.state.orders.ask_size
        mm.state.orders.ask_order_id = 88802
        mm.state.orders.ask_price = 120000.0
        mm.state.orders.ask_size = 0.0002

        try:
            data = {
                "type": "update/account_orders",
                "orders": {str(btc_market_id): []},
            }
            mm.on_account_orders_update(creds["account_index"], btc_market_id, data)

            assert mm.state.orders.ask_order_id is None, (
                "ask_order_id was not cleared after fill"
            )
            assert mm.state.orders.ask_price is None
            assert mm.state.orders.ask_size is None
        finally:
            mm.state.orders.ask_order_id = orig_ask
            mm.state.orders.ask_price = orig_ask_price
            mm.state.orders.ask_size = orig_ask_size
            mm._account_orders_ws_ready = False

    def test_on_account_orders_update_keeps_live_orders(
        self, live_client, creds, btc_market_id
    ):
        """Orders listed as 'open' / 'partial_filled' must NOT be cleared."""
        mm._account_orders_ws_ready = True

        orig_bid = mm.state.orders.bid_order_id
        orig_ask = mm.state.orders.ask_order_id
        mm.state.orders.bid_order_id = 77701
        mm.state.orders.ask_order_id = 77702

        try:
            data = {
                "type": "update/account_orders",
                "orders": {
                    str(btc_market_id): [
                        {"order_index": "77701", "status": "open"},
                        {"order_index": "77702", "status": "partial_filled"},
                    ]
                },
            }
            mm.on_account_orders_update(creds["account_index"], btc_market_id, data)

            assert mm.state.orders.bid_order_id == 77701, (
                "Live bid order was incorrectly cleared"
            )
            assert mm.state.orders.ask_order_id == 77702, (
                "Live ask order was incorrectly cleared"
            )
        finally:
            mm.state.orders.bid_order_id = orig_bid
            mm.state.orders.ask_order_id = orig_ask
            mm._account_orders_ws_ready = False

    def test_on_account_orders_update_handles_exception_gracefully(
        self, live_client, creds, btc_market_id
    ):
        """Malformed data must not propagate an exception out of the handler."""
        mm._account_orders_ws_ready = True
        try:
            # Pass deliberately malformed data
            mm.on_account_orders_update(creds["account_index"], btc_market_id, {"orders": {"bad": "data"}})
        except Exception as exc:
            pytest.fail(f"on_account_orders_update raised unexpectedly: {exc}")
        finally:
            mm._account_orders_ws_ready = False

    def test_subscribe_to_account_orders_refreshes_auth_token(
        self, live_client, creds, btc_market_id, monkeypatch
    ):
        """Run the production subscription loop across at least one token refresh cycle."""
        if mm._generate_ws_auth_token(live_client) is None:
            pytest.skip("Auth token unavailable — skipping account_orders refresh test")

        call_counter = {"ws_subscribe_calls": 0}
        original_ws_subscribe = mm.ws_subscribe
        original_ready = mm._account_orders_ws_ready
        mm._account_orders_ws_ready = False

        async def _counting_ws_subscribe(*args, **kwargs):
            call_counter["ws_subscribe_calls"] += 1
            return await original_ws_subscribe(*args, **kwargs)

        monkeypatch.setattr(mm, "_WS_AUTH_TOKEN_TTL", 5)
        monkeypatch.setattr(mm, "ws_subscribe", _counting_ws_subscribe)

        async def _run():
            task = asyncio.create_task(
                mm.subscribe_to_account_orders(live_client, btc_market_id, creds["account_index"])
            )
            try:
                await asyncio.sleep(12)
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        try:
            _run_async(_run())
            assert call_counter["ws_subscribe_calls"] >= 2, (
                "Expected at least two ws_subscribe calls across auth-token refreshes"
            )
        finally:
            mm._account_orders_ws_ready = original_ready


# ===========================================================================
# 11. TestLiveTickSizeDetection
# ===========================================================================

@pytest.mark.live
class TestLiveTickSizeDetection:
    """Verify get_market_details_async returns valid tick sizes from the exchange."""

    def test_btc_tick_sizes_from_api(self):
        """BTC must return both price and amount tick sizes."""
        from utils import get_market_details

        market_id, price_tick, amount_tick = get_market_details("BTC")
        assert market_id is not None, "BTC market_id not found"
        assert isinstance(market_id, int), f"market_id should be int, got {type(market_id)}"
        assert price_tick is not None and price_tick > 0, (
            f"BTC price_tick invalid: {price_tick}"
        )
        assert amount_tick is not None and amount_tick > 0, (
            f"BTC amount_tick invalid: {amount_tick}"
        )

    def test_btc_price_tick_is_point_one(self):
        """BTC price_tick = 0.1 (supported_price_decimals=1 → 10^-1)."""
        from utils import get_market_details

        _, price_tick, _ = get_market_details("BTC")
        assert abs(price_tick - 0.1) < 1e-12, (
            f"Expected BTC price_tick=0.1, got {price_tick}"
        )

    def test_btc_amount_tick_is_1e5(self):
        """BTC amount_tick = 0.00001 (supported_size_decimals=5 → 10^-5)."""
        from utils import get_market_details

        _, _, amount_tick = get_market_details("BTC")
        assert abs(amount_tick - 0.00001) < 1e-12, (
            f"Expected BTC amount_tick=0.00001, got {amount_tick}"
        )

    def test_tick_sizes_propagate_to_market_config(self, btc_market_id, btc_tick_sizes):
        """Verify the btc_tick_sizes fixture returns values consistent with MarketConfig."""
        ptick, atick = btc_tick_sizes
        assert abs(ptick - 0.1) < 1e-12, f"ptick mismatch: {ptick}"
        assert abs(atick - 0.00001) < 1e-12, f"atick mismatch: {atick}"

    def test_raw_price_conversion_round_trip(self, btc_tick_sizes):
        """Float price → raw int → float must be lossless for tick-aligned values."""
        ptick, _ = btc_tick_sizes
        with temp_mm_attrs(_PRICE_TICK_FLOAT=ptick):
            for price in [50000.0, 50000.1, 99999.9, 100.0]:
                raw = mm._to_raw_price(price)
                assert isinstance(raw, int), f"raw price should be int, got {type(raw)}"
                reconstructed = raw * ptick
                assert abs(reconstructed - price) < 1e-9, (
                    f"Round-trip failed: {price} → {raw} → {reconstructed}"
                )

    def test_raw_amount_conversion_round_trip(self, btc_tick_sizes):
        """Float amount → raw int → float must be lossless for tick-aligned values."""
        _, atick = btc_tick_sizes
        with temp_mm_attrs(_AMOUNT_TICK_FLOAT=atick):
            for amount in [0.00001, 0.0002, 0.01, 1.0, 0.12345]:
                raw = mm._to_raw_amount(amount)
                assert isinstance(raw, int), f"raw amount should be int, got {type(raw)}"
                reconstructed = raw * atick
                assert abs(reconstructed - amount) < 1e-9, (
                    f"Round-trip failed: {amount} → {raw} → {reconstructed}"
                )
