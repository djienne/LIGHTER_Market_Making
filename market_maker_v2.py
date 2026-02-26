# pragma pylint: disable=missing-docstring, invalid-name, pointless-string-statement
# flake8: noqa: F401
# isort: skip_file

import asyncio
import logging
import lighter
import os
import time
import orjson as json
import math
from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime
from lighter.exceptions import ApiException
from decimal import Decimal
import signal
from collections import deque
import argparse
from sortedcontainers import SortedDict
from utils import avellaneda_quotes, EPSILON, get_market_details_async
from adjust_leverage import adjust_leverage
from orderbook import apply_orderbook_update
from ws_manager import ws_subscribe

# =========================
# Env & constants
# =========================
BASE_URL = "https://mainnet.zklighter.elliot.ai"
WEBSOCKET_URL = "wss://mainnet.zklighter.elliot.ai/stream"
API_KEY_PRIVATE_KEY = os.getenv("API_KEY_PRIVATE_KEY")
ACCOUNT_INDEX = int(os.getenv("ACCOUNT_INDEX", "0"))
API_KEY_INDEX = int(os.getenv("API_KEY_INDEX", "0"))

MARKET_SYMBOL = os.getenv("MARKET_SYMBOL", "PAXG")
MARKET_ID = None
PRICE_TICK_SIZE = None
AMOUNT_TICK_SIZE = None

LEVERAGE = int(os.getenv("LEVERAGE", "1"))
MARGIN_MODE = os.getenv("MARGIN_MODE", "cross")
FLIP_DEFAULT = os.getenv("FLIP", "false").lower() == "true"
flip_state = FLIP_DEFAULT
flip_target_state = flip_state
POSITION_VALUE_THRESHOLD_USD = 15.0

# Directories (mounted by docker-compose)
PARAMS_DIR = os.getenv("PARAMS_DIR", "params")
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Trading config
SPREAD = 0.035 / 100.0       # static fallback spread (if allowed)
FALLBACK_SPREAD_FRACTION = 0.001  # fallback spread as fraction of mid price
BASE_AMOUNT = 0.047          # static fallback amount
USE_DYNAMIC_SIZING = True
CAPITAL_USAGE_PERCENT = 0.12
SAFETY_MARGIN_PERCENT = 0.01
ORDER_TIMEOUT = 0.5         # seconds — event-driven wake; low timeout for fast requoting
MINIMUM_SPREAD_PERCENT = 0.005 # Safety net for calculated spreads
QUOTE_UPDATE_THRESHOLD_BPS = float(os.getenv("QUOTE_UPDATE_THRESHOLD_BPS", "1.0"))  # 1 bps
MIN_LOOP_INTERVAL = 0.1     # Avoid spinning 100% CPU on fast updates

# WebSocket tuning
WS_PING_INTERVAL = 20          # seconds between pings
WS_RECV_TIMEOUT = 30.0         # watchdog: no data triggers reconnect
WS_RECONNECT_BASE_DELAY = 5    # initial reconnect backoff (seconds)
WS_RECONNECT_MAX_DELAY = 60    # maximum reconnect backoff (seconds)

# Pre-computed tick sizes as floats (set once in main())
_PRICE_TICK_FLOAT = 0.0
_AMOUNT_TICK_FLOAT = 0.0

# Avellaneda
AVELLANEDA_REFRESH_INTERVAL = 900  # seconds
REQUIRE_PARAMS = os.getenv("REQUIRE_PARAMS", "false").lower() == "true"
_avellaneda_candidates = []  # populated once in main() after MARKET_SYMBOL is known

# Global WS / state
order_book_received = asyncio.Event()
account_state_received = asyncio.Event()
account_all_received = asyncio.Event()
ws_connection_healthy = False
last_order_book_update = 0
current_mid_price_cached = None
ws_client = None
ws_task = None

current_bid_order_id = None
current_ask_order_id = None
current_bid_price = None
current_ask_price = None
current_bid_size = None
current_ask_size = None
last_mid_price = None
available_capital = None
portfolio_value = None
current_position_size = 0
last_client_order_index = 0

avellaneda_params = None
avellaneda_cache = None  # AvellanedaCache instance, populated by poll_avellaneda_parameters
last_avellaneda_update = 0

account_positions = {}
recent_trades = deque(maxlen=20)

# Local Order Book State
local_order_book = {'bids': SortedDict(), 'asks': SortedDict(), 'initialized': False}

# =========================
# Logging setup
# =========================
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

log_file = os.path.join(LOG_DIR, "market_maker_debug.txt")
try:
    if os.path.exists(log_file):
        os.remove(log_file)
except Exception:
    pass

file_handler = logging.FileHandler(log_file, mode='w')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.propagate = False

logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.root.setLevel(logging.WARNING)

# =========================
# Data structures
# =========================
@dataclass(frozen=True, slots=True)
class AvellanedaCache:
    """Pre-extracted Avellaneda parameters for hot path access."""
    gamma: float
    sigma: float
    k_bid: float
    k_ask: float
    time_remaining: float

# =========================
# Helpers
# =========================
def trim_exception(e: Exception) -> str:
    return str(e).strip().split("\n")[-1]


def next_client_order_index() -> int:
    global last_client_order_index
    new_id = time.time_ns()
    if new_id <= last_client_order_index:
        new_id = last_client_order_index + 1
    last_client_order_index = new_id
    return new_id


def price_change_bps(old_price: Optional[float], new_price: Optional[float]) -> float:
    if old_price is None or new_price is None or old_price <= 0:
        return float("inf")
    return abs(new_price - old_price) / old_price * 10000.0


def get_position_value_usd(position_size: float, mid_price: Optional[float]) -> float:
    if not mid_price:
        return 0.0
    return abs(position_size) * mid_price


def position_label(position_size: float) -> str:
    if position_size > 0:
        return "long"
    if position_size < 0:
        return "short"
    return "flat"


def get_best_prices() -> Tuple[Optional[float], Optional[float]]:
    if local_order_book['bids'] and local_order_book['asks']:
        try:
            best_bid = local_order_book['bids'].peekitem(-1)[0]
            best_ask = local_order_book['asks'].peekitem(0)[0]
            return best_bid, best_ask
        except (ValueError, IndexError):
            pass
    return None, None


def is_position_significant(position_size: float, mid_price: Optional[float]) -> bool:
    if abs(position_size) < EPSILON:
        return False
    if not mid_price or mid_price <= 0:
        return True
    return get_position_value_usd(position_size, mid_price) >= POSITION_VALUE_THRESHOLD_USD


def on_order_book_update(market_id, payload):
    global ws_connection_healthy, last_order_book_update, current_mid_price_cached, local_order_book
    try:
        if market_id == MARKET_ID:
            bids_in = payload.get('bids', [])
            asks_in = payload.get('asks', [])

            is_snapshot = apply_orderbook_update(
                local_order_book['bids'],
                local_order_book['asks'],
                local_order_book['initialized'],
                bids_in,
                asks_in,
            )
            if is_snapshot:
                local_order_book['initialized'] = True
                logger.info(f"Initializing/snapshot local orderbook for market {market_id}")

            # Calculate mid price using SortedDict O(1) peek
            if local_order_book['bids'] and local_order_book['asks']:
                best_bid = local_order_book['bids'].peekitem(-1)[0]
                best_ask = local_order_book['asks'].peekitem(0)[0]
                current_mid_price_cached = (best_bid + best_ask) / 2

            ws_connection_healthy = True
            last_order_book_update = time.monotonic()
            order_book_received.set()

    except Exception as e:
        logger.error(f"Error in order book callback: {e}", exc_info=True)
        ws_connection_healthy = False

def on_trade_update(market_id, trades):
    try:
        if market_id == MARKET_ID:
            for trade in trades:
                price = trade.get('price')
                size = trade.get('size')
                side = "SELL" if trade.get('is_maker_ask') else "BUY"
                logger.debug(f"📈 Market Trade: {side} {size} @ {price}")
    except Exception as e:
        logger.error(f"❌ Error in trade callback: {e}", exc_info=True)


async def subscribe_to_market_data(market_id):
    """Connects to the websocket, subscribes to orderbook AND trades."""
    global ws_connection_healthy

    def _on_disconnect():
        global ws_connection_healthy
        ws_connection_healthy = False

    async def _on_connect():
        global ws_connection_healthy
        ws_connection_healthy = True

    def _on_message(data):
        msg_type = data.get("type")
        if msg_type == "update/order_book":
            if 'order_book' in data:
                on_order_book_update(market_id, data['order_book'])
        elif msg_type == "update/trade":
            if 'trades' in data:
                on_trade_update(market_id, data['trades'])

    await ws_subscribe(
        channels=[f"order_book/{market_id}", f"trade/{market_id}"],
        label="market data",
        on_message=_on_message,
        url=WEBSOCKET_URL,
        ping_interval=WS_PING_INTERVAL,
        recv_timeout=WS_RECV_TIMEOUT,
        reconnect_base=WS_RECONNECT_BASE_DELAY,
        reconnect_max=WS_RECONNECT_MAX_DELAY,
        on_connect=_on_connect,
        on_disconnect=_on_disconnect,
        logger=logger,
    )

def on_user_stats_update(account_id, stats):
    global available_capital, portfolio_value
    try:
        if account_id == ACCOUNT_INDEX:
            if not isinstance(stats, dict):
                logger.warning(f"Received user stats with unexpected payload: {stats}")
                return
            if "available_balance" not in stats or "portfolio_value" not in stats:
                logger.warning(f"Received user stats missing fields: {stats}")
                return

            new_available_capital = float(stats.get("available_balance"))
            new_portfolio_value = float(stats.get("portfolio_value"))

            if new_available_capital >= 0 and new_portfolio_value >= 0:
                available_capital = new_available_capital
                portfolio_value = new_portfolio_value
                logger.info(
                    f"Received user stats for account {account_id}: "
                    f"Available Capital=${available_capital}, Portfolio Value=${portfolio_value}"
                )
                account_state_received.set()
            else:
                logger.warning(
                    f"Received user stats with negative values: "
                    f"available_balance={stats.get('available_balance')}, "
                    f"portfolio_value={stats.get('portfolio_value')}"
                )
    except (ValueError, TypeError) as e:
        logger.error(f"Error processing user stats update: {e}", exc_info=True)

async def subscribe_to_user_stats(account_id):
    """Connects to the websocket, subscribes to user_stats, and updates global state."""

    def _on_message(data):
        msg_type = data.get("type")
        if msg_type in ("update/user_stats", "subscribed/user_stats"):
            on_user_stats_update(account_id, data.get("stats", {}))

    await ws_subscribe(
        channels=[f"user_stats/{account_id}"],
        label="user stats",
        on_message=_on_message,
        url=WEBSOCKET_URL,
        ping_interval=WS_PING_INTERVAL,
        recv_timeout=WS_RECV_TIMEOUT,
        reconnect_base=WS_RECONNECT_BASE_DELAY,
        reconnect_max=WS_RECONNECT_MAX_DELAY,
        logger=logger,
    )

def on_account_all_update(account_id, data):
    global account_positions, recent_trades, current_position_size
    try:
        if account_id == ACCOUNT_INDEX:
            positions_updated = False
            if isinstance(data, dict) and "positions" in data:
                new_positions = data.get("positions") or {}
                account_positions = new_positions
                positions_updated = True

                market_position = new_positions.get(str(MARKET_ID))
                new_size = 0.0
                if market_position:
                    size = float(market_position.get("position", 0))
                    sign = int(market_position.get("sign", 1))
                    new_size = -size if sign == -1 else size
                else:
                    new_size = 0.0

                if new_size != current_position_size:
                    logger.info(
                        f"WebSocket position update for market {MARKET_ID}: "
                        f"{current_position_size} -> {new_size}"
                    )
                    current_position_size = new_size

            new_trades_by_market = data.get("trades", {}) if isinstance(data, dict) else {}
            if new_trades_by_market:
                all_new_trades = [trade for trades in new_trades_by_market.values() for trade in trades]
                all_new_trades.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
                for trade in reversed(all_new_trades):
                    if trade not in recent_trades:
                        recent_trades.append(trade)
                        logger.info(
                            f"WebSocket trade update: Market {trade.get('market_id')}, "
                            f"Type {trade.get('type')}, Size {trade.get('size')}, "
                            f"Price {trade.get('price')}"
                        )

            if positions_updated and not account_all_received.is_set():
                account_all_received.set()

    except (ValueError, TypeError) as e:
        logger.error(f"Error processing account_all update: {e}", exc_info=True)

async def subscribe_to_account_all(account_id):
    """Connects to the websocket, subscribes to account_all, and updates global state."""

    def _on_message(data):
        msg_type = data.get("type")
        if msg_type in ("update/account_all", "update/account", "subscribed/account_all"):
            on_account_all_update(account_id, data)

    await ws_subscribe(
        channels=[f"account_all/{account_id}"],
        label="account data",
        on_message=_on_message,
        url=WEBSOCKET_URL,
        ping_interval=WS_PING_INTERVAL,
        recv_timeout=WS_RECV_TIMEOUT,
        reconnect_base=WS_RECONNECT_BASE_DELAY,
        reconnect_max=WS_RECONNECT_MAX_DELAY,
        logger=logger,
    )

async def restart_websocket():
    global ws_task, local_order_book, order_book_received
    logger.info("🔄 Restarting websocket connection...")
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
    
    # Reset state
    order_book_received.clear()
    local_order_book['initialized'] = False
    local_order_book['bids'].clear()
    local_order_book['asks'].clear()
    
    # Start new task
    ws_task = asyncio.create_task(subscribe_to_market_data(MARKET_ID))
    
    try:
        logger.info("⏳ Waiting for websocket reconnection...")
        await asyncio.wait_for(order_book_received.wait(), timeout=15.0)
        logger.info("✅ Websocket reconnected successfully")
        return True
    except asyncio.TimeoutError:
        logger.error("❌ Websocket reconnection failed - timeout.")
        return False

def check_websocket_health():
    if not ws_connection_healthy:
        return False
    if time.monotonic() - last_order_book_update > WS_RECV_TIMEOUT:
        return False
    return True

_cached_base_amount = None
_cached_base_amount_inputs = (None, None)  # (rounded_capital, rounded_mid)


def calculate_dynamic_base_amount(mid_price, capital=None):
    global _cached_base_amount, _cached_base_amount_inputs
    if not mid_price or mid_price <= 0:
        return None

    effective_capital = capital if capital is not None else available_capital
    if not effective_capital:
        return BASE_AMOUNT

    try:
        # Cache key: capital rounded to nearest dollar, mid to 4 decimals
        rounded_capital = round(effective_capital)
        rounded_mid = round(mid_price, 4)
        cache_key = (rounded_capital, rounded_mid)

        if cache_key == _cached_base_amount_inputs and _cached_base_amount is not None:
            return _cached_base_amount

        usd_amount = effective_capital * CAPITAL_USAGE_PERCENT
        size = usd_amount / mid_price

        if _AMOUNT_TICK_FLOAT > 0:
             size = round(size / _AMOUNT_TICK_FLOAT) * _AMOUNT_TICK_FLOAT

        _cached_base_amount = size
        _cached_base_amount_inputs = cache_key
        return size
    except Exception:
        return BASE_AMOUNT

async def place_order(client, side, price, order_id, size):
    try:
        is_ask = (side == 'sell')

        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_ID,
            client_order_index=order_id,
            base_amount=size,
            price=price,
            is_ask=is_ask,
            order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
            time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME
        )

        if err:
            logger.error(f"Failed to place {side} order: {err}")
            return False
        logger.debug(f"Placed {side} order: {size} @ {price}")
        return True
    except Exception as e:
        logger.error(f"Failed to place {side} order: {e}", exc_info=True)
        return False


async def cancel_order(client, order_id):
    if order_id is None:
        return True
    try:
        tx, tx_hash, err = await client.cancel_order(
            market_index=MARKET_ID,
            order_index=order_id,
        )
        if err:
            logger.error(f"Failed to cancel order {order_id}: {err}")
            return False
        logger.debug(f"Cancelled order {order_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to cancel order {order_id}: {e}", exc_info=True)
        return False


async def cancel_all_orders(client):
    try:
        await client.cancel_all_orders(
            time_in_force=lighter.SignerClient.CANCEL_ALL_TIF_IMMEDIATE, 
            time=0
        )
        logger.info("🗑️ Cancelled all orders")
    except Exception as e:
        logger.error(f"❌ Failed to cancel orders: {e}", exc_info=True)

# === HOT PATH ===

async def refresh_order_if_needed(client, is_buy, new_price, new_size, _log_debug=False):
    """Update a quote, preferring atomic modify_order over cancel+place."""
    global current_bid_order_id, current_ask_order_id
    global current_bid_price, current_ask_price, current_bid_size, current_ask_size

    side = "buy" if is_buy else "sell"

    if new_price is None or new_size is None:
        if _log_debug:
            logger.debug(f"Skipping {side} update: missing price or size.")
        return

    if new_price <= 0 or new_size <= 0:
        if _log_debug:
            logger.debug(f"Skipping {side} update: invalid price or size.")
        return

    if is_buy:
        existing_id = current_bid_order_id
        existing_price = current_bid_price
    else:
        existing_id = current_ask_order_id
        existing_price = current_ask_price

    if existing_id is not None and existing_price is not None:
        change_bps = price_change_bps(existing_price, new_price)
        if change_bps <= QUOTE_UPDATE_THRESHOLD_BPS:
            if _log_debug:
                logger.debug(
                    f"Keeping {side} order: price change {change_bps:.2f} bps <= {QUOTE_UPDATE_THRESHOLD_BPS:.2f}"
                )
            return

    # Try atomic modify if we have an existing order (single round-trip, no unquoted window)
    if existing_id is not None:
        try:
            tx, tx_hash, err = await client.modify_order(
                market_index=MARKET_ID,
                order_index=existing_id,
                base_amount=new_size,
                price=new_price,
            )
            if not err:
                if _log_debug:
                    logger.debug(f"Modified {side} order {existing_id}: {new_size} @ {new_price}")
                if is_buy:
                    current_bid_price = new_price
                    current_bid_size = new_size
                else:
                    current_ask_price = new_price
                    current_ask_size = new_size
                return
            else:
                if _log_debug:
                    logger.debug(f"Modify failed for {side} order {existing_id}: {err}; falling back to cancel+place")
        except Exception as e:
            if _log_debug:
                logger.debug(f"Modify exception for {side} order {existing_id}: {e}; falling back to cancel+place")

        # Fallback: cancel then place
        cancelled = await cancel_order(client, existing_id)
        if not cancelled:
            logger.warning(f"Cancel failed for {side} order {existing_id}; skipping replace.")
            return
        if is_buy:
            current_bid_order_id = None
            current_bid_price = None
            current_bid_size = None
        else:
            current_ask_order_id = None
            current_ask_price = None
            current_ask_size = None

    new_order_id = next_client_order_index()
    placed = await place_order(client, side, new_price, new_order_id, new_size)
    if not placed:
        return

    if is_buy:
        current_bid_order_id = new_order_id
        current_bid_price = new_price
        current_bid_size = new_size
    else:
        current_ask_order_id = new_order_id
        current_ask_price = new_price
        current_ask_size = new_size

def _read_avellaneda_file_sync(candidates):
    for p in candidates:
        try:
            with open(p, "r") as f:
                return json.loads(f.read())
        except FileNotFoundError:
            continue
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in {p}: {e}.")
            return None
    return None

async def poll_avellaneda_parameters():
    """Continuously poll and load Avellaneda parameters from PARAMS_DIR asynchronously."""
    global avellaneda_params, avellaneda_cache, last_avellaneda_update
    while True:
        try:
            now = time.monotonic()

            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, _read_avellaneda_file_sync, _avellaneda_candidates)

            if not data:
                logger.warning(f"Params file not found for {MARKET_SYMBOL}.")
            else:
                try:
                    gamma = float(data["optimal_parameters"]["gamma"])
                    market_data = data["market_data"]
                    sigma = float(market_data["sigma"])
                    k_bid = float(market_data.get("k_bid", market_data.get("k", 0)))
                    k_ask = float(market_data.get("k_ask", market_data.get("k", 0)))
                    time_remaining = float(data["current_state"]["time_remaining"])

                    if not (math.isfinite(gamma) and gamma > 0):
                        logger.warning("Avellaneda gamma must be finite and > 0.")
                    elif not (math.isfinite(sigma) and sigma >= 0):
                        logger.warning("Avellaneda sigma must be finite and >= 0.")
                    elif not (math.isfinite(k_bid) and k_bid > 0 and math.isfinite(k_ask) and k_ask > 0):
                        logger.warning("Avellaneda k_bid/k_ask must be finite and > 0.")
                    elif not (math.isfinite(time_remaining) and time_remaining > 0):
                        logger.warning("Avellaneda time_remaining must be finite and > 0.")
                    else:
                        avellaneda_params = data
                        avellaneda_cache = AvellanedaCache(
                            gamma=gamma,
                            sigma=sigma,
                            k_bid=k_bid,
                            k_ask=k_ask,
                            time_remaining=time_remaining,
                        )
                        last_avellaneda_update = now
                        logger.debug(
                            f"Avellaneda params loaded: gamma={gamma}, sigma={sigma}, "
                            f"k_bid={k_bid}, k_ask={k_ask}, T={time_remaining}"
                        )
                except (KeyError, TypeError, ValueError) as e:
                    logger.warning(f"Invalid Avellaneda params structure: {e}")

        except Exception as e:
            logger.error(f"Unexpected error loading params: {e}", exc_info=True)

        await asyncio.sleep(AVELLANEDA_REFRESH_INTERVAL)


def calculate_order_prices(mid_price, inventory_factor, cache=None):
    """
    Calculates both bid and ask prices using Avellaneda-Stoikov formula.
    Returns (buy_price, sell_price) — either may be None on error.
    """
    if cache is None:
        cache = avellaneda_cache

    if cache is not None:
        try:
            _, buy_price, sell_price = avellaneda_quotes(
                mid_price, cache.gamma, cache.sigma,
                cache.k_bid, cache.k_ask, inventory_factor,
                cache.time_remaining,
            )

            if _PRICE_TICK_FLOAT > 0:
                buy_price = round(buy_price / _PRICE_TICK_FLOAT) * _PRICE_TICK_FLOAT
                sell_price = round(sell_price / _PRICE_TICK_FLOAT) * _PRICE_TICK_FLOAT

            return buy_price, sell_price
        except Exception as e:
            logger.error(f"Error calculating order prices: {e}", exc_info=True)
            return None, None

    if REQUIRE_PARAMS:
        logger.warning("REQUIRE_PARAMS enabled and no valid Avellaneda params; skipping quoting.")
        return None, None

    spread = mid_price * FALLBACK_SPREAD_FRACTION
    return mid_price - (spread / 2), mid_price + (spread / 2)

async def market_making_loop(client, account_api, order_api):
    logger.info("Starting 2-sided market making loop...")
    _log_info = logger.isEnabledFor(logging.INFO)
    _log_debug = logger.isEnabledFor(logging.DEBUG)

    while True:
        try:
            if not check_websocket_health():
                logger.warning("Websocket connection unhealthy, attempting restart...")
                if not await restart_websocket():
                    await asyncio.sleep(10)
                    continue

            # Wait for fresh order book data but wake up to check state
            # Clear before wait to avoid losing updates between wait() returning and clear()
            order_book_received.clear()
            try:
                await asyncio.wait_for(order_book_received.wait(), timeout=ORDER_TIMEOUT)
            except asyncio.TimeoutError:
                pass

            # Snapshot volatile globals into locals for speed and consistency
            snap_mid = current_mid_price_cached
            snap_cache = avellaneda_cache
            snap_position = current_position_size
            snap_capital = available_capital

            if snap_mid is None:
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            base_amount = calculate_dynamic_base_amount(snap_mid, capital=snap_capital)
            if base_amount is None or base_amount <= 0:
                if _log_info:
                    logger.warning("Base amount is zero or invalid; skipping order refresh.")
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            inventory_param = snap_position / base_amount

            buy_price, sell_price = calculate_order_prices(snap_mid, inventory_param, cache=snap_cache)

            if buy_price and sell_price:
                if _log_info:
                    bid_spread_pct = (snap_mid - buy_price) / snap_mid * 100 if snap_mid > 0 else 0
                    ask_spread_pct = (sell_price - snap_mid) / snap_mid * 100 if snap_mid > 0 else 0
                    logger.info(
                        "QUOTING | Inventory: %+.2f | Mid: $%.4f | Bid: $%.4f (-%.4f%%) | Ask: $%.4f (+%.4f%%)",
                        inventory_param, snap_mid, buy_price, bid_spread_pct, sell_price, ask_spread_pct,
                    )
            else:
                if _log_debug:
                    logger.debug("Could not calculate one or both quotes; skipping refresh for missing side(s).")

            # Parallelize bid/ask refresh — safe because they touch disjoint globals
            coros = []
            if buy_price:
                coros.append(refresh_order_if_needed(client, True, buy_price, base_amount, _log_debug))
            if sell_price:
                coros.append(refresh_order_if_needed(client, False, sell_price, base_amount, _log_debug))
            if coros:
                await asyncio.gather(*coros)

        except Exception as e:
            logger.error(f"Unhandled error in market_making_loop: {e}", exc_info=True)
            await asyncio.sleep(5)


async def track_balance():
    log_path = os.path.join(LOG_DIR, "balance_log.txt")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    loop = asyncio.get_running_loop()
    while True:
        try:
            if current_position_size == 0 and portfolio_value is not None:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                line = f"[{timestamp}] Portfolio Value: ${portfolio_value:,.2f}\n"
                await loop.run_in_executor(None, _append_line, log_path, line)
                logger.info(f"💰 Portfolio value of ${portfolio_value:,.2f} logged to {log_path}")
            elif current_position_size != 0:
                logger.info(f"⏸️ Skipping balance logging (open position: {current_position_size})")
            else:
                logger.info("⏸️ Skipping balance logging (portfolio value not yet received)")
        except Exception as e:
            logger.error(f"❌ Error in track_balance: {e}", exc_info=True)
        await asyncio.sleep(300)


def _append_line(path, line):
    with open(path, "a") as f:
        f.write(line)


# === COLD PATH ===

async def main():
    global MARKET_ID, PRICE_TICK_SIZE, AMOUNT_TICK_SIZE
    global _PRICE_TICK_FLOAT, _AMOUNT_TICK_FLOAT
    global ws_task, last_order_book_update
    global last_mid_price, current_position_size

    logger.info("🚀 === Market Maker v2 Starting (2-Sided Quoting) ===")

    # Pre-compute Avellaneda parameter file candidates once
    _avellaneda_candidates.clear()
    _avellaneda_candidates.extend([
        os.path.join(PARAMS_DIR, f"avellaneda_parameters_{MARKET_SYMBOL}.json"),
        f"params/avellaneda_parameters_{MARKET_SYMBOL}.json",
        f"avellaneda_parameters_{MARKET_SYMBOL}.json",
        f"TRADER/avellaneda_parameters_{MARKET_SYMBOL}.json",
    ])

    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
    account_api = lighter.AccountApi(api_client)
    order_api = lighter.OrderApi(api_client)

    market_id, price_tick, amount_tick = await get_market_details_async(MARKET_SYMBOL)
    if market_id is None:
        logger.error(f"❌ Could not retrieve market details for {MARKET_SYMBOL}. Exiting.")
        return
    MARKET_ID = market_id
    PRICE_TICK_SIZE = Decimal(str(price_tick))
    AMOUNT_TICK_SIZE = Decimal(str(amount_tick)) if amount_tick else Decimal(0)
    _PRICE_TICK_FLOAT = float(PRICE_TICK_SIZE)
    _AMOUNT_TICK_FLOAT = float(AMOUNT_TICK_SIZE)
    logger.info(f"📊 Market {MARKET_SYMBOL}: id={MARKET_ID}, tick(price)={PRICE_TICK_SIZE}, tick(amount)={AMOUNT_TICK_SIZE}")

    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=API_KEY_PRIVATE_KEY,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )
    err = client.check_client()
    if err is not None:
        logger.error(f"❌ CheckClient error: {trim_exception(err)}")
        await api_client.close()
        await client.close()
        return
    logger.info("✅ Client connected successfully")

    # Clean slate: cancel all at startup
    await cancel_all_orders(client)
    await asyncio.sleep(3)


    last_order_book_update = time.monotonic()
    # Start WebSocket Tasks
    ws_task = asyncio.create_task(subscribe_to_market_data(MARKET_ID))

    user_stats_task = asyncio.create_task(subscribe_to_user_stats(ACCOUNT_INDEX))
    account_all_task = asyncio.create_task(subscribe_to_account_all(ACCOUNT_INDEX))
    poll_avellaneda_task = asyncio.create_task(poll_avellaneda_parameters())

    try:
        logger.info("⏳ Waiting for initial order book, account data, and position data...")
        await asyncio.wait_for(order_book_received.wait(), timeout=30.0)
        logger.info(f"✅ Websocket connected for market {MARKET_ID}")
        
        logger.info("⏳ Waiting for valid account capital...")
        await asyncio.wait_for(account_state_received.wait(), timeout=30.0)
        logger.info(f"✅ Received valid account capital: ${available_capital}; and portfolio value: ${portfolio_value}.")

        logger.info("⏳ Waiting for initial position data...")
        await asyncio.wait_for(account_all_received.wait(), timeout=30.0)
        logger.info(f"✅ Received initial position data. Current size: {current_position_size}")

        logger.info(f"⚙️ Attempting to set leverage to {LEVERAGE}x with {MARGIN_MODE} margin...")
        _, _, err = await adjust_leverage(client, MARKET_ID, LEVERAGE, MARGIN_MODE, logger=logger)
        if err:
            logger.error(f"❌ Failed to adjust leverage: {err}. Continuing with default leverage.")
        else:
            logger.info(f"✅ Successfully set leverage to {LEVERAGE}x")

        balance_task = asyncio.create_task(track_balance())
        await market_making_loop(client, account_api, order_api)

    except asyncio.TimeoutError:
        logger.error("❌ Timeout waiting for initial data from websockets.")
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("🛑 === Shutdown signal received - Stopping... ===")
    finally:
        logger.info("🧹 === Market Maker Cleanup Starting ===")
        tasks_to_cancel = []
        if 'user_stats_task' in locals(): tasks_to_cancel.append(user_stats_task)
        if 'account_all_task' in locals(): tasks_to_cancel.append(account_all_task)
        if 'balance_task' in locals(): tasks_to_cancel.append(balance_task)
        if 'ws_task' in locals(): tasks_to_cancel.append(ws_task)
        if 'poll_avellaneda_task' in locals(): tasks_to_cancel.append(poll_avellaneda_task)

        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        try:
            logger.info("🛡️ Final safety measure: attempting to cancel all orders.")
            await asyncio.wait_for(cancel_all_orders(client), timeout=10)
        except asyncio.TimeoutError:
            logger.error("Timeout during final order cancellation.")
        except Exception as e:
            logger.error(f"Error during final order cancellation: {e}")

        await client.close()
        await api_client.close()
        logger.info("🛑 Market maker stopped.")

# ============ Entrypoint with signal handling ============ 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Lighter market maker")
    parser.add_argument("--symbol", default=os.getenv("MARKET_SYMBOL", "PAXG"), help="Market symbol to trade")
    args = parser.parse_args()
    MARKET_SYMBOL = args.symbol.upper()
    os.environ["MARKET_SYMBOL"] = MARKET_SYMBOL

    async def main_with_signal_handling():
        loop = asyncio.get_running_loop()
        main_task = asyncio.create_task(main())

        def shutdown_handler(sig):
            logger.info(f"🛑 Received exit signal {sig.name}. Starting graceful shutdown...")
            if not main_task.done():
                main_task.cancel()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, shutdown_handler, sig)
            except NotImplementedError:
                pass

        try:
            await main_task
        except asyncio.CancelledError:
            logger.info("🛑 Main task cancelled. Cleanup is handled in main().")

    try:
        asyncio.run(main_with_signal_handling())
        logger.info("✅ Application has finished gracefully.")
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 Application exiting.")
