# pragma pylint: disable=missing-docstring, invalid-name, pointless-string-statement
# flake8: noqa: F401
# isort: skip_file

import asyncio
import logging
import lighter
import os
import time
import json
import math
import websockets
from typing import Tuple, Optional
from datetime import datetime
from lighter.exceptions import ApiException
import signal
from collections import deque
import argparse

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
SUPER_TREND_REFRESH_SECONDS = 120
POSITION_VALUE_THRESHOLD_USD = 15.0

# Directories (mounted by docker-compose)
PARAMS_DIR = os.getenv("PARAMS_DIR", "params")
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Trading config
SPREAD = 0.035 / 100.0       # static fallback spread (if allowed)
BASE_AMOUNT = 0.047          # static fallback amount
USE_DYNAMIC_SIZING = True
CAPITAL_USAGE_PERCENT = 0.25
SAFETY_MARGIN_PERCENT = 0.01
ORDER_TIMEOUT = 15           # seconds

# Avellaneda
AVELLANEDA_REFRESH_INTERVAL = 900  # seconds
REQUIRE_PARAMS = os.getenv("REQUIRE_PARAMS", "false").lower() == "true"

# Global WS / state
latest_order_book = None
order_book_received = asyncio.Event()
account_state_received = asyncio.Event()
account_all_received = asyncio.Event()
ws_connection_healthy = False
last_order_book_update = 0
current_mid_price_cached = None
ws_client = None
ws_task = None

current_order_id = None
current_order_timestamp = None
last_mid_price = None
order_side = "sell" if flip_state else "buy"
available_capital = None
portfolio_value = None
last_capital_check = 0
current_position_size = 0
last_order_base_amount = 0

avellaneda_params = None
last_avellaneda_update = 0

account_positions = {}
recent_trades = deque(maxlen=20)
supertrend_issue_logged = False
flip_change_block_logged = False

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
# Helpers
# =========================
def trim_exception(e: Exception) -> str:
    return str(e).strip().split("\n")[-1]


def get_opening_side() -> str:
    return "sell" if flip_state else "buy"


def get_closing_side() -> str:
    return "buy" if flip_state else "sell"


def mode_label(state: Optional[bool] = None) -> str:
    value = flip_state if state is None else state
    return "short" if value else "long"


def has_position_to_close(position_size: float) -> bool:
    mid_price = get_current_mid_price()
    if not is_position_significant(position_size, mid_price):
        return False
    return position_size > 0 if not flip_state else position_size < 0


def get_closable_units(position_size: float) -> float:
    return abs(position_size) if has_position_to_close(position_size) else 0.0


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
    if latest_order_book:
        bids = latest_order_book.get('bids', [])
        asks = latest_order_book.get('asks', [])
        best_bid = float(bids[0]['price']) if bids else None
        best_ask = float(asks[0]['price']) if asks else None
        return best_bid, best_ask
    return None, None


def is_position_significant(position_size: float, mid_price: Optional[float]) -> bool:
    if abs(position_size) < 1e-9:
        return False
    if not mid_price or mid_price <= 0:
        return True
    return get_position_value_usd(position_size, mid_price) >= POSITION_VALUE_THRESHOLD_USD


def read_supertrend_trend() -> Optional[int]:
    candidates = [
        os.path.join(PARAMS_DIR, f"supertrend_params_{MARKET_SYMBOL}.json"),
        f"params/supertrend_params_{MARKET_SYMBOL}.json",
        f"supertrend_params_{MARKET_SYMBOL}.json",
    ]

    for path in candidates:
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            continue
        except json.JSONDecodeError as exc:
            logger.warning(f"Invalid JSON in {path}: {exc}")
            return None
        except Exception as exc:
            logger.error(f"Unexpected error reading {path}: {exc}", exc_info=True)
            return None

        trend_value = data.get("trend")
        if isinstance(trend_value, str):
            try:
                trend_value = float(trend_value)
            except ValueError:
                logger.warning(f"Non-numeric trend value in {path}: {trend_value}")
                return None
        if isinstance(trend_value, (int, float)):
            if trend_value > 0:
                return 1
            if trend_value < 0:
                return -1
            logger.info(f"Trend value 0 encountered in {path}; retaining current orientation.")
            return None

        logger.warning(f"Trend entry missing or invalid in {path}.")
        return None

    return None


def update_flip_target_from_supertrend(initial: bool = False) -> None:
    global flip_target_state, supertrend_issue_logged

    trend = read_supertrend_trend()
    if trend is None:
        desired_state = False
        if flip_target_state != desired_state:
            logger.info(
                f"🧭 Supertrend file missing or invalid → defaulting to {mode_label(desired_state)} mode."
            )
        flip_target_state = desired_state
        if initial or not supertrend_issue_logged:
            logger.warning(
                f"Supertrend parameters not available for {MARKET_SYMBOL}; defaulting to long mode until file appears."
            )
            supertrend_issue_logged = True
        return

    supertrend_issue_logged = False
    target_state = (trend == -1)
    if flip_target_state != target_state or initial:
        logger.info(
            f"🧭 Supertrend trend {trend:+d} detected → targeting {mode_label(target_state)} mode."
        )
    flip_target_state = target_state


def apply_flip_target_if_idle(force: bool = False) -> bool:
    global flip_state, order_side, last_order_base_amount, flip_change_block_logged

    if not force and flip_state == flip_target_state:
        flip_change_block_logged = False
        return False

    if not force:
        block_reason = None
        mid_price = get_current_mid_price()
        if is_position_significant(current_position_size, mid_price):
            block_reason = (
                f"open {position_label(current_position_size)} position of {current_position_size}"
            )
        elif current_order_id is not None:
            block_reason = f"active order {current_order_id}"

        if block_reason:
            if not flip_change_block_logged:
                logger.info(
                    f"Supertrend requests {mode_label(flip_target_state)} mode, but {block_reason} persists. Will retry after flattening."
                )
                flip_change_block_logged = True
            return False

    previous_mode = mode_label(flip_state)
    flip_state = flip_target_state
    order_side = get_opening_side()
    last_order_base_amount = 0
    flip_change_block_logged = False
    logger.info(f"Orientation updated: {previous_mode} → {mode_label()} mode.")
    return True


async def monitor_supertrend_params():
    while True:
        try:
            update_flip_target_from_supertrend()
            apply_flip_target_if_idle()
        except Exception as exc:
            logger.error(f"Supertrend monitor error: {exc}", exc_info=True)
        await asyncio.sleep(SUPER_TREND_REFRESH_SECONDS)

async def adjust_leverage(client: lighter.SignerClient, market_id: int, leverage: int, margin_mode_str: str):
    """
    Adjusts the leverage for a given market.
    """
    margin_mode = client.CROSS_MARGIN_MODE if margin_mode_str == "cross" else client.ISOLATED_MARGIN_MODE
    
    logger.info(f"Attempting to set leverage to {leverage} for market {market_id} with {margin_mode_str} margin.")

    try:
        tx, response, err = await client.update_leverage(market_id, margin_mode, leverage)
        if err:
            logger.error(f"Error updating leverage: {err}")
            return None, None, err
        else:
            logger.info("Leverage updated successfully.")
            logger.debug(f"Transaction: {tx}")
            logger.debug(f"Response: {response}")
            return tx, response, None
    except Exception as e:
        logger.error(f"An exception occurred: {e}")
        return None, None, e

async def get_market_details(order_api, symbol: str) -> Optional[Tuple[int, float, float]]:
    try:
        order_books_response = await order_api.order_books()
        for ob in order_books_response.order_books:
            if ob.symbol.upper() == symbol.upper():
                market_id = ob.market_id
                price_tick_size = 10 ** -ob.supported_price_decimals
                amount_tick_size = 10 ** -ob.supported_size_decimals
                return market_id, price_tick_size, amount_tick_size
        return None
    except Exception as e:
        logger.error(f"An error occurred while fetching market details: {e}")
        return None

async def submit_reduce_only_close_order(client, position_size, current_mid_price):
    """Submit a reduce-only order to close an existing position using Avellaneda pricing when available."""
    global current_order_id

    magnitude = abs(position_size)
    if magnitude < 1e-9:
        logger.info("Position already flat; no close order submitted.")
        return True

    base_units = int(magnitude / AMOUNT_TICK_SIZE)
    if base_units <= 0:
        logger.warning(
            f"Position size {position_size} insufficient for tick size {AMOUNT_TICK_SIZE}; skipping close order."
        )
        return False

    is_long = position_size > 0
    side = "sell" if is_long else "buy"
    target_price = calculate_order_price(current_mid_price, side)
    if target_price is None or target_price <= 0:
        if side == "sell":
            target_price = current_mid_price * (1.0 + SPREAD)
        else:
            target_price = current_mid_price * max(1.0 - SPREAD, 0.0001)
        logger.warning(
            f"Falling back to static spread pricing for {side} close order: target ${target_price:.6f}."
        )

    target_price = max(target_price, PRICE_TICK_SIZE)

    order_id = int(time.time() * 1_000_000) % 1_000_000
    logger.info(
        f"Placing reduce-only {side} order at ${target_price:.6f} to close {position_label(position_size)} position of {position_size}."
    )

    try:
        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_ID,
            client_order_index=order_id,
            base_amount=base_units,
            price=int(target_price / PRICE_TICK_SIZE),
            is_ask=is_long,
            order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
            time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=True
        )
        if err is not None:
            logger.error(f"Error placing position closing order: {trim_exception(err)}")
            return False
        logger.info(f"Successfully placed position closing order: tx={getattr(tx_hash,'tx_hash',tx_hash)}")
        current_order_id = order_id
        return True
    except Exception as e:
        logger.error(f"Exception while submitting close order: {e}", exc_info=True)
        return False


async def close_open_position_and_wait(client) -> bool:
    """Continuously attempts to flatten any significant existing position before the strategy starts."""
    global last_mid_price

    last_logged_size = None
    last_submit_time = 0.0

    while True:
        mid = get_current_mid_price()
        if not mid:
            logger.info("Waiting for market data before evaluating startup position.")
            await asyncio.sleep(3)
            continue

        current_size = current_position_size
        significant = is_position_significant(current_size, mid)

        if not significant:
            if current_order_id is not None:
                await cancel_order(client, current_order_id)
            value = get_position_value_usd(current_size, mid)
            logger.info(
                f"Startup inventory {current_size} ({position_label(current_size)}) valued at ${value:.2f} < ${POSITION_VALUE_THRESHOLD_USD:.2f}; treating as flat."
            )
            return True

        if last_logged_size != current_size:
            value = get_position_value_usd(current_size, mid)
            logger.info(
                f"🔄 Ensuring flat start. Outstanding {position_label(current_size)} position {current_size} worth ${value:.2f}."
            )
            last_logged_size = current_size

        need_new_order = (current_order_id is None) or ((time.time() - last_submit_time) >= ORDER_TIMEOUT)
        if need_new_order:
            if current_order_id is not None:
                await cancel_order(client, current_order_id)

            success = await submit_reduce_only_close_order(client, current_size, mid)
            if not success:
                logger.warning("Failed to submit reduce-only close order; retrying shortly.")
                await asyncio.sleep(5)
                continue

            last_submit_time = time.time()
            last_mid_price = mid
        else:
            logger.info(
                f"Waiting for reduce-only order {current_order_id} to flatten startup position {current_size}."
            )

        await asyncio.sleep(3)

def on_order_book_update(market_id, order_book):
    global latest_order_book, ws_connection_healthy, last_order_book_update, current_mid_price_cached
    try:
        if int(market_id) == MARKET_ID:
            bids = order_book.get('bids', [])
            asks = order_book.get('asks', [])
            if bids and asks:
                best_bid = float(bids[0]['price'])
                best_ask = float(asks[0]['price'])
                current_mid_price_cached = (best_bid + best_ask) / 2
            latest_order_book = order_book
            ws_connection_healthy = True
            last_order_book_update = time.time()
            order_book_received.set()
    except Exception as e:
        logger.error(f"Error in order book callback: {e}", exc_info=True)
        ws_connection_healthy = False

class RobustWsClient(lighter.WsClient):
    def handle_unhandled_message(self, message):
        try:
            if isinstance(message, dict):
                t = message.get('type', 'unknown')
                if t in ['ping', 'pong', 'heartbeat', 'keepalive', 'health']:
                    logger.debug(f"Received {t} message")
                    return
                else:
                    logger.warning(f"Unknown WS message: {message}")
        except Exception as e:
            logger.error(f"WS handle error: {e}", exc_info=True)

def on_user_stats_update(account_id, stats):
    global available_capital, portfolio_value
    try:
        if int(account_id) == ACCOUNT_INDEX:
            new_available_capital = float(stats.get('available_balance', 0))
            new_portfolio_value = float(stats.get('portfolio_value', 0))

            if new_available_capital > 0 and new_portfolio_value > 0:
                available_capital = new_available_capital
                portfolio_value = new_portfolio_value
                logger.info(f"Received user stats for account {account_id}: Available Capital=${available_capital}, Portfolio Value=${portfolio_value}")
                account_state_received.set()
            else:
                logger.warning(f"Received user stats with invalid values: available_balance={stats.get('available_balance')}, portfolio_value={stats.get('portfolio_value')}")
    except (ValueError, TypeError) as e:
        logger.error(f"Error processing user stats update: {e}", exc_info=True)

async def subscribe_to_user_stats(account_id):
    """Connects to the websocket, subscribes to user_stats, and updates global state."""
    subscription_msg = {
        "type": "subscribe",
        "channel": f"user_stats/{account_id}"
    }
    
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL) as ws:
                logger.info(f"Connected to {WEBSOCKET_URL} for user stats")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"Subscribed to user_stats for account {account_id}")
                
                async for message in ws:
                    logger.debug(f"Raw user_stats message received: {message}")
                    data = json.loads(message)
                    if data.get("type") == "update/user_stats" or data.get("type") == "subscribed/user_stats":
                        stats = data.get("stats", {})
                        on_user_stats_update(account_id, stats)
                    elif data.get("type") == "ping":
                        logger.debug("Received application-level ping, ignoring.")
                    else:
                        logger.debug(f"Received unhandled message on user_stats socket: {data}")
                        
        except websockets.exceptions.ConnectionClosed as e:
            logger.error(f"User stats connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"An unexpected error occurred in user_stats socket: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

def on_account_all_update(account_id, data):
    global account_positions, recent_trades, current_position_size
    try:
        if int(account_id) == ACCOUNT_INDEX:
            # Update positions
            new_positions = data.get("positions", {})
            account_positions = new_positions
            
            market_position = new_positions.get(str(MARKET_ID))
            new_size = 0.0
            if market_position:
                new_size = float(market_position.get('position', 0))
            else:
                # Explicitly set to zero if no position for this market exists in the update
                new_size = 0.0

            if new_size != current_position_size:
                logger.info(f"WebSocket position update for market {MARKET_ID}: {current_position_size} -> {new_size}")
                current_position_size = new_size
            
            # Update trades
            new_trades_by_market = data.get("trades", {})
            if new_trades_by_market:
                all_new_trades = [trade for trades in new_trades_by_market.values() for trade in trades]
                all_new_trades.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
                for trade in reversed(all_new_trades):
                     if trade not in recent_trades:
                        recent_trades.append(trade)
                        logger.info(f"WebSocket trade update: Market {trade.get('market_id')}, Type {trade.get('type')}, Size {trade.get('size')}, Price {trade.get('price')}")

            if not account_all_received.is_set():
                account_all_received.set()

    except (ValueError, TypeError) as e:
        logger.error(f"Error processing account_all update: {e}", exc_info=True)

async def subscribe_to_account_all(account_id):
    """Connects to the websocket, subscribes to account_all, and updates global state."""
    subscription_msg = {
        "type": "subscribe",
        "channel": f"account_all/{account_id}"
    }
    
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL) as ws:
                logger.info(f"Connected to {WEBSOCKET_URL} for account_all")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"Subscribed to account_all for account {account_id}")
                
                async for message in ws:
                    logger.debug(f"Raw account_all message received: {message}")
                    data = json.loads(message)
                    msg_type = data.get("type")
                    if msg_type in ("update/account_all", "update/account", "subscribed/account_all"):
                        on_account_all_update(account_id, data)
                    elif data.get("type") == "ping":
                        logger.debug("Received application-level ping, ignoring.")
                    else:
                        logger.debug(f"Received unhandled message on account_all socket: {data}")
                        
        except websockets.exceptions.ConnectionClosed as e:
            logger.error(f"account_all connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"An unexpected error occurred in account_all socket: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

def get_current_mid_price():
    if current_mid_price_cached is not None and (time.time() - last_order_book_update) < 10:
        return current_mid_price_cached
    if latest_order_book is None:
        return None
    bids = latest_order_book.get('bids', [])
    asks = latest_order_book.get('asks', [])
    if not bids or not asks:
        return None
    return (float(bids[0]['price']) + float(asks[0]['price'])) / 2

async def calculate_dynamic_base_amount(current_mid_price):
    global available_capital, LEVERAGE
    if not USE_DYNAMIC_SIZING:
        return BASE_AMOUNT * float(LEVERAGE)

    if available_capital is None or available_capital <= 0:
        logger.warning(f"No available capital from websocket, using static BASE_AMOUNT: {BASE_AMOUNT}")
        return BASE_AMOUNT * float(LEVERAGE)

    usable_capital = available_capital * (1.0 - SAFETY_MARGIN_PERCENT) * float(LEVERAGE)
    order_capital = usable_capital * CAPITAL_USAGE_PERCENT
    if current_mid_price and current_mid_price > 0:
        dynamic = order_capital / current_mid_price
        dynamic = max(dynamic, 0.001)
        logger.info(f"Dynamic sizing: ${order_capital:.2f} / ${current_mid_price:.2f} = {dynamic:.6f} units")
        return dynamic
    else:
        logger.warning(f"Invalid mid price, using static BASE_AMOUNT: {BASE_AMOUNT}")
        return BASE_AMOUNT * float(LEVERAGE)

def load_avellaneda_parameters() -> bool:
    """
    Load and validate Avellaneda parameters from PARAMS_DIR with priority.
    """
    global avellaneda_params, last_avellaneda_update
    try:
        now = time.time()
        if avellaneda_params is not None and (now - last_avellaneda_update) < AVELLANEDA_REFRESH_INTERVAL:
            return True

        avellaneda_params = None

        candidates = [
            os.path.join(PARAMS_DIR, f'avellaneda_parameters_{MARKET_SYMBOL}.json'),
            f'params/avellaneda_parameters_{MARKET_SYMBOL}.json',
            f'avellaneda_parameters_{MARKET_SYMBOL}.json',
            f'TRADER/avellaneda_parameters_{MARKET_SYMBOL}.json',
        ]
        data = None
        for p in candidates:
            try:
                with open(p, 'r') as f:
                    data = json.load(f)
                logger.info(f"📁 Loaded Avellaneda params from: {p}")
                break
            except FileNotFoundError:
                continue
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON in {p}: {e}.")
                return False

        if not data:
            logger.warning(f"Params file not found for {MARKET_SYMBOL}.")
            return False

        lo = data.get('limit_orders')
        if not isinstance(lo, dict):
            logger.warning("'limit_orders' missing/invalid in params.")
            return False

        da = lo.get('delta_a')
        db = lo.get('delta_b')
        try:
            da = float(da); db = float(db)
            if not (math.isfinite(da) and math.isfinite(db)) or da < 0 or db < 0:
                logger.warning("delta_a/delta_b invalid (NaN/Inf/negative).")
                return False
        except Exception:
            logger.warning("delta_a/delta_b not numeric.")
            return False

        avellaneda_params = data
        last_avellaneda_update = now
        mid_price = get_current_mid_price()
        if mid_price and mid_price > 0:
            delta_a_pct = (da / mid_price) * 100
            delta_b_pct = (db / mid_price) * 100
            logger.info(f"📊 Avellaneda OK. delta_a={da} (+{delta_a_pct:.4f}%) delta_b={db} (-{delta_b_pct:.4f}%)")
        else:
            logger.info(f"📊 Avellaneda OK. delta_a={da} delta_b={db}")
        return True

    except Exception as e:
        logger.error(f"Unexpected error loading params: {e}", exc_info=True)
        avellaneda_params = None
        return False

def calculate_order_price(mid_price, side) -> Optional[float]:
    ok = load_avellaneda_parameters()
    if ok and avellaneda_params:
        lo = avellaneda_params['limit_orders']
        return mid_price - float(lo['delta_b']) if side == "buy" else mid_price + float(lo['delta_a'])

    if REQUIRE_PARAMS:
        logger.info("REQUIRE_PARAMS enabled and no valid params → skipping quoting.")
        return None

    # fallback static
    return mid_price * (1.0 - SPREAD) if side == "buy" else mid_price * (1.0 + SPREAD)

async def place_order(client, side, price, order_id, base_amount):
    global current_order_id
    is_ask = (side == "sell")
    reduce_only_flag = (side == get_closing_side())
    base_amount_scaled = int(base_amount / AMOUNT_TICK_SIZE)
    price_scaled = int(price / PRICE_TICK_SIZE)
    logger.info(f"Placing {side} order: {base_amount:.6f} units at ${price:.6f} (ID: {order_id})")
    try:
        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_ID,
            client_order_index=order_id,
            base_amount=base_amount_scaled,
            price=price_scaled,
            is_ask=is_ask,
            order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
            time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=reduce_only_flag
        )
        if err is not None:
            logger.error(f"Error placing {side} order: {trim_exception(err)}")
            return False
        logger.info(f"Successfully placed {side} order: tx={getattr(tx_hash,'tx_hash',tx_hash)}")
        current_order_id = order_id
        return True
    except Exception as e:
        logger.error(f"Exception in place_order: {e}", exc_info=True)
        return False

async def cancel_order(client, order_id):
    """
    Replaced by cancel_all_orders since each bot runs on a dedicated sub-account.
    """
    global current_order_id
    logger.info(f"Cancelling all orders (was targeting order {order_id})")
    try:
        tx, tx_hash, err = await client.cancel_all_orders(
            time_in_force=client.CANCEL_ALL_TIF_IMMEDIATE,
            time=0
        )
        if err is not None:
            logger.error(f"Error cancelling all orders: {trim_exception(err)}")
            return False
        logger.info(f"Successfully cancelled all orders: tx={getattr(tx_hash,'tx_hash',tx_hash) if tx_hash else 'OK'}")
        current_order_id = None
        return True
    except Exception as e:
        logger.error(f"Exception in cancel_order: {e}", exc_info=True)
        return False

async def check_websocket_health():
    global ws_connection_healthy, last_order_book_update, ws_task
    if (time.time() - last_order_book_update) > 30:
        logger.warning(f"Websocket unhealthy - no updates for {time.time() - last_order_book_update:.1f}s")
        ws_connection_healthy = False
        return False
    if ws_task and ws_task.done():
        logger.warning("Websocket task finished unexpectedly")
        try:
            ws_task.result()
        except Exception as e:
            logger.error(f"WS task exception: {e}", exc_info=True)
        ws_connection_healthy = False
        return False
    return ws_connection_healthy

async def restart_websocket():
    global ws_client, ws_task, order_book_received, ws_connection_healthy
    logger.info("🔄 Restarting websocket connection...")
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
    ws_connection_healthy = False
    order_book_received.clear()
    ws_client = RobustWsClient(order_book_ids=[MARKET_ID], account_ids=[], on_order_book_update=on_order_book_update)
    ws_task = asyncio.create_task(ws_client.run_async())
    try:
        logger.info("Waiting for websocket reconnection...")
        await asyncio.wait_for(order_book_received.wait(), timeout=15.0)
        logger.info("✅ Websocket reconnected successfully")
        return True
    except asyncio.TimeoutError:
        logger.error("❌ Websocket reconnection failed - timeout.")
        return False

async def market_making_loop(client, account_api, order_api):
    global last_mid_price, order_side, current_order_id
    global current_position_size, last_order_base_amount

    logger.info("🚀 Starting Avellaneda-Stoikov market making loop...")

    while True:
        try:
            opening_side = get_opening_side()
            closing_side = get_closing_side()
            if not await check_websocket_health():
                logger.warning("⚠ Websocket connection unhealthy, attempting restart...")
                if not await restart_websocket():
                    logger.error("Failed to restart websocket, retrying in 10 seconds")
                    await asyncio.sleep(10)
                    continue

            current_mid_price = get_current_mid_price()
            if current_mid_price is None:
                logger.info("No order book data yet, sleeping...")
                await asyncio.sleep(2)
                continue

            price_changed = (last_mid_price is None or abs(current_mid_price - last_mid_price) / last_mid_price > 0.001)

            order_price = calculate_order_price(current_mid_price, order_side)
            if order_price is None:
                # REQUIRE_PARAMS true and no params → we wait
                await asyncio.sleep(3)
                continue

            # Log the spread in %
            if current_mid_price > 0:
                pct = ((order_price - current_mid_price) / current_mid_price) * 100.0
            else:
                pct = 0.0
            logger.info(f"Mid: ${current_mid_price:.6f}, Target {order_side}: ${order_price:.6f} ({pct:+.4f}%), Price changed: {price_changed}")

            if current_order_id is not None and price_changed:
                await cancel_order(client, current_order_id)
            elif current_order_id is not None and not price_changed:
                logger.info(f"Order {current_order_id} still active - price unchanged")

            if current_order_id is None:
                order_id = int(time.time() * 1_000_000) % 1_000_000
                base_amt = 0.0

                if order_side == opening_side:
                    base_amt = await calculate_dynamic_base_amount(current_mid_price)
                else:
                    closable_units = get_closable_units(current_position_size)
                    if closable_units > 0:
                        position_value_usd = closable_units * current_mid_price
                        if position_value_usd >= POSITION_VALUE_THRESHOLD_USD:
                            base_amt = closable_units
                        else:
                            logger.info(
                                f"Position value ${position_value_usd:.2f} is below ${POSITION_VALUE_THRESHOLD_USD:.2f}; skipping closing order this cycle."
                            )
                    else:
                        logger.info("No position to close; skipping closing order.")

                if base_amt > 0:
                    last_order_base_amount = base_amt
                    ok = await place_order(client, order_side, order_price, order_id, base_amt)
                    if not ok:
                        await asyncio.sleep(5)
                        continue
                    last_mid_price = current_mid_price
                else:
                    if order_side == opening_side:
                        logger.warning("Calculated opening order size is zero, skip.")

            # This is the main logic for the "reconcile-after-timeout" strategy.
            # We wait for a set period, then cancel the order and check our final position.
            await asyncio.sleep(ORDER_TIMEOUT)

            if current_order_id is not None:
                logger.info(f"⏰ Order {current_order_id} timeout reached. Cancelling and assessing fills.")
                await cancel_order(client, current_order_id) # This also sets current_order_id to None

            # After cancelling, current_position_size (updated by the websocket) is our ground truth.
            # Now, we decide if we need to flip the side based on our actual inventory.
            position_value_usd = get_position_value_usd(current_position_size, current_mid_price)
            inventory_desc = position_label(current_position_size)

            if order_side == opening_side:
                if has_position_to_close(current_position_size):
                    if position_value_usd >= POSITION_VALUE_THRESHOLD_USD:
                        logger.info(
                            f"✅ Position opened after {order_side} cycle with {inventory_desc} inventory {current_position_size}. Value ${position_value_usd:.2f} is sufficient; switching to {closing_side} side."
                        )
                        order_side = closing_side
                    else:
                        logger.info(
                            f"Position opened after {order_side} cycle, but value ${position_value_usd:.2f} is below ${POSITION_VALUE_THRESHOLD_USD:.2f}. Remaining on {opening_side} side to accumulate more. Inventory: {current_position_size}."
                        )
                else:
                    logger.info(
                        f"No fill detected during {order_side} cycle. Inventory: {current_position_size} ({inventory_desc}). Remaining on {opening_side} side."
                    )
            else:
                if not has_position_to_close(current_position_size):
                    logger.info(f"✅ Position closed after {order_side} cycle. Inventory: 0 (flat).")
                    order_side = opening_side
                    last_order_base_amount = 0
                elif position_value_usd < POSITION_VALUE_THRESHOLD_USD:
                    logger.info(
                        f"Position value ${position_value_usd:.2f} is below ${POSITION_VALUE_THRESHOLD_USD:.2f}. Switching to {opening_side} side to adjust exposure. Inventory: {current_position_size} ({inventory_desc})."
                    )
                    order_side = opening_side
                else:
                    logger.info(
                        f"No fill or partial fill left {inventory_desc} inventory {current_position_size}. Remaining on {order_side} side."
                    )

            apply_flip_target_if_idle()
            await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"Loop error: {e}", exc_info=True)
            if "websocket" in str(e).lower():
                global ws_connection_healthy
                ws_connection_healthy = False
            await asyncio.sleep(5)

async def track_balance():
    log_path = os.path.join(LOG_DIR, "balance_log.txt")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    while True:
        try:
            if current_position_size == 0 and portfolio_value is not None:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with open(log_path, "a") as f:
                    f.write(f"[{timestamp}] Portfolio Value: ${portfolio_value:,.2f}\n")
                logger.info(f"Portfolio value of ${portfolio_value:,.2f} logged to {log_path}")
            elif current_position_size != 0:
                logger.info(f"Skipping balance logging (open position: {current_position_size})")
            else:
                logger.info("Skipping balance logging (portfolio value not yet received)")
        except Exception as e:
            logger.error(f"Error in track_balance: {e}", exc_info=True)
        await asyncio.sleep(300)


async def main():
    global MARKET_ID, PRICE_TICK_SIZE, AMOUNT_TICK_SIZE
    global ws_client, ws_task, last_order_book_update, position_detected_at_startup
    global last_mid_price, current_order_id, current_position_size, order_side
    global flip_state, flip_target_state, flip_change_block_logged

    logger.info("=== Market Maker Starting ===")

    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
    account_api = lighter.AccountApi(api_client)
    order_api = lighter.OrderApi(api_client)

    details = await get_market_details(order_api, MARKET_SYMBOL)
    if not details:
        logger.error(f"Could not retrieve market details for {MARKET_SYMBOL}. Exiting.")
        return
    MARKET_ID, PRICE_TICK_SIZE, AMOUNT_TICK_SIZE = details
    logger.info(f"Market {MARKET_SYMBOL}: id={MARKET_ID}, tick(price)={PRICE_TICK_SIZE}, tick(amount)={AMOUNT_TICK_SIZE}")

    client = lighter.SignerClient(
        url=BASE_URL,
        private_key=API_KEY_PRIVATE_KEY,
        account_index=ACCOUNT_INDEX,
        api_key_index=API_KEY_INDEX,
    )
    err = client.check_client()
    if err is not None:
        logger.error(f"CheckClient error: {trim_exception(err)}")
        await api_client.close()
        await client.close()
        return
    logger.info("Client connected successfully")

    # Clean slate: cancel all
    try:
        tx, tx_hash, err = await client.cancel_all_orders(
            time_in_force=client.CANCEL_ALL_TIF_IMMEDIATE,
            time=0
        )
        if err is not None:
            logger.error(f"Failed to cancel existing orders at startup: {trim_exception(err)}")
            await api_client.close()
            await client.close()
            return
        await asyncio.sleep(3)
    except Exception as e:
        logger.error(f"Exception during cancel-all: {e}", exc_info=True)
        await api_client.close()
        await client.close()
        return

    last_order_book_update = time.time()
    ws_client = RobustWsClient(order_book_ids=[MARKET_ID], account_ids=[], on_order_book_update=on_order_book_update)
    ws_task = asyncio.create_task(ws_client.run_async())

    user_stats_task = asyncio.create_task(subscribe_to_user_stats(ACCOUNT_INDEX))
    account_all_task = asyncio.create_task(subscribe_to_account_all(ACCOUNT_INDEX))

    try:
        logger.info("Waiting for initial order book, account data, and position data...")
        await asyncio.wait_for(order_book_received.wait(), timeout=30.0)
        logger.info(f"✅ Websocket connected for market {MARKET_ID}")
        
        logger.info("Waiting for valid account capital...")
        await asyncio.wait_for(account_state_received.wait(), timeout=30.0)
        logger.info(f"✅ Received valid account capital: ${available_capital}; and portfolio value: ${portfolio_value}.")

        logger.info("Waiting for initial position data...")
        await asyncio.wait_for(account_all_received.wait(), timeout=30.0)
        logger.info(f"✅ Received initial position data. Current size: {current_position_size}")

        if current_position_size > 0:
            flip_state = False
        elif current_position_size < 0:
            flip_state = True
        flip_target_state = flip_state
        flip_change_block_logged = False
        order_side = get_opening_side()

        opening_side = get_opening_side()
        closing_side = get_closing_side()

        if abs(current_position_size) >= 1e-9:
            order_side = closing_side
            closed = await close_open_position_and_wait(client)
            if not closed:
                await api_client.close()
                await client.close()
                return
            current_position_size = 0
            order_side = get_opening_side()

        update_flip_target_from_supertrend(initial=True)
        apply_flip_target_if_idle()

        # Adjust leverage at startup if no position is open
        mid_price = get_current_mid_price()
        if mid_price and abs(current_position_size) * mid_price <= POSITION_VALUE_THRESHOLD_USD and current_order_id is None:
            if LEVERAGE > 1:
                logger.info(f"Attempting to set leverage to {LEVERAGE}x with {MARGIN_MODE} margin...")
                _, _, err = await adjust_leverage(client, MARKET_ID, LEVERAGE, MARGIN_MODE)
                if err:
                    logger.error(f"Failed to adjust leverage. Please check permissions or API key capabilities. Error: {err}")
                    # Depending on the strategy, you might want to exit here.
                    # For now, we will just log the error and continue with default leverage.
                else:
                    logger.info(f"Successfully set leverage to {LEVERAGE}x")
            else:
                logger.info("Leverage is set to 1, no adjustment needed.")
        elif has_position_to_close(current_position_size):
            logger.info(
                f"Existing {position_label(current_position_size)} position of {current_position_size} detected. Evaluating startup mode..."
            )
            mid_price = get_current_mid_price()
            position_value_usd = get_position_value_usd(current_position_size, mid_price)
            if mid_price:
                if position_value_usd < POSITION_VALUE_THRESHOLD_USD:
                    logger.info(
                        f"Position value ${position_value_usd:.2f} is below ${POSITION_VALUE_THRESHOLD_USD:.2f}. Starting in {opening_side} mode."
                    )
                    order_side = opening_side
                else:
                    logger.info(
                        f"Position value ${position_value_usd:.2f} meets threshold. Starting in {closing_side} mode."
                    )
                    order_side = closing_side
            else:
                logger.warning(
                    "Could not get mid price to evaluate existing position, defaulting to closing mode."
                )
                order_side = closing_side
        else:
            order_side = opening_side

        balance_task = asyncio.create_task(track_balance())
        supertrend_task = asyncio.create_task(monitor_supertrend_params())
        await market_making_loop(client, account_api, order_api)

    except asyncio.TimeoutError:
        logger.error("❌ Timeout waiting for initial data from websockets.")
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("=== Shutdown signal received - Stopping... ===")
    finally:
        logger.info("=== Market Maker Cleanup Starting ===")
        if 'user_stats_task' in locals() and not user_stats_task.done():
            user_stats_task.cancel()
            try:
                await user_stats_task
            except asyncio.CancelledError:
                pass
        if 'account_all_task' in locals() and not account_all_task.done():
            account_all_task.cancel()
            try:
                await account_all_task
            except asyncio.CancelledError:
                pass
        if 'balance_task' in locals() and not balance_task.done():
            balance_task.cancel()
            try:
                await balance_task
            except asyncio.CancelledError:
                pass
        if 'supertrend_task' in locals() and not supertrend_task.done():
            supertrend_task.cancel()
            try:
                await supertrend_task
            except asyncio.CancelledError:
                pass
        if current_order_id is not None:
            logger.info(f"Cancelling open order {current_order_id} before exit...")
            await cancel_order(client, current_order_id)
        if ws_task and not ws_task.done():
            ws_task.cancel()
            try:
                await ws_task
            except asyncio.CancelledError:
                pass
        await client.close()
        await api_client.close()
        logger.info("Market maker stopped.")

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
            logger.info(f"Received exit signal {sig.name}, cancelling main task.")
            if not main_task.done():
                main_task.cancel()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, shutdown_handler, sig)
            except NotImplementedError:
                # add_signal_handler not available on some platforms (e.g., Windows event loop)
                pass

        try:
            await main_task
        except asyncio.CancelledError:
            logger.info("Main task cancelled. Cleanup is handled in main().")

    try:
        asyncio.run(main_with_signal_handling())
        logger.info("Application has finished gracefully.")
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application exiting.")
