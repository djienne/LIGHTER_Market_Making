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
import websockets
from typing import Tuple, Optional
from datetime import datetime
from lighter.exceptions import ApiException
from decimal import Decimal, getcontext
import signal
from collections import deque
import argparse
from sortedcontainers import SortedDict

# Set Decimal precision
getcontext().prec = 28

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
BASE_AMOUNT = 0.047          # static fallback amount
USE_DYNAMIC_SIZING = True
CAPITAL_USAGE_PERCENT = 0.12
SAFETY_MARGIN_PERCENT = 0.01
ORDER_TIMEOUT = 3           # seconds
MINIMUM_SPREAD_PERCENT = 0.005 # Safety net for calculated spreads
QUOTE_UPDATE_THRESHOLD_BPS = float(os.getenv("QUOTE_UPDATE_THRESHOLD_BPS", "1.0"))  # 1 bps
MIN_LOOP_INTERVAL = 0.1     # Avoid spinning 100% CPU on fast updates

# Avellaneda
AVELLANEDA_REFRESH_INTERVAL = 900  # seconds
REQUIRE_PARAMS = os.getenv("REQUIRE_PARAMS", "false").lower() == "true"

# Global WS / state
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
current_bid_order_id = None
current_ask_order_id = None
current_bid_price = None
current_ask_price = None
current_bid_size = None
current_ask_size = None
last_mid_price = None
available_capital = None
portfolio_value = None
last_capital_check = 0
current_position_size = 0
last_client_order_index = 0

avellaneda_params = None
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
    if abs(position_size) < 1e-9:
        return False
    if not mid_price or mid_price <= 0:
        return True
    return get_position_value_usd(position_size, mid_price) >= POSITION_VALUE_THRESHOLD_USD


async def adjust_leverage(client: lighter.SignerClient, market_id: int, leverage: int, margin_mode_str: str):
    """
    Adjusts the leverage for a given market.
    """
    margin_mode = client.CROSS_MARGIN_MODE if margin_mode_str == "cross" else client.ISOLATED_MARGIN_MODE
    
    logger.info(f"⚙️ Attempting to set leverage to {leverage} for market {market_id} with {margin_mode_str} margin.")

    try:
        tx, response, err = await client.update_leverage(market_id, margin_mode, leverage)
        if err:
            logger.error(f"❌ Error updating leverage: {err}")
            return None, None, err
        else:
            logger.info("✅ Leverage updated successfully.")
            logger.debug(f"Transaction: {tx}")
            logger.debug(f"Response: {response}")
            return tx, response, None
    except Exception as e:
        logger.error(f"❌ An exception occurred: {e}")
        return None, None, e

async def get_market_details(order_api, symbol: str) -> Optional[Tuple[int, Decimal, Decimal]]:
    try:
        order_books_response = await order_api.order_books()
        for ob in order_books_response.order_books:
            if ob.symbol.upper() == symbol.upper():
                market_id = ob.market_id
                # 10 ** -decimals
                price_tick_size = Decimal("10") ** -ob.supported_price_decimals
                amount_tick_size = Decimal("10") ** -ob.supported_size_decimals
                return market_id, price_tick_size, amount_tick_size
        return None
    except Exception as e:
        logger.error(f"❌ An error occurred while fetching market details: {e}")
        return None

def on_order_book_update(market_id, payload):
    global ws_connection_healthy, last_order_book_update, current_mid_price_cached, local_order_book
    try:
        if int(market_id) == MARKET_ID:
            bids_in = payload.get('bids', [])
            asks_in = payload.get('asks', [])
            
            # Heuristic for snapshot vs delta
            is_snapshot = False
            if not local_order_book['initialized']:
                is_snapshot = True
                logger.info(f"📚 Initializing local orderbook for market {market_id}")
            elif len(bids_in) > 100 or len(asks_in) > 100:
                is_snapshot = True
                logger.info(f"📚 Received snapshot for market {market_id}")

            if is_snapshot:
                local_order_book['bids'].clear()
                local_order_book['asks'].clear()
                local_order_book['initialized'] = True
                for item in bids_in:
                    price, size = float(item['price']), float(item['size'])
                    if size > 0: local_order_book['bids'][price] = size
                for item in asks_in:
                    price, size = float(item['price']), float(item['size'])
                    if size > 0: local_order_book['asks'][price] = size
            else:
                # Delta update
                for item in bids_in:
                    price, size = float(item['price']), float(item['size'])
                    if size == 0:
                        local_order_book['bids'].pop(price, None)
                    else:
                        local_order_book['bids'][price] = size
                
                for item in asks_in:
                    price, size = float(item['price']), float(item['size'])
                    if size == 0:
                        local_order_book['asks'].pop(price, None)
                    else:
                        local_order_book['asks'][price] = size

            # Calculate mid price using SortedDict O(1) peek
            if local_order_book['bids'] and local_order_book['asks']:
                best_bid = local_order_book['bids'].peekitem(-1)[0]
                best_ask = local_order_book['asks'].peekitem(0)[0]
                current_mid_price_cached = (best_bid + best_ask) / 2
            
            ws_connection_healthy = True
            last_order_book_update = time.time()
            order_book_received.set()
            
    except Exception as e:
        logger.error(f"❌ Error in order book callback: {e}", exc_info=True)
        ws_connection_healthy = False

def on_trade_update(market_id, trades):
    try:
        if int(market_id) == MARKET_ID:
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
    
    ob_sub_msg = {
        "type": "subscribe",
        "channel": f"order_book/{market_id}"
    }
    trade_sub_msg = {
        "type": "subscribe",
        "channel": f"trade/{market_id}"
    }
    
    while True:
        try:
            ws_connection_healthy = False
            async with websockets.connect(WEBSOCKET_URL, ping_interval=20, ping_timeout=20) as ws:
                logger.info(f"🔌 Connected to {WEBSOCKET_URL} for market data")
                
                await ws.send(json.dumps(ob_sub_msg))
                await ws.send(json.dumps(trade_sub_msg))
                logger.info(f"📡 Subscribed to order_book/{market_id} and trade/{market_id}")
                
                ws_connection_healthy = True
                
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        try:
                            data = json.loads(message)
                            msg_type = data.get("type")
                            
                            if msg_type == "update/order_book":
                                if 'order_book' in data:
                                    on_order_book_update(market_id, data['order_book'])
                            elif msg_type == "update/trade":
                                if 'trades' in data:
                                    on_trade_update(market_id, data['trades'])
                            elif msg_type == "ping":
                                logger.debug("Received application-level ping, sending pong.")
                                await ws.send(json.dumps({"type": "pong"}))
                            elif msg_type == "subscribed":
                                logger.info(f"✅ Successfully subscribed to channel: {data.get('channel')}")
                            else:
                                logger.debug(f"Received unhandled message on market data socket: {msg_type}")
                                
                        except json.JSONDecodeError:
                            logger.warning(f"❌ Failed to decode JSON from market data: {message}")
                            
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ Market data WebSocket watchdog triggered (no data for 30s). Reconnecting...")
                        break 
                        
        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
            logger.info(f"🔌 Market data WebSocket disconnected ({e}), reconnecting in 5 seconds...")
            ws_connection_healthy = False
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred in market data socket: {e}. Reconnecting in 5 seconds...")
            ws_connection_healthy = False
            await asyncio.sleep(5)

def on_user_stats_update(account_id, stats):
    global available_capital, portfolio_value
    try:
        if int(account_id) == ACCOUNT_INDEX:
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
    subscription_msg = {
        "type": "subscribe",
        "channel": f"user_stats/{account_id}"
    }
    
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL, ping_interval=20, ping_timeout=20) as ws:
                logger.info(f"🔌 Connected to {WEBSOCKET_URL} for user stats")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"📡 Subscribed to user_stats for account {account_id}")
                
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        try:
                            data = json.loads(message)
                            msg_type = data.get("type")
                            
                            if msg_type in ("update/user_stats", "subscribed/user_stats"):
                                stats = data.get("stats", {})
                                on_user_stats_update(account_id, stats)
                            elif msg_type == "ping":
                                logger.debug("Received application-level ping, sending pong.")
                                await ws.send(json.dumps({"type": "pong"}))
                            else:
                                logger.debug(f"Received unhandled message on user_stats socket: {data}")
                        except json.JSONDecodeError:
                            logger.warning(f"❌ Failed to decode JSON from user_stats: {message}")
                            
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ User stats WebSocket watchdog triggered (no data for 30s). Reconnecting...")
                        break
                        
        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
            logger.info(f"🔌 User stats WebSocket disconnected ({e}), reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred in user_stats socket: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

def on_account_all_update(account_id, data):
    global account_positions, recent_trades, current_position_size
    try:
        if int(account_id) == ACCOUNT_INDEX:
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
    subscription_msg = {
        "type": "subscribe",
        "channel": f"account_all/{account_id}"
    }
    
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL, ping_interval=20, ping_timeout=20) as ws:
                logger.info(f"🔌 Connected to {WEBSOCKET_URL} for account_all")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"📡 Subscribed to account_all for account {account_id}")
                
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        try:
                            data = json.loads(message)
                            msg_type = data.get("type")
                            
                            if msg_type in ("update/account_all", "update/account", "subscribed/account_all"):
                                on_account_all_update(account_id, data)
                            elif msg_type == "ping":
                                logger.debug("Received application-level ping, sending pong.")
                                await ws.send(json.dumps({"type": "pong"}))
                            else:
                                logger.debug(f"Received unhandled message on account_all socket: {data}")
                        except json.JSONDecodeError:
                            logger.warning(f"❌ Failed to decode JSON from account_all: {message}")
                            
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ Account all WebSocket watchdog triggered (no data for 30s). Reconnecting...")
                        break
                        
        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
            logger.info(f"🔌 Account data WebSocket disconnected ({e}), reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred in account_all socket: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

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

def get_current_mid_price():
    global current_mid_price_cached
    return current_mid_price_cached

async def check_websocket_health():
    global ws_connection_healthy, last_order_book_update
    if not ws_connection_healthy:
        return False
    if time.time() - last_order_book_update > 30:
        return False
    return True

async def calculate_dynamic_base_amount(mid_price):
    global available_capital
    if not mid_price or mid_price <= 0:
        return None
    
    if not available_capital:
        return Decimal(str(BASE_AMOUNT))
        
    try:
        usd_amount = available_capital * CAPITAL_USAGE_PERCENT
        size = usd_amount / mid_price
        
        if AMOUNT_TICK_SIZE:
             size = round(size / float(AMOUNT_TICK_SIZE)) * float(AMOUNT_TICK_SIZE)
             
        return Decimal(str(size))
    except:
        return Decimal(str(BASE_AMOUNT))

async def place_order(client, side, price, order_id, size):
    try:
        is_ask = (side == 'sell')

        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_ID,
            client_order_index=order_id,
            base_amount=float(size),
            price=float(price),
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

async def refresh_order_if_needed(client, side, new_price, new_size):
    global current_bid_order_id, current_ask_order_id
    global current_bid_price, current_ask_price, current_bid_size, current_ask_size

    if new_price is None or new_size is None:
        logger.debug(f"Skipping {side} update: missing price or size.")
        return

    new_price_f = float(new_price)
    new_size_f = float(new_size)

    if new_price_f <= 0 or new_size_f <= 0:
        logger.debug(f"Skipping {side} update: invalid price or size.")
        return

    if side == 'buy':
        existing_id = current_bid_order_id
        existing_price = current_bid_price
    elif side == 'sell':
        existing_id = current_ask_order_id
        existing_price = current_ask_price
    else:
        logger.warning(f"Unknown side {side}; cannot refresh order.")
        return

    if existing_id is not None and existing_price is not None:
        change_bps = price_change_bps(existing_price, new_price_f)
        if change_bps <= QUOTE_UPDATE_THRESHOLD_BPS:
            logger.debug(
                f"Keeping {side} order: price change {change_bps:.2f} bps <= {QUOTE_UPDATE_THRESHOLD_BPS:.2f}"
            )
            return

    if existing_id is not None:
        cancelled = await cancel_order(client, existing_id)
        if not cancelled:
            logger.warning(f"Cancel failed for {side} order {existing_id}; skipping replace.")
            return
        if side == 'buy':
            current_bid_order_id = None
            current_bid_price = None
            current_bid_size = None
        else:
            current_ask_order_id = None
            current_ask_price = None
            current_ask_size = None

    new_order_id = next_client_order_index()
    placed = await place_order(client, side, new_price_f, new_order_id, new_size_f)
    if not placed:
        return

    if side == 'buy':
        current_bid_order_id = new_order_id
        current_bid_price = new_price_f
        current_bid_size = new_size_f
    else:
        current_ask_order_id = new_order_id
        current_ask_price = new_price_f
        current_ask_size = new_size_f

def _read_avellaneda_file_sync(candidates):
    for p in candidates:
        try:
            with open(p, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            continue
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in {p}: {e}.")
            return None
    return None

async def poll_avellaneda_parameters():
    """Continuously poll and load Avellaneda parameters from PARAMS_DIR asynchronously."""
    global avellaneda_params, last_avellaneda_update
    while True:
        try:
            now = time.time()
            candidates = [
                os.path.join(PARAMS_DIR, f"avellaneda_parameters_{MARKET_SYMBOL}.json"),
                f"params/avellaneda_parameters_{MARKET_SYMBOL}.json",
                f"avellaneda_parameters_{MARKET_SYMBOL}.json",
                f"TRADER/avellaneda_parameters_{MARKET_SYMBOL}.json",
            ]
            
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, _read_avellaneda_file_sync, candidates)

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


async def calculate_order_price(mid_price, side, inventory_factor):
    """
    Calculates the optimal bid or ask price using Avellaneda-Stoikov formula.
    """
    global avellaneda_params

    if avellaneda_params:
        try:
            gamma = float(avellaneda_params["optimal_parameters"]["gamma"])
            sigma = float(avellaneda_params["market_data"]["sigma"])

            market_data = avellaneda_params.get("market_data", {})
            k_bid = float(market_data.get("k_bid", market_data.get("k", 0.5)))
            k_ask = float(market_data.get("k_ask", market_data.get("k", 0.5)))

            time_horizon = float(avellaneda_params["current_state"]["time_remaining"])  # in days

            reservation_price = mid_price - (inventory_factor * gamma * (sigma**2) * time_horizon)

            if side == "buy":
                val_inside_log = 1.0 + gamma / (mid_price * k_bid + 1e-9)
                spread = (2.0 * mid_price / gamma) * math.log(val_inside_log)
                final_price = reservation_price - (spread / 2.0)
            else:
                val_inside_log = 1.0 + gamma / (mid_price * k_ask + 1e-9)
                spread = (2.0 * mid_price / gamma) * math.log(val_inside_log)
                final_price = reservation_price + (spread / 2.0)

            if PRICE_TICK_SIZE:
                final_price = round(final_price / float(PRICE_TICK_SIZE)) * float(PRICE_TICK_SIZE)

            return final_price
        except Exception as e:
            logger.error(f"Error calculating order price: {e}", exc_info=True)
            return None

    if REQUIRE_PARAMS:
        logger.warning("REQUIRE_PARAMS enabled and no valid Avellaneda params; skipping quoting.")
        return None

    spread = mid_price * 0.001
    return mid_price - (spread / 2) if side == "buy" else mid_price + (spread / 2)

async def market_making_loop(client, account_api, order_api):
    logger.info("Starting 2-sided market making loop...")

    while True:
        try:
            if not await check_websocket_health():
                logger.warning("Websocket connection unhealthy, attempting restart...")
                if not await restart_websocket():
                    await asyncio.sleep(10)
                    continue

            # Wait for fresh order book data but wake up to check state
            try:
                await asyncio.wait_for(order_book_received.wait(), timeout=ORDER_TIMEOUT)
                order_book_received.clear() # Clear so we block next time
            except asyncio.TimeoutError:
                pass # Wake up gracefully and evaluate anyway

            current_mid_price = get_current_mid_price()
            if current_mid_price is None:
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            base_amount = await calculate_dynamic_base_amount(current_mid_price)
            if base_amount is None or base_amount <= 0:
                logger.warning("Base amount is zero or invalid; skipping order refresh.")
                await asyncio.sleep(MIN_LOOP_INTERVAL)
                continue

            inventory_param = current_position_size / float(base_amount)

            buy_price = await calculate_order_price(current_mid_price, "buy", inventory_param)
            sell_price = await calculate_order_price(current_mid_price, "sell", inventory_param)

            if buy_price and sell_price:
                bp_f = float(buy_price)
                sp_f = float(sell_price)
                bid_spread_pct = (current_mid_price - bp_f) / current_mid_price * 100 if current_mid_price > 0 else 0
                ask_spread_pct = (sp_f - current_mid_price) / current_mid_price * 100 if current_mid_price > 0 else 0

                logger.info(
                    f"QUOTING | "
                    f"Inventory: {inventory_param:+.2f} | "
                    f"Mid: ${current_mid_price:.4f} | "
                    f"Bid: ${bp_f:.4f} (-{bid_spread_pct:.4f}%) | "
                    f"Ask: ${sp_f:.4f} (+{ask_spread_pct:.4f}%)"
                )
            else:
                logger.debug("Could not calculate one or both quotes; skipping refresh for missing side(s).")

            # Execute quoting in parallel
            tasks = []
            if buy_price:
                tasks.append(refresh_order_if_needed(client, "buy", buy_price, base_amount))
            if sell_price:
                tasks.append(refresh_order_if_needed(client, "sell", sell_price, base_amount))
            
            if tasks:
                await asyncio.gather(*tasks)

        except Exception as e:
            logger.error(f"Unhandled error in market_making_loop: {e}", exc_info=True)
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
                logger.info(f"💰 Portfolio value of ${portfolio_value:,.2f} logged to {log_path}")
            elif current_position_size != 0:
                logger.info(f"⏸️ Skipping balance logging (open position: {current_position_size})")
            else:
                logger.info("⏸️ Skipping balance logging (portfolio value not yet received)")
        except Exception as e:
            logger.error(f"❌ Error in track_balance: {e}", exc_info=True)
        await asyncio.sleep(300)


async def main():
    global MARKET_ID, PRICE_TICK_SIZE, AMOUNT_TICK_SIZE
    global ws_task, last_order_book_update
    global last_mid_price, current_order_id, current_position_size

    logger.info("🚀 === Market Maker v2 Starting (2-Sided Quoting) ===")

    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
    account_api = lighter.AccountApi(api_client)
    order_api = lighter.OrderApi(api_client)

    details = await get_market_details(order_api, MARKET_SYMBOL)
    if not details:
        logger.error(f"❌ Could not retrieve market details for {MARKET_SYMBOL}. Exiting.")
        return
    MARKET_ID, PRICE_TICK_SIZE, AMOUNT_TICK_SIZE = details
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


    last_order_book_update = time.time()
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
        _, _, err = await adjust_leverage(client, MARKET_ID, LEVERAGE, MARGIN_MODE)
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
