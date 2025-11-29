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
from lighter import CandlestickApi
import signal
from collections import deque, namedtuple
import argparse

# =========================
# Kline & EMA constants
# =========================
KLINE_RESOLUTION = '1m'
# We need enough klines to calculate the longest EMA, plus some buffer
KLINE_HISTORY_SIZE = 200
Kline = namedtuple("Kline", ["timestamp", "open", "high", "low", "close"])


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

# Directories (mounted by docker-compose)
CLOSE_LONG_ON_STARTUP = os.getenv("CLOSE_LONG_ON_STARTUP", "false").lower() == "true"
PARAMS_DIR = os.getenv("PARAMS_DIR", "params")
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Trading config
SPREAD = 0.035 / 100.0       # static fallback spread (if allowed)
BASE_AMOUNT = 0.047          # static fallback amount
USE_DYNAMIC_SIZING = True
CAPITAL_USAGE_PERCENT = 0.98
SAFETY_MARGIN_PERCENT = 0.01
ORDER_TIMEOUT = 90           # seconds
POSITION_MULTIPLIER = 1 #position size multiplier
ENABLE_EMA_CHECK = True
EMA_FAST_PERIOD = 9
EMA_SLOW_PERIOD = 26

# Avellaneda
AVELLANEDA_REFRESH_INTERVAL = 900  # seconds
REQUIRE_PARAMS = os.getenv("REQUIRE_PARAMS", "false").lower() == "true"
EMA_STATE_FILE = os.path.join(PARAMS_DIR, f"ema_state_{MARKET_SYMBOL}.json")

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
order_side = "buy"
available_capital = None
portfolio_value = None
last_capital_check = 0
current_position_size = 0
last_order_base_amount = 0

avellaneda_params = None
last_avellaneda_update = 0

account_positions = {}
recent_trades = deque(maxlen=20)

# EMA & Kline state
klines = deque(maxlen=KLINE_HISTORY_SIZE)
current_kline = {}
ema_fast = None
ema_slow = None


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

# =========================
# EMA Calculation Engine
# =========================

def save_ema_state():
    """Saves the current EMA values to a file."""
    if ema_fast is None or ema_slow is None:
        return
    try:
        state = {
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
            "timestamp": time.time()
        }
        with open(EMA_STATE_FILE, 'w') as f:
            json.dump(state, f)
        logger.debug(f"EMA state saved to {EMA_STATE_FILE}")
    except Exception as e:
        logger.error(f"Error saving EMA state: {e}", exc_info=True)

def load_ema_state():
    """Loads the EMA values from a file on startup."""
    global ema_fast, ema_slow
    if not os.path.exists(EMA_STATE_FILE):
        logger.info("No EMA state file found. Starting fresh.")
        return
    try:
        with open(EMA_STATE_FILE, 'r') as f:
            state = json.load(f)

        # If state is older than 5 minutes, treat as stale and ignore
        if (time.time() - state.get("timestamp", 0)) > 300:
            logger.info("EMA state is stale, will be recalculated from fresh klines.")
            return

        ema_fast = state.get("ema_fast")
        ema_slow = state.get("ema_slow")
        if ema_fast and ema_slow:
            logger.info(f"Loaded non-stale EMA state: Fast={ema_fast:.4f}, Slow={ema_slow:.4f}")
    except Exception as e:
        logger.error(f"Error loading EMA state: {e}. Starting fresh.", exc_info=True)

def _calculate_ema(prices, period):
    if len(prices) < period:
        return None
    multiplier = 2 / (period + 1)
    sma = sum(prices[:period]) / period
    ema = sma
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_emas_from_klines():
    global ema_fast, ema_slow
    
    prices = [k.close for k in klines]
    if len(prices) < EMA_SLOW_PERIOD:
        logger.debug(f"Not enough kline prices ({len(prices)}) to calculate slow EMA ({EMA_SLOW_PERIOD} required).")
        return

    new_ema_fast = _calculate_ema(prices, EMA_FAST_PERIOD)
    new_ema_slow = _calculate_ema(prices, EMA_SLOW_PERIOD)

    if new_ema_fast is not None and new_ema_slow is not None:
        ema_fast = new_ema_fast
        ema_slow = new_ema_slow
        logger.info(f"EMAs updated from klines: Fast={ema_fast:.4f}, Slow={ema_slow:.4f}")
        save_ema_state()

async def fetch_initial_klines(candlestick_api: CandlestickApi):
    global klines
    try:
        logger.info(f"Fetching initial {KLINE_HISTORY_SIZE} klines with '{KLINE_RESOLUTION}' resolution...")
        end_ts = int(time.time() * 1000)
        # For 1m resolution, each kline is 60 seconds
        duration_ms = KLINE_HISTORY_SIZE * 60 * 1000 
        start_ts = end_ts - duration_ms

        api_response = await candlestick_api.candlesticks(
            market_id=MARKET_ID,
            resolution=KLINE_RESOLUTION,
            start_timestamp=start_ts,
            end_timestamp=end_ts,
            count_back=KLINE_HISTORY_SIZE
        )

        if not api_response or not hasattr(api_response, 'candlesticks') or not api_response.candlesticks:
            logger.warning("Could not fetch initial candlestick data or response is empty.")
            return

        fetched_klines = sorted(api_response.candlesticks, key=lambda k: k.timestamp)
        
        klines.clear()
        for k in fetched_klines:
            # The API returns a model object, convert it to our simple Kline namedtuple
            klines.append(Kline(
                timestamp=k.timestamp,
                open=float(k.open),
                high=float(k.high),
                low=float(k.low),
                close=float(k.close)
            ))
        
        logger.info(f"✅ Successfully fetched {len(klines)} initial klines.")
        calculate_emas_from_klines()

    except ApiException as e:
        logger.error(f"API Error fetching initial candlesticks: {e}. Body: {getattr(e, 'body', '')}", exc_info=True)
    except Exception as e:
        logger.error(f"Error processing initial klines: {e}", exc_info=True)

def on_trade_update(trade):
    global current_kline, klines
    try:
        trade_price = float(trade['price'])
        trade_ts_raw = int(trade['timestamp'])
        # Normalize timestamp to seconds, as it may be in milliseconds
        trade_ts = trade_ts_raw / 1000 if trade_ts_raw > 1e12 else trade_ts_raw
        current_minute = int(trade_ts // 60)

        if not current_kline:
            current_kline = {
                'timestamp': current_minute * 60,
                'open': trade_price, 'high': trade_price,
                'low': trade_price, 'close': trade_price
            }
            logger.info(f"📊 Started new 1m kline at {datetime.fromtimestamp(current_kline['timestamp'])} from trade.")
            return

        kline_minute = current_kline['timestamp'] // 60

        if current_minute == kline_minute:
            current_kline['high'] = max(current_kline['high'], trade_price)
            current_kline['low'] = min(current_kline['low'], trade_price)
            current_kline['close'] = trade_price
        elif current_minute > kline_minute:
            logger.info(f"✅ Closed 1m kline: O={current_kline['open']}, H={current_kline['high']}, L={current_kline['low']}, C={current_kline['close']}")
            
            closed_kline = Kline(**current_kline)
            klines.append(closed_kline)
            
            calculate_emas_from_klines()
            
            current_kline = {
                'timestamp': current_minute * 60,
                'open': trade_price, 'high': trade_price,
                'low': trade_price, 'close': trade_price
            }
            logger.info(f"📊 Started new 1m kline at {datetime.fromtimestamp(current_kline['timestamp'])} from trade.")

    except Exception as e:
        logger.error(f"Error in on_trade_update: {e}", exc_info=True)

async def subscribe_to_trades(market_id):
    subscription_msg = {"type": "subscribe", "channel": f"trade/{market_id}"}
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL) as ws:
                logger.info(f"Connected to {WEBSOCKET_URL} for trades")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"Subscribed to trades for market {market_id}")
                
                async for message in ws:
                    logger.debug(f"Raw trade message received: {message}")
                    data = json.loads(message)
                    if data.get("type") == "update/trade":
                        for trade in data.get("trades", []):
                            on_trade_update(trade)
                    elif data.get("type") == "ping":
                        logger.debug("Received application-level ping, ignoring.")
        except websockets.exceptions.ConnectionClosed as e:
            logger.error(f"Trade connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"An unexpected error occurred in trade socket: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

# =========================
# Main Application Logic
# =========================

async def close_long_position(client, position_size, current_mid_price):
    """Close a long position with a reduce-only limit sell order."""
    global current_order_id
    logger.info(f"🔄 Attempting to close long position of size {position_size}")
    sell_price = current_mid_price * (1.0 + SPREAD) 
    order_id = int(time.time() * 1_000_000) % 1_000_000
    logger.info(f"📉 Placing reduce-only sell order at {sell_price} to close long position")
    try:
        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_ID,
            client_order_index=order_id,
            base_amount=int(abs(position_size) / AMOUNT_TICK_SIZE),
            price=int(sell_price / PRICE_TICK_SIZE),
            is_ask=True,
            order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
            time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=True
        )
        if err is not None:
            logger.error(f"Error placing position closing order: {trim_exception(err)}")
            return False
        logger.info(f"✅ Successfully placed position closing order: tx={getattr(tx_hash,'tx_hash',tx_hash)}")
        current_order_id = order_id
        return True
    except Exception as e:
        logger.error(f"Exception in close_long_position: {e}", exc_info=True)
        return False

async def close_long_position_market(client, position_size):
    """Closes a long position with a reduce-only market sell order for immediate execution."""
    logger.info(f"⚡ Attempting to close long position of size {position_size} with a MARKET order.")
    order_id = int(time.time() * 1_000_000) % 1_000_000
    try:
        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_ID,
            client_order_index=order_id,
            base_amount=int(abs(position_size) / AMOUNT_TICK_SIZE),
            price=0,  # Price is 0 for a market order
            is_ask=True,
            order_type=lighter.SignerClient.ORDER_TYPE_MARKET,
            time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=True
        )
        if err is not None:
            logger.error(f"Error placing market closing order: {trim_exception(err)}")
            return False
        logger.info(f"✅ Successfully placed market closing order: tx={getattr(tx_hash,'tx_hash',tx_hash)}")
        return True
    except Exception as e:
        logger.error(f"Exception in close_long_position_market: {e}", exc_info=True)
        return False

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
                logger.info(f"💰 Received user stats for account {account_id}: Available Capital=${available_capital}, Portfolio Value=${portfolio_value}")
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
            new_positions = data.get("positions", {})
            account_positions = new_positions
            
            market_position = new_positions.get(str(MARKET_ID))
            new_size = 0.0
            if market_position:
                new_size = float(market_position.get('position', 0))
            else:
                new_size = 0.0

            if new_size != current_position_size:
                logger.info(f"📍 WebSocket position update for market {MARKET_ID}: {current_position_size} -> {new_size}")
                current_position_size = new_size
            
            new_trades_by_market = data.get("trades", {})
            if new_trades_by_market:
                all_new_trades = [trade for trades in new_trades_by_market.values() for trade in trades]
                all_new_trades.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
                for trade in reversed(all_new_trades):
                     if trade not in recent_trades:
                        recent_trades.append(trade)
                        logger.info(f"💱 WebSocket trade update: Market {trade.get('market_id')}, Type {trade.get('type')}, Size {trade.get('size')}, Price {trade.get('price')}")

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
    global available_capital
    if not USE_DYNAMIC_SIZING:
        return BASE_AMOUNT * POSITION_MULTIPLIER

    if available_capital is None or available_capital <= 0:
        logger.warning(f"No available capital from websocket, using static BASE_AMOUNT: {BASE_AMOUNT}")
        return BASE_AMOUNT * POSITION_MULTIPLIER

    usable_capital = available_capital * (1 - SAFETY_MARGIN_PERCENT)
    order_capital = usable_capital * CAPITAL_USAGE_PERCENT
    if current_mid_price and current_mid_price > 0:
        dynamic = order_capital / current_mid_price
        dynamic = max(dynamic, 0.001)
        dynamic *= POSITION_MULTIPLIER
        logger.info(f"📏 Dynamic sizing: ${order_capital:.2f} / ${current_mid_price:.2f} = {dynamic:.6f} units")
        return dynamic
    else:
        logger.warning(f"Invalid mid price, using static BASE_AMOUNT: {BASE_AMOUNT}")
        return BASE_AMOUNT * POSITION_MULTIPLIER

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

    return mid_price * (1.0 - SPREAD) if side == "buy" else mid_price * (1.0 + SPREAD)

async def place_order(client, side, price, order_id, base_amount):
    global current_order_id
    is_ask = (side == "sell")
    base_amount_scaled = int(base_amount / AMOUNT_TICK_SIZE)
    price_scaled = int(price / PRICE_TICK_SIZE)
    logger.info(f"📝 Placing {side} order: {base_amount:.6f} units at ${price:.6f} (ID: {order_id})")
    try:
        tx, tx_hash, err = await client.create_order(
            market_index=MARKET_ID,
            client_order_index=order_id,
            base_amount=base_amount_scaled,
            price=price_scaled,
            is_ask=is_ask,
            order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
            time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=(side == "sell")
        )
        if err is not None:
            logger.error(f"Error placing {side} order: {trim_exception(err)}")
            return False
        logger.info(f"✅ Successfully placed {side} order: tx={getattr(tx_hash,'tx_hash',tx_hash)}")
        current_order_id = order_id
        return True
    except Exception as e:
        logger.error(f"Exception in place_order: {e}", exc_info=True)
        return False

async def cancel_order(client, order_id):
    global current_order_id
    logger.info(f"❌ Cancelling all orders (was targeting order {order_id})")
    try:
        tx, tx_hash, err = await client.cancel_all_orders(
            time_in_force=client.CANCEL_ALL_TIF_IMMEDIATE,
            time=0
        )
        if err is not None:
            logger.error(f"Error cancelling all orders: {trim_exception(err)}")
            return False
        logger.info(f"✅ Successfully cancelled all orders: tx={getattr(tx_hash,'tx_hash',tx_hash) if tx_hash else 'OK'}")
        current_order_id = None
        return True
    except Exception as e:
        logger.error(f"Exception in cancel_order: {e}", exc_info=True)
        return False

async def check_websocket_health():
    global ws_connection_healthy, last_order_book_update, ws_task
    if (time.time() - last_order_book_update) > 30:
        logger.debug(f"Websocket unhealthy - no updates for {time.time() - last_order_book_update:.1f}s")
        ws_connection_healthy = False
        return False
    if ws_task and ws_task.done():
        logger.debug("Websocket task finished unexpectedly")
        try:
            # Retrieve exception to prevent "Task exception was never retrieved" warnings
            ws_task.exception()
        except Exception as e:
            # Only log non-connection errors at warning level
            if "ConnectionClosed" not in type(e).__name__:
                logger.warning(f"WS task exception: {type(e).__name__}: {e}")
            else:
                logger.debug(f"WS connection closed: {type(e).__name__}")
        ws_connection_healthy = False
        return False
    return ws_connection_healthy

async def restart_websocket():
    global ws_client, ws_task, order_book_received, ws_connection_healthy
    logger.debug("🔄 Restarting websocket connection...")
    if ws_task and not ws_task.done():
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # Suppress exception logging for expected disconnection scenarios
            logger.debug(f"Websocket task cleanup: {type(e).__name__}")
    elif ws_task and ws_task.done():
        # Retrieve exception to prevent "Task exception was never retrieved" warnings
        try:
            ws_task.exception()
        except Exception:
            pass

    ws_connection_healthy = False
    order_book_received.clear()
    ws_client = RobustWsClient(order_book_ids=[MARKET_ID], account_ids=[], on_order_book_update=on_order_book_update)
    ws_task = asyncio.create_task(ws_client.run_async())
    try:
        logger.debug("⏳ Waiting for websocket reconnection...")
        await asyncio.wait_for(order_book_received.wait(), timeout=15.0)
        logger.info("✅ Websocket reconnected")
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
            if not await check_websocket_health():
                if not await restart_websocket():
                    logger.error("Failed to restart websocket, retrying in 10 seconds")
                    await asyncio.sleep(10)
                    continue

            current_mid_price = get_current_mid_price()
            if current_mid_price is None:
                logger.info("No order book data yet, sleeping...")
                await asyncio.sleep(2)
                continue

            if ENABLE_EMA_CHECK:
                if ema_fast is None or ema_slow is None:
                    logger.info("EMAs are not ready yet, waiting for more kline data...")
                    await asyncio.sleep(5)
                    continue
                if ema_fast < ema_slow:
                    logger.info(f"📉 EMA condition not met: Fast EMA ({ema_fast:.4f}) < Slow EMA ({ema_slow:.4f}). Holding off new buy trades.")
                    # If we have a position, allow closing it regardless of EMA
                    if current_position_size > 0:
                        logger.info(f"📊 Position exists ({current_position_size}), allowing sell orders to close position.")
                        # Don't continue - let the logic proceed to place sell orders
                    else:
                        # No position and bearish trend - hold off on new trades
                        logger.info("⏸️ Holding off on new buy trades due to bearish trend.")
                        await asyncio.sleep(15)
                        continue

            price_changed = (last_mid_price is None or abs(current_mid_price - last_mid_price) / last_mid_price > 0.001)

            order_price = calculate_order_price(current_mid_price, order_side)
            if order_price is None:
                await asyncio.sleep(3)
                continue

            if current_mid_price > 0:
                pct = ((order_price - current_mid_price) / current_mid_price) * 100.0
            else:
                pct = 0.0
            logger.info(f"📊 Mid: ${current_mid_price:.6f}, Target {order_side}: ${order_price:.6f} ({pct:+.4f}%), Price changed: {price_changed}")

            if current_order_id is not None and price_changed:
                await cancel_order(client, current_order_id)
            elif current_order_id is not None and not price_changed:
                logger.info(f"⏳ Order {current_order_id} still active - price unchanged")

            if current_order_id is None:
                order_id = int(time.time() * 1_000_000) % 1_000_000
                base_amt = 0
                if order_side == "buy":
                    base_amt = await calculate_dynamic_base_amount(current_mid_price)
                else:
                    if current_position_size > 0:
                        position_value_usd = current_position_size * current_mid_price
                        if position_value_usd >= 15.0:
                            base_amt = current_position_size
                        else:
                            logger.info(f"⏭️ Position value ${position_value_usd:.2f} is less than $15, skipping sell order for this cycle.")
                    else:
                        logger.info("⏭️ Sell side but no inventory to sell → skip.")

                if base_amt > 0:
                    last_order_base_amount = base_amt
                    ok = await place_order(client, order_side, order_price, order_id, base_amt)
                    if not ok:
                        await asyncio.sleep(5)
                        continue
                    last_mid_price = current_mid_price
                else:
                    if order_side == "buy":
                        logger.warning("⚠️ Calculated order size is zero, skip.")

            await asyncio.sleep(ORDER_TIMEOUT)

            if current_order_id is not None:
                logger.info(f"⏰ Order {current_order_id} timeout reached. Cancelling and assessing fills.")
                await cancel_order(client, current_order_id)

            position_value_usd = (current_position_size * current_mid_price) if current_mid_price else 0
            if order_side == "buy" and current_position_size > 0:
                if position_value_usd >= 15.0:
                    logger.info(f"✅ Position opened after buy cycle, value ${position_value_usd:.2f} is sufficient. Flipping to sell side. New inventory: {current_position_size}")
                    order_side = "sell"
                else:
                    logger.info(f"📊 Position opened after buy cycle, but value ${position_value_usd:.2f} is < $15. Remaining on buy side to accumulate more. Inventory: {current_position_size}")
            elif order_side == "sell" and abs(current_position_size) < 1e-9:
                logger.info(f"✅ Position closed after sell cycle. New inventory: 0")
                order_side = "buy"
                last_order_base_amount = 0
            elif order_side == "sell" and current_position_size > 0 and position_value_usd < 15.0:
                logger.info(f"🔄 Position value ${position_value_usd:.2f} is too small to sell. Flipping to buy side to accumulate more.")
                order_side = "buy"
            else:
                logger.info(f"⏸️ No fill or partial fill did not close position. Current inventory: {current_position_size}. Remaining on {order_side} side.")

            await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"Loop error: {e}", exc_info=True)
            if "websocket" in str(e).lower():
                global ws_connection_healthy
                ws_connection_healthy = False
            await asyncio.sleep(5)

async def main():
    global MARKET_ID, PRICE_TICK_SIZE, AMOUNT_TICK_SIZE
    global ws_client, ws_task, last_order_book_update
    global last_mid_price, current_order_id, current_position_size, order_side

    logger.info("🚀 === Market Maker Starting ===")

    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
    account_api = lighter.AccountApi(api_client)
    order_api = lighter.OrderApi(api_client)
    candlestick_api = CandlestickApi(api_client)

    details = await get_market_details(order_api, MARKET_SYMBOL)
    if not details:
        logger.error(f"Could not retrieve market details for {MARKET_SYMBOL}. Exiting.")
        return
    MARKET_ID, PRICE_TICK_SIZE, AMOUNT_TICK_SIZE = details
    logger.info(f"📋 Market {MARKET_SYMBOL}: id={MARKET_ID}, tick(price)={PRICE_TICK_SIZE}, tick(amount)={AMOUNT_TICK_SIZE}")

    load_ema_state()
    await fetch_initial_klines(candlestick_api)

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
    logger.info("✅ Client connected successfully")

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
    
    tasks = {
        "orderbook": asyncio.create_task(ws_client.run_async()),
        "user_stats": asyncio.create_task(subscribe_to_user_stats(ACCOUNT_INDEX)),
        "account_all": asyncio.create_task(subscribe_to_account_all(ACCOUNT_INDEX)),
        "trades": asyncio.create_task(subscribe_to_trades(MARKET_ID))
    }
    ws_task = tasks["orderbook"]


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

        if CLOSE_LONG_ON_STARTUP:
            if current_position_size > 0:
                mid = get_current_mid_price()
                if mid:
                    logger.info(f"🔄 Closing existing long position of size {current_position_size} at startup (flag ON)")
                    ok = await close_long_position(client, current_position_size, mid)
                    if ok:
                        order_side = "sell"
                        last_mid_price = mid
                        logger.info(f"✅ Reduce-only sell placed. Waiting for websocket to confirm position change.")
                        
                        close_confirmed = False
                        start_time = time.time()
                        while time.time() - start_time < 60:
                            if abs(current_position_size) < 1e-9:
                                logger.info("✅ Position successfully closed.")
                                close_confirmed = True
                                break
                            await asyncio.sleep(1)
                        
                        if not close_confirmed:
                            logger.error("❌ Timed out waiting for position to close. Exiting.")
                            return
                else:
                    logger.warning("⚠️ No fresh mid price yet; skip auto-close this boot.")
        elif current_position_size > 0:
            logger.info(f"🔍 Existing position of {current_position_size} detected. Evaluating...")
            mid_price = get_current_mid_price()
            if mid_price:
                position_value_usd = current_position_size * mid_price
                if position_value_usd < 15.0:
                    logger.info(f"📊 Position value ${position_value_usd:.2f} is < $15. Starting in buy mode.")
                    order_side = "buy"
                else:
                    logger.info(f"📊 Position value ${position_value_usd:.2f} is >= $15. Starting in sell mode.")
                    order_side = "sell"
            else:
                logger.warning("⚠️ Could not get mid price to evaluate existing position, defaulting to sell mode.")
                order_side = "sell"

        await market_making_loop(client, account_api, order_api)

    except asyncio.TimeoutError:
        logger.error("❌ Timeout waiting for initial data from websockets.")
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("🛑 === Shutdown signal received - Stopping... ===")
    finally:
        logger.info("🧹 === Market Maker Cleanup Starting ===")
        for task_name, task in tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info(f"Task '{task_name}' cancelled.")

        if current_order_id is not None:
            logger.info(f"🧹 Cancelling open order {current_order_id} before exit...")
            await cancel_order(client, current_order_id)

        await client.close()
        await api_client.close()
        logger.info("✅ Market maker stopped.")

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
            logger.info(f"🛑 Received exit signal {sig.name}, cancelling main task.")
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
            logger.info("✅ Main task cancelled. Cleanup is handled in main().")

    try:
        asyncio.run(main_with_signal_handling())
        logger.info("✅ Application has finished gracefully.")
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 Application exiting.")
