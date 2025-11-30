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
from decimal import Decimal, getcontext
import signal
from collections import deque
import argparse

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
CAPITAL_USAGE_PERCENT = 0.12
SAFETY_MARGIN_PERCENT = 0.01
ORDER_TIMEOUT = 3           # seconds
MINIMUM_SPREAD_PERCENT = 0.005 # Safety net for calculated spreads

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
available_capital = None
portfolio_value = None
last_capital_check = 0
current_position_size = 0

avellaneda_params = None
last_avellaneda_update = 0

account_positions = {}
recent_trades = deque(maxlen=20)

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


def calculate_microprice_deviation(mid_price: float) -> Decimal:
    """Calculates the microprice deviation from the mid-price using Decimal."""
    if not latest_order_book:
        return Decimal("0.0")

    bids = latest_order_book.get('bids', [])
    asks = latest_order_book.get('asks', [])

    if not bids or not asks:
        return Decimal("0.0")

    try:
        bid_p = Decimal(str(bids[0]['price']))
        bid_q = Decimal(str(bids[0]['size']))
        ask_p = Decimal(str(asks[0]['price']))
        ask_q = Decimal(str(asks[0]['size']))

        denominator = bid_q + ask_q
        if denominator > 0:
            microprice = (ask_p * bid_q + bid_p * ask_q) / denominator
            # mid_price comes in as float, convert to Decimal
            return microprice - Decimal(str(mid_price))
        else:
            return Decimal("0.0")
    except (ValueError, TypeError, KeyError, Exception) as e:
        logger.warning(f"⚠️ Could not calculate microprice: {e}")
        return Decimal("0.0")


def calculate_spreads_from_params(mid_price: float, inventory: float, micro_price_deviation: Decimal, params: dict) -> Optional[Tuple[Decimal, Decimal]]:
    """Calculates final bid and ask prices using the full formula with Decimal precision."""
    try:
        # Convert inputs to Decimal
        mid_price_d = Decimal(str(mid_price))
        inventory_d = Decimal(str(inventory))
        
        # Extract parameters and convert to Decimal
        gamma = Decimal(str(params['gamma']))
        tau = Decimal(str(params['tau']))
        beta_alpha = Decimal(str(params['beta_alpha']))
        sigma = Decimal(str(params['latest_volatility']))
        
        # k_mid calculation
        k_bid = Decimal(str(params['k_bid']))
        k_ask = Decimal(str(params['k_ask']))
        k_mid = (k_bid + k_ask) / Decimal("2")
        
        c_as_bid = Decimal(str(params['c_AS_bid']))
        c_as_ask = Decimal(str(params['c_AS_ask']))
        maker_fee_bps = Decimal(str(params['maker_fee_bps']))

        # --- Reservation Price ---
        # r = s + (beta * micro_dev) - (q * gamma * sigma^2 * T)
        reservation_price = mid_price_d + (beta_alpha * micro_price_deviation) - (inventory_d * gamma * (sigma**2) * tau)

        # --- Half Spread (phi) ---
        # k_price_based = k_mid * 10000 / mid_price (Assuming typical A-S scaling, but let's stick to the code's logic)
        # Original: k_price_based = k_mid * 10000 / mid_price
        if mid_price_d > 0:
            k_price_based = k_mid * Decimal("10000") / mid_price_d
        else:
            k_price_based = Decimal("0")
        
        phi_inventory = Decimal("0")
        if gamma > 0 and k_price_based > 0:
            # (1 / gamma) * ln(1 + gamma / k)
            # Using Decimal.ln()
            arg = Decimal("1") + (gamma / k_price_based)
            phi_inventory = (Decimal("1") / gamma) * arg.ln()

        phi_adverse_sel = Decimal("0.5") * gamma * (sigma**2) * tau
        phi_adverse_cost = (c_as_bid + c_as_ask) / Decimal("2")
        phi_fee = (maker_fee_bps / Decimal("10000")) * mid_price_d
        
        half_spread = phi_inventory + phi_adverse_sel + phi_adverse_cost + phi_fee

        # --- Asymmetric Adjustments ---
        c_as_mid = (c_as_bid + c_as_ask) / Decimal("2")
        delta_bid_as = c_as_bid - c_as_mid
        delta_ask_as = c_as_ask - c_as_mid

        # --- Final Quotes ---
        final_bid = reservation_price - half_spread - delta_bid_as
        final_ask = reservation_price + half_spread + delta_ask_as
        
        # Sanity check to prevent crossed markets
        if final_bid >= final_ask:
            logger.warning("⚠️ Calculated spread is negative or zero. Re-centering around mid-price.")
            final_bid = mid_price_d - half_spread
            final_ask = mid_price_d + half_spread

        # Enforce minimum spread as a safety guard
        # MINIMUM_SPREAD_PERCENT is float, convert
        min_spread_pct_d = Decimal(str(MINIMUM_SPREAD_PERCENT))
        min_spread_value = mid_price_d * (min_spread_pct_d / Decimal("100.0"))
        
        current_spread = final_ask - final_bid
        if current_spread < min_spread_value:
            # logger.warning(f"⚠️ Calculated spread {current_spread:.6f} is below minimum {min_spread_value:.6f}. Adjusting.")
            spread_diff = min_spread_value - current_spread
            final_bid -= spread_diff / Decimal("2")
            final_ask += spread_diff / Decimal("2")

        return final_bid, final_ask

    except Exception as e:
        logger.error(f"❌ Error during spread calculation: {e}", exc_info=True)
        return None, None


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


# Local Order Book State
local_order_book = {'bids': {}, 'asks': {}, 'initialized': False}

def on_order_book_update(market_id, payload):
    global latest_order_book, ws_connection_healthy, last_order_book_update, current_mid_price_cached, local_order_book
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
                local_order_book['bids'] = {}
                local_order_book['asks'] = {}
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

            # Reconstruct sorted lists for compatibility and mid-price calc
            sorted_bids = sorted(local_order_book['bids'].items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(local_order_book['asks'].items(), key=lambda x: x[0])

            # Update latest_order_book to match the expected format: {'bids': [{'price':.., 'size':..}], ...}
            latest_order_book = {
                'bids': [{'price': str(p), 'size': str(s)} for p, s in sorted_bids],
                'asks': [{'price': str(p), 'size': str(s)} for p, s in sorted_asks]
            }

            if sorted_bids and sorted_asks:
                best_bid = sorted_bids[0][0]
                best_ask = sorted_asks[0][0]
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
                # Log significant trades or update internal metrics if needed
                # For now, just log to show it's working
                price = trade.get('price')
                size = trade.get('size')
                side = "SELL" if trade.get('is_maker_ask') else "BUY" # Simplified inference
                logger.info(f"📈 Market Trade: {side} {size} @ {price}")
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
            new_available_capital = float(stats.get('available_balance', 0))
            new_portfolio_value = float(stats.get('portfolio_value', 0))

            if new_available_capital > 0 and new_portfolio_value > 0:
                available_capital = new_available_capital
                portfolio_value = new_portfolio_value
                logger.info(f"💰 Received user stats for account {account_id}: Available Capital=${available_capital}, Portfolio Value=${portfolio_value}")
                account_state_received.set()
            else:
                logger.warning(f"⚠️ Received user stats with invalid values: available_balance={stats.get('available_balance')}, portfolio_value={stats.get('portfolio_value')}")
    except (ValueError, TypeError) as e:
        logger.error(f"❌ Error processing user stats update: {e}", exc_info=True)

async def subscribe_to_user_stats(account_id):
    """Connects to the websocket, subscribes to user_stats, and updates global state."""
    subscription_msg = {
        "type": "subscribe",
        "channel": f"user_stats/{account_id}"
    }
    
    while True:
        try:
            # ping_interval=20: automatically sends a ping every 20s
            # ping_timeout=20: waits 20s for a pong, else closes connection
            async with websockets.connect(WEBSOCKET_URL, ping_interval=20, ping_timeout=20) as ws:
                logger.info(f"🔌 Connected to {WEBSOCKET_URL} for user stats")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"📡 Subscribed to user_stats for account {account_id}")
                
                while True:
                    try:
                        # Watchdog: wait for message with timeout
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
                        break # Break inner loop to trigger reconnection
                        
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
            # Update positions
            new_positions = data.get("positions", {})
            account_positions = new_positions
            
            market_position = new_positions.get(str(MARKET_ID))
            new_size = 0.0
            if market_position:
                size = float(market_position.get('position', 0))
                # The Lighter API uses a 'sign' field for positions: -1 for short, 1 for long.
                sign = int(market_position.get('sign', 1))
                if sign == -1:
                    new_size = -size
                else:
                    new_size = size
            else:
                # Explicitly set to zero if no position for this market exists in the update
                new_size = 0.0

            if new_size != current_position_size:
                logger.info(f"📊 WebSocket position update for market {MARKET_ID}: {current_position_size} -> {new_size}")
                current_position_size = new_size
            
            # Update trades
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
        logger.error(f"❌ Error processing account_all update: {e}", exc_info=True)

async def subscribe_to_account_all(account_id):
    """Connects to the websocket, subscribes to account_all, and updates global state."""
    subscription_msg = {
        "type": "subscribe",
        "channel": f"account_all/{account_id}"
    }
    
    while True:
        try:
            # ping_interval=20: automatically sends a ping every 20s
            # ping_timeout=20: waits 20s for a pong, else closes connection
            async with websockets.connect(WEBSOCKET_URL, ping_interval=20, ping_timeout=20) as ws:
                logger.info(f"🔌 Connected to {WEBSOCKET_URL} for account_all")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"📡 Subscribed to account_all for account {account_id}")
                
                while True:
                    try:
                        # Watchdog: wait for message with timeout
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
                        break # Break inner loop to trigger reconnection
                        
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
    local_order_book['bids'] = {}
    local_order_book['asks'] = {}
    
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

def calculate_order_price(mid_price, side, inventory_factor):
    """
    Calculates the optimal bid or ask price using Avellaneda-Stoikov formula.
    """
    global avellaneda_params
    
    if not avellaneda_params:
        # Fallback: simple fixed percentage spread
        spread = mid_price * 0.001 # 0.1% spread
        if side == 'buy':
            return mid_price - (spread / 2)
        else:
            return mid_price + (spread / 2)

    try:
        # Extract parameters
        gamma = float(avellaneda_params['optimal_parameters']['gamma'])
        sigma = float(avellaneda_params['market_data']['sigma'])
        
        # Use separate k for bid and ask if available, otherwise fallback to 'k' or defaults
        market_data = avellaneda_params.get('market_data', {})
        k_bid = float(market_data.get('k_bid', market_data.get('k', 0.5)))
        k_ask = float(market_data.get('k_ask', market_data.get('k', 0.5)))
        
        time_horizon = float(avellaneda_params['current_state']['time_remaining']) # in days
        
        # 1. Calculate Reservation Price (r)
        # r = s - q * gamma * sigma^2 * (T - t)
        # inventory_factor is q (current inventory)
        reservation_price = mid_price - (inventory_factor * gamma * (sigma**2) * time_horizon)
        
        # 2. Calculate Spread (delta)
        # delta = (2 / gamma) * ln(1 + gamma / k)
        # We use k_bid for bid spread and k_ask for ask spread
        
        if side == 'buy':
            # Bid Spread
            val_inside_log = 1.0 + gamma / (mid_price * k_bid + 1e-9)
            spread = (2.0 * mid_price / gamma) * math.log(val_inside_log)
            final_price = reservation_price - (spread / 2.0)
        else:
            # Ask Spread
            val_inside_log = 1.0 + gamma / (mid_price * k_ask + 1e-9)
            spread = (2.0 * mid_price / gamma) * math.log(val_inside_log)
            final_price = reservation_price + (spread / 2.0)

        # Ensure price is valid tick size
        if PRICE_TICK_SIZE:
            final_price = round(final_price / float(PRICE_TICK_SIZE)) * float(PRICE_TICK_SIZE)
        
        return final_price

    except Exception as e:
        logger.error(f"❌ Error calculating order price: {e}", exc_info=True)
        return None

async def market_making_loop(client, account_api, order_api):
    logger.info("🚀 Starting 2-sided market making loop...")

    while True:
        try:
            # 1. Always start by cancelling existing orders to ensure a clean slate for the cycle
            await cancel_all_orders(client)

            # 2. Wait for fresh data and check websocket health
            if not await check_websocket_health():
                logger.warning("⚠ Websocket connection unhealthy, attempting restart...")
                if not await restart_websocket():
                    await asyncio.sleep(10)
                    continue

            try:
                # Wait for a fresh order book update before proceeding
                await asyncio.wait_for(order_book_received.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("⏳ Timed out waiting for fresh order book data, retrying...")
                continue

            current_mid_price = get_current_mid_price()
            if current_mid_price is None:
                logger.info("⏳ No mid-price available, sleeping...")
                await asyncio.sleep(2)
                continue

            # 3. Calculate order parameters
            base_amount = await calculate_dynamic_base_amount(current_mid_price)
            
            inventory_param = 0.0
            if base_amount and base_amount > 0:
                # base_amount is Decimal, current_position_size is float. 
                # Convert base_amount to float for this ratio calculation.
                inventory_param = current_position_size / float(base_amount)
            else:
                logger.warning("⚠️ Base amount is zero or invalid; using inventory of 0.")

            buy_price = calculate_order_price(current_mid_price, 'buy', inventory_param)
            sell_price = calculate_order_price(current_mid_price, 'sell', inventory_param)

            # 4. Place new buy and sell orders
            if buy_price and sell_price and base_amount > 0:
                # Convert Decimals to float for logging calculation
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

                # Place buy order
                buy_order_id = int(time.time() * 1_000_000) % 1_000_000
                await place_order(client, 'buy', buy_price, buy_order_id, base_amount)

                # Place sell order
                sell_order_id = buy_order_id + 1
                await place_order(client, 'sell', sell_price, sell_order_id, base_amount)
            else:
                logger.warning("⚠️ Could not calculate valid orders this cycle, skipping placement.")

            # 5. Wait for the defined interval before the next cycle
            logger.info(f"Cycle complete. Waiting for {ORDER_TIMEOUT} seconds...")
            await asyncio.sleep(ORDER_TIMEOUT)

        except Exception as e:
            logger.error(f"❌ Unhandled error in market_making_loop: {e}", exc_info=True)
            await asyncio.sleep(5) # Wait before retrying on error

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
    last_order_book_update = time.time()
    # Start WebSocket Tasks
    # We now use a single task for market data (orderbook + trades)
    ws_task = asyncio.create_task(subscribe_to_market_data(MARKET_ID))

    user_stats_task = asyncio.create_task(subscribe_to_user_stats(ACCOUNT_INDEX))
    account_all_task = asyncio.create_task(subscribe_to_account_all(ACCOUNT_INDEX))

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

        # Adjust leverage at startup
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

        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)


        # As a final safety measure, try to cancel all orders one last time.
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
                # add_signal_handler not available on some platforms (e.g., Windows event loop)
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
