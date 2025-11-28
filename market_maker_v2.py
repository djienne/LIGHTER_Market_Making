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


def calculate_microprice_deviation(mid_price: float) -> float:
    """Calculates the microprice deviation from the mid-price."""
    if not latest_order_book:
        return 0.0

    bids = latest_order_book.get('bids', [])
    asks = latest_order_book.get('asks', [])

    if not bids or not asks:
        return 0.0

    try:
        bid_p = float(bids[0]['price'])
        bid_q = float(bids[0]['size'])
        ask_p = float(asks[0]['price'])
        ask_q = float(asks[0]['size'])

        denominator = bid_q + ask_q
        if denominator > 0:
            microprice = (ask_p * bid_q + bid_p * ask_q) / denominator
            return microprice - mid_price
        else:
            return 0.0
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"⚠️ Could not calculate microprice: {e}")
        return 0.0


def calculate_spreads_from_params(mid_price: float, inventory: float, micro_price_deviation: float, params: dict) -> Optional[Tuple[float, float]]:
    """Calculates final bid and ask prices using the full formula."""
    try:
        # Extract parameters
        gamma = params['gamma']
        tau = params['tau']
        beta_alpha = params['beta_alpha']
        sigma = params['latest_volatility']
        k_mid = (params['k_bid'] + params['k_ask']) / 2
        c_as_bid = params['c_AS_bid']
        c_as_ask = params['c_AS_ask']
        maker_fee_bps = params['maker_fee_bps']

        # --- Reservation Price ---
        reservation_price = mid_price + (beta_alpha * micro_price_deviation) - (inventory * gamma * (sigma**2) * tau)

        # --- Half Spread (phi) ---
        k_price_based = k_mid * 10000 / mid_price
        
        phi_inventory = 0
        if gamma > 0 and k_price_based > 0:
            phi_inventory = (1 / gamma) * math.log(1 + gamma / k_price_based)

        phi_adverse_sel = 0.5 * gamma * (sigma**2) * tau
        phi_adverse_cost = (c_as_bid + c_as_ask) / 2
        phi_fee = (maker_fee_bps / 10000) * mid_price
        
        half_spread = phi_inventory + phi_adverse_sel + phi_adverse_cost + phi_fee

        # --- Asymmetric Adjustments ---
        c_as_mid = (c_as_bid + c_as_ask) / 2
        delta_bid_as = c_as_bid - c_as_mid
        delta_ask_as = c_as_ask - c_as_mid

        # --- Final Quotes ---
        final_bid = reservation_price - half_spread - delta_bid_as
        final_ask = reservation_price + half_spread + delta_ask_as
        
        # Sanity check to prevent crossed markets
        if final_bid >= final_ask:
            logger.warning("⚠️ Calculated spread is negative or zero. Re-centering around mid-price.")
            final_bid = mid_price - half_spread
            final_ask = mid_price + half_spread

        # Enforce minimum spread as a safety guard
        min_spread_value = mid_price * (MINIMUM_SPREAD_PERCENT / 100.0)
        current_spread = final_ask - final_bid
        if current_spread < min_spread_value:
            logger.warning(f"⚠️ Calculated spread {current_spread:.6f} is below minimum {min_spread_value:.6f}. Adjusting.")
            spread_diff = min_spread_value - current_spread
            final_bid -= spread_diff / 2
            final_ask += spread_diff / 2

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
        logger.error(f"❌ An error occurred while fetching market details: {e}")
        return None

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
        logger.error(f"❌ Error in order book callback: {e}", exc_info=True)
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
            async with websockets.connect(WEBSOCKET_URL) as ws:
                logger.info(f"🔌 Connected to {WEBSOCKET_URL} for user stats")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"📡 Subscribed to user_stats for account {account_id}")
                
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
            logger.info(f"🔌 User stats WebSocket disconnected, reconnecting in 5 seconds...")
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
            async with websockets.connect(WEBSOCKET_URL) as ws:
                logger.info(f"🔌 Connected to {WEBSOCKET_URL} for account_all")
                await ws.send(json.dumps(subscription_msg))
                logger.info(f"📡 Subscribed to account_all for account {account_id}")
                
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
            logger.info(f"🔌 Account data WebSocket disconnected, reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred in account_all socket: {e}. Reconnecting in 5 seconds...")
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
        logger.warning(f"⚠️ No available capital from websocket, using static BASE_AMOUNT: {BASE_AMOUNT}")
        return BASE_AMOUNT * float(LEVERAGE)

    usable_capital = available_capital * (1.0 - SAFETY_MARGIN_PERCENT) * float(LEVERAGE)
    order_capital = usable_capital * CAPITAL_USAGE_PERCENT
    if current_mid_price and current_mid_price > 0:
        dynamic = order_capital / current_mid_price
        dynamic = max(dynamic, 0.001)
        logger.info(f"📏 Dynamic sizing: ${order_capital:.2f} / ${current_mid_price:.2f} = {dynamic:.6f} units")
        return dynamic
    else:
        logger.warning(f"⚠️ Invalid mid price, using static BASE_AMOUNT: {BASE_AMOUNT}")
        return BASE_AMOUNT * float(LEVERAGE)

def load_spread_parameters() -> bool:
    """
    Load and validate spread calculation parameters from the JSON file.
    """
    global avellaneda_params, last_avellaneda_update
    try:
        now = time.time()
        # Check if we need to refresh
        if avellaneda_params is not None and (now - last_avellaneda_update) < AVELLANEDA_REFRESH_INTERVAL:
            return True

        avellaneda_params = None # Reset

        # Find the params file
        candidates = [
            os.path.join(PARAMS_DIR, f'spread_parameters_{MARKET_SYMBOL}.json'),
            f'params/spread_parameters_{MARKET_SYMBOL}.json',
        ]
        data = None
        path_found = None
        for p in candidates:
            if os.path.exists(p):
                path_found = p
                break
        
        if not path_found:
            logger.warning(f"⚠️ Spread parameters file not found for {MARKET_SYMBOL}.")
            return False

        # Load and validate
        with open(path_found, 'r') as f:
            data = json.load(f)
        
        required_keys = [
            "gamma", "tau", "beta_alpha", "latest_volatility", 
            "k_bid", "k_ask", "c_AS_bid", "c_AS_ask", "maker_fee_bps"
        ]
        
        if not all(key in data for key in required_keys):
            logger.warning(f"⚠️ Spread parameters file {path_found} is missing one or more required keys.")
            return False

        avellaneda_params = data
        last_avellaneda_update = now
        logger.info(f"✅ Loaded and validated spread parameters from: {path_found}")
        return True

    except Exception as e:
        logger.error(f"❌ Unexpected error loading spread parameters: {e}", exc_info=True)
        avellaneda_params = None
        return False


def calculate_order_price(mid_price: float, side: str, inventory_param: float) -> Optional[float]:
    """Calculates the order price using dynamic params or falls back to a static spread."""
    # Try to use the full formula first
    params_loaded = load_spread_parameters()
    if params_loaded and avellaneda_params:
        micro_dev = calculate_microprice_deviation(mid_price)
        
        final_bid, final_ask = calculate_spreads_from_params(mid_price, inventory_param, micro_dev, avellaneda_params)
        
        if final_bid is not None and final_ask is not None:
            return final_bid if side == 'buy' else final_ask

    # Fallback logic if params fail or are not required
    if REQUIRE_PARAMS:
        logger.warning("⚠️ REQUIRE_PARAMS is true, but spread calculation failed. No orders will be placed.")
        return None

    logger.warning("Falling back to static spread calculation.")
    return mid_price * (1.0 - SPREAD) if side == "buy" else mid_price * (1.0 + SPREAD)

async def place_order(client, side, price, order_id, base_amount):
    is_ask = (side == "sell")

    # For a pure market making strategy, orders are never reduce-only.
    # Inventory is managed by skewing the spread.
    reduce_only_flag = False

    base_amount_scaled = int(base_amount / AMOUNT_TICK_SIZE)
    price_scaled = int(price / PRICE_TICK_SIZE)
    logger.info(f"📤 Placing {side} order: {base_amount:.6f} units at ${price:.6f} (ID: {order_id}), reduce_only={reduce_only_flag}")
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
            logger.error(f"❌ Error placing {side} order: {trim_exception(err)}")
            return False
        logger.info(f"✅ Successfully placed {side} order: tx={getattr(tx_hash,'tx_hash',tx_hash)}")
        return True
    except Exception as e:
        logger.error(f"❌ Exception in place_order: {e}", exc_info=True)
        return False

async def cancel_all_orders(client):
    """
    Cancels all open orders for the account.
    """
    logger.info(f"🛑 Cancelling all orders...")
    try:
        tx, tx_hash, err = await client.cancel_all_orders(
            time_in_force=client.CANCEL_ALL_TIF_IMMEDIATE,
            time=0
        )
        if err is not None:
            logger.error(f"❌ Error cancelling all orders: {trim_exception(err)}")
            return False
        logger.info(f"✅ Successfully cancelled all orders: tx={getattr(tx_hash,'tx_hash',tx_hash) if tx_hash else 'OK'}")
        return True
    except Exception as e:
        logger.error(f"❌ Exception in cancel_all_orders: {e}", exc_info=True)
        return False

async def check_websocket_health():
    global ws_connection_healthy, last_order_book_update, ws_task
    if (time.time() - last_order_book_update) > 30:
        logger.info(f"🔌 WebSocket inactive for {time.time() - last_order_book_update:.1f}s - reconnecting...")
        ws_connection_healthy = False
        return False
    if ws_task and ws_task.done():
        try:
            ws_task.result()
        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError) as e:
            # WebSocket disconnections are normal during network issues
            reason = getattr(e, 'reason', 'connection closed')
            logger.debug(f"WebSocket disconnected (reason: {reason}) - will reconnect")
        except Exception as e:
            # Log other unexpected exceptions with full traceback
            logger.error(f"❌ WS task exception: {e}", exc_info=True)
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
        logger.info("⏳ Waiting for websocket reconnection...")
        await asyncio.wait_for(order_book_received.wait(), timeout=15.0)
        logger.info("✅ Websocket reconnected successfully")
        return True
    except asyncio.TimeoutError:
        logger.error("❌ Websocket reconnection failed - timeout.")
        return False

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
                inventory_param = current_position_size / base_amount
            else:
                logger.warning("⚠️ Base amount is zero or invalid; using inventory of 0.")

            buy_price = calculate_order_price(current_mid_price, 'buy', inventory_param)
            sell_price = calculate_order_price(current_mid_price, 'sell', inventory_param)

            # 4. Place new buy and sell orders
            if buy_price and sell_price and base_amount > 0:
                bid_spread_pct = (current_mid_price - buy_price) / current_mid_price * 100 if current_mid_price > 0 else 0
                ask_spread_pct = (sell_price - current_mid_price) / current_mid_price * 100 if current_mid_price > 0 else 0
                
                logger.info(
                    f"QUOTING | "
                    f"Inventory: {inventory_param:+.2f} | "
                    f"Mid: ${current_mid_price:.4f} | "
                    f"Bid: ${buy_price:.4f} (-{bid_spread_pct:.4f}%) | "
                    f"Ask: ${sell_price:.4f} (+{ask_spread_pct:.4f}%)"
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
    global ws_client, ws_task, last_order_book_update
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
    ws_client = RobustWsClient(order_book_ids=[MARKET_ID], account_ids=[], on_order_book_update=on_order_book_update)
    ws_task = asyncio.create_task(ws_client.run_async())

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
