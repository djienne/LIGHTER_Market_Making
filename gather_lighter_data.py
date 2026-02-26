import asyncio
import os
import time
import logging
import traceback
import re
from datetime import datetime
from collections import defaultdict

import json
import pandas as pd
from orderbook import apply_orderbook_update
from ws_manager import ws_subscribe

# It is recommended to install the lighter sdk using pip
# pip install git+https://github.com/elliottech/lighter-python.git
import lighter

# Setup comprehensive logging
def setup_logging():
    """Setup detailed logging to both file and console."""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)

    # Configure logging
    log_format = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'

    # File handler for detailed logs
    file_handler = logging.FileHandler('logs/debug.log', mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format))

    # Console handler for summaries and essential messages (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Silence noisy third-party loggers completely from console
    logging.getLogger('websockets').setLevel(logging.ERROR)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('parso').setLevel(logging.ERROR)
    logging.getLogger('fsspec').setLevel(logging.ERROR)

    # Configure main logger for summaries
    main_logger = logging.getLogger(__name__)
    # Remove existing handlers that might add console output
    main_logger.handlers.clear()
    main_logger.addHandler(file_handler)
    main_logger.propagate = False  # Don't propagate to root logger

    # Create a separate console logger for summaries only
    summary_logger = logging.getLogger('summary')
    summary_logger.handlers.clear()
    summary_logger.addHandler(console_handler)
    summary_logger.addHandler(file_handler)
    summary_logger.setLevel(logging.INFO)
    summary_logger.propagate = False

    # Also log to a separate performance log
    perf_handler = logging.FileHandler('logs/performance.log', mode='w', encoding='utf-8')
    perf_handler.setLevel(logging.INFO)
    perf_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

    perf_logger = logging.getLogger('performance')
    perf_logger.addHandler(perf_handler)
    perf_logger.setLevel(logging.INFO)

    return main_logger, summary_logger

logger, summary_logger = setup_logging()



# --- Configuration ---
CONFIG_FILE = 'config.json'

def load_config():
    """Loads configuration from config.json."""
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"Configuration file '{CONFIG_FILE}' not found. Using default symbols.")
        return ['ETH', 'BTC', 'SOL', 'PAXG'] # Default if config file is missing

    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            tickers = config.get('CRYPTO_TICKERS')
            if not tickers or not isinstance(tickers, list):
                logger.error(f"'CRYPTO_TICKERS' not found or invalid in '{CONFIG_FILE}'. Using default symbols.")
                return ['ETH', 'BTC', 'SOL', 'PAXG'] # Default if key is missing or invalid
            logger.info(f"Loaded CRYPTO_TICKERS from {CONFIG_FILE}: {tickers}")
            return tickers
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from '{CONFIG_FILE}': {e}. Using default symbols.")
        return ['ETH', 'BTC', 'SOL', 'PAXG'] # Default on JSON decode error
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading '{CONFIG_FILE}': {e}. Using default symbols.")
        return ['ETH', 'BTC', 'SOL', 'PAXG'] # Default on any other error

CRYPTO_TICKERS = load_config() # Symbols to track
DATA_FOLDER = 'lighter_data'             # Directory for Parquet output
BUFFER_SECONDS = 5                       # Interval to write buffered data to disk
PARQUET_PART_WIDTH = 6

def _get_max_parquet_bytes():
    raw_value = os.getenv("PARQUET_MAX_MB", "5")
    try:
        max_mb = float(raw_value)
        if max_mb <= 0:
            raise ValueError
    except ValueError:
        logger.warning(f"Invalid PARQUET_MAX_MB={raw_value}; using 5MB")
        max_mb = 5.0
    return int(max_mb * 1024 * 1024)

MAX_PARQUET_BYTES = _get_max_parquet_bytes()
parquet_part_indices = {"prices": {}, "trades": {}}


# --- Global State ---
# In-memory buffers to store data points before writing them to files in batches.
price_buffers = defaultdict(list)
trade_buffers = defaultdict(list)
last_offsets = {} # Track the last offset per symbol to detect gaps

# Local Order Book State: {symbol: {'bids': {price: size}, 'asks': {price: size}, 'initialized': False}}
local_order_books = defaultdict(lambda: {'bids': {}, 'asks': {}, 'initialized': False})

# Global mapping from the API's numerical market_id to the human-readable symbol (e.g., 1 -> 'ETH').
market_info = {}

# Dictionary for tracking real-time application statistics.
stats = {
    'orderbook_updates': 0,
    'trade_fetches': 0,
    'parquet_writes': 0,
    'errors': 0,
    'start_time': None,
    'last_orderbook_time': {},
    'last_trade_time': {},
    'buffer_flushes': 0,
    'gaps_detected': 0,
    'ws_trades': 0
}

# Ensure the data directory exists on startup.
logger.info(f"Creating data directory: {DATA_FOLDER}")
os.makedirs(DATA_FOLDER, exist_ok=True)
logger.info(f"Data directory created successfully")

def _parse_part_index(filename: str) -> int | None:
    match = re.search(r"_part(\d+)\.parquet$", filename)
    if not match:
        return None
    return int(match.group(1))

def _list_parquet_parts(kind: str, symbol: str) -> list[tuple[int, str]]:
    prefix = f"{kind}_{symbol}_part"
    parts = []
    for name in os.listdir(DATA_FOLDER):
        if not name.startswith(prefix) or not name.endswith(".parquet"):
            continue
        idx = _parse_part_index(name)
        if idx is None:
            continue
        parts.append((idx, os.path.join(DATA_FOLDER, name)))
    parts.sort(key=lambda item: item[0])
    return parts

def _parquet_part_path(kind: str, symbol: str, index: int) -> str:
    return os.path.join(
        DATA_FOLDER,
        f"{kind}_{symbol}_part{index:0{PARQUET_PART_WIDTH}d}.parquet"
    )

def _get_or_init_part_index(kind: str, symbol: str) -> int:
    current = parquet_part_indices[kind].get(symbol)
    if current is not None:
        return current
    parts = _list_parquet_parts(kind, symbol)
    current = parts[-1][0] if parts else 0
    parquet_part_indices[kind][symbol] = current
    return current

def _get_latest_parquet_file(kind: str, symbol: str) -> str | None:
    parts = _list_parquet_parts(kind, symbol)
    if parts:
        return parts[-1][1]
    legacy_path = os.path.join(DATA_FOLDER, f"{kind}_{symbol}.parquet")
    if os.path.exists(legacy_path):
        return legacy_path
    return None

def _get_parquet_write_path(kind: str, symbol: str) -> str:
    index = _get_or_init_part_index(kind, symbol)
    file_path = _parquet_part_path(kind, symbol, index)
    if os.path.exists(file_path):
        try:
            size_bytes = os.path.getsize(file_path)
        except OSError:
            size_bytes = 0
        if size_bytes >= MAX_PARQUET_BYTES:
            index += 1
            parquet_part_indices[kind][symbol] = index
            file_path = _parquet_part_path(kind, symbol, index)
            logger.info(
                f"Rotating {kind} parquet for {symbol}: "
                f"{size_bytes / (1024 * 1024):.1f}MB -> {os.path.basename(file_path)}"
            )
    return file_path

def _write_parquet_dataframe(df: pd.DataFrame, file_path: str, label: str, symbol: str) -> None:
    if os.path.exists(file_path):
        df.to_parquet(file_path, engine='fastparquet', append=True, index=False, compression='zstd')
    else:
        df.to_parquet(file_path, engine='fastparquet', index=False, compression='zstd')
    logger.info(f"Saved {len(df)} {label} for {symbol}")
    stats['parquet_writes'] += len(df)


async def print_summary():
    """Print periodic summary statistics."""
    while True:
        await asyncio.sleep(20)  # Print summary every 20 seconds

        if stats['start_time']:
            uptime = time.time() - stats['start_time']

            # Calculate data rates
            orderbook_rate = stats['orderbook_updates'] / uptime if uptime > 0 else 0
            trade_rate = (stats['trade_fetches'] + stats['ws_trades']) / uptime if uptime > 0 else 0

            # Format last data times and check for stale data
            last_data_info = []
            stale_warnings = []
            for symbol in CRYPTO_TICKERS:
                if symbol in stats['last_orderbook_time']:
                    time_since = time.time() - stats['last_orderbook_time'][symbol]
                    last_data_info.append(f"{symbol}_orderbook: {time_since:.1f}s ago")
                    # Warn if orderbook data is stale (no updates for 60+ seconds)
                    if time_since > 60:
                        stale_warnings.append(f"⚠️ {symbol} orderbook stale ({time_since:.0f}s)")
                if symbol in stats['last_trade_time']:
                    time_since = time.time() - stats['last_trade_time'][symbol]
                    last_data_info.append(f"{symbol}_trades: {time_since:.1f}s ago")

            summary_msg = (f"SUMMARY - Uptime: {uptime:.0f}s | "
                          f"OrderBook: {stats['orderbook_updates']} ({orderbook_rate:.1f}/s) | "
                          f"Trades: {stats['trade_fetches'] + stats['ws_trades']} ({trade_rate:.1f}/s) | "
                          f"Flushes: {stats['buffer_flushes']} | "
                          f"Gaps: {stats['gaps_detected']} | "
                          f"Errors: {stats['errors']}")

            if last_data_info:
                summary_msg += f" | Last: {', '.join(last_data_info)}"

            summary_logger.info(summary_msg)

            # Log stale data warnings separately
            for warning in stale_warnings:
                summary_logger.warning(warning)


async def get_market_id(order_api: lighter.OrderApi, symbol: str) -> int | None:
    """Fetches the numerical market ID for a given symbol (e.g., 'ETH')."""
    logger.debug(f"Getting market ID for symbol: {symbol}")
    try:
        # The API provides a list of all markets; we must iterate to find the one we want.
        order_books_response = await order_api.order_books()
        if hasattr(order_books_response, 'order_books'):
            for market in order_books_response.order_books:
                if hasattr(market, 'symbol') and market.symbol == symbol:
                    logger.info(f"Found market ID {market.market_id} for symbol {symbol}")
                    return market.market_id
        logger.warning(f"No market found for symbol: {symbol}")
        return None
    except Exception as e:
        logger.error(f"Error fetching market list for {symbol}: {e}", exc_info=True)
        stats['errors'] += 1
        return None


def get_last_recorded_trade_ids():
    """Reads existing Parquet files to find the last recorded trade ID for each symbol."""
    ids = {}
    logger.info("Checking for existing trade data...")
    for symbol in CRYPTO_TICKERS:
        file_path = _get_latest_parquet_file("trades", symbol)
        if file_path:
            try:
                # Read only trade_id column to minimize I/O
                df = pd.read_parquet(file_path, engine='fastparquet', columns=['trade_id'])
                if not df.empty and 'trade_id' in df.columns:
                    max_id = df['trade_id'].max()
                    ids[symbol] = int(max_id)
                    logger.info(f"Resuming {symbol} from trade_id {max_id}")
            except Exception as e:
                logger.error(f"Error reading last trade ID for {symbol}: {e}")
    return ids


async def fetch_recent_trades_periodically(order_api: lighter.OrderApi, last_trade_ids=None):
    """Periodically polls the API for recent trades (fallback/redundancy)."""
    logger.info("Starting periodic trade fetching (backup)...")
    if last_trade_ids is None:
        last_trade_ids = {}  # Track the last seen trade ID per symbol to avoid duplicates.
    else:
        logger.info(f"Initialized with last trade IDs: {last_trade_ids}")

    while True:
        for market_id, symbol in market_info.items():
            try:
                trades_response = await order_api.recent_trades(market_id=market_id, limit=50)
                if hasattr(trades_response, 'trades') and trades_response.trades:
                    new_trades_count = 0
                    for trade in trades_response.trades:
                        # Skip if we've already processed this trade
                        if symbol in last_trade_ids and trade.trade_id <= last_trade_ids[symbol]:
                            continue

                        trade_data = {
                            'timestamp': datetime.fromtimestamp(int(trade.timestamp) / 1000).isoformat(),
                            'price': float(trade.price),
                            'size': float(trade.size),
                            'side': 'buy' if hasattr(trade, 'is_maker_ask') and trade.is_maker_ask else 'sell',
                            'trade_id': int(trade.trade_id),
                            'usd_amount': float(trade.usd_amount) if hasattr(trade, 'usd_amount') else None,
                        }
                        trade_buffers[symbol].append(trade_data)
                        new_trades_count += 1
                        last_trade_ids[symbol] = max(last_trade_ids.get(symbol, 0), trade.trade_id)

                    if new_trades_count > 0:
                        logger.info(f"REST: Added {new_trades_count} trades for {symbol}")
                        stats['trade_fetches'] += new_trades_count
                        stats['last_trade_time'][symbol] = time.time()
            except Exception as e:
                logger.error(f"Error fetching trades for {symbol}: {e}")
                stats['errors'] += 1
        # Poll less frequently since we have WS
        await asyncio.sleep(60)


async def write_buffers_to_parquet():
    """Periodically writes the content of in-memory buffers to their respective Parquet files."""
    logger.info(f"Starting Parquet writer with {BUFFER_SECONDS}s intervals...")
    while True:
        await asyncio.sleep(BUFFER_SECONDS)
        # logger.debug("Starting Parquet write cycle...")

        try:
            if any(price_buffers.values()) or any(trade_buffers.values()):
                stats['buffer_flushes'] += 1
                summary_logger.info(f"FLUSH #{stats['buffer_flushes']} - Writing buffers to Parquet...")

            # --- Write Price Data ---
            for symbol, data_points in list(price_buffers.items()):
                if not data_points: continue

                price_data = price_buffers[symbol].copy()
                price_buffers[symbol].clear()
                if not price_data: continue
                
                # Deduplication logic (simple consecutive)
                deduplicated_data = []
                if price_data:
                    deduplicated_data.append(price_data[0])
                    for i in range(1, len(price_data)):
                        curr = price_data[i]
                        prev = price_data[i-1]
                        is_duplicate = True
                        for k in curr:
                            if k == 'timestamp': continue
                            if curr[k] != prev.get(k):
                                is_duplicate = False
                                break
                        if not is_duplicate:
                            deduplicated_data.append(curr)

                if not deduplicated_data: continue

                prices_file = _get_parquet_write_path("prices", symbol)
                try:
                    df = pd.DataFrame(deduplicated_data)
                    # Convert float columns explicitly to avoid object types
                    for col in df.columns:
                        if 'price' in col or 'size' in col:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            
                    df.sort_values(by='timestamp', inplace=True)
                    _write_parquet_dataframe(df, prices_file, "price points", symbol)
                except Exception as e:
                    logger.error(f"Error writing price data for {symbol}: {e}", exc_info=True)
                    stats['errors'] += 1

            # --- Write Trade Data ---
            for symbol, data_points in list(trade_buffers.items()):
                if not data_points: continue

                trade_data = trade_buffers[symbol].copy()
                trade_buffers[symbol].clear()
                if not trade_data: continue

                trades_file = _get_parquet_write_path("trades", symbol)
                try:
                    df = pd.DataFrame(trade_data)
                    df.drop_duplicates(subset=['trade_id'], keep='last', inplace=True)
                    df.sort_values(by='timestamp', inplace=True)
                    
                    # Convert types
                    if 'price' in df.columns: df['price'] = pd.to_numeric(df['price'])
                    if 'size' in df.columns: df['size'] = pd.to_numeric(df['size'])

                    _write_parquet_dataframe(df, trades_file, "trades", symbol)
                except Exception as e:
                    logger.error(f"Error writing trade data for {symbol}: {e}", exc_info=True)
                    stats['errors'] += 1

            perf_logger = logging.getLogger('performance')
            uptime = time.time() - stats['start_time'] if stats['start_time'] else 0
            perf_logger.info(f"Stats - Uptime: {uptime:.1f}s, OrderBook: {stats['orderbook_updates']}, "
                           f"Trades: {stats['ws_trades']}, Parquet: {stats['parquet_writes']}, Errors: {stats['errors']}")
        except Exception as e:
            logger.error(f"Error in Parquet write cycle: {e}", exc_info=True)
            stats['errors'] += 1


def handle_orderbook_message(market_id, payload):
    """Processes an orderbook message (snapshot or delta) and updates local state."""
    symbol = market_info.get(int(market_id))
    if not symbol: return

    lb = local_order_books[symbol]

    bids_in = payload.get('bids', [])
    asks_in = payload.get('asks', [])
    offset = payload.get('offset', 0)

    is_snapshot = apply_orderbook_update(
        lb['bids'], lb['asks'], lb['initialized'], bids_in, asks_in,
    )
    if is_snapshot:
        lb['initialized'] = True
        logger.info(f"Initializing/snapshot local orderbook for {symbol} (Offset: {offset})")

    # Update offset
    if symbol in last_offsets:
        if offset > last_offsets[symbol] + 1:
             # Gap detected
             stats['gaps_detected'] += 1
             # If gap is huge, maybe we should re-fetch snapshot?
             # For now just log.
    last_offsets[symbol] = offset

    # --- Construct Top 10 for Buffer ---
    # Sort Bids (Desc) and Asks (Asc)
    sorted_bids = sorted(lb['bids'].items(), key=lambda x: x[0], reverse=True)
    sorted_asks = sorted(lb['asks'].items(), key=lambda x: x[0])

    price_data = {
        'timestamp': datetime.now().isoformat(),
        'offset': int(offset),
        'gap_detected': False # Logic simplified
    }

    for i in range(10):
        if i < len(sorted_bids):
            price_data[f'bid_price_{i}'] = sorted_bids[i][0]
            price_data[f'bid_size_{i}'] = sorted_bids[i][1]
        else:
            price_data[f'bid_price_{i}'] = None
            price_data[f'bid_size_{i}'] = None
        
        if i < len(sorted_asks):
            price_data[f'ask_price_{i}'] = sorted_asks[i][0]
            price_data[f'ask_size_{i}'] = sorted_asks[i][1]
        else:
            price_data[f'ask_price_{i}'] = None
            price_data[f'ask_size_{i}'] = None

    price_buffers[symbol].append(price_data)
    stats['orderbook_updates'] += 1
    stats['last_orderbook_time'][symbol] = time.time()


def handle_trade_message(market_id, payload):
    """Processes real-time trades from WebSocket."""
    symbol = market_info.get(int(market_id))
    if not symbol: return

    trades = payload.get('trades', [])
    for t in trades:
        try:
            raw_ts = int(t.get('timestamp', 0))
            # Heuristic: If timestamp > 30,000,000,000 (year 2920), it's likely milliseconds.
            # Current time (2025) in ms is approx 1.7e12
            # Current time (2025) in s is approx 1.7e9
            if raw_ts > 30000000000: 
                ts_val = raw_ts / 1000.0
            else:
                ts_val = float(raw_ts)

            trade_data = {
                'timestamp': datetime.fromtimestamp(ts_val).isoformat(),
                'price': float(t.get('price', 0)),
                'size': float(t.get('size', 0)),
                'side': 'buy' if t.get('is_maker_ask') else 'sell', 
                'trade_id': int(t.get('trade_id', 0)),
                'usd_amount': float(t.get('usd_amount', 0)) if t.get('usd_amount') else None,
            }
            # Fallback for side if 'type' is explicit
            if 'type' in t and t['type'] in ['buy', 'sell']:
                 trade_data['side'] = t['type']
            
            trade_buffers[symbol].append(trade_data)
            stats['ws_trades'] += 1
            stats['last_trade_time'][symbol] = time.time()
        except Exception as e:
            logger.error(f"Error parsing trade msg: {e} | Raw TS: {t.get('timestamp')}")

async def custom_websocket_manager(market_ids):
    """WebSocket manager using shared ws_subscribe infrastructure."""
    channels = []
    for mid in market_ids:
        channels.append(f"order_book/{mid}")
        channels.append(f"trade/{mid}")

    def _on_message(data):
        channel = data.get('channel', '')
        if ':' not in channel:
            return
        ctype, mid_str = channel.split(':', 1)
        try:
            mid = int(mid_str)
        except ValueError:
            return
        if ctype == 'order_book' and 'order_book' in data:
            handle_orderbook_message(mid, data['order_book'])
        elif ctype == 'trade':
            handle_trade_message(mid, data)

    await ws_subscribe(
        channels=channels,
        label="data collector",
        on_message=_on_message,
        ping_interval=20,
        recv_timeout=30.0,
        reconnect_base=5,
        reconnect_max=60,
        logger=summary_logger,
    )



async def main():
    """Main function to initialize and run the data collector."""
    global market_info
    stats['start_time'] = time.time()

    summary_logger.info("=== Starting Lighter DEX Data Collector (Parquet) ===")
    summary_logger.info(f"Tracking symbols: {CRYPTO_TICKERS}")

    try:
        # 1. Initialize API Client
        summary_logger.info("Initializing API client...")
        client = lighter.ApiClient()
        order_api = lighter.OrderApi(client)
        summary_logger.info("✓ API client initialized")

        # 2. Discover and map market IDs from symbols
        summary_logger.info("Discovering market IDs...")
        for ticker in CRYPTO_TICKERS:
            market_id = await get_market_id(order_api, ticker)
            if market_id is not None:
                market_info[market_id] = ticker
            else:
                summary_logger.warning(f"✗ Could not find market ID for {ticker}. It will be skipped.")

        if not market_info:
            summary_logger.error("No valid market IDs found. Exiting.")
            await client.close()
            return
        summary_logger.info(f"✓ Mapped {len(market_info)} markets: {list(market_info.values())}")

        # 3. Get market IDs for WebSocket
        market_ids = list(market_info.keys())

        # Initialize last trade IDs from disk to prevent duplicates
        initial_trade_ids = get_last_recorded_trade_ids()

        # 4. Start all background tasks concurrently
        summary_logger.info("Starting background tasks...")
        tasks = {
            'writer': asyncio.create_task(write_buffers_to_parquet()),
            'trades': asyncio.create_task(fetch_recent_trades_periodically(order_api, initial_trade_ids)),
            'summary': asyncio.create_task(print_summary()),
            'websocket': asyncio.create_task(custom_websocket_manager(market_ids))
        }
        summary_logger.info("✓ All tasks started")
        summary_logger.info("=== Data collector is running (Press Ctrl+C to stop) ===")

        # 5. Wait indefinitely until an interrupt signal is received
        await asyncio.Event().wait()

    except KeyboardInterrupt:
        summary_logger.info("\nShutdown signal received...")
    except Exception as e:
        summary_logger.error(f"A fatal error occurred: {e}", exc_info=True)
    finally:
        # 6. Graceful shutdown sequence
        summary_logger.info("Starting cleanup...")
        if 'tasks' in locals():
            for name, task in tasks.items():
                task.cancel()
                logger.info(f"✓ {name.capitalize()} task cancelled")
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            logger.info("✓ All tasks completed cleanup")
        
        if 'client' in locals():
            await client.close()
            logger.info("✓ API client closed")

        # Final summary
        if stats['start_time']:
            uptime = time.time() - stats['start_time']
            summary_logger.info("=== Final Statistics ===")
            summary_logger.info(f"Total uptime: {uptime:.1f} seconds")
            summary_logger.info(f"Order book updates: {stats['orderbook_updates']}")
            summary_logger.info(f"Trade fetches: {stats['trade_fetches']}")
            summary_logger.info(f"Parquet writes: {stats['parquet_writes']}")
            summary_logger.info(f"Gaps detected: {stats['gaps_detected']}")
            summary_logger.info(f"Errors: {stats['errors']}")
        
        summary_logger.info("=== Cleanup complete ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript stopped by user.")
