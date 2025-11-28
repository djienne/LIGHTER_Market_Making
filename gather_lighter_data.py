import asyncio
import csv
import os
import time
import logging
import traceback
from datetime import datetime
from collections import defaultdict
from decimal import Decimal

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
CRYPTO_TICKERS = ['PAXG']  # Symbols to track
DATA_FOLDER = 'lighter_data'             # Directory for CSV output
BUFFER_SECONDS = 5                       # Interval to write buffered data to disk

# --- Global State ---
# In-memory buffers to store data points before writing them to files in batches.
# This reduces the frequency of disk I/O operations.
price_buffers = defaultdict(list)
trade_buffers = defaultdict(list)

# Global mapping from the API's numerical market_id to the human-readable symbol (e.g., 1 -> 'ETH').
market_info = {}

# Dictionary for tracking real-time application statistics.
stats = {
    'orderbook_updates': 0,
    'trade_fetches': 0,
    'csv_writes': 0,
    'errors': 0,
    'start_time': None,
    'last_orderbook_time': {},
    'last_trade_time': {},
    'buffer_flushes': 0
}

# Ensure the data directory exists on startup.
logger.info(f"Creating data directory: {DATA_FOLDER}")
os.makedirs(DATA_FOLDER, exist_ok=True)
logger.info(f"Data directory created successfully")


async def print_summary():
    """Print periodic summary statistics."""
    while True:
        await asyncio.sleep(20)  # Print summary every 20 seconds

        if stats['start_time']:
            uptime = time.time() - stats['start_time']

            # Calculate data rates
            orderbook_rate = stats['orderbook_updates'] / uptime if uptime > 0 else 0
            trade_rate = stats['trade_fetches'] / uptime if uptime > 0 else 0

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
                          f"Trades: {stats['trade_fetches']} ({trade_rate:.1f}/s) | "
                          f"Flushes: {stats['buffer_flushes']} | "
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


async def fetch_recent_trades_periodically(order_api: lighter.OrderApi):
    """Periodically polls the API for recent trades for all tracked markets."""
    logger.info("Starting periodic trade fetching...")
    last_trade_ids = {}  # Track the last seen trade ID per symbol to avoid duplicates.

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
                            'price': Decimal(trade.price),
                            'size': Decimal(trade.size),
                            'side': 'buy' if hasattr(trade, 'is_maker_ask') and trade.is_maker_ask else 'sell',
                            'trade_id': trade.trade_id,
                            'usd_amount': Decimal(trade.usd_amount) if hasattr(trade, 'usd_amount') else None,
                        }
                        trade_buffers[symbol].append(trade_data)
                        new_trades_count += 1
                        last_trade_ids[symbol] = max(last_trade_ids.get(symbol, 0), trade.trade_id)

                    if new_trades_count > 0:
                        logger.info(f"Added {new_trades_count} new trades for {symbol}")
                        stats['trade_fetches'] += new_trades_count
                        stats['last_trade_time'][symbol] = time.time()
            except Exception as e:
                logger.error(f"Error fetching trades for {symbol}: {e}")
                stats['errors'] += 1
        # Wait before the next polling cycle
        await asyncio.sleep(2)


async def write_buffers_to_csv():
    """Periodically writes the content of in-memory buffers to their respective CSV files."""
    logger.info(f"Starting CSV writer with {BUFFER_SECONDS}s intervals...")
    while True:
        await asyncio.sleep(BUFFER_SECONDS)
        logger.debug("Starting CSV write cycle...")

        try:
            if any(price_buffers.values()) or any(trade_buffers.values()):
                stats['buffer_flushes'] += 1
                summary_logger.info(f"FLUSH #{stats['buffer_flushes']} - Writing buffers to CSV...")

            # --- Write Price Data ---
            # Iterate over a copy of items, allowing us to safely remove keys from the original dict.
            for symbol, data_points in list(price_buffers.items()):
                if not data_points: continue

                # Atomically copy and clear the data to avoid race conditions with the websocket callback.
                price_data = price_buffers[symbol].copy()
                price_buffers[symbol].clear()
                if not price_data: continue

                prices_file = os.path.join(DATA_FOLDER, f'prices_{symbol}.csv')
                file_exists = os.path.isfile(prices_file)
                try:
                    with open(prices_file, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=price_data[0].keys())
                        if not file_exists:
                            writer.writeheader()
                        writer.writerows(price_data)
                        logger.info(f"Saved {len(price_data)} price data points for {symbol}")
                        stats['csv_writes'] += len(price_data)
                except Exception as e:
                    logger.error(f"Error writing price data for {symbol}: {e}", exc_info=True)
                    stats['errors'] += 1

            # --- Write Trade Data ---
            for symbol, data_points in list(trade_buffers.items()):
                if not data_points: continue

                # Atomically copy and clear the data to avoid race conditions
                trade_data = trade_buffers[symbol].copy()
                trade_buffers[symbol].clear()
                if not trade_data: continue

                trades_file = os.path.join(DATA_FOLDER, f'trades_{symbol}.csv')
                file_exists = os.path.isfile(trades_file)
                try:
                    with open(trades_file, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=trade_data[0].keys())
                        if not file_exists:
                            writer.writeheader()
                        writer.writerows(trade_data)
                        logger.info(f"Saved {len(trade_data)} trades for {symbol}")
                        stats['csv_writes'] += len(trade_data)
                except Exception as e:
                    logger.error(f"Error writing trade data for {symbol}: {e}", exc_info=True)
                    stats['errors'] += 1

            # Log performance stats after each write cycle
            perf_logger = logging.getLogger('performance')
            uptime = time.time() - stats['start_time'] if stats['start_time'] else 0
            perf_logger.info(f"Stats - Uptime: {uptime:.1f}s, OrderBook: {stats['orderbook_updates']}, "
                           f"Trades: {stats['trade_fetches']}, CSV: {stats['csv_writes']}, Errors: {stats['errors']}")
        except Exception as e:
            logger.error(f"Error in CSV write cycle: {e}", exc_info=True)
            stats['errors'] += 1


def on_order_book_update(market_id, order_book):
    """Callback function executed on each order book update from the WebSocket."""
    try:
        symbol = market_info.get(int(market_id))
        if not symbol:
            logger.warning(f"No symbol found for market_id: {market_id}")
            return

        bids = order_book.get('bids', [])
        asks = order_book.get('asks', [])

        if bids and asks:
            best_bid = bids[0]
            best_ask = asks[0]
            
            price_data = {
                'timestamp': datetime.now().isoformat(),
                'bid_price': Decimal(best_bid['price']),
                'bid_size': Decimal(best_bid['size']),
                'ask_price': Decimal(best_ask['price']),
                'ask_size': Decimal(best_ask['size']),
            }
            price_buffers[symbol].append(price_data)
            stats['orderbook_updates'] += 1
            stats['last_orderbook_time'][symbol] = time.time()
            logger.info(f"Order book update for {symbol}: bid={best_bid['price']}, ask={best_ask['price']}")
        else:
            logger.warning(f"Empty bids or asks for {symbol}")
    except Exception as e:
        logger.error(f"Error handling order book update: {e}", exc_info=True)
        logger.error(f"Market ID: {market_id}, Order book: {order_book}")
        stats['errors'] += 1


def on_account_update(account_id, account):
    """Callback for account updates (not used in this script)."""
    logger.debug(f"Account update received for account_id: {account_id} (ignored)")
    pass


async def websocket_manager(market_ids, on_order_book_update, on_account_update):
    """Manages the websocket connection and restarts it if it fails."""
    reconnect_count = 0
    while True:
        try:
            if reconnect_count > 0:
                summary_logger.info(f"Reconnection attempt #{reconnect_count}")
            summary_logger.info(f"Creating WebSocket client for {len(market_ids)} markets...")
            ws_client = lighter.WsClient(
                order_book_ids=market_ids,
                account_ids=[],
                on_order_book_update=on_order_book_update,
                on_account_update=on_account_update,
            )
            summary_logger.info("✓ WebSocket client created, running...")
            reconnect_count = 0  # Reset counter on successful connection
            await ws_client.run_async()
        except asyncio.CancelledError:
            summary_logger.info("WebSocket manager task cancelled.")
            break
        except Exception as e:
            stats['errors'] += 1
            reconnect_count += 1
            summary_logger.error(f"WebSocket client failed (attempt #{reconnect_count}): {e}. Reconnecting in 10s.")
            logger.error(f"Full websocket error details:", exc_info=True)
            await asyncio.sleep(10)


async def main():
    """Main function to initialize and run the data collector."""
    global market_info
    stats['start_time'] = time.time()

    summary_logger.info("=== Starting Lighter DEX Data Collector ===")
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

        # 4. Start all background tasks concurrently
        summary_logger.info("Starting background tasks...")
        tasks = {
            'writer': asyncio.create_task(write_buffers_to_csv()),
            'trades': asyncio.create_task(fetch_recent_trades_periodically(order_api)),
            'summary': asyncio.create_task(print_summary()),
            'websocket': asyncio.create_task(websocket_manager(
                market_ids, on_order_book_update, on_account_update
            ))
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
            summary_logger.info(f"CSV writes: {stats['csv_writes']}")
            summary_logger.info(f"Errors: {stats['errors']}")
        
        summary_logger.info("=== Cleanup complete ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript stopped by user.")
