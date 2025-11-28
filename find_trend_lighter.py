import os
os.environ['NUMBA_DISABLE_JIT'] = '0'
os.environ['NUMBA_DISABLE_PERFORMANCE_WARNINGS'] = '1'
os.environ['NUMEXPR_MAX_THREADS'] = '1'

import logging
logging.getLogger('numba').setLevel(logging.WARNING)
logging.getLogger('numexpr').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

import lighter
import asyncio
import argparse
import pandas as pd
import numpy as np
import sys
import json
from datetime import datetime
import time
from numba import jit, njit

@jit(nopython=True)
def _calculate_performance_numba(entry_prices, exit_prices, signals, trading_fee):
    """
    Calculates performance metrics using Numba for speed.
    Assumes inputs are clean NumPy arrays.
    """
    # Calculate logarithmic returns for each trade
    log_returns = np.log(exit_prices / entry_prices)
    
    # Adjust for short positions (where the signal was -1)
    # Numba supports this kind of advanced indexing
    log_returns[signals == -1] *= -1
    
    # Apply trading fee
    log_returns -= trading_fee

    # Calculate Metrics
    num_flips = len(log_returns)

    std_dev = np.std(log_returns)
    if std_dev == 0.0 or np.isnan(std_dev):
        sharpe_ratio = 0.0
    else:
        sharpe_ratio = np.mean(log_returns) / std_dev

    cumulative_return = np.exp(np.sum(log_returns)) - 1

    return num_flips, sharpe_ratio, cumulative_return


@njit(cache=True)
def _supertrend_direction_numba(high, low, close, period, multiplier):
    n = close.shape[0]
    if n <= period:
        return np.zeros(n, dtype=np.int8), -1

    tr = np.empty(n, dtype=np.float64)
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        high_low = high[i] - low[i]
        high_close = abs(high[i] - close[i - 1])
        low_close = abs(low[i] - close[i - 1])
        tr[i] = max(high_low, high_close, low_close)

    atr = np.empty(n, dtype=np.float64)
    for i in range(period - 1):
        atr[i] = 0.0

    rolling_sum = 0.0
    for i in range(period):
        rolling_sum += tr[i]
    atr[period - 1] = rolling_sum / period

    factor = period - 1
    for i in range(period, n):
        atr[i] = (atr[i - 1] * factor + tr[i]) / period

    hl2 = (high + low) * 0.5
    upper = hl2 + multiplier * atr
    lower = hl2 - multiplier * atr

    final_upper = np.empty(n, dtype=np.float64)
    final_lower = np.empty(n, dtype=np.float64)
    direction = np.zeros(n, dtype=np.int8)

    start = period - 1
    final_upper[start] = upper[start]
    final_lower[start] = lower[start]
    direction[start] = 1

    for i in range(start + 1, n):
        prev_upper = final_upper[i - 1]
        prev_lower = final_lower[i - 1]

        if upper[i] < prev_upper or close[i - 1] > prev_upper:
            final_upper[i] = upper[i]
        else:
            final_upper[i] = prev_upper

        if lower[i] > prev_lower or close[i - 1] < prev_lower:
            final_lower[i] = lower[i]
        else:
            final_lower[i] = prev_lower

        if close[i] > final_upper[i - 1]:
            direction[i] = 1
        elif close[i] < final_lower[i - 1]:
            direction[i] = -1
        else:
            direction[i] = direction[i - 1]

        if direction[i] == 1 and final_lower[i] < prev_lower:
            final_lower[i] = prev_lower
        elif direction[i] == -1 and final_upper[i] > prev_upper:
            final_upper[i] = prev_upper

    return direction, start


@njit(cache=True)
def _run_backtest_numba(open_prices, high, low, close, period, multiplier, trading_fee):
    n = close.shape[0]
    if n <= period + 1:
        return 0, 0.0, 0.0, 0, False

    direction, start = _supertrend_direction_numba(high, low, close, period, multiplier)
    if start == -1:
        return 0, 0.0, 0.0, 0, False

    max_trades = n
    entry_prices = np.empty(max_trades, dtype=np.float64)
    exit_prices = np.empty(max_trades, dtype=np.float64)
    signals = np.empty(max_trades, dtype=np.int8)

    trade_count = 0
    previous_signal = direction[start]
    if previous_signal == 0:
        for idx in range(start + 1, n):
            if direction[idx] != 0:
                previous_signal = direction[idx]
                break
        if previous_signal == 0:
            return 0, 0.0, 0.0, 0, False

    for i in range(start + 1, n - 1):
        current_signal = direction[i]
        if current_signal == 0 or current_signal == previous_signal:
            continue

        entry_prices[trade_count] = open_prices[i + 1]
        signals[trade_count] = current_signal

        if trade_count > 0:
            exit_prices[trade_count - 1] = entry_prices[trade_count]

        trade_count += 1
        previous_signal = current_signal

    if trade_count < 2:
        return 0, 0.0, 0.0, int(direction[n - 1]), False

    exit_prices[trade_count - 1] = close[n - 1]

    entry_prices = entry_prices[:trade_count]
    exit_prices = exit_prices[:trade_count]
    signals = signals[:trade_count]

    num_flips, sharpe_ratio, cumulative_return = _calculate_performance_numba(
        entry_prices, exit_prices, signals, trading_fee
    )

    return num_flips, sharpe_ratio, cumulative_return, int(direction[n - 1]), True


def run_backtest(price_data, atr_period, atr_multiplier, trading_fee=0.0010):
    """
    Runs a simple stop-and-reverse backtest on the Supertrend strategy.
    Expects pre-cached NumPy arrays in price_data to avoid per-iteration DataFrame work.
    """
    open_prices = price_data['open']
    high = price_data['high']
    low = price_data['low']
    close = price_data['close']

    if open_prices.shape[0] < atr_period + 2:
        return None

    num_flips, sharpe_ratio, cumulative_return, last_signal, is_valid = _run_backtest_numba(
        open_prices, high, low, close, atr_period, atr_multiplier, trading_fee
    )

    if not is_valid:
        return None

    return {
        'period': atr_period,
        'multiplier': atr_multiplier,
        'flips': num_flips,
        'sharpe': sharpe_ratio,
        'return': cumulative_return,
        'last_signal': last_signal
    }

async def get_market_id_from_symbol(symbol: str, client: lighter.ApiClient) -> int:
    """Fetches the market ID for a given symbol."""
    order_api = lighter.OrderApi(client)
    try:
        order_books_response = await order_api.order_books()
        available_symbols = []
        for ob in order_books_response.order_books:
            available_symbols.append(ob.symbol)
            if ob.symbol.upper() == symbol.upper():
                return ob.market_id

        print(f"Available symbols: {', '.join(available_symbols)}")

    except Exception as e:
        print(f"Error fetching market info: {e}")
    return None

async def perform_grid_search(symbol, interval):
    """
    Performs a grid search to find the best Supertrend parameters using Lighter exchange data.
    """
    # --- Parameter Grid ---
    atr_periods = range(10, 1201, 5)
    atr_multipliers = np.arange(1.8, 10.4, 0.1)

    # --- Fetch Data ---
    limit = 1000 
    total_candles = 10000
    
    async with lighter.ApiClient() as api_client:
        market_id = await get_market_id_from_symbol(symbol, api_client)
        if market_id is None:
            print(f"Could not find market ID for symbol '{symbol}'. Please ensure the symbol is correct.")
            return

        # --- Caching Logic ---
        os.makedirs('params', exist_ok=True)
        kline_cache_file = f"params/klines_lighter_{symbol}_{interval}.csv"
        
        all_klines = []

        if os.path.exists(kline_cache_file):
            print(f"Loading cached k-lines from {kline_cache_file}...")
            df_cache = pd.read_csv(kline_cache_file)
            all_klines = df_cache.values.tolist()

        candlestick_api = lighter.CandlestickApi(api_client)

        try:
            # Fetch new candles since the last one in the cache
            if all_klines:
                last_timestamp = int(all_klines[-1][0])
                print(f"Fetching new candles since {pd.to_datetime(last_timestamp, unit='ms')}...")
                
                response = await candlestick_api.candlesticks(
                    market_id=market_id,
                    resolution=interval,
                    start_timestamp=last_timestamp + 1,
                    end_timestamp=int(time.time() * 1000),
                    count_back=limit
                )

                if response.candlesticks:
                    new_klines_data = response.candlesticks
                    print(f"Fetched {len(new_klines_data)} new candles.")
                    # Assuming the API returns candles in chronological order
                    for c in new_klines_data:
                        # Check available attributes and use appropriate ones
                        timestamp = getattr(c, 'timestamp', getattr(c, 'open_time', getattr(c, 't', None)))
                        open_price = getattr(c, 'open', getattr(c, 'o', None))
                        high_price = getattr(c, 'high', getattr(c, 'h', None))
                        low_price = getattr(c, 'low', getattr(c, 'l', None))
                        close_price = getattr(c, 'close', getattr(c, 'c', None))
                        volume = getattr(c, 'volume', getattr(c, 'v', None))

                        if timestamp and open_price and high_price and low_price and close_price:
                            all_klines.append([timestamp, float(open_price), float(high_price), float(low_price), float(close_price), float(volume) if volume else 0, 0, 0, 0, 0, 0, 0])
                        else:
                            print(f"Debug: Candlestick attributes: {dir(c)}")
                            break

            # Fetch older candles if we don't have enough
            while len(all_klines) < total_candles:
                needed = total_candles - len(all_klines)
                fetch_limit = min(needed, limit)
                
                if not all_klines:
                    print(f"Cache is empty. Fetching most recent {fetch_limit} candles...")
                    end_timestamp = int(time.time() * 1000)
                    start_timestamp = 0
                else:
                    oldest_timestamp = int(all_klines[0][0])
                    print(f"Fetching {fetch_limit} older candles before {pd.to_datetime(oldest_timestamp, unit='ms')}...")
                    end_timestamp = oldest_timestamp - 1
                    start_timestamp = 0

                await asyncio.sleep(0.5)
                
                response = await candlestick_api.candlesticks(
                    market_id=market_id,
                    resolution=interval,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    count_back=fetch_limit
                )

                if not response.candlesticks:
                    print("No more older k-lines available.")
                    break
                
                older_klines_data = response.candlesticks
                # Prepend older klines
                older_processed = []
                for c in older_klines_data:
                    timestamp = getattr(c, 'timestamp', getattr(c, 'open_time', getattr(c, 't', None)))
                    open_price = getattr(c, 'open', getattr(c, 'o', None))
                    high_price = getattr(c, 'high', getattr(c, 'h', None))
                    low_price = getattr(c, 'low', getattr(c, 'l', None))
                    close_price = getattr(c, 'close', getattr(c, 'c', None))
                    volume = getattr(c, 'volume', getattr(c, 'v', None))

                    if timestamp and open_price and high_price and low_price and close_price:
                        older_processed.append([timestamp, float(open_price), float(high_price), float(low_price), float(close_price), float(volume) if volume else 0, 0, 0, 0, 0, 0, 0])

                all_klines = older_processed + all_klines

        except Exception as e:
            print(f"Error fetching data from Lighter API: {e}")
            if not all_klines:
                return

    # --- Process and Save Final Dataset ---
    if not all_klines:
        print("No k-line data to process.")
        return

    df = pd.DataFrame(all_klines, columns=['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 
                                           'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 
                                           'Taker Buy Quote Asset Volume', 'Ignore'])
    df.drop_duplicates(subset=['Open Time'], keep='last', inplace=True)
    df.sort_values(by='Open Time', inplace=True)
    df = df.tail(total_candles)
    df.to_csv(kline_cache_file, index=False)
    
    klines = df.values.tolist()
    print(f"Saved {len(klines)} k-lines to cache.")

    df = pd.DataFrame(klines, columns=['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 
                                       'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 
                                       'Taker Buy Quote Asset Volume', 'Ignore'])
    df['Open Time'] = pd.to_numeric(df['Open Time'])
    df = df.astype({'Open': 'float', 'High': 'float', 'Low': 'float', 'Close': 'float', 'Volume': 'float'})

    price_data = {
        'open': np.ascontiguousarray(df['Open'].to_numpy(dtype=np.float64)),
        'high': np.ascontiguousarray(df['High'].to_numpy(dtype=np.float64)),
        'low': np.ascontiguousarray(df['Low'].to_numpy(dtype=np.float64)),
        'close': np.ascontiguousarray(df['Close'].to_numpy(dtype=np.float64)),
    }

    # --- Data Continuity Check ---
    print("Verifying data continuity...")
    interval_map = {'m': 60000, 'h': 3600000, 'd': 86400000}
    try:
        interval_unit = interval[-1]
        interval_value = int(interval[:-1])
        expected_interval_ms = interval_value * interval_map[interval_unit]
    except (KeyError, ValueError):
        print(f"Warning: Could not parse interval '{interval}'. Skipping continuity check.")
        expected_interval_ms = None

    if expected_interval_ms:
        time_diffs = df['Open Time'].diff().dropna()
        gaps = time_diffs[time_diffs > expected_interval_ms * 1.5] # Allow for some leeway

        if not gaps.empty:
            print(f"Warning: Found {len(gaps)} gaps in the data. The largest gap is {gaps.max() / expected_interval_ms:.1f} candles.")
        else:
            print("Data continuity verified. No significant gaps found.")

    print(f"Using {len(klines)} candles for backtest.")
    print("Running backtest grid search...")
    results = []
    total_tests = len(atr_periods) * len(atr_multipliers)
    test_count = 0

    for period in atr_periods:
        for multiplier in atr_multipliers:
            test_count += 1
            progress = (test_count / total_tests) * 100
            sys.stdout.write(f"\rProgress: {progress:.1f}% ({test_count}/{total_tests})")
            sys.stdout.flush()

            result = run_backtest(price_data, period, multiplier)
            if result:
                results.append(result)

    print("\n\nFinding the best result based on Sharpe Ratio...")
    if not results:
        print("No valid backtest results found.")
        return

    sorted_results = sorted(results, key=lambda x: x['sharpe'], reverse=True)

    if not sorted_results:
        print("Could not determine a best result.")
        return

    best = sorted_results[0]
    print("\n--- Best Overall Result (based on Sharpe Ratio) ---")
    print(f"Period: {best['period']}, Multiplier: {best['multiplier']:.1f}, Flips: {best['flips']}, Sharpe: {best['sharpe']:.4f}, Return: {best['return']:.2%}")

    top_count = max(1, int(len(sorted_results) * 0.05))
    print(f"\nDetermining consensus trend from top {top_count} results (~5% of grid)...")
    top_slice = sorted_results[:top_count]

    consensus_signals = [params['last_signal'] for params in top_slice if params['last_signal'] != 0]

    if not consensus_signals:
        print("Could not determine a consensus signal. Defaulting to +1.")
        consensus_trend = 1
    else:
        signal_sum = sum(consensus_signals)
        if signal_sum >= 0:
            consensus_trend = 1
        else:
            consensus_trend = -1
        print(f"Consensus signal sum: {signal_sum} -> Final Trend: {consensus_trend} ({consensus_signals.count(1)} UP vs {consensus_signals.count(-1)} DOWN)")

    print("\nSaving best parameters and consensus trend to JSON file...")
    
    last_candle_timestamp = pd.to_datetime(klines[-1][0], unit='ms').isoformat()

    output_data = {
        'best_parameters': {
            'atr_period': best['period'],
            'atr_multiplier': best['multiplier']
        },
        'backtest_performance': {
            'sharpe_ratio': best['sharpe'],
            'cumulative_return_pct': best['return'] * 100,
            'trades': best['flips']
        },
        'current_signal': {
            'trend': consensus_trend,
            'interval': interval,
            'timestamp_utc': last_candle_timestamp
        }
    }

    if not os.path.exists('params'):
        os.makedirs('params')

    file_path = f'params/supertrend_params_{symbol.replace("-","")}.json'
    with open(file_path, 'w') as f:
        json.dump(output_data, f, indent=4)
    
    print(f"Successfully saved data to {file_path}")
    print("\n--- JSON File Content ---")
    print(json.dumps(output_data, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backtest Supertrend strategy on Lighter exchange data.')
    parser.add_argument('--symbol', type=str, default='PAXG',
                        help='The trading symbol to backtest (e.g., BTC). Defaults to PAXG.')
    parser.add_argument('--interval', type=str, default='1m',
                        help='The k-line interval (e.g., 1m, 5m, 1h, 1d). Defaults to 1m.')
    args = parser.parse_args()

    asyncio.run(perform_grid_search(args.symbol, args.interval))
