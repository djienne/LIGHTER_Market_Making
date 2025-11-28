# Avellaneda-Stoikov Market Making Model Parameter Calculator
# This script implements the optimal market making strategy from
# "High-frequency trading in a limit order book" by Avellaneda & Stoikov (2008)

import numpy as np
import pandas as pd
import scipy.optimize
from scipy.optimize import brentq, fsolve
import sys
import os
import argparse
from pathlib import Path
import warnings
# warnings.filterwarnings('ignore')
from numba import jit
import json
import lighter
import asyncio
from typing import Tuple, Optional
import logging
import math
from typing import Dict, Any
from arch import arch_model

PARAMS_DIR = os.getenv("PARAMS_DIR", "params")
os.makedirs(PARAMS_DIR, exist_ok=True)

PERIODS_TO_USE = 8

logging.getLogger('numba').setLevel(logging.WARNING)

def _finite_nonneg(x) -> bool:
    try:
        v = float(x)
        return math.isfinite(v) and v >= 0.0
    except Exception:
        return False

def save_avellaneda_params_atomic(params: Dict[str, Any], symbol: str) -> bool:
    """
    Writes params to PARAMS_DIR/avellaneda_parameters_<SYMBOL>.json
    via a .tmp file + os.replace (atomic) if validation passes.
    Returns True if the final file was updated, False otherwise.
    """
    limit_orders = (params or {}).get("limit_orders") or {}
    da = limit_orders.get("delta_a")
    db = limit_orders.get("delta_b")

    final_path = os.path.join(PARAMS_DIR, f"avellaneda_parameters_{symbol}.json")
    tmp_path = final_path + ".tmp"

    # Strict validation
    if not (_finite_nonneg(da) and _finite_nonneg(db)):
        # Do NOT overwrite a good file with a bad calculation
        return False

    with open(tmp_path, "w") as f:
        json.dump(params, f, indent=4)

    # Atomic replacement
    os.replace(tmp_path, final_path)
    return True

def running_in_docker() -> bool:
    """Return True if running inside a Docker container."""
    return os.path.exists('/.dockerenv')

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Calculate Avellaneda-Stoikov market making parameters')
    parser.add_argument('ticker', nargs='?', default='PAXG', help='Ticker symbol (default: BTC)')
    parser.add_argument('--minutes', type=int, default=15, help='Frequency in minutes to recalculate parameters (default: 15)')
    return parser.parse_args()

def get_fallback_tick_size(ticker):
    """Get tick size based on the ticker symbol."""
    if ticker == 'BTC':
        return 0.1
    elif ticker == 'ETH':
        return 0.01
    elif ticker == 'SOL':
        return 0.001
    elif ticker == 'WLFI':
        return 0.00001
    elif ticker == 'PAXG':
        return 0.01
    elif ticker == 'ASTER':
        return 0.00001
    else:
        return 0.01

async def _get_market_details_async(symbol: str) -> Tuple[Optional[int], float, Optional[float]]:
    """
    Retrieves the market index, price tick size, and amount tick size for a given symbol.
    Falls back to a hardcoded price tick size if the API call fails.
    """
    api_client = None
    try:
        BASE_URL = "https://mainnet.zklighter.elliot.ai"
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
        order_api = lighter.OrderApi(api_client)
        order_books_response = await order_api.order_books()

        for order_book in order_books_response.order_books:
            if order_book.symbol.upper() == symbol.upper():
                market_id = order_book.market_id
                price_tick_size = 10 ** -order_book.supported_price_decimals
                amount_tick_size = 10 ** -order_book.supported_size_decimals
                await api_client.close()
                return market_id, price_tick_size, amount_tick_size

        # Symbol not found
        print(f"Warning: Market details for {symbol} not found in API response. Using fallback.")
        price_tick_size = get_fallback_tick_size(symbol)
        await api_client.close()
        return None, price_tick_size, None

    except Exception as e:
        print(f"An error occurred while fetching market details: {e}")
        print("Using fallback tick size due to API error.")
        if api_client:
            await api_client.close()
        price_tick_size = get_fallback_tick_size(symbol)
        return None, price_tick_size, None

def get_market_details(symbol: str) -> Tuple[Optional[int], float, Optional[float]]:
    return asyncio.run(_get_market_details_async(symbol))

def load_trades_data(csv_path):
    """Load trades data from a CSV file."""
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['timestamp'], format='mixed')
    df = df.set_index('datetime')
    return df

def load_and_resample_mid_price(csv_path):
    """Load and resample mid-price data from a CSV file."""
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['timestamp'], format='mixed')

    # Drop duplicate timestamps before setting the index, keeping the last entry
    if df['datetime'].duplicated().any():
        print("Warning: Duplicate timestamps found in price data. Keeping last entry for each.")
        df.drop_duplicates(subset=['datetime'], keep='last', inplace=True)

    df = df.set_index('datetime')

    # Create bid and ask price columns from the new format
    df['price_bid'] = df['bid_price']
    df['price_ask'] = df['ask_price']

    # Resample and forward fill
    merged = df[['price_bid', 'price_ask']].resample('s').ffill()
    merged['mid_price'] = (merged['price_bid'] + merged['price_ask']) / 2
    merged.dropna(inplace=True)
    return merged


def prepare_calculation_windows(mid_price_df: pd.DataFrame, trades_df: pd.DataFrame,
                                window_minutes: int, freq_str: str,
                                periods_to_keep: int = PERIODS_TO_USE) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Trim datasets to the latest complete periods and limit history for parameter calculations."""
    period = pd.Timedelta(minutes=window_minutes)
    expected_points = max(1, int(period.total_seconds()))
    tolerance = 0.10  # Allow 10% missing data points per period

    mid_price_df = mid_price_df.sort_index()
    trades_df = trades_df.sort_index()

    def get_period_starts(df: pd.DataFrame) -> pd.DatetimeIndex:
        if df.empty:
            return pd.DatetimeIndex([])
        return df.index.floor(freq_str).unique()

    def is_period_complete(df: pd.DataFrame, period_start, period_end, expected_pts, tol) -> bool:
        """Check if a period has sufficient data (within tolerance)."""
        mask = (df.index >= period_start) & (df.index < period_end)
        actual_points = df.loc[mask].shape[0]
        min_required = int(expected_pts * (1.0 - tol))
        return actual_points >= min_required

    # Drop incomplete trailing periods
    while True:
        period_starts = get_period_starts(mid_price_df)
        if len(period_starts) == 0:
            break
        last_period_start = period_starts[-1]
        last_period_end = last_period_start + period

        if not is_period_complete(mid_price_df, last_period_start, last_period_end, expected_points, tolerance):
            print(f"Dropping incomplete trailing period: {last_period_start}")
            mid_price_df = mid_price_df.loc[mid_price_df.index < last_period_start]
            trades_df = trades_df.loc[trades_df.index < last_period_start]
            continue
        break

    # Drop incomplete leading periods
    while True:
        period_starts = get_period_starts(mid_price_df)
        if len(period_starts) == 0:
            break
        first_period_start = period_starts[0]
        first_period_end = first_period_start + period

        if not is_period_complete(mid_price_df, first_period_start, first_period_end, expected_points, tolerance):
            print(f"Dropping incomplete leading period: {first_period_start}")
            mid_price_df = mid_price_df.loc[mid_price_df.index >= first_period_end]
            trades_df = trades_df.loc[trades_df.index >= first_period_end]
            continue
        break

    period_starts = get_period_starts(mid_price_df)
    if len(period_starts) > periods_to_keep:
        selected_starts = period_starts[-periods_to_keep:]
        window_start = selected_starts[0]
        window_end = selected_starts[-1] + period
        mid_price_df = mid_price_df.loc[(mid_price_df.index >= window_start) & (mid_price_df.index < window_end)]
        trades_df = trades_df.loc[(trades_df.index >= window_start) & (trades_df.index < window_end)]

    return mid_price_df, trades_df

def calculate_garch_volatility(mid_price_df, window_minutes, freq_str):
    """
    Calculate volatility using GARCH(1,1) model with Student's t distribution.
    """
    print("Calculating GARCH(1,1) Volatility...")
    
    list_of_periods = mid_price_df.index.floor(freq_str).unique().tolist()

    if len(list_of_periods) < 1:
        print("Insufficient data for GARCH modeling.")
        return []
    
    sigma_garch_list = []
    
    for i, period_start in enumerate(list_of_periods):
        period_end = period_start + pd.Timedelta(minutes=window_minutes)
        
        # For GARCH, we want to use all data up to the current period
        # This gives us better estimates with more historical context
        mask = mid_price_df.index <= period_end
        historical_data = mid_price_df.loc[mask]
        
        if len(historical_data) < 100:
            sigma_garch_list.append(np.nan)
            continue

        scale_fact = 1000.0 # scale factor to avoid warnings.
        
        returns = historical_data['mid_price'].pct_change().dropna() * 100.0 * scale_fact
        
        if len(returns) < 100:
            sigma_garch_list.append(np.nan)
            continue
        
        try:
            # Fit GARCH(1,1) model with Student's t distribution
            am = arch_model(returns, mean='Constant', vol='GARCH', p=1, q=1, dist='t')
            res = am.fit(disp='off', show_warning=False)
            
            # Forecast variance/volatility for the next period
            forecasts = res.forecast(horizon=1)
            
            # Access the conditional variance forecast for the next period
            variance_next = forecasts.variance.iloc[-1, 0]
            volatility_next = variance_next**0.5
            
            # Convert from percentage returns to decimal scale
            volatility_decimal = volatility_next / 100.0 / scale_fact
            
            # Scale to daily volatility
            sigma_daily = volatility_decimal * np.sqrt(60 * window_minutes)
            
            sigma_garch_list.append(sigma_daily)
            
            if i == len(list_of_periods) - 1:
                print(f"\nGARCH Model Results for latest period:")
                print(f"  Omega (α₀): {res.params['omega']:.6f}")
                print(f"  Alpha (α₁): {res.params['alpha[1]']:.6f}")
                print(f"  Beta (β₁):  {res.params['beta[1]']:.6f}")
                print(f"  Nu (df):    {res.params['nu']:.2f}")
                print(f"  Persistence: {res.params['alpha[1]'] + res.params['beta[1]']:.6f}")
                
        except Exception as e:
            sigma_garch_list.append(np.nan)
    
    valid_sigmas = [s for s in sigma_garch_list if not pd.isna(s)]
    
    if valid_sigmas:
        print(f"Calculated {len(valid_sigmas)} valid GARCH sigma values.")
        print("Latest GARCH volatility values:")
        for s in sigma_garch_list[-3:]:
            if not pd.isna(s):
                print(f"  - {s:.6f}")
    else:
        print("No valid GARCH sigma values calculated.")
    
    return sigma_garch_list

def calculate_rolling_volatility(mid_price_df, window_minutes, freq_str):
    """Calculate rolling volatility (sigma) using a period-based window."""
    print("Calculating rolling volatility as fallback...")
    
    window_periods = 6  # Default window from calculate_sigma.py

    log_returns = np.log(mid_price_df.loc[:, 'mid_price']).diff().dropna()
    period_std = log_returns.groupby(pd.Grouper(freq=freq_str)).std()
    
    num_periods = len(period_std)
    
    # Adjust window if we have fewer periods than desired
    if num_periods < window_periods:
        actual_window = max(2, num_periods // 2) if num_periods > 1 else 1
    else:
        actual_window = window_periods
    
    total_minutes = actual_window * window_minutes
    print(f"Using rolling window of {actual_window} periods ({total_minutes} minutes)")
    
    # Apply rolling mean for smoothing
    smoothed_std = period_std.rolling(window=actual_window, min_periods=1).mean()
    
    sigma_list = (smoothed_std * np.sqrt(60 * window_minutes)).tolist()
    
    if sigma_list:
        print("Latest rolling sigma values:")
        for s in sigma_list[-3:]:
            if not pd.isna(s):
                print(f"  - {s:.6f}")
    else:
        print("Rolling sigma values not available.")
    return sigma_list

def calculate_volatility(mid_price_df, window_minutes, freq_str):
    """
    Calculate volatility (sigma) using GARCH and falling back to rolling standard deviation for missing values.
    """
    print("\n" + "-"*20)
    print("Calculating volatility (sigma)...")

    period_starts = mid_price_df.index.floor(freq_str).unique().tolist()
    num_periods = len(period_starts)

    min_periods_for_garch = 5
    if num_periods < min_periods_for_garch:
        print(f"Fewer than {min_periods_for_garch} periods available, using rolling volatility only.")
        final_sigma = calculate_rolling_volatility(mid_price_df, window_minutes, freq_str)
    else:
        # 1. Calculate GARCH volatility
        garch_sigma = calculate_garch_volatility(mid_price_df, window_minutes, freq_str)

        # 2. Calculate rolling volatility as a fallback
        rolling_sigma = calculate_rolling_volatility(mid_price_df, window_minutes, freq_str)

        # Ensure lists are of the same length
        if len(garch_sigma) < num_periods:
            garch_sigma.extend([np.nan] * (num_periods - len(garch_sigma)))
        if len(rolling_sigma) < num_periods:
            rolling_sigma.extend([np.nan] * (num_periods - len(rolling_sigma)))

        garch_sigma = garch_sigma[:num_periods]
        rolling_sigma = rolling_sigma[:num_periods]

        # 3. Combine results
        final_sigma = []
        print("\nCombining GARCH and rolling volatility...")
        for i, (g, r) in enumerate(zip(garch_sigma, rolling_sigma)):
            if pd.notna(g):
                use_garch = True
                if pd.notna(r) and abs(r) > 1e-12:
                    ratio = g / r
                    if ratio > 5.0 or ratio < 1.0/5.0:
                        print(f"  - Period {i}: GARCH sigma {g:.6f} deviates by factor >5 from rolling {r:.6f} (ratio: {ratio:.2f}). Using rolling value.")
                        use_garch = False
                if use_garch:
                    final_sigma.append(g)
                    continue
            if pd.notna(r):
                if pd.isna(g):
                    print(f"  - Period {i}: GARCH failed, using rolling value: {r:.6f}")
                final_sigma.append(r)
            else:
                print(f"  - Period {i}: Both GARCH and rolling failed.")
                final_sigma.append(np.nan)
    
    # Forward fill any remaining NaNs from the start
    final_sigma_series = pd.Series(final_sigma).ffill()
    final_sigma = final_sigma_series.tolist()

    if final_sigma and not all(pd.isna(s) for s in final_sigma):
        print("\nFinal combined sigma values:")
        for s in final_sigma[-3:]:
            if pd.notna(s):
                print(f"  - {s:.6f}")
            else:
                print("  - nan")
    else:
        print("\nCould not calculate any sigma values.")

    return final_sigma

def calculate_intensity_params(list_of_periods, window_minutes, buy_orders, sell_orders, deltalist, mid_price_df):
    """Calculate order arrival intensity parameters (A and k)."""
    print("\n" + "-"*20)
    print("Calculating order arrival intensity (A and k)...")

    def exp_fit(x, a, b):
        return a * np.exp(-b * x)

    Alist, klist = [], []

    for i in range(len(list_of_periods)):
        period_start = list_of_periods[i]
        period_end = period_start + pd.Timedelta(minutes=window_minutes)

        mask_buy = (buy_orders.index >= period_start) & (buy_orders.index < period_end)
        period_buy_orders = buy_orders.loc[mask_buy].copy()

        mask_sell = (sell_orders.index >= period_start) & (sell_orders.index < period_end)
        period_sell_orders = sell_orders.loc[mask_sell].copy()

        if period_buy_orders.empty and period_sell_orders.empty:
            Alist.append(float('nan'))
            klist.append(float('nan'))
            continue

        best_bid = period_buy_orders['price'].max() if not period_buy_orders.empty else np.nan
        best_ask = period_sell_orders['price'].min() if not period_sell_orders.empty else np.nan

        if pd.isna(best_bid) or pd.isna(best_ask):
            s_period = mid_price_df.loc[period_start:period_end]
            reference_mid = s_period['mid_price'].mean() if not s_period.empty else np.nan
        else:
            reference_mid = (best_bid + best_ask) / 2

        if pd.isna(reference_mid):
            Alist.append(float('nan'))
            klist.append(float('nan'))
            continue

        deltadict = {}
        for price_delta in deltalist:
            limit_bid = reference_mid - price_delta
            limit_ask = reference_mid + price_delta
            
            bid_hits = []
            if not period_sell_orders.empty:
                sell_hits_bid = period_sell_orders[period_sell_orders['price'] <= limit_bid]
                if not sell_hits_bid.empty:
                    bid_hits = sell_hits_bid.index.tolist()
            
            ask_hits = []
            if not period_buy_orders.empty:
                buy_hits_ask = period_buy_orders[period_buy_orders['price'] >= limit_ask]
                if not buy_hits_ask.empty:
                    ask_hits = buy_hits_ask.index.tolist()
            
            all_hits = sorted(bid_hits + ask_hits)
            
            if len(all_hits) > 1:
                hit_times = pd.DatetimeIndex(all_hits)
                deltas = hit_times.to_series().diff().dt.total_seconds().dropna()
                deltadict[price_delta] = deltas
            else:
                deltadict[price_delta] = pd.Series([window_minutes * 60])

        lambdas = pd.DataFrame({
            "delta": list(deltadict.keys()),
            "lambda_delta": [1 / d.mean() if len(d) > 0 else 1e-6 for d in deltadict.values()]
        }).set_index("delta")

        try:
            paramsB, _ = scipy.optimize.curve_fit(exp_fit, lambdas.index.values, lambdas["lambda_delta"].values, maxfev=5000)
            A, k = paramsB
            Alist.append(A)
            klist.append(k)
        except (RuntimeError, ValueError):
            Alist.append(float('nan'))
            klist.append(float('nan'))

    if Alist and klist:
        print("Latest A and k values:")
        for i in range(max(0, len(Alist) - 3), len(Alist)):
            print(f"  - A: {Alist[i]:.4f}, k: {klist[i]:.6f}")
    else:
        print("A and k values not available.")
    return Alist, klist

def optimize_gamma(list_of_periods, sigma_list, Alist, klist, window_minutes, ma_window, mid_price_df, buy_trades, sell_trades, tick_size):
    """Optimize risk aversion parameter (gamma) via backtesting."""
    print("\n" + "-"*20)
    print("Optimizing risk aversion (gamma) via backtesting...")

    GAMMA_CALCULATION_WINDOW = 4
    gammalist = []
    gamma_grid_to_test = None

    start_index = max(1, len(list_of_periods) - GAMMA_CALCULATION_WINDOW)
    period_index_range = range(start_index, len(list_of_periods))

    for j in period_index_range:
        if ma_window > 1:
            a_slice = Alist[max(0, j - ma_window):j]
            k_slice = klist[max(0, j - ma_window):j]
            A = pd.Series(a_slice).mean()
            k = pd.Series(k_slice).mean()
        else:
            A = Alist[j-1]
            k = klist[j-1]

        sigma = sigma_list[j-1]

        if pd.isna(sigma) or pd.isna(A) or pd.isna(k):
            gammalist.append(np.nan)
            continue

        period_start = list_of_periods[j]
        period_end = period_start + pd.Timedelta(minutes=window_minutes)
        print(f"\nProcessing period: {period_start} to {period_end}")

        mask = (mid_price_df.index >= period_start) & (mid_price_df.index < period_end)
        s_df = mid_price_df.loc[mask]
        s = s_df.resample('s').asfreq(fill_value=np.nan).ffill()['mid_price']

        if s.empty:
            gammalist.append(np.nan)
            continue

        if gamma_grid_to_test is None:
            gamma_grid_to_test = generate_gamma_grid(s.iloc[-1], sigma, k, window_minutes)

        if gamma_grid_to_test is None:
            print("Could not find a reasonable gamma interval. Aborting.")
            sys.exit()

        buy_mask = (buy_trades.index >= period_start) & (buy_trades.index < period_end)
        buy_trades_period = buy_trades.loc[buy_mask]
        sell_mask = (sell_trades.index >= period_start) & (sell_trades.index < period_end)
        sell_trades_period = sell_trades.loc[sell_mask]

        gamma_results = []
        for i, gamma_to_test in enumerate(gamma_grid_to_test):
            print(f"  - Testing gamma: {gamma_to_test:.5f} ({i+1}/{len(gamma_grid_to_test)})")
            result = evaluate_gamma(gamma_to_test, s, buy_trades_period, sell_trades_period, k, sigma, window_minutes)
            gamma_results.append(result)

        results_df = pd.DataFrame(gamma_results, columns=['gamma', 'pnl', 'spread'])
        valid_results = results_df.dropna(subset=['pnl'])
        
        if valid_results.empty:
            print("Warning: All backtests resulted in NaN or 0 PnL. Using fallback gamma.")
            best_gamma = 0.5
        else:
            positive_pnl_results = valid_results[valid_results['pnl'] > 0]
            if not positive_pnl_results.empty:
                best_gamma = positive_pnl_results.loc[positive_pnl_results['spread'].idxmax()]['gamma']
            else:
                best_gamma = valid_results.loc[valid_results['pnl'].idxmax()]['gamma']
        
        print(f"Best gamma for period: {best_gamma:.5f}")
        gammalist.append(best_gamma)
        
    return gammalist

def evaluate_gamma(gamma, mid_prices_period, buy_trades_period, sell_trades_period, k, sigma, window_minutes):
    """Run backtest for a single gamma value and return results."""
    res = run_backtest(mid_prices_period, buy_trades_period, sell_trades_period, gamma, k, sigma, window_minutes)
    final_pnl = res['pnl'][-1]
    
    if np.isnan(final_pnl) or final_pnl == 0:
        return [round(gamma, 5), np.nan, np.nan]

    spread_base = gamma * sigma**2.0 * 0.5 + (2.0 / gamma) * np.log(1.0 + (gamma / k))
    return [round(gamma, 5), final_pnl, spread_base]

def generate_gamma_grid(s, sigma, k, window_minutes):
    """Generate a grid of gamma values to test."""
    window_days = window_minutes / 1440.0
    time_remaining = window_days / 2.0
    
    def spread(gamma):
        return (gamma * sigma**2 * time_remaining + (2.0 / gamma) * np.log(1.0 + (gamma / k))) / s * 100.0
    
    try:
        gamma_001 = find_gamma(0.01, spread, k)
    except ValueError:
        _, gamma_001 = find_workable_spread(0.01, spread, k, 'up')
        if gamma_001 is None: return None
    
    try:
        gamma_1 = find_gamma(2.0, spread, k)
    except ValueError:
        _, gamma_1 = find_workable_spread(2.0, spread, k, 'down')
        if gamma_1 is None: return None
        
    return np.logspace(np.log10(gamma_1 * 0.99), np.log10(gamma_001 * 1.01), 32)

def find_gamma(target_spread, spread_func, k):
    """Find gamma for a given target spread."""
    # ... (implementation from previous steps, unchanged)
    def equation(gamma):
        if gamma <= 0: return float('inf')
        try: return spread_func(gamma) - target_spread
        except: return float('inf')
    
    def is_valid(gamma, tolerance=1e-6):
        if gamma <= 0: return False
        try: return abs(spread_func(gamma) - target_spread) < tolerance
        except: return False

    try:
        gamma_min, gamma_max = 1e-8, 1000.0
        if equation(gamma_min) * equation(gamma_max) < 0:
            gamma = brentq(equation, gamma_min, gamma_max)
            if is_valid(gamma): return gamma
    except: pass

    for guess in [1.0, k, 0.1, 10.0, k*10, k*0.1]:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = fsolve(equation, guess, full_output=True)
                if result[2] == 1 and is_valid(result[0][0]): return result[0][0]
        except: continue
    
    raise ValueError(f"Could not find gamma for target_spread = {target_spread}")

def find_workable_spread(initial_spread, spread_func, k, direction='up', factor=1.05, max_iterations=100):
    """Find a workable spread if the target is not achievable."""
    # ... (implementation from previous steps, unchanged)
    spread = initial_spread
    for i in range(max_iterations):
        try:
            gamma = find_gamma(spread, spread_func, k)
            return spread, gamma
        except ValueError:
            spread *= factor if direction == 'up' else (1/factor)
    return None, None

@jit(nopython=True)
def jit_backtest_loop(s_values, buy_min_values, sell_max_values, gamma, k, sigma, fee, time_remaining, spread_base, half_spread):
    """Core JIT-compiled backtest loop."""
    # ... (implementation from previous steps, unchanged)
    N = len(s_values)
    q, x, pnl, spr, r, r_a, r_b = np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1)
    gamma_sigma2 = gamma * sigma**2
    for i in range(N):
        r[i] = s_values[i] - q[i] * gamma_sigma2 * time_remaining[i]
        spr[i] = spread_base[i]
        gap = abs(r[i] - s_values[i])
        if r[i] >= s_values[i]:
            delta_a, delta_b = half_spread[i] + gap, half_spread[i] - gap
        else:
            delta_a, delta_b = half_spread[i] - gap, half_spread[i] + gap
        r_a[i], r_b[i] = r[i] + delta_a, r[i] - delta_b
        sell = 1 if not np.isnan(sell_max_values[i]) and sell_max_values[i] >= r_a[i] else 0
        buy = 1 if not np.isnan(buy_min_values[i]) and buy_min_values[i] <= r_b[i] else 0
        q[i+1] = q[i] + (sell - buy)
        sell_net = (r_a[i] * (1 - fee)) if sell else 0
        buy_total = (r_b[i] * (1 + fee)) if buy else 0
        x[i+1] = x[i] + sell_net - buy_total
        pnl[i+1] = x[i+1] + q[i+1] * s_values[i]
    return pnl, x, q, spr, r, r_a, r_b

def run_backtest(mid_prices, buy_trades, sell_trades, gamma, k, sigma, window_minutes, fee=0.00030):
    """Simulate the market making strategy."""
    # ... (implementation from previous steps, unchanged)
    time_index = mid_prices.index
    buy_trades_clean = buy_trades.groupby(level=0).min()
    sell_trades_clean = sell_trades.groupby(level=0).max()
    buy_min = buy_trades_clean['price'].resample('5s').min().reindex(time_index, method='ffill')
    sell_max = sell_trades_clean['price'].resample('5s').max().reindex(time_index, method='ffill')
    mid_prices = mid_prices.resample('5s').first().reindex(time_index, method='ffill')
    N = len(time_index)
    T = window_minutes / 1440.0
    dt = T / N
    s_values = mid_prices.values
    buy_min_values = buy_min.values
    sell_max_values = sell_max.values
    time_remaining = T - np.arange(len(s_values)) * dt
    spread_base = gamma * sigma**2.0 * time_remaining + (2.0 / gamma) * np.log(1.0 + (gamma / k))
    half_spread = spread_base / 2.0
    pnl, x, q, spr, r, r_a, r_b = jit_backtest_loop(s_values, buy_min_values, sell_max_values, gamma, k, sigma, fee, time_remaining, spread_base, half_spread)
    return {'pnl': pnl, 'x': x, 'q': q, 'spread': spr, 'r': r, 'r_a': r_a, 'r_b': r_b}

def calculate_final_quotes(gamma, sigma, A, k, window_minutes, mid_price_df, ma_window, period_start, period_end):
    """Calculate the final reservation price and quotes."""
    print("\n" + "-"*20)
    print("Calculating final parameters for current state...")

    s = mid_price_df.loc[:, 'mid_price'].iloc[-1]
    time_remaining = 0.5
    q = 1.0  # Placeholder for current inventory
    window_hours = window_minutes / 60.0

    spread_base = gamma * sigma**2.0 * time_remaining + (2.0 / gamma) * np.log(1.0 + (gamma / k))
    half_spread = spread_base / 2.0
    r = s - q * gamma * sigma**2.0 * time_remaining
    gap = abs(r - s)

    if r >= s:
        delta_a, delta_b = half_spread + gap, half_spread - gap
    else:
        delta_a, delta_b = half_spread - gap, half_spread + gap

    r_a, r_b = r + delta_a, r - delta_b

    return {
        "ticker": TICKER,
        "timestamp": pd.Timestamp.now().isoformat(),
        "market_data": {"mid_price": float(s), "sigma": float(sigma), "A": float(A), "k": float(k)},
        "optimal_parameters": {"gamma": float(gamma)},
        "current_state": {
            "time_remaining": float(time_remaining),
            "inventory": int(q),
            "minutes_window": window_minutes,
            "hours_window": window_hours,
            "ma_window": ma_window
        },
        "calculation_period": {
            "start": period_start.isoformat() if period_start is not None else None,
            "end": period_end.isoformat() if period_end is not None else None
        },
        "calculated_values": {"reservation_price": float(r), "gap": float(gap), "spread_base": float(spread_base), "half_spread": float(half_spread)},
        "limit_orders": {"ask_price": float(r_a), "bid_price": float(r_b), "delta_a": float(delta_a), "delta_b": float(delta_b),
                         "delta_a_percent": (delta_a / s) * 100.0, "delta_b_percent": (delta_b / s) * 100.0}
    }

def print_summary(results, list_of_periods):
    """Print a summary of the results to the terminal."""
    if not results:
        print("\n" + "="*80)
        print("AVELLANEDA-STOIKOV MARKET MAKING PARAMETERS")
        print("="*80)
        print("⚠️  DATA WARNING: Insufficient data for robust parameter estimation.")
        print("="*80)
        return

    TICKER = results['ticker']
    current_state = results['current_state']
    window_minutes = current_state.get('minutes_window')
    window_hours = current_state.get('hours_window')
    ma_window = current_state['ma_window']
    
    print("\n" + "="*80)
    print(f"AVELLANEDA-STOIKOV MARKET MAKING PARAMETERS - {TICKER}")
    if window_minutes is not None:
        if window_hours is not None:
            print(f"Analysis Period: {window_minutes} minutes ({window_hours:.2f} hours)")
        else:
            print(f"Analysis Period: {window_minutes} minutes")
    elif window_hours is not None:
        print(f"Analysis Period: {window_hours} hours")
    if ma_window > 1:
        print(f"Moving Average Window: {ma_window} periods")
    print("="*80)

    if len(list_of_periods) <= 1:
        print("⚠️  DATA WARNING: Insufficient data for robust parameter estimation.")
        print("="*80)

    # Print calculation period if available
    calc_period = results.get('calculation_period', {})
    if calc_period.get('start') and calc_period.get('end'):
        print(f"\nCalculation Period Used:")
        print(f"   Start: {calc_period['start']}")
        print(f"   End:   {calc_period['end']}")

    print(f"\nMarket Data:")
    print(f"   Mid Price:                        ${results['market_data']['mid_price']:,.4f}")
    print(f"   Volatility (sigma):               {results['market_data']['sigma']:.6f}")
    print(f"   Intensity (A):                    {results['market_data']['A']:.4f}")
    print(f"   Order arrival rate decay (k):     {results['market_data']['k']:.6f}")
    print(f"\nOptimal Parameters:")
    print(f"   Risk Aversion (gamma): {results['optimal_parameters']['gamma']:.6f}")
    print(f"\nCurrent State:")
    print(f"   Time Remaining:        {results['current_state']['time_remaining']:.4f} (in days)")
    print(f"   Inventory (q):         {results['current_state']['inventory']:.4f}")
    print(f"\nCalculated Prices:")
    print(f"   Reservation Price:     ${results['calculated_values']['reservation_price']:.4f}")
    print(f"   Ask Price:             ${results['limit_orders']['ask_price']:.4f}")
    print(f"   Bid Price:             ${results['limit_orders']['bid_price']:.4f}")
    print(f"\nSpreads:")
    print(f"   Delta Ask:             ${results['limit_orders']['delta_a']:.6f} ({results['limit_orders']['delta_a_percent']:.6f}%)")
    print(f"   Delta Bid:             ${results['limit_orders']['delta_b']:.6f} ({results['limit_orders']['delta_b_percent']:.6f}%)")
    print(f"   Total Spread:          {(results['limit_orders']['delta_a_percent'] + results['limit_orders']['delta_b_percent']):.4f}%")
    
    ok = save_avellaneda_params_atomic(results, TICKER)
    out_path = os.path.join(PARAMS_DIR, f"avellaneda_parameters_{TICKER}.json")
    if ok:
        print(f"\nResults saved to: {out_path}")
    else:
        print("\n⚠️ Invalid params (delta_a/delta_b). Keeping previous file.")

    print("="*80)

def main():
    """Main execution function."""
    global TICKER  # Make TICKER a global variable
    args = parse_arguments()
    TICKER = args.ticker
    window_minutes = args.minutes
    window_hours = window_minutes / 60.0

    if window_minutes <= 8 * 60:
        ma_window = 3
    elif 8 * 60 < window_minutes < 20 * 60:
        ma_window = 2
    else: ma_window = 1

    print("-" * 20)
    print(f"DOING: {TICKER}")
    print(f"Using analysis period of {window_minutes} minutes ({window_hours:.2f} hours).")
    if ma_window > 1:
        print(f"Using a {ma_window}-period moving average for parameters.")

    market_details = get_market_details(TICKER)
    market_id, price_tick_size, amount_tick_size = market_details

    if market_id is not None:
        print(f"Successfully fetched market details for {TICKER}:")
        print(f"  Market ID: {market_id}")
        print(f"  Price Tick Size: {price_tick_size}")
        print(f"  Amount Tick Size: {amount_tick_size}")
    else:
        print(f"Could not fetch market details online. Using fallback price tick size: {price_tick_size}")

    tick_size = price_tick_size
    delta_list = np.arange(tick_size, 50.0 * tick_size, tick_size)
    
    # Load data
    script_dir = Path(__file__).parent.absolute()
    default_if_not_env = script_dir / 'lighter_data'
    HL_DATA_DIR = os.getenv('HL_DATA_LOC', default_if_not_env)
    csv_file_path = os.path.join(HL_DATA_DIR, f'prices_{TICKER}.csv')

    if not os.path.exists(csv_file_path):
        print(f"Error: File {csv_file_path} not found!")
        sys.exit(1)

    mid_price_full_df = load_and_resample_mid_price(csv_file_path)
    trades_full_df = load_trades_data(os.path.join(HL_DATA_DIR, f'trades_{TICKER}.csv'))
    print(f"Loaded {len(mid_price_full_df)} data points from {mid_price_full_df.index.min()} to {mid_price_full_df.index.max()}.")

    freq_str = f'{window_minutes}min'
    calc_mid_price_df, calc_trades_df = prepare_calculation_windows(mid_price_full_df.copy(), trades_full_df.copy(), window_minutes, freq_str, PERIODS_TO_USE)

    buy_trades = calc_trades_df[calc_trades_df['side'] == 'buy'].copy()
    sell_trades = calc_trades_df[calc_trades_df['side'] == 'sell'].copy()

    list_of_periods = calc_mid_price_df.index.floor(freq_str).unique().tolist()

    if not list_of_periods:
        print("No complete periods available for parameter calculation.")
        print_summary({}, list_of_periods)
        sys.exit()

    used_minutes = window_minutes * len(list_of_periods)
    print(f"Using last {len(list_of_periods)} completed periods ({used_minutes} minutes) for parameter estimation.")

    # Calculate parameters
    sigma_list = calculate_volatility(calc_mid_price_df, window_minutes, freq_str)
    Alist, klist = calculate_intensity_params(list_of_periods, window_minutes, buy_trades, sell_trades, delta_list, calc_mid_price_df)

    if len(list_of_periods) <= 1:
        print_summary({}, list_of_periods)
        sys.exit()

    gammalist = optimize_gamma(list_of_periods, sigma_list, Alist, klist, window_minutes, ma_window, calc_mid_price_df, buy_trades, sell_trades, tick_size)

    # Final calculations
    if len(gammalist) > 0:
        if ma_window > 1:
            gamma_slice = gammalist[max(0, len(gammalist) - ma_window):]
            gamma = pd.Series(gamma_slice).mean()
        else:
            gamma = gammalist[-1]
        if pd.isna(gamma): gamma = 0.1
    else:
        gamma = 0.1

    if ma_window > 1:
        start_index = max(0, len(Alist) - ma_window)
        a_slice = Alist[start_index:]
        k_slice = klist[start_index:]
        A = pd.Series(a_slice).mean()
        k = pd.Series(k_slice).mean()
    else:
        A = Alist[-1]
        k = klist[-1]

    if pd.isna(A):
        A = Alist[-1]
    if pd.isna(k):
        k = klist[-1]

    sigma = sigma_list[-1]

    # Get the actual calculation period start and end times
    period_start = list_of_periods[0] if list_of_periods else None
    period_end = list_of_periods[-1] + pd.Timedelta(minutes=window_minutes) if list_of_periods else None

    results = calculate_final_quotes(gamma, sigma, A, k, window_minutes, mid_price_full_df, ma_window, period_start, period_end)
    print_summary(results, list_of_periods)

if __name__ == "__main__":
    main()
