import numpy as np
import pandas as pd
from numba import jit
from scipy.optimize import brentq, fsolve
import warnings
import sys
import os
from pathlib import Path

# Import utils relative to current script if running as main, or normally
try:
    import utils
except ImportError:
    # Fallback if running directly and utils is in the same dir but not in path
    sys.path.append(str(Path(__file__).parent))
    import utils

# Set logging level for numba
import logging
logging.getLogger('numba').setLevel(logging.WARNING)

def optimize_horizon(list_of_periods, sigma_list, Alist, klist, window_minutes, ma_window, mid_price_df, buy_trades, sell_trades, fixed_gamma):
    """Optimize time horizon parameter via backtesting with fixed gamma."""
    print("\n" + "-"*20)
    print(f"Optimizing time horizon via backtesting (Fixed Gamma={fixed_gamma})...")

    GAMMA_CALCULATION_WINDOW = 4
    best_horizons = []
    
    # Grid of time horizons to test (in minutes)
    # 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 24h
    time_horizons_to_test = [5, 15, 30, 60, 120, 240, 480, 720, 1440]

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
            best_horizons.append(np.nan)
            continue

        period_start = list_of_periods[j]
        period_end = period_start + pd.Timedelta(minutes=window_minutes)
        print(f"\nProcessing period: {period_start} to {period_end}")

        mask = (mid_price_df.index >= period_start) & (mid_price_df.index < period_end)
        s_df = mid_price_df.loc[mask]
        s = s_df.resample('s').asfreq(fill_value=np.nan).ffill()['mid_price']

        if s.empty:
            best_horizons.append(np.nan)
            continue

        buy_mask = (buy_trades.index >= period_start) & (buy_trades.index < period_end)
        buy_trades_period = buy_trades.loc[buy_mask]
        sell_mask = (sell_trades.index >= period_start) & (sell_trades.index < period_end)
        sell_trades_period = sell_trades.loc[sell_mask]

        best_pnl_period = -np.inf
        best_horizon_period = window_minutes # Default to window length if optimization fails

        # Iterate over Time Horizons
        for horizon_minutes in time_horizons_to_test:
            # Run backtest with fixed gamma and current horizon
            res = run_backtest(s, buy_trades_period, sell_trades_period, fixed_gamma, k, sigma, window_minutes, horizon_minutes)
            pnl = res['pnl'][-1]
            
            if not np.isnan(pnl) and pnl != 0:
                # Simple optimization: Maximize PnL
                if pnl > best_pnl_period:
                    best_pnl_period = pnl
                    best_horizon_period = horizon_minutes

        print(f"Best horizon for period: {best_horizon_period}m (PnL: {best_pnl_period:.4f})")
        best_horizons.append(best_horizon_period)
        
    return best_horizons

@jit(nopython=True)
def jit_backtest_loop(s_values, buy_max_values, sell_min_values, gamma, k, sigma, fee, time_remaining):
    """
    Core JIT-compiled backtest loop.
    Assumes gamma and sigma are DIMENSIONLESS.
    k is ABSOLUTE (1/$).
    """
    N = len(s_values)
    q, x, pnl, spr, r, r_a, r_b = np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1), np.zeros(N + 1)
    
    # Pre-calculate constants where possible, but s_values[i] is dynamic
    gamma_sigma2 = gamma * sigma**2
    
    for i in range(N):
        s = s_values[i]
        t_rem = time_remaining[i]
        
        # Reservation Price: r = s * (1 - q * gamma * sigma^2 * T)
        r[i] = s * (1.0 - q[i] * gamma_sigma2 * t_rem)
        
        # Spread Calculation (Dimensionless Gamma Logic)
        # Standard AS: Width depends on liquidity (gamma, k), Shift depends on inventory/vol.
        # Spread Width (Total) = (2/gamma_abs) * log(1 + gamma_abs/k)
        #                      = (2*s/gamma) * log(1 + gamma/(s*k))
        
        val_inside_log = 1.0 + gamma / (s * k + 1e-9)
        spread_base = (2.0 * s / gamma) * np.log(val_inside_log)
        
        half_spread = spread_base / 2.0
        
        spr[i] = spread_base
        gap = abs(r[i] - s)
        
        if r[i] >= s:
            delta_a, delta_b = half_spread + gap, half_spread - gap
        else:
            delta_a, delta_b = half_spread - gap, half_spread + gap
            
        r_a[i], r_b[i] = r[i] + delta_a, r[i] - delta_b
        
        # Execution logic
        # WE SELL (at Ask r_a) if an Aggressor BUY matches or exceeds our price
        # buy_max_values[i] is the highest price a BUYER paid in this interval
        sell = 1 if not np.isnan(buy_max_values[i]) and buy_max_values[i] >= r_a[i] else 0
        
        # WE BUY (at Bid r_b) if an Aggressor SELL matches or drops below our price
        # sell_min_values[i] is the lowest price a SELLER accepted in this interval
        buy = 1 if not np.isnan(sell_min_values[i]) and sell_min_values[i] <= r_b[i] else 0
        
        q[i+1] = q[i] + (buy - sell)
        sell_net = (r_a[i] * (1 - fee)) if sell else 0
        buy_total = (r_b[i] * (1 + fee)) if buy else 0
        x[i+1] = x[i] + sell_net - buy_total
        pnl[i+1] = x[i+1] + q[i+1] * s
        
    return pnl, x, q, spr, r, r_a, r_b

def run_backtest(mid_prices, buy_trades, sell_trades, gamma, k, sigma, window_minutes, horizon_minutes, fee=0.00030):
    """Simulate the market making strategy."""
    time_index = mid_prices.index
    buy_trades_clean = buy_trades.groupby(level=0).min()
    sell_trades_clean = sell_trades.groupby(level=0).max()
    
    # We need the MAX price buyers paid to see if they hit our ASK
    buy_max = buy_trades_clean['price'].resample('5s').max().reindex(time_index)
    
    # We need the MIN price sellers accepted to see if they hit our BID
    sell_min = sell_trades_clean['price'].resample('5s').min().reindex(time_index)
    
    mid_prices = mid_prices.resample('5s').first().reindex(time_index, method='ffill')
    N = len(time_index)
    
    # Simulation duration (based on window_minutes data)
    T_sim = window_minutes / 1440.0
    dt = T_sim / N
    
    # Horizon duration (start time for AS model)
    T_horizon = horizon_minutes / 1440.0
    
    s_values = mid_prices.values
    buy_max_values = buy_max.values
    sell_min_values = sell_min.values
    
    # Time remaining array: Starts at T_horizon, decreases by dt each step
    time_remaining = T_horizon - np.arange(len(s_values)) * dt
    # Clip negative time remaining to 0
    time_remaining = np.maximum(time_remaining, 0.0)
    
    # Pass raw params to JIT loop, which handles the unit conversions dynamic to price
    pnl, x, q, spr, r, r_a, r_b = jit_backtest_loop(s_values, buy_max_values, sell_min_values, gamma, k, sigma, fee, time_remaining)
    return {'pnl': pnl, 'x': x, 'q': q, 'spread': spr, 'r': r, 'r_a': r_a, 'r_b': r_b}

def calculate_final_quotes(gamma, sigma, A, k, window_minutes, mid_price_df, ma_window, period_start, period_end, ticker, time_horizon_days):
    """Calculate the final reservation price and quotes."""
    print("\n" + "-"*20)
    print("Calculating final parameters for current state...")

    s = mid_price_df.loc[:, 'mid_price'].iloc[-1]
    
    # Use optimized horizon
    time_remaining = time_horizon_days
    
    q = 1.0  # Placeholder for current inventory
    window_hours = window_minutes / 60.0

    # Dimensionless Logic Application for Final Quotes
    # r = s * (1 - q * gamma * sigma^2 * T)
    r = s * (1.0 - q * gamma * sigma**2 * time_remaining)
    
    # Spread (Pure Liquidity component)
    term2 = (2.0 * s / gamma) * np.log(1.0 + gamma / (s * k + 1e-9))
    spread_base = term2
    
    half_spread = spread_base / 2.0
    gap = abs(r - s)

    if r >= s:
        delta_a, delta_b = half_spread + gap, half_spread - gap
    else:
        delta_a, delta_b = half_spread - gap, half_spread + gap

    r_a, r_b = r + delta_a, r - delta_b

    return {
        "ticker": ticker,
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

if __name__ == "__main__":
    # Integration test style main
    args = utils.parse_arguments("Backtest & Gamma Optimization")
    TICKER = args.ticker
    window_minutes = args.minutes
    freq_str = f'{window_minutes}min'
    
    print(f"Testing Backtest/Gamma for {TICKER}")
    
    try:
        import volatility
        import intensity
    except ImportError:
        # If running from local dir
        sys.path.append(str(Path(__file__).parent))
        import volatility
        import intensity

    _, price_tick_size, _ = utils.get_market_details(TICKER)
    delta_list = np.arange(price_tick_size, 50.0 * price_tick_size, price_tick_size)

    script_dir = Path(__file__).parent.absolute()
    default_if_not_env = script_dir / 'lighter_data'
    HL_DATA_DIR = os.getenv('HL_DATA_LOC', default_if_not_env)
    
    prices_file_path = os.path.join(HL_DATA_DIR, f'prices_{TICKER}.parquet')
    if not os.path.exists(prices_file_path):
        print(f"Error: File {prices_file_path} not found!")
        sys.exit(1)

    trades_file_path = os.path.join(HL_DATA_DIR, f'trades_{TICKER}.parquet')
    if not os.path.exists(trades_file_path):
        print(f"Error: File {trades_file_path} not found!")
        sys.exit(1)

    mid_price_full_df = utils.load_and_resample_mid_price(prices_file_path)
    trades_full_df = utils.load_trades_data(trades_file_path)
    
    calc_mid_price_df, calc_trades_df = utils.prepare_calculation_windows(
        mid_price_full_df.copy(), 
        trades_full_df.copy(), 
        window_minutes, 
        freq_str, 
        utils.PERIODS_TO_USE
    )

    buy_trades = calc_trades_df[calc_trades_df['side'] == 'buy'].copy()
    sell_trades = calc_trades_df[calc_trades_df['side'] == 'sell'].copy()
    list_of_periods = calc_mid_price_df.index.floor(freq_str).unique().tolist()
    
    if len(list_of_periods) > 1:
        sigma_list = volatility.calculate_volatility(calc_mid_price_df, window_minutes, freq_str)
        Alist, klist = intensity.calculate_intensity_params(list_of_periods, window_minutes, buy_trades, sell_trades, delta_list, calc_mid_price_df)
        
        # Test optimization of HORIZON
        gammalist = optimize_horizon(list_of_periods, sigma_list, Alist, klist, window_minutes, 1, calc_mid_price_df, buy_trades, sell_trades, 0.05)
        print("Optimized Horizons:", gammalist)
    else:
        print("Not enough periods for full backtest test.")