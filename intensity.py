import numpy as np
import pandas as pd
import scipy.optimize
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

        # Always use orderbook mid-price for reference
        s_period = mid_price_df.loc[period_start:period_end]
        if s_period.empty:
            Alist.append(float('nan'))
            klist.append(float('nan'))
            continue
            
        reference_mid = s_period['mid_price'].mean()

        if pd.isna(reference_mid):
            Alist.append(float('nan'))
            klist.append(float('nan'))
            continue

        lambdas_list = []
        valid_deltas = []

        for price_delta in deltalist:
            limit_bid = reference_mid - price_delta
            limit_ask = reference_mid + price_delta
            
            # Count fills relative to this delta
            # A "fill" at delta d means a market sell hit our bid at (mid - d)
            # or a market buy hit our ask at (mid + d).
            # In trade data: 
            #   Sell trade price <= limit_bid
            #   Buy trade price >= limit_ask
            
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
            num_hits = len(all_hits)
            
            if num_hits > 1:
                hit_times = pd.DatetimeIndex(all_hits)
                deltas = hit_times.to_series().diff().dt.total_seconds().dropna()
                avg_inter_arrival = deltas.mean()
                if avg_inter_arrival > 0:
                    lambdas_list.append(1.0 / avg_inter_arrival)
                    valid_deltas.append(price_delta)
                else:
                    # Instantaneous fills? Use very high intensity or skip
                    lambdas_list.append(1.0 / 0.001) # Cap at 1ms
                    valid_deltas.append(price_delta)
            elif num_hits == 1:
                # 1 hit in the window. Rate approx 1/window
                lambdas_list.append(1.0 / (window_minutes * 60.0))
                valid_deltas.append(price_delta)
            else:
                # 0 hits. Intensity is 0.
                lambdas_list.append(0.0)
                valid_deltas.append(price_delta)

        if not valid_deltas:
            Alist.append(float('nan'))
            klist.append(float('nan'))
            continue

        try:
            # Constrain optimization to positive A and k
            # Initial guess (p0) helps convergence
            p0 = [1.0, 0.5]
            paramsB, _ = scipy.optimize.curve_fit(
                exp_fit, 
                valid_deltas, 
                lambdas_list, 
                p0=p0,
                bounds=(0, np.inf),
                maxfev=5000
            )
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

if __name__ == "__main__":
    args = utils.parse_arguments("Calculate Intensity (A, k)")
    TICKER = args.ticker
    window_minutes = args.minutes
    freq_str = f'{window_minutes}min'
    
    print(f"Testing Intensity Calculation for {TICKER}")
    
    # Get tick size (needed for deltalist)
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
    
    Alist, klist = calculate_intensity_params(list_of_periods, window_minutes, buy_trades, sell_trades, delta_list, calc_mid_price_df)
    print("A values:", Alist)
    print("k values:", klist)
