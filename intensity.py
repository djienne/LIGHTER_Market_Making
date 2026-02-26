import logging
import numpy as np
import pandas as pd
import scipy.optimize
import sys
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

    A_bid_list, k_bid_list = [], []
    A_ask_list, k_ask_list = [], []

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
            A_bid_list.append(float('nan')); k_bid_list.append(float('nan'))
            A_ask_list.append(float('nan')); k_ask_list.append(float('nan'))
            continue
            
        reference_mid = s_period['mid_price'].mean()

        if pd.isna(reference_mid):
            A_bid_list.append(float('nan')); k_bid_list.append(float('nan'))
            A_ask_list.append(float('nan')); k_ask_list.append(float('nan'))
            continue

        lambdas_bid = []
        lambdas_ask = []
        valid_deltas_bid = []
        valid_deltas_ask = []

        for price_delta in deltalist:
            limit_bid = reference_mid - price_delta
            limit_ask = reference_mid + price_delta
            
            # --- BID SIDE INTENSITY (Hit by Sells) ---
            bid_hits = []
            if not period_sell_orders.empty:
                sell_hits_bid = period_sell_orders[period_sell_orders['price'] <= limit_bid]
                if not sell_hits_bid.empty:
                    bid_hits = sell_hits_bid.index.tolist()
            
            num_hits_bid = len(bid_hits)
            if num_hits_bid > 1:
                hit_times = pd.DatetimeIndex(bid_hits)
                deltas = hit_times.to_series().diff().dt.total_seconds().dropna()
                avg_inter = deltas.mean()
                lambdas_bid.append(1.0 / avg_inter if avg_inter > 0 else 1.0/0.001)
                valid_deltas_bid.append(price_delta)
            elif num_hits_bid == 1:
                lambdas_bid.append(1.0 / (window_minutes * 60.0))
                valid_deltas_bid.append(price_delta)
            else:
                lambdas_bid.append(0.0)
                valid_deltas_bid.append(price_delta)

            # --- ASK SIDE INTENSITY (Hit by Buys) ---
            ask_hits = []
            if not period_buy_orders.empty:
                buy_hits_ask = period_buy_orders[period_buy_orders['price'] >= limit_ask]
                if not buy_hits_ask.empty:
                    ask_hits = buy_hits_ask.index.tolist()

            num_hits_ask = len(ask_hits)
            if num_hits_ask > 1:
                hit_times = pd.DatetimeIndex(ask_hits)
                deltas = hit_times.to_series().diff().dt.total_seconds().dropna()
                avg_inter = deltas.mean()
                lambdas_ask.append(1.0 / avg_inter if avg_inter > 0 else 1.0/0.001)
                valid_deltas_ask.append(price_delta)
            elif num_hits_ask == 1:
                lambdas_ask.append(1.0 / (window_minutes * 60.0))
                valid_deltas_ask.append(price_delta)
            else:
                lambdas_ask.append(0.0)
                valid_deltas_ask.append(price_delta)

        # Fit Bid
        if valid_deltas_bid:
            try:
                p0 = [1.0, 0.5]
                paramsB, _ = scipy.optimize.curve_fit(exp_fit, valid_deltas_bid, lambdas_bid, p0=p0, bounds=(0, np.inf), maxfev=5000)
                A_bid_list.append(paramsB[0])
                k_bid_list.append(paramsB[1])
            except Exception as e:
                logging.getLogger(__name__).warning(f"Bid intensity fit failed: {e}")
                A_bid_list.append(float('nan')); k_bid_list.append(float('nan'))
        else:
            A_bid_list.append(float('nan')); k_bid_list.append(float('nan'))

        # Fit Ask
        if valid_deltas_ask:
            try:
                p0 = [1.0, 0.5]
                paramsA, _ = scipy.optimize.curve_fit(exp_fit, valid_deltas_ask, lambdas_ask, p0=p0, bounds=(0, np.inf), maxfev=5000)
                A_ask_list.append(paramsA[0])
                k_ask_list.append(paramsA[1])
            except Exception as e:
                logging.getLogger(__name__).warning(f"Ask intensity fit failed: {e}")
                A_ask_list.append(float('nan')); k_ask_list.append(float('nan'))
        else:
            A_ask_list.append(float('nan')); k_ask_list.append(float('nan'))

    if A_bid_list and k_bid_list:
        print("Latest Intensity Values:")
        for i in range(max(0, len(A_bid_list) - 3), len(A_bid_list)):
            print(f"  - Bid: A={A_bid_list[i]:.4f}, k={k_bid_list[i]:.6f} | Ask: A={A_ask_list[i]:.4f}, k={k_ask_list[i]:.6f}")
    else:
        print("A and k values not available.")
        
    return A_bid_list, k_bid_list, A_ask_list, k_ask_list

if __name__ == "__main__":
    args = utils.parse_arguments("Calculate Intensity (A, k)")
    TICKER = args.ticker
    window_minutes = args.minutes
    freq_str = f'{window_minutes}min'
    
    print(f"Testing Intensity Calculation for {TICKER}")
    
    # Get tick size (needed for deltalist)
    _, price_tick_size, _ = utils.get_market_details(TICKER)
    delta_list = np.arange(price_tick_size, 50.0 * price_tick_size, price_tick_size)

    try:
        HL_DATA_DIR, prices_file_path, trades_file_path = utils.require_data_files(TICKER)
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
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
