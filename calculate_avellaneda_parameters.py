# Avellaneda-Stoikov Market Making Model Parameter Calculator
# This script implements the optimal market making strategy from
# "High-frequency trading in a limit order book" by Avellaneda & Stoikov (2008)

import sys
import os
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import utils
import volatility
import intensity
import backtest

def main():
    """Main execution function."""
    args = utils.parse_arguments()
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

    market_details = utils.get_market_details(TICKER)
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
    print(f"Loaded {len(mid_price_full_df)} data points from {mid_price_full_df.index.min()} to {mid_price_full_df.index.max()}.")

    freq_str = f'{window_minutes}min'
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

    if not list_of_periods:
        print("No complete periods available for parameter calculation.")
        utils.print_summary({}, list_of_periods)
        sys.exit()

    used_minutes = window_minutes * len(list_of_periods)
    print(f"Using last {len(list_of_periods)} completed periods ({used_minutes} minutes) for parameter estimation.")

    # Calculate parameters
    sigma_list = volatility.calculate_volatility(calc_mid_price_df, window_minutes, freq_str)
    A_bid_list, k_bid_list, A_ask_list, k_ask_list = intensity.calculate_intensity_params(list_of_periods, window_minutes, buy_trades, sell_trades, delta_list, calc_mid_price_df)

    if len(list_of_periods) <= 1:
        utils.print_summary({}, list_of_periods)
        sys.exit()

    # Check for fixed GAMMA in config
    config_params = utils.load_config_params()
    fixed_gamma = float(config_params.get('GAMMA', 0.05))
    print(f"\nUsing fixed GAMMA: {fixed_gamma}")

    # Optimize Time Horizon (T)
    # We pass the fixed gamma and search for the best horizon
    best_horizons_list = backtest.optimize_horizon(list_of_periods, sigma_list, A_bid_list, k_bid_list, A_ask_list, k_ask_list, window_minutes, ma_window, calc_mid_price_df, buy_trades, sell_trades, fixed_gamma)
    
    # Determine final horizon from optimization results
    if len(best_horizons_list) > 0:
        if ma_window > 1:
            # Use mean of recent optimal horizons to smooth out noise
            horizon_slice = best_horizons_list[max(0, len(best_horizons_list) - ma_window):]
            # horizon_slice contains minutes. Convert mean to days.
            avg_minutes = pd.Series(horizon_slice).mean()
            final_horizon_days = avg_minutes / 1440.0
        else:
            final_horizon_days = best_horizons_list[-1] / 1440.0
            
        if pd.isna(final_horizon_days): 
            final_horizon_days = 0.5 # Fallback 12h
    else:
        final_horizon_days = 0.5 # Fallback 12h

    if ma_window > 1:
        start_index = max(0, len(A_bid_list) - ma_window)
        
        A_bid = pd.Series(A_bid_list[start_index:]).mean()
        k_bid = pd.Series(k_bid_list[start_index:]).mean()
        A_ask = pd.Series(A_ask_list[start_index:]).mean()
        k_ask = pd.Series(k_ask_list[start_index:]).mean()
    else:
        A_bid = A_bid_list[-1]
        k_bid = k_bid_list[-1]
        A_ask = A_ask_list[-1]
        k_ask = k_ask_list[-1]

    # Fallbacks
    if pd.isna(A_bid): A_bid = A_bid_list[-1]
    if pd.isna(k_bid): k_bid = k_bid_list[-1]
    if pd.isna(A_ask): A_ask = A_ask_list[-1]
    if pd.isna(k_ask): k_ask = k_ask_list[-1]

    sigma = sigma_list[-1]

    # Get the actual calculation period start and end times
    period_start = list_of_periods[0] if list_of_periods else None
    period_end = list_of_periods[-1] + pd.Timedelta(minutes=window_minutes) if list_of_periods else None

    # Calculate final quotes using fixed gamma and optimized horizon
    results = backtest.calculate_final_quotes(fixed_gamma, sigma, A_bid, k_bid, A_ask, k_ask, window_minutes, mid_price_full_df, ma_window, period_start, period_end, TICKER, final_horizon_days)
    utils.print_summary(results, list_of_periods)

if __name__ == "__main__":
    main()