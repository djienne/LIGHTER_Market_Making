#!/usr/bin/env python3
"""
Volatility (Sigma) Calculator for Market Making
Calculates volatility using both rolling standard deviation and GARCH(1,1) model
"""

import numpy as np
import pandas as pd
import os
import argparse
from pathlib import Path
import warnings
from arch import arch_model
import json
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

warnings.filterwarnings('ignore')

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Calculate volatility (sigma) for market making')
    parser.add_argument('ticker', nargs='?', default='PAXG', help='Ticker symbol (default: PAXG)')
    parser.add_argument('--hours', type=int, default=4, help='Period length in hours (default: 4)')
    parser.add_argument('--window', type=int, default=6, help='Rolling window in periods (default: 6)')
    parser.add_argument('--plot', action='store_true', help='Generate plots comparing methods')
    parser.add_argument('--output-dir', type=str, default='params', help='Output directory for results')
    return parser.parse_args()

def load_and_resample_mid_price(csv_path):
    """Load and resample mid-price data from a CSV file."""
    df = pd.read_csv(csv_path)
    df['datetime'] = pd.to_datetime(df['timestamp'], format='mixed')
    
    # Drop duplicate timestamps before setting the index
    if df['datetime'].duplicated().any():
        print("Warning: Duplicate timestamps found in price data. Keeping last entry for each.")
        df.drop_duplicates(subset=['datetime'], keep='last', inplace=True)
    
    df = df.set_index('datetime')
    
    # Create bid and ask price columns
    df['price_bid'] = df['bid_price']
    df['price_ask'] = df['ask_price']
    
    # Resample to 1-second frequency and forward fill
    merged = df[['price_bid', 'price_ask']].resample('s').ffill()
    merged['mid_price'] = (merged['price_bid'] + merged['price_ask']) / 2
    merged.dropna(inplace=True)
    
    return merged

def calculate_rolling_volatility(mid_price_df, H, freq_str, window_periods=6):
    """
    Calculate rolling volatility (sigma) using standard deviation method.
    
    Parameters:
    -----------
    mid_price_df : pd.DataFrame
        DataFrame with mid_price column
    H : int
        Hours per period
    freq_str : str
        Frequency string for resampling (e.g., '4h')
    window_periods : int
        Number of periods for rolling window (default: 6)
    
    Returns:
    --------
    list : List of sigma values per period
    """
    print("\n" + "-"*50)
    print("Calculating Rolling Standard Deviation Volatility...")
    
    # Calculate log returns
    log_returns = np.log(mid_price_df['mid_price']).diff().dropna()
    
    # Group by period and calculate standard deviation
    period_std = log_returns.groupby(pd.Grouper(freq=freq_str)).std()
    
    # Determine rolling window based on number of periods available
    num_periods = len(period_std)
    
    # Adjust window if we have fewer periods than desired
    if num_periods < window_periods:
        actual_window = max(2, num_periods // 2)  # At least 2 periods, or half of available
        print(f"Insufficient data for {window_periods}-period window. Using {actual_window} periods.")
    else:
        actual_window = window_periods
    
    print(f"Using rolling window of {actual_window} periods ({actual_window * H} hours)")
    
    # Apply rolling mean for smoothing (using integer window for periods)
    smoothed_std = period_std.rolling(window=actual_window, min_periods=1).mean()
    
    # Convert to daily volatility (annualized sqrt(365))
    sigma_list = (smoothed_std * np.sqrt(60 * 60 * 24)).tolist()
    sigma_list = sigma_list[:-1]  # Remove last incomplete period
    
    if sigma_list:
        print(f"Calculated {len(sigma_list)} sigma values")
        print("Latest rolling volatility values:")
        display_count = min(10, len(sigma_list))
        for i, s in enumerate(sigma_list[-display_count:], start=1):
            if not pd.isna(s):
                print(f"  Period -{display_count-i+1}: {s:.6f}")
    else:
        print("No sigma values calculated")
    
    return sigma_list

def calculate_garch_volatility(mid_price_df, H, freq_str):
    """
    Calculate volatility using GARCH(1,1) model with Student's t distribution.
    
    Parameters:
    -----------
    mid_price_df : pd.DataFrame
        DataFrame with mid_price column
    H : int
        Hours per period
    freq_str : str
        Frequency string for resampling (e.g., '4h')
    
    Returns:
    --------
    list : List of sigma values per period
    """
    print("\n" + "-"*50)
    print("Calculating GARCH(1,1) Volatility...")
    
    # Get list of periods
    list_of_periods = mid_price_df.index.floor(freq_str).unique().tolist()[:-1]
    
    if len(list_of_periods) < 2:
        print("Insufficient data for GARCH modeling")
        return []
    
    sigma_garch_list = []
    
    for i, period_start in enumerate(list_of_periods):
        period_end = period_start + pd.Timedelta(hours=H)
        
        # For GARCH, we want to use all data up to the current period
        # This gives us better estimates with more historical context
        mask = mid_price_df.index <= period_end
        historical_data = mid_price_df.loc[mask]
        
        if len(historical_data) < 100:  # Need minimum data for GARCH
            sigma_garch_list.append(np.nan)
            continue
        
        # Calculate returns (percentage returns work better for GARCH)
        returns = historical_data['mid_price'].pct_change().dropna() * 100
        
        if len(returns) < 100:
            sigma_garch_list.append(np.nan)
            continue
        
        try:
            # Fit GARCH(1,1) model with Student's t distribution
            am = arch_model(returns, mean='Constant', vol='GARCH', p=1, q=1, dist='t')
            res = am.fit(disp='off', show_warning=False)
            
            # Forecast variance/volatility for the next period
            # This gives us the forward-looking volatility estimate
            forecasts = res.forecast(horizon=1)
            
            # Access the conditional variance forecast for the next period
            # This is the proper way to get the volatility estimate for the current period
            variance_next = forecasts.variance.iloc[-1, 0]  # last row, horizon 1
            volatility_next = variance_next**0.5  # square root to get volatility (in percentage)
            
            # Convert from percentage returns to decimal scale
            volatility_decimal = volatility_next / 100
            
            # Scale to daily volatility
            # Since we're working with second-level data resampled to periods
            seconds_per_day = 86400
            sigma_daily = volatility_decimal * np.sqrt(seconds_per_day)
            
            sigma_garch_list.append(sigma_daily)
            
            if i == len(list_of_periods) - 1:  # Print details for the last period
                print(f"\nGARCH Model Results for latest period:")
                print(f"  Omega (α₀): {res.params['omega']:.6f}")
                print(f"  Alpha (α₁): {res.params['alpha[1]']:.6f}")
                print(f"  Beta (β₁):  {res.params['beta[1]']:.6f}")
                print(f"  Nu (df):    {res.params['nu']:.2f}")
                print(f"  Persistence: {res.params['alpha[1]'] + res.params['beta[1]']:.6f}")
                
        except Exception as e:
            print(f"GARCH fitting failed for period {i}: {str(e)}")
            sigma_garch_list.append(np.nan)
    
    # Clean up the list
    valid_sigmas = [s for s in sigma_garch_list if not pd.isna(s)]
    
    if valid_sigmas:
        print(f"\nCalculated {len(valid_sigmas)} valid GARCH sigma values")
        print("Latest GARCH volatility values:")
        display_count = min(10, len(sigma_garch_list))
        for i, s in enumerate(sigma_garch_list[-display_count:], start=1):
            if not pd.isna(s):
                print(f"  Period -{display_count-i+1}: {s:.6f}")
    else:
        print("No valid GARCH sigma values calculated")
    
    return sigma_garch_list

def save_sigma_results(ticker, rolling_sigma, garch_sigma, periods, output_dir):
    """Save sigma calculation results to JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    
    results = {
        "ticker": ticker,
        "timestamp": datetime.now().isoformat(),
        "periods": [p.isoformat() if pd.notna(p) else None for p in periods],
        "rolling_std_sigma": [float(s) if not pd.isna(s) else None for s in rolling_sigma],
        "garch_sigma": [float(s) if not pd.isna(s) else None for s in garch_sigma],
        "statistics": {
            "rolling_std": {
                "mean": float(pd.Series(rolling_sigma).mean()) if rolling_sigma else None,
                "std": float(pd.Series(rolling_sigma).std()) if rolling_sigma else None,
                "latest": float(rolling_sigma[-1]) if rolling_sigma and not pd.isna(rolling_sigma[-1]) else None
            },
            "garch": {
                "mean": float(pd.Series(garch_sigma).mean()) if garch_sigma else None,
                "std": float(pd.Series(garch_sigma).std()) if garch_sigma else None,
                "latest": float(garch_sigma[-1]) if garch_sigma and not pd.isna(garch_sigma[-1]) else None
            }
        }
    }
    
    output_path = os.path.join(output_dir, f"sigma_calculations_{ticker}.json")
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to: {output_path}")
    return output_path

def plot_sigma_comparison(ticker, rolling_sigma, garch_sigma, periods, output_dir):
    """Create plots comparing rolling std and GARCH volatility estimates."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to DataFrame for easier plotting
    df = pd.DataFrame({
        'period': periods,
        'rolling_std': rolling_sigma,
        'garch': garch_sigma
    })
    df = df.dropna(subset=['period'])
    df.set_index('period', inplace=True)
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    
    # Plot 1: Both methods on same axis
    ax1.plot(df.index, df['rolling_std'], label='Rolling Std', marker='o', alpha=0.7)
    ax1.plot(df.index, df['garch'], label='GARCH(1,1)', marker='s', alpha=0.7)
    ax1.set_title(f'Volatility (Sigma) Comparison - {ticker}')
    ax1.set_xlabel('Period')
    ax1.set_ylabel('Daily Volatility')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # Plot 2: Difference between methods
    diff = df['garch'] - df['rolling_std']
    ax2.bar(df.index, diff, alpha=0.6, color=['green' if d > 0 else 'red' for d in diff])
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax2.set_title('Difference (GARCH - Rolling Std)')
    ax2.set_xlabel('Period')
    ax2.set_ylabel('Difference in Volatility')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    
    # Save plot
    plot_path = os.path.join(output_dir, f"sigma_comparison_{ticker}.png")
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"Plot saved to: {plot_path}")
    
    plt.show()

def main():
    """Main execution function."""
    args = parse_arguments()
    ticker = args.ticker
    H = args.hours
    window_periods = args.window
    output_dir = args.output_dir
    
    print("="*50)
    print(f"VOLATILITY CALCULATION - {ticker}")
    print(f"Period: {H} hours")
    print(f"Rolling window: {window_periods} periods")
    print("="*50)
    
    # Set up data paths
    script_dir = Path(__file__).parent.absolute()
    default_if_not_env = script_dir / 'lighter_data'
    HL_DATA_DIR = os.getenv('HL_DATA_LOC', default_if_not_env)
    csv_file_path = os.path.join(HL_DATA_DIR, f'prices_{ticker}.csv')
    
    if not os.path.exists(csv_file_path):
        print(f"Error: File {csv_file_path} not found!")
        return
    
    # Load data
    mid_price_df = load_and_resample_mid_price(csv_file_path)
    print(f"Loaded {len(mid_price_df)} data points")
    print(f"Date range: {mid_price_df.index.min()} to {mid_price_df.index.max()}")
    
    # Calculate periods
    freq_str = f'{H}h'
    list_of_periods = mid_price_df.index.floor(freq_str).unique().tolist()[:-1]
    
    if len(list_of_periods) < 2:
        print("\nInsufficient data for volatility calculation. Need at least 2 periods.")
        return
    
    # Calculate volatility using both methods
    rolling_sigma = calculate_rolling_volatility(mid_price_df, H, freq_str, window_periods)
    garch_sigma = calculate_garch_volatility(mid_price_df, H, freq_str)
    
    # Save results
    save_sigma_results(ticker, rolling_sigma, garch_sigma, list_of_periods, output_dir)
    
    # Create comparison plots if requested
    if args.plot and rolling_sigma and garch_sigma:
        plot_sigma_comparison(ticker, rolling_sigma, garch_sigma, list_of_periods, output_dir)
    
    # Print summary statistics
    print("\n" + "="*50)
    print("SUMMARY STATISTICS")
    print("="*50)
    
    if rolling_sigma:
        valid_rolling = [s for s in rolling_sigma if not pd.isna(s)]
        if valid_rolling:
            print(f"Rolling Std Method:")
            print(f"  Mean:   {np.mean(valid_rolling):.6f}")
            print(f"  Std:    {np.std(valid_rolling):.6f}")
            print(f"  Latest: {valid_rolling[-1]:.6f}")
    
    if garch_sigma:
        valid_garch = [s for s in garch_sigma if not pd.isna(s)]
        if valid_garch:
            print(f"\nGARCH(1,1) Method:")
            print(f"  Mean:   {np.mean(valid_garch):.6f}")
            print(f"  Std:    {np.std(valid_garch):.6f}")
            print(f"  Latest: {valid_garch[-1]:.6f}")
    
    print("="*50)

if __name__ == "__main__":
    main()