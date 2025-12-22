import numpy as np
import pandas as pd
from arch import arch_model
import sys
from pathlib import Path

# Import utils relative to current script if running as main, or normally
try:
    import utils
except ImportError:
    # Fallback if running directly and utils is in the same dir but not in path
    sys.path.append(str(Path(__file__).parent))
    import utils

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
            
            # Scale to daily volatility (assuming 24h trading = 86400 seconds)
            sigma_daily = volatility_decimal * np.sqrt(86400)
            
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
        print("Latest GARCH volatility values (Daily):")
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
    
    # Scale to daily volatility (sqrt(86400))
    sigma_list = (smoothed_std * np.sqrt(86400)).tolist()
    
    if sigma_list:
        print("Latest rolling sigma values (Daily):")
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

if __name__ == "__main__":
    args = utils.parse_arguments("Calculate Volatility (Sigma)")
    TICKER = args.ticker
    window_minutes = args.minutes
    freq_str = f'{window_minutes}min'
    
    print(f"Testing Volatility Calculation for {TICKER} with {window_minutes} min window")

    try:
        HL_DATA_DIR, prices_file_path, trades_file_path = utils.require_data_files(TICKER)
    except FileNotFoundError as exc:
        print(f"Error: {exc}")
        sys.exit(1)

    mid_price_full_df = utils.load_and_resample_mid_price(prices_file_path)
    trades_full_df = utils.load_trades_data(trades_file_path)
    
    calc_mid_price_df, _ = utils.prepare_calculation_windows(
        mid_price_full_df.copy(), 
        trades_full_df.copy(), 
        window_minutes, 
        freq_str, 
        utils.PERIODS_TO_USE
    )
    
    if calc_mid_price_df.empty:
        print("No data available after preparation.")
        sys.exit(1)

    sigmas = calculate_volatility(calc_mid_price_df, window_minutes, freq_str)
    print("Result:", sigmas)
