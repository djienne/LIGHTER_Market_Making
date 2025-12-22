import os
import json
import math
import argparse
import asyncio
import logging
import zipfile
import re
from typing import Tuple, Optional, Dict, Any, List
from pathlib import Path

import pandas as pd
import numpy as np
import lighter

# Global Constants
PARAMS_DIR = os.getenv("PARAMS_DIR", "params")
os.makedirs(PARAMS_DIR, exist_ok=True)

PERIODS_TO_USE = 8

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

def parse_arguments(description='Calculate Avellaneda-Stoikov market making parameters'):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('ticker', nargs='?', default='PAXG', help='Ticker symbol (default: PAXG)')
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

def load_config_params():
    """Load parameters from config.json."""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        print("Error decoding config.json")
        return {}

def get_market_details(symbol: str) -> Tuple[Optional[int], float, Optional[float]]:
    return asyncio.run(_get_market_details_async(symbol))

def get_data_paths(ticker: str) -> Tuple[str, str, str]:
    """Return data directory and expected price/trade parquet paths for a ticker."""
    script_dir = Path(__file__).parent.absolute()
    default_if_not_env = script_dir / 'lighter_data'
    data_dir = Path(os.getenv('HL_DATA_LOC', default_if_not_env))
    prices_file_path = data_dir / f'prices_{ticker}.parquet'
    trades_file_path = data_dir / f'trades_{ticker}.parquet'
    return str(data_dir), str(prices_file_path), str(trades_file_path)

def require_data_files(ticker: str) -> Tuple[str, str, str]:
    """Return paths or raise if price/trade data is missing."""
    data_dir, prices_file_path, trades_file_path = get_data_paths(ticker)
    if not has_parquet_data(prices_file_path):
        raise FileNotFoundError(f"No price data found for {ticker} in {data_dir}!")
    if not has_parquet_data(trades_file_path):
        raise FileNotFoundError(f"No trade data found for {ticker} in {data_dir}!")
    return data_dir, prices_file_path, trades_file_path

def load_trades_data(file_path):
    """Load trades data from a Parquet file."""
    df = _read_parquet_with_fallback(file_path)
    if df is None:
        df = _read_csv_fallback(file_path)
    if 'trade_id' in df.columns:
        df = df.drop_duplicates(subset=['trade_id'])
    else:
        df = df.drop_duplicates()
    df['datetime'] = pd.to_datetime(df['timestamp'], format='mixed')
    df = df.set_index('datetime')
    return df

def find_parquet_parts(file_path: str) -> List[str]:
    """Return ordered part files for a base parquet path (e.g., prices_ETH_part*.parquet)."""
    base = Path(file_path)
    pattern = f"{base.stem}_part*.parquet"
    parts = list(base.parent.glob(pattern))

    def sort_key(path: Path):
        match = re.search(r"_part(\d+)\.parquet$", path.name)
        if match:
            return (0, int(match.group(1)))
        return (1, path.name)

    parts.sort(key=sort_key)
    return [str(path) for path in parts]

def has_parquet_data(file_path: str) -> bool:
    """Check for either a legacy parquet file or chunked part files."""
    return bool(find_parquet_parts(file_path)) or os.path.exists(file_path)

def read_parquet_data(file_path: str, columns=None):
    """Read parquet data from chunked part files or a legacy file."""
    return _read_parquet_with_fallback(file_path, columns=columns)

def calculate_depth_vwap(df, side, levels=10, target_notional=1000.0):
    """
    Calculates the Volume Weighted Average Price (VWAP) for a specific side (bid/ask)
    up to a target notional amount.
    
    Args:
        df: DataFrame containing flattened order book columns (e.g., bid_price_0, bid_size_0).
        side: 'bid' or 'ask'.
        levels: Number of order book levels available (default 10).
        target_notional: The amount of USD to cover (default $1000).
        
    Returns:
        Series containing the effective VWAP price.
    """
    # 1. Extract Price and Size matrices (N x levels)
    # Fill NaNs: sizes with 0, prices with previous level or 0? 
    # If price is NaN, size should be 0, so price value doesn't matter for notional, 
    # but matters for VWAP division if we aren't careful.
    # We'll fill size NaNs with 0.
    
    price_cols = [f'{side}_price_{i}' for i in range(levels)]
    size_cols = [f'{side}_size_{i}' for i in range(levels)]
    
    # Use float32/64 for calculations
    prices = df[price_cols].fillna(0.0).values
    sizes = df[size_cols].fillna(0.0).values
    
    # 2. Calculate Notionals at each level
    level_notionals = prices * sizes
    
    # 3. Cumulative Sum of Notional and Size
    cum_notionals = np.cumsum(level_notionals, axis=1)
    cum_sizes = np.cumsum(sizes, axis=1)
    
    # 4. Find the level where we cross the target notional
    # Mask: True where cum_notional >= target
    # argmax gives the index of the first True. If all False (book < target), returns 0.
    # We need to handle the case where book < target separately.
    
    # Check if total book value is less than target
    total_book_value = cum_notionals[:, -1]
    insufficient_depth = total_book_value < target_notional
    
    mask_cross = cum_notionals >= target_notional
    cross_indices = np.argmax(mask_cross, axis=1) # Shape (N,)
    
    # If insufficient depth, set index to last level (levels - 1)
    cross_indices[insufficient_depth] = levels - 1
    
    # 5. Calculate VWAP up to the crossing level
    # We need to be precise: full levels up to k-1, plus partial level k.
    
    # Helpers to access values at specific indices per row
    rows = np.arange(len(df))
    
    # Values at crossing level k
    p_k = prices[rows, cross_indices]
    
    # Cumulative values up to k (inclusive)
    # Note: cum_notionals includes the full k-th level. We need to subtract the excess.
    
    cum_not_k = cum_notionals[rows, cross_indices]
    cum_sz_k = cum_sizes[rows, cross_indices]
    
    # Excess notional = cum_not_k - target
    # If insufficient depth, excess is 0 (or negative effectively, but we use full book)
    excess_notional = cum_not_k - target_notional
    excess_notional[insufficient_depth] = 0.0
    
    # Calculate excess size to subtract
    # size_to_remove = excess_notional / p_k
    # Avoid division by zero if price is 0 (unlikely for valid levels)
    with np.errstate(divide='ignore', invalid='ignore'):
        excess_size = excess_notional / p_k
        excess_size[p_k == 0] = 0.0 # Should not happen for valid data
        
    # Effective Total Size = Size up to k - Excess Size
    effective_total_size = cum_sz_k - excess_size
    
    # Effective Total Notional is exactly target_notional (except if insufficient depth)
    effective_total_notional = np.full(len(df), target_notional)
    effective_total_notional[insufficient_depth] = total_book_value[insufficient_depth]
    
    # VWAP = Total Notional / Total Size
    with np.errstate(divide='ignore', invalid='ignore'):
        vwap = effective_total_notional / effective_total_size
        # Handle 0 size (empty book) -> result NaN or 0
        vwap[effective_total_size == 0] = np.nan
        
    return pd.Series(vwap, index=df.index)

def load_and_resample_mid_price(file_path):
    """Load and resample mid-price data from a Parquet file."""
    df = _read_parquet_with_fallback(file_path)
    if df is None:
        df = _read_csv_fallback(file_path)
    df['datetime'] = pd.to_datetime(df['timestamp'], format='mixed')

    # Drop duplicate timestamps before setting the index, keeping the last entry
    if df['datetime'].duplicated().any():
        print("Warning: Duplicate timestamps found in price data. Keeping last entry for each.")
        df.drop_duplicates(subset=['datetime'], keep='last', inplace=True)

    df = df.set_index('datetime')

    # Calculate Effective Bid and Ask Price
    if 'bid_price_0' in df.columns and 'ask_price_0' in df.columns:
        # Use depth-based VWAP when full order book levels are available.
        TARGET_NOTIONAL = 1000.0
        df['price_bid'] = calculate_depth_vwap(df, 'bid', levels=10, target_notional=TARGET_NOTIONAL)
        df['price_ask'] = calculate_depth_vwap(df, 'ask', levels=10, target_notional=TARGET_NOTIONAL)
    elif 'bid_price' in df.columns and 'ask_price' in df.columns:
        # CSV fallback provides top-of-book only.
        df['price_bid'] = df['bid_price']
        df['price_ask'] = df['ask_price']
    else:
        raise ValueError("Price data missing bid/ask columns for mid-price calculation.")

    # Resample and forward fill
    merged = df[['price_bid', 'price_ask']].resample('s').ffill()
    merged['mid_price'] = (merged['price_bid'] + merged['price_ask']) / 2
    merged.dropna(inplace=True)
    return merged

def _read_parquet_with_fallback(file_path, columns=None):
    """Read parquet with pyarrow first, then fastparquet as a fallback."""
    part_files = find_parquet_parts(file_path)
    if part_files:
        return _read_parquet_parts(part_files, columns=columns)

    last_err = None
    for engine in ("pyarrow", "fastparquet"):
        try:
            return pd.read_parquet(file_path, engine=engine, columns=columns)
        except Exception as exc:
            last_err = exc
            continue
    print(f"Warning: failed to read parquet {file_path}: {last_err}")
    return None

def _read_parquet_parts(part_files: List[str], columns=None):
    """Read multiple parquet part files and concatenate them."""
    dataframes = []
    errors = []
    for part_file in part_files:
        last_err = None
        df = None
        for engine in ("pyarrow", "fastparquet"):
            try:
                df = pd.read_parquet(part_file, engine=engine, columns=columns)
                break
            except Exception as exc:
                last_err = exc
                continue
        if df is None:
            errors.append((part_file, last_err))
            continue
        dataframes.append(df)

    if errors:
        print(f"Warning: failed to read {len(errors)} parquet part file(s).")
        for part_file, err in errors[:3]:
            print(f"  - {part_file}: {err}")
        if len(errors) > 3:
            print("  - (additional errors omitted)")

    if not dataframes:
        return None
    if len(dataframes) == 1:
        return dataframes[0]
    return pd.concat(dataframes, ignore_index=True)

def _read_csv_fallback(file_path):
    """Read CSV from disk or lighter_data.zip when parquet is invalid."""
    csv_path = str(Path(file_path).with_suffix('.csv'))
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)

    zip_path = Path(file_path).resolve().parent.parent / 'lighter_data.zip'
    if not zip_path.exists():
        zip_path = Path.cwd() / 'lighter_data.zip'

    inner_name = f"lighter_data/{Path(file_path).with_suffix('.csv').name}"
    if zip_path.exists():
        with zipfile.ZipFile(zip_path) as zf:
            if inner_name in zf.namelist():
                with zf.open(inner_name) as f:
                    return pd.read_csv(f)

    raise FileNotFoundError(f"CSV fallback not found for {file_path}")


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
        """Check if a period has sufficient data (within tolerance) and covers the full time range."""
        mask = (df.index >= period_start) & (df.index < period_end)
        subset = df.loc[mask]
        
        # 1. Data Count Check
        actual_points = subset.shape[0]
        min_required = int(expected_pts * (1.0 - tol))
        if actual_points < min_required:
            return False
            
        # 2. Time Coverage Check (Strict Full Interval)
        # Ensure the data actually reaches the end of the period (minus a small buffer)
        # This prevents accepting a currently-accumulating period as "complete" just because it has many points.
        if subset.empty:
            return False
            
        last_timestamp = subset.index.max()
        # Allow a 5-second buffer for data arrival/bucketing latency
        if last_timestamp < (period_end - pd.Timedelta(seconds=5)):
            return False
            
        return True

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
    if len(period_starts) > 0:
        # Determine the exact time range of the valid periods
        if len(period_starts) > periods_to_keep:
            selected_starts = period_starts[-periods_to_keep:]
        else:
            selected_starts = period_starts
            
        window_start = selected_starts[0]
        window_end = selected_starts[-1] + period
        
        # Enforce strict boundaries for the used data
        mid_price_df = mid_price_df.loc[(mid_price_df.index >= window_start) & (mid_price_df.index < window_end)]
        trades_df = trades_df.loc[(trades_df.index >= window_start) & (trades_df.index < window_end)]

    return mid_price_df, trades_df

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
    print(f"   Intensity Bid (A_bid):            {results['market_data']['A_bid']:.4f}")
    print(f"   Decay Bid (k_bid):                {results['market_data']['k_bid']:.6f}")
    print(f"   Intensity Ask (A_ask):            {results['market_data']['A_ask']:.4f}")
    print(f"   Decay Ask (k_ask):                {results['market_data']['k_ask']:.6f}")
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
