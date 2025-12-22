import pandas as pd
import os

def read_parquet_with_fallback(file_path, columns=None):
    """Read parquet with pyarrow first, then fastparquet as a fallback."""
    last_err = None
    for engine in ("pyarrow", "fastparquet"):
        try:
            return pd.read_parquet(file_path, engine=engine, columns=columns)
        except Exception as exc:
            last_err = exc
            continue
    raise last_err

data_folder = 'lighter_data'
files = [f for f in os.listdir(data_folder) if f.endswith('.parquet') and 'prices' in f]

if not files:
    print("No price parquet files found!")
else:
    file_path = os.path.join(data_folder, files[0])
    print(f"Reading {file_path}...")
    try:
        df = read_parquet_with_fallback(file_path)
        print("Columns:", df.columns.tolist())
        print("\nLast 5 rows:")
        print(df.tail(5).to_string())
        
        # Check sorting validation
        # Bid 0 should be > Bid 1
        # Ask 0 should be < Ask 1
        last_row = df.iloc[-1]
        
        bid0 = last_row.get('bid_price_0')
        bid1 = last_row.get('bid_price_1')
        ask0 = last_row.get('ask_price_0')
        ask1 = last_row.get('ask_price_1')
        
        print("\n--- Validation ---")
        if bid0 is not None and bid1 is not None:
            print(f"Bid 0 ({bid0}) > Bid 1 ({bid1})? {bid0 >= bid1}")
        if ask0 is not None and ask1 is not None:
            print(f"Ask 0 ({ask0}) < Ask 1 ({ask1})? {ask0 <= ask1}")

    except Exception as e:
        print(f"Error reading parquet: {e}")
