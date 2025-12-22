import os
import re
import pandas as pd
import utils

def _resolve_price_file(data_folder: str) -> str | None:
    files = [f for f in os.listdir(data_folder) if f.startswith('prices_') and f.endswith('.parquet')]
    if not files:
        return None
    files.sort()
    base_name = re.sub(r"_part\\d+\\.parquet$", ".parquet", files[0])
    return os.path.join(data_folder, base_name)

data_folder = 'lighter_data'
file_path = _resolve_price_file(data_folder)

if not file_path:
    print("No price parquet files found!")
else:
    print(f"Reading {file_path}...")
    try:
        df = utils.read_parquet_data(file_path)
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
