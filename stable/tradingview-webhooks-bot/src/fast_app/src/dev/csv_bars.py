import os
import asyncio
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import aiosqlite

from my_util import (
    convert_pine_timeframe_to_barsize,
    ensure_agg_table,
    upsert_agg_bar,
)
local_directory = Path("C:\\Users\\mikep\\OneDrive\\Downloads Chrome")
BASE_DIR = Path(__file__).parent
CSV_DIR = BASE_DIR.parent / "bar-export-csv-data"
DB_PATH = BASE_DIR.parent / "orders.db"
def get_latest_csv(directory):
    """Retrieve the latest CSV file from the specified directory."""
    csv_files = list(Path(directory).glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV files found in directory")
    return str(max(csv_files, key=os.path.getctime))
async def import_csv_file(file_path: Path):
    """
    Import one CSV of ohlc+volume into bars_{N}s table.
    Filename format: EXCHANGE_SYMBOL, BAR.csv (ignore trailing "(1)")
    """
    # parse filename
    name = file_path.stem
    # drop any trailing parenthesis
    name = name.split("(",1)[0].strip()
    # split "NASDAQ_NVDA, 5S"
    exch_sym, tf = [p.strip() for p in name.split(",",1)]
    exchange, symbol = exch_sym.split("_",1)
    # convert timeframe → (barSizeSetting, interval_seconds)
    barSizeSetting, interval = await convert_pine_timeframe_to_barsize(tf)

    # load CSV
    df = pd.read_csv(file_path)
    now_ts = datetime.now(timezone.utc).timestamp()
    cutoff = now_ts - 24 * 3600
    if "time" not in df.columns or not (df["time"].dropna().astype(float) >= cutoff).any():
        print(f"Skipping {file_path.name}: no bars in the last 24 hours")
        return

    # connect DB
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        # ensure target table exists
        await ensure_agg_table(db, interval)
        # insert each row
        for _, row in df.iterrows():
            ts = int(row["time"])
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            bar = {
                "time":   dt,
                "open":   float(row["open"]),
                "high":   float(row["high"]),
                "low":    float(row["low"]),
                "close":  float(row["close"]),
                "volume": float(row["volume"]),
            }
            # upsert into bars_{interval}s
            await upsert_agg_bar(db, symbol, interval, dt, bar)
    print(f"Imported {file_path.name} → bars_{interval}s for {symbol}")

async def import_all(local=False):
    tasks = []
    
    
    if local:
        print(f"Importing CSV files from local directory: {local_directory}")
        for f in local_directory.glob("*.csv"):
            print(f"Found local CSV file: {f.name}")
            tasks.append(import_csv_file(f))
        await asyncio.gather(*tasks)
        
        
    else:
        # import all CSV files in the directory
        for f in CSV_DIR.glob("*.csv"):
            tasks.append(import_csv_file(f))
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(import_all(local=True))