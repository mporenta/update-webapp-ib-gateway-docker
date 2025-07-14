import os
import asyncio
from pathlib import Path
import pandas as pd
from ib_async import IB, Contract
from log_config import log_config, logger
log_config.setup()
# ─── Configuration ──────────────────────────────────────────────────────────────
IB_HOST = "127.0.0.1"
IB_PORT = 4002
IB_CLIENT_ID = 1111
OUTPUT_FILENAME = "symbols_by_volume.txt"


def get_latest_csv(directory: str) -> str:
    directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
    """Return the path to the most recently modified CSV in `directory`."""
    csv_files = list(Path(directory).glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {directory}")
    latest = max(csv_files, key=lambda p: p.stat().st_mtime)
    return str(latest)


async def get_contract(ib: IB, symbol: str) -> Contract | None:
    """Qualify and return an IB Contract for a given stock symbol."""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"

    qualified = await ib.qualifyContractsAsync(contract)
    if not qualified:
        return None
    return qualified[0]


async def fetch_10min_volume(ib: IB, contract: Contract) -> int:
    """
    Request 1‑minute bars for the last 10 minutes and return the total volume.
    Uses durationStr="600 S" (600 seconds = 10 minutes) and barSizeSetting="1 min".
    """
    bars = await ib.reqHistoricalDataAsync(
        contract,
        endDateTime="",              # use current time as end
        durationStr="600 S",         # 600 seconds → last 10 minutes
        barSizeSetting="1 min",
        whatToShow="TRADES",
        useRTH=False,
        formatDate=1
    )
    return sum(bar.volume for bar in bars) if bars else 0


async def main():
    directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
    csv_path = get_latest_csv(directory)

    # Read Symbol and Exchange columns
    df = pd.read_csv(csv_path, usecols=["Symbol", "Exchange"])
    df = df.dropna(subset=["Symbol", "Exchange"])
    symbols = df["Symbol"].astype(str).unique().tolist()

    # Build a map: symbol → exchange
    exchange_map = {
        row.Symbol: row.Exchange
        for row in df.itertuples(index=False)
    }

    ib = IB()
    await ib.connectAsync(host=IB_HOST, port=IB_PORT, clientId=IB_CLIENT_ID)

    volumes: dict[str, int] = {}
    for symbol in symbols:
        contract = await get_contract(ib, symbol)
        if not contract:
            print(f"[Warning] Could not qualify contract for {symbol}; skipping.")
            continue
        vol = await fetch_10min_volume(ib, contract)
        volumes[symbol] = vol

    ib.disconnect()

    # Build DataFrame and sort by volume descending
    result_df = pd.DataFrame({
        "Symbol": list(volumes.keys()),
        "Volume_10min": list(volumes.values()),
    })
    result_df = result_df.sort_values("Volume_10min", ascending=False)

    # Print table to terminal
    print(result_df.to_string(index=False))

    # Write comma-separated "Exchange:Symbol" to .txt file (in sorted order)
    output_path = os.path.join(directory, OUTPUT_FILENAME)
    directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
    with open(output_path, "w") as f:
        f.write(",".join(
            f"{exchange_map.get(symbol, '')}:{symbol}"
            for symbol in result_df["Symbol"]
        ))

    print(f"\nWritten sorted symbols to: {output_path}")


if __name__ == "__main__":
    log_config.setup()
    asyncio.run(main())
