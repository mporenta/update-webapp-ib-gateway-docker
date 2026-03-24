from __future__ import annotations

import sys

from ib_async import IB, Stock


def main() -> int:
    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=8884488)
    print(f"IB connected: {ib.isConnected()}")

    contract = ib.qualifyContracts(Stock("NVDA", "SMART", "USD"))[0]
    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr="4 D",
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=1,
    )
    if not bars:
        print("No bars received")
        ib.disconnect()
        return 1

    print(f"Received {len(bars)} bars")
    print(f"Last close: {bars[-1].close}")
    ib.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
