from collections import defaultdict, deque


from typing import *
import os, asyncio, time
import re
import threading
from threading import Lock
from datetime import datetime, timedelta
from matplotlib.pyplot import bar
import pytz
import aiosqlite
import math
from math import *


import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

from ib_async.util import isNan
from ib_async import *
from ib_async import (
    IB,
    Ticker,
    Contract,
    Stock,
    LimitOrder,
    StopOrder,
    util,
    Trade,
    Order,
    BarDataList,
    BarData,
    MarketOrder,
)


# from pnl import IBManager

from timestamps import current_millis
from log_config import log_config, logger

from dev.break_change import VolatilityScanner  # Import our new scanner


from my_util import *

log_config.setup()


async def tv_volatility_scan_ib(contract, barSizeSetting: str = "1 min", ibM=None):
    """
    Scan market data from TWS for volatility patterns.

    Parameters:
    - ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
    - barSizeSetting: Time period of one bar (default: '1 min')
                      Must be one of: '1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
                      '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
                      '20 mins', '30 mins', '1 hour', '2 hours', '3 hours',
                      '4 hours', '8 hours', '1 day', '1 week', '1 month'
    """

    symbol = contract.symbol

    try:
        logger.info(
            f"Processing TWS market data for {symbol} with bar size {barSizeSetting}"
        )

        # Validate bar size setting
        valid_bar_sizes = [
            "1 secs",
            "5 secs",
            "10 secs",
            "15 secs",
            "30 secs",
            "1 min",
            "2 mins",
            "3 mins",
            "5 mins",
            "10 mins",
            "15 mins",
            "20 mins",
            "30 mins",
            "1 hour",
            "2 hours",
            "3 hours",
            "4 hours",
            "8 hours",
            "1 day",
            "1 week",
            "1 month",
        ]

        if barSizeSetting not in valid_bar_sizes:
            logger.error(f"Invalid bar size setting: {barSizeSetting}")
            return {
                "error": f"Invalid bar size setting. Must be one of: {', '.join(valid_bar_sizes)}"
            }

        # Determine durations based on bar size to capture enough data
        # for 9:30-9:36 AM analysis
        if "secs" in barSizeSetting:
            duration_str = "1 D"  # 1 day for seconds data
        elif "min" in barSizeSetting:
            duration_str = "5 D"  # 5 days for minute data
        elif "hour" in barSizeSetting:
            duration_str = "2 W"  # 2 weeks for hourly data
        else:
            duration_str = "1 M"  # 1 month for daily or larger

        # Request historical data from TWS
        start_time = time.time()
        bars = await ibM.ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",  # Current time
            durationStr=duration_str,
            barSizeSetting=barSizeSetting,
            whatToShow="TRADES",
            useRTH=True,
        )
        logger.info(
            f"Data fetch took {time.time() - start_time:.2f} seconds for {len(bars) if bars else 0} bars"
        )

        if not bars or len(bars) < 2:
            logger.error(f"Insufficient data received for {symbol}")
            return {"error": f"Insufficient data received for {symbol}"}

        # Convert IB bars to pandas DataFrame with proper timezone
        df = pd.DataFrame(
            {
                "datetime": [pd.to_datetime(bar.date) for bar in bars],
                "symbol": symbol,
                "open": [bar.open for bar in bars],
                "high": [bar.high for bar in bars],
                "close": [bar.close for bar in bars],
                "low": [bar.low for bar in bars],
                "volume": [bar.volume for bar in bars],
            }
        )

        # Use our scanner to analyze the data directly
        scanner = VolatilityScanner()
        start_time = time.time()
        matches = scanner.scan_dataframe(df)
        logger.info(
            f"Scan took {time.time() - start_time:.2f} seconds, found {len(matches)} matches"
        )

        # Prepare result data
        match_data = []
        for match in matches:
            # Handle different datetime formats
            try:
                dt_str = (
                    match["datetime"].strftime("%Y-%m-%d %H:%M:%S")
                    if hasattr(match["datetime"], "strftime")
                    else str(match["datetime"])
                )

                match_data.append(
                    {
                        "datetime": dt_str,
                        "symbol": match["symbol"],
                        "open": float(match["open"]),
                        "close": float(match["close"]),
                        "change_pct": float(match["change_pct"]),
                        "volume": float(match["volume"]),
                    }
                )
                logger.info(
                    f"Added match: {dt_str}, change: {match['change_pct']}%, volume: {match['volume']}"
                )
            except Exception as e:
                logger.error(f"Error formatting match data: {e}")
                # Continue processing other matches even if one fails

        # Build the response with useful information
        response = {
            "symbol": symbol,
            "bar_size": barSizeSetting,
            "data_points": len(df),
            "matches_found": len(matches),
            "matches": match_data,
        }

        # Add time range info if available
        if not df.empty:
            response["time_range"] = {
                "start": df["datetime"].min().strftime("%Y-%m-%d %H:%M:%S"),
                "end": df["datetime"].max().strftime("%Y-%m-%d %H:%M:%S"),
            }

        return response
    except Exception as e:
        logger.error(f"Error in tv_volatility_scan_ib: {e}")
        return {"error": str(e), "type": str(type(e).__name__)}
