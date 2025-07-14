from math import log
from ib_insync import *
import asyncio
import talib as ta  
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import time as dt_time
import logging
logger=logging.basicConfig(level=logging.INFO)
from my_util import add_pivot_points
from models import TickerRequest
from datetime import datetime, timezone, timedelta, time as dt_time



# --- Configure logging ---
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# --- Initialize FastAPI and Polygon client ---
app = FastAPI()
from polygon import RESTClient

poly_client = RESTClient(api_key="YOUR_POLYGON_API_KEY")


def _aggs_to_df(aggs: list[dict]) -> pd.DataFrame:
    """
    Convert a list of Polygon.io agg dicts into a DataFrame with datetime index.
    """
    df = pd.DataFrame(aggs)
    df = df.rename(columns={
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "t": "timestamp"
    })
    # timestamp is in milliseconds since epoch UTC
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("date")[["open", "high", "low", "close", "volume"]].copy()
    return df


async def gpt_data_poly(daily_aggs: list[dict], minute_aggs: list[dict]) -> dict:
    # convert raw aggs to DataFrames
    df_d = _aggs_to_df(daily_aggs)
    df_m = _aggs_to_df(minute_aggs)

    if df_d.empty or df_m.empty:
        raise ValueError("No historical data available")

    # calculate pivot points
    df_d = add_pivot_points(df_d.reset_index(), left=5, right=5).set_index("date")
    df_m = add_pivot_points(df_m.reset_index(), left=10, right=10).set_index("date")

    # extract pivot records
    def extract_pivots(df, field):
        piv = df[df[field].notna()][[field]].copy()
        return [
            {"date": idx.isoformat(), "level": float(val)}
            for idx, val in piv[field].iteritems()
        ]

    pivot_highs_d = extract_pivots(df_d, "pivot_high")
    pivot_lows_d = extract_pivots(df_d, "pivot_low")
    pivot_highs_m = extract_pivots(df_m, "pivot_high")
    pivot_lows_m = extract_pivots(df_m, "pivot_low")

    # DAILY INDICATORS
    close_d = df_d["close"].values
    df_d["SMA5"] = ta.SMA(close_d, timeperiod=5)
    df_d["SMA10"] = ta.SMA(close_d, timeperiod=10)
    df_d["SMA20"] = ta.SMA(close_d, timeperiod=20)
    df_d["SMA50"] = ta.SMA(close_d, timeperiod=50)
    df_d["SMA200"] = ta.SMA(close_d, timeperiod=200)
    df_d["RSI14"] = ta.RSI(close_d, timeperiod=14)
    macd, macds, macdh = ta.MACD(close_d, 12, 26, 9)
    df_d["MACD"], df_d["MACDs"], df_d["MACDh"] = macd, macds, macdh
    high_d, low_d = df_d["high"].values, df_d["low"].values
    df_d["ATR14"] = ta.ATR(high_d, low_d, close_d, timeperiod=14)
    df_d["ATR10"] = ta.ATR(high_d, low_d, close_d, timeperiod=10)
    df_d["EMA20"] = ta.EMA(close_d, timeperiod=20)

    # 1-MINUTE INDICATORS
    close_m = df_m["close"].values
    high_m, low_m, vol_m = df_m["high"].values, df_m["low"].values, df_m["volume"].values
    df_m["EMA21"] = ta.EMA(close_m, timeperiod=21)
    df_m["EMA20"] = ta.EMA(close_m, timeperiod=20)
    df_m["EMA9"] = ta.EMA(close_m, timeperiod=9)
    up, mid, lowb = ta.BBANDS(close_m, timeperiod=20, nbdevup=2, nbdevdn=2)
    df_m["BBU20"], df_m["BBM20"], df_m["BBL20"] = up, mid, lowb
    df_m["RSI14"] = ta.RSI(close_m, timeperiod=14)
    df_m["ATR14"] = ta.ATR(high_m, low_m, close_m, timeperiod=14)
    df_m["ATR10"] = ta.ATR(high_m, low_m, close_m, timeperiod=10)

    # VWAP
    vwap = np.cumsum(close_m * vol_m) / np.maximum(np.cumsum(vol_m), 1)
    df_m["VWAP"] = vwap

    # key levels
    prev_d = df_d.iloc[-1]
    prev_m = df_m.iloc[-1]

    # pre-market high/low 04:00-09:29 ET
    premkt = df_m.between_time(dt_time(4, 0), dt_time(9, 29))
    premkt_high = float(premkt["high"].max()) if not premkt.empty else None
    premkt_low = float(premkt["low"].min()) if not premkt.empty else None

    levels = {
        "prev_high": float(prev_d["high"]),
        "prev_low": float(prev_d["low"]),
        "prev_close": float(prev_d["close"]),
        "prev_high_m": float(prev_m["high"]),
        "prev_low_m": float(prev_m["low"]),
        "prev_close_m": float(prev_m["close"]),
        "pre_market_high": premkt_high,
        "pre_market_low": premkt_low,
        "pivot_highs_d": pivot_highs_d,
        "pivot_lows_d": pivot_lows_d,
        "pivot_highs_m": pivot_highs_m,
        "pivot_lows_m": pivot_lows_m,
    }

    return {
        "levels": levels,
        "current_price": float(close_m[-1]),
        "current_vwap": float(vwap[-1])
    }


@app.post("/gpt_poly")
async def gpt_poly_prices(ticker: TickerRequest):
    try:
        now = datetime.now(timezone.utc)
        # fetch 3 days of 1-min bars
        aggs_one_min = list(poly_client.list_aggs(
            ticker=ticker.ticker,
            multiplier=1,
            timespan="minute",
            from_=now - timedelta(days=3),
            to=now,
            adjusted=True,
            sort="asc",
            limit=5000,
        ))
        # fetch 60 days of daily bars
        aggs_daily = list(poly_client.list_aggs(
            ticker=ticker.ticker,
            multiplier=1,
            timespan="day",
            from_=now - timedelta(days=60),
            to=now,
            adjusted=True,
            sort="asc",
            limit=5000,
        ))

        if not aggs_one_min or not aggs_daily:
            raise HTTPException(status_code=404,
                detail="Insufficient historical data")

        data = await gpt_data_poly(aggs_daily, aggs_one_min)
        return JSONResponse(content=jsonable_encoder(data))

    except HTTPException as he:
        logger.error(f"Data fetch error for {ticker.ticker}: {he.detail}")
        return JSONResponse(status_code=he.status_code, content={"error": he.detail})
    except Exception as e:
        logger.error(f"Unexpected error in /gpt_poly: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
