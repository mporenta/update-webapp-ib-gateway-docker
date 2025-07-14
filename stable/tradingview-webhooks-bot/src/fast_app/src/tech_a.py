


import talib as ta  
from datetime import datetime as dt, time as dt_time
import numpy as np
from pathlib import Path
from datetime import datetime as dt
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from log_config import log_config, logger
from ib_async import *
from tv_ticker_price_data import price_data_dict, barSizeSetting_dict
import json
from fastapi.encoders import jsonable_encoder
from dataclasses import asdict
from my_util import clean_nan

def get_ib_pivots(df: pd.DataFrame, *, left: int = 5, right: int = 5) -> pd.DataFrame:
    df = df.copy()
    df.set_index("date", inplace=True)

    # Classic pivots
    df["PP"] = (df.high + df.low + df.close) / 3
    df["R1"] = 2 * df.PP - df.low
    df["S1"] = 2 * df.PP - df.high
    df["R2"] = df.PP + (df.high - df.low)
    df["S2"] = df.PP - (df.high - df.low)
    df[["PP", "R1", "R2", "S1", "S2"]] = df[["PP", "R1", "R2", "S1", "S2"]].shift(1)

    # Swing pivots
    df["pivotHigh"] = df.high.where(
        df.high == df.high.rolling(window=2 * right + 1, center=True).max()
    )
    df["pivotLow"] = df.low.where(
        df.low == df.low.rolling(window=2 * left + 1, center=True).min()
    )

    # Keep rows where PP is available
    df = df.dropna(subset=["PP"])

    # Convert NaNs in swing-pivot columns to 0.0 without inplace
    df["pivotHigh"] = df["pivotHigh"].fillna(0.0)
    df["pivotLow"]  = df["pivotLow"].fillna(0.0)

    logger.debug(df)
    return df



async def plot_pivots(df_min: pd.DataFrame, last_daily: pd.Series, symbol: str) -> None:
    fig = go.Figure()

    # 1-minute candles
    fig.add_trace(go.Candlestick(
        x=df_min.index.to_pydatetime(),          # ensure pure Python datetimes
        open=df_min.open, high=df_min.high,
        low=df_min.low,  close=df_min.close,
        name=f"{symbol} 1-min"
    ))

    # horizontal lines ...

    # layout
    fig.update_layout(
        title=f"{symbol} — intraday pivots",
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        xaxis_title="Time (ET)",
        yaxis_title="Price",
    )

    # ← NEW: no hard-coded dtick, limit tick count, tilt labels
    fig.update_xaxes(
        type="date",
        tickformat="%m-%d %H:%M",   # 06-28 11:00, 11:30 …
        nticks=10,                  # let Plotly pick ~10 evenly-spaced ticks
        tickangle=-45,              # tilt so text doesn’t collide
        ticklabelmode="period"      # cleaner label placement
    )

    
    outfile = f"{symbol}_{dt.utcnow():%Y%m%d_%H%M%S}.png"
    pio.write_image(fig, outfile, engine="kaleido")
    logger.info("Pivot chart saved → %s", outfile)



def _aggs_to_df(aggs: list[dict]) -> pd.DataFrame:
    """
    Convert Polygon agg dicts → DataFrame indexed by *local Eastern* time.
    """
    EASTERN = "America/New_York"
    df = pd.DataFrame(aggs).rename(columns={
        "o": "open", "h": "high", "l": "low",
        "c": "close", "v": "volume", "t": "timestamp"
    })

    # 1) UTC → 2) tz-aware Eastern → 3) make naive  
    df["date"] = (
        pd.to_datetime(df["timestamp"], unit="ms", utc=True)
          .dt.tz_convert(EASTERN)
          .dt.tz_localize(None)
    )

    # Always sort so rolling-windows work as expected
    df = (
        df.set_index("date")[["open", "high", "low", "close", "volume"]]
          .sort_index()
          .copy()
    )
    return df


def safe_float(x):
    try:
        v = float(x)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except Exception:
        return None
async def gpt_data_poly(symbol: str, daily_aggs: list[dict], minute_aggs: list[dict], ibData=False) -> dict:
    try:
        data= {}
        logger.info("1 Processing GPT data from Polygon.io")
        df_d = daily_aggs
        df_m = minute_aggs
        
        
        
       
        # convert raw aggs to DataFrames
        if not ibData:
            logger.info(f"Polygon.io  data being used with minute_aggs len {len(minute_aggs)} - sending aggs to DataFrame")
            df_d = _aggs_to_df(daily_aggs)
            df_m = _aggs_to_df(minute_aggs)

        if df_d.empty or df_m.empty:
            raise ValueError("No historical data available")
        logger.info(f"daily_aggs len {len(daily_aggs)} is ibData? {ibData}")
        logger.debug(daily_aggs)
        
        
        logger.info(f"minute_aggs len {len(minute_aggs)} ")
        logger.debug(minute_aggs)
        logger.info("2 Processing GPT data from Polygon.io")
        logger.info(f"Daily DataFrame shape: {df_d.shape}, Minute DataFrame shape: {df_m.shape}")
        logger.info(f"df_d len {len(df_d)} Polygon.io aggs to DataFrame")
        logger.debug(df_d)
        logger.info(f"df_m len {len(df_m)} Polygon.io aggs to DataFrame")
        logger.debug(df_m)
        # --- usage inside gpt_data_poly ----------------------------------------------
        # Daily pivots (no chart)
        df_d = get_ib_pivots(df_d.reset_index(), left=5, right=5)

        # 1-min pivots + chart
        df_m = get_ib_pivots(df_m.reset_index(), left=10, right=10)
        logger.info(f"df_d len {len(df_d)} Polygon.io get_ib_pivots to DataFrame")
        logger.debug(df_d)
        logger.info(f"df_m len {len(df_m)} Polygon.io get_ib_pivots to DataFrame")
        logger.debug(df_m)
        

        # extract pivot records
        def extract_pivots(df, field):
            return [
                {"date": idx.isoformat(), "level": float(val)}
                for idx, val in df[field].items()
                if not np.isnan(val) and val > 0.0
            ]

        logger.info("3 Processing GPT data from Polygon.io")
        pivotHighs_d = extract_pivots(df_d, "pivotHigh")
        pivotLows_d = extract_pivots(df_d, "pivotLow")
        pivotHighs_m = extract_pivots(df_m, "pivotHigh")
        pivotLows_m = extract_pivots(df_m, "pivotLow")
        logger.info("3.5 Processing GPT data from Polygon.io")
       

        # DAILY INDICATORS
        close_d = df_d["close"].values
        df_d["10DAY_AVG_VOLUME"] = ta.SMA(df_d["volume"].values, timeperiod=10)
        df_d["10DAY_RELATIVE_VOL"] = df_d["volume"].pct_change().rolling(window=10).mean() * 100
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
        logger.info(f"4 Processing GPT data from Polygon.io pivotHighs_d {pivotHighs_d}")
        

        

        levels = {
            "prev_high":   safe_float(prev_d["high"]),
            "prev_low":    safe_float(prev_d["low"]),
            "prev_close":  safe_float(prev_d["close"]),
            "prev_high_m": safe_float(prev_m["high"]),
            "prev_low_m":  safe_float(prev_m["low"]),
            "prev_close_m": safe_float(prev_m["close"]),
            "pre_market_high": safe_float(premkt_high),
            "pre_market_low":  safe_float(premkt_low),
            "pivotHighs_d": pivotHighs_d,
            "pivotLows_d": pivotLows_d,
            "pivotHighs_m": pivotHighs_m,
            "pivotLows_m": pivotLows_m,

            # Daily indicators (most recent)
            "current_10DAY_AVG_VOLUME": safe_float(df_d["10DAY_AVG_VOLUME"].iloc[-1]),
            "current_10DAY_REL_VOL":   safe_float(df_d["10DAY_RELATIVE_VOL"].iloc[-1]),
            "current_SMA5":   safe_float(df_d["SMA5"].iloc[-1]),
            "current_SMA10":  safe_float(df_d["SMA10"].iloc[-1]),
            "current_SMA20":  safe_float(df_d["SMA20"].iloc[-1]),
            "current_SMA50":  safe_float(df_d["SMA50"].iloc[-1]),
            "current_SMA200": safe_float(df_d["SMA200"].iloc[-1]),
            "current_RSI14":  safe_float(df_d["RSI14"].iloc[-1]),
            "current_MACD":   safe_float(df_d["MACD"].iloc[-1]),
            "current_MACDs":  safe_float(df_d["MACDs"].iloc[-1]),
            "current_MACDh":  safe_float(df_d["MACDh"].iloc[-1]),
            "current_ATR14":  safe_float(df_d["ATR14"].iloc[-1]),
            
            "current_ATR10":  safe_float(df_d["ATR10"].iloc[-1]),
            "current_EMA20":  safe_float(df_d["EMA20"].iloc[-1]),

            # 1-minute indicators (most recent)
            "current_EMA21":   safe_float(df_m["EMA21"].iloc[-1]),
            "current_EMA20_m": safe_float(df_m["EMA20"].iloc[-1]),
            "current_EMA9":    safe_float(df_m["EMA9"].iloc[-1]),
            "current_BBU20":   safe_float(df_m["BBU20"].iloc[-1]),
            "current_BBM20":   safe_float(df_m["BBM20"].iloc[-1]),
            "current_BBL20":   safe_float(df_m["BBL20"].iloc[-1]),
            "current_RSI14_m": safe_float(df_m["RSI14"].iloc[-1]),
            "current_ATR14_m": safe_float(df_m["ATR14"].iloc[-1]),
            "current_ATR10_m": safe_float(df_m["ATR10"].iloc[-1])
        }

        def extract_series(df, fields):
            out = []
            for idx, row in df.iterrows():
                entry = {"date": idx.isoformat()}
                for f in fields:
                    v = row[f]
                    # if it's a numpy NaN or pandas NA, convert to None
                    entry[f] = None if (pd.isna(v) or not np.isfinite(v)) else float(v)
                out.append(entry)
            return out


        # define which indicators to export
        daily_fields  = ["open", "high", "low", "close","10DAY_AVG_VOLUME","10DAY_RELATIVE_VOL","SMA5","SMA10","SMA20","SMA50","SMA200",
                         "RSI14","MACD","MACDs","MACDh","ATR14","ATR10","EMA20",]
        minute_fields = ["EMA21","EMA20","EMA9",
                         "BBU20","BBM20","BBL20","RSI14","ATR14","ATR10","VWAP"]

        # build the per-bar lists
        daily_indicators  = extract_series(df_d, daily_fields)
        minute_indicators = extract_series(df_m, minute_fields)
        
        # Load your daily_indicators array into DataFrame
        df = pd.DataFrame(daily_indicators)
        logger.debug(f"5.25 Processing GPT data from Polygon.io df of daily_indicators: {pd.DataFrame(daily_indicators)}")

        # Find the most recent row with all critical indicators present
        valid_daily = df.dropna(subset=['SMA200', 'RSI14', 'MACD', 'ATR14'])
        logger.info(f"5.5 Processing GPT data from Polygon.io valid_daily: {valid_daily}")
        if valid_daily.shape[0] > 0:
            latest_valid_daily = valid_daily.iloc[-1]
        else:
            latest_valid_daily = {}

        df_m_recent = pd.DataFrame(minute_indicators)
        valid_minute = df_m_recent.dropna(subset=["EMA21","EMA20","EMA9","BBU20","BBM20","BBL20","RSI14","ATR14","ATR10","VWAP"])
        if valid_minute.shape[0] > 0:
            latest_valid_minute = valid_minute.iloc[-1]
        else:
            latest_valid_minute = {}

        logger.info(f"6 Processing GPT data from Polygon.io latest_valid_daily: {latest_valid_daily}")
        logger.info(f"7 Processing GPT data from Polygon.io latest_valid_minute: {latest_valid_minute}")


        # your levels dict stays the same…
        data = {
            "levels": levels,
            "current_price": safe_float(close_m[-1]),
            "current_vwap" : safe_float(vwap[-1]),

            # per-bar indicator values
            "daily_data" :{
            "daily_indicators":  daily_indicators,
            "latest_valid_daily_indicators":  latest_valid_daily,
            },
            "minute_data": {
            "minute_indicators": minute_indicators,
            "latest_valid_minute_indicators": latest_valid_minute
            },
            
        }
        logger.info(f"GPT data for {symbol} processed successfully")
       

        return data
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"Error processing GPT data for {symbol}: {e}")
        return {
            "levels": {},
            "current_price": None,
            "current_vwap": None
        }




async def ema_check(contract: Contract, ib: IB=None, barSizeSetting=None) -> str:
    """
    Check if the last close is above the EMA for the given period.
    """
    try:
        barSizeSetting= barSizeSetting_dict[contract.symbol]
        if barSizeSetting is None:
            barSizeSetting = "1 min"
        df_d, df_m, daily_bars, minute_bars=await ib_data(contract, ib, barSizeSetting)  # Ensure df is populated with historical data
        if df_m.empty:
            raise ValueError("No historical data available for the contract")
        logger.info(f"Checking EMA for {contract.symbol} with df_m length {len(df_m)}")
        emaCheck = ""
        
        logger.debug(f"Checking EMA for {contract.symbol} ")
        
        snapshot=await price_data_dict.get_ticker(contract.symbol)
        snapshot_json_cleaned = {}
       
        try:
        
            # turn the dataclass into a dict…
            data = asdict(snapshot)
            # drop the Ticker object (not serializable) and any others you don’t want
            data.pop("ticker", None)
            # serialize datetime → ISO string
            if data.get("time") is not None:
                data["time"] = data["time"].isoformat()
            # now clean out NaNs/inf
            snapshot_json_cleaned = clean_nan(data)

        except Exception as e:
            logger.error(f"Error processing json ticker data for {contract.symbol}: {e}")
            return None


        # Calculate the EMA
        close_m = df_m["close"].values
        last_close = close_m[-1]
        logger.debug(f"Checking EMA for {contract.symbol} with close_m  {close_m}")
        df_m["EMA9"] = ta.EMA(close_m, timeperiod=9)
        current_EMA9 = safe_float(df_m["EMA9"].iloc[-1])

        logger.debug(f"Checking EMA for {contract.symbol} with current_EMA9  {current_EMA9}")
        

        # Get the last close and last EMA value
        
        

        logger.debug(f"Checking EMA for {contract.symbol} with last_close  {last_close}")
        if current_EMA9 < safe_float(last_close):
            emaCheck = "LONG"
        elif current_EMA9 > safe_float(last_close):
            emaCheck = "SHORT" 
        else:
            emaCheck = None

        # Check if the last close is above the EMA
        logger.info(f"EMA check for {contract.symbol}: last_close={last_close}, current_EMA9={current_EMA9}, result={emaCheck}")
        return emaCheck, snapshot_json_cleaned
    except Exception as e:
        logger.error(f"Error in ema_check for {contract.symbol}: {e}")
        return None
async def ib_data(contract: Contract, ib: IB = None, barSizeSetting=None, daily=False,  durationStr_min = None) -> dict:
    """
    Pull daily + 1-min history, enrich with TA-Lib indicators, and return JSON-ready dict.
    """
    try:
        if durationStr_min is None:
            durationStr_min = "2700 S"
        daily_bars = []
        df_d = pd.DataFrame()
        logger.info(f"ib_data for {contract.symbol} started, will return data asynchronously for contract : {contract}")
        if barSizeSetting is None:
            barSizeSetting = "1 min"
        
        # -------- download history -------------------------------------------------
        if daily:
            daily_bars = await ib.reqHistoricalDataAsync(
                contract=contract, 
                endDateTime="", 
                durationStr="1 Y",
                barSizeSetting="1 day", 
                whatToShow="TRADES", 
                useRTH=True, 
                formatDate=1,
            )
            df_d = util.df(daily_bars)
            if df_d.empty:
                logger.warning(f"Empty daily dataframes for {contract.symbol}")
                return {"error": "No daily historical data available"}
        minute_bars = await ib.reqHistoricalDataAsync(
            contract=contract, 
            endDateTime="", 
            durationStr=durationStr_min,
            barSizeSetting=barSizeSetting, 
            whatToShow="TRADES", 
            useRTH=False, 
            formatDate=1,
        )
       

        # Fixed: Use util.df correctly from ib_insync
        logger.info(f"fetched ib_data for {contract.symbol} with barSizeSetting {barSizeSetting} started")
        df_m = util.df(minute_bars)
        
        
        if df_m.empty:
            logger.warning(f"Empty dataframes for {contract.symbol}")
            return {"error": "No historical data available"}

        logger.info(f"fetched ib_data for {contract.symbol} with barSizeSetting {barSizeSetting} and df_m length {len(df_m)}")

        return df_d, df_m, daily_bars, minute_bars
          

    except Exception as e:
        logger.error(f"Error in gpt_data for {contract.symbol}: {e}")
        return {"error": str(e)}
