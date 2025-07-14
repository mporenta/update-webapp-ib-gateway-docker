# my_util.py
import json
from typing import Optional, List, Tuple
from pathlib import Path
import asyncio, os, time, pytz
import re
from ib_async import *
from ib_async.util import isNan
import math
import talib as ta  

import pandas as pd
from log_config import log_config, logger
from collections import defaultdict
from pandas_ta.volatility import atr
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
from dataclasses import is_dataclass, asdict
from datetime import datetime, timezone, timedelta, date
from ib_async.objects import RealTimeBar
import aiosqlite
from bar_size import convert_pine_timeframe_to_barsize
from typing import Dict, Any
from tv_ticker_price_data import  tv_store_data, price_data_dict, daily_volatility, yesterday_close, yesterday_close_bar, timeframe_dict, volatility_stop_data




from aiosqlite import Connection
from models import (
    OrderRequest,
    AccountPnL,
    TickerResponse,
    QueryModel,
    WebhookRequest,
    TickerSub,
    TickerRefresh,
    TickerRequest,
    PriceSnapshot,
    VolatilityStopData,
)
# my_util.py (or wherever you like)
last_daily_bar_dict=price_data_dict.last_daily_bar_dict

active_tickers: Dict[str, Ticker] = defaultdict(Ticker)
# my_util.py

def _is_null(x: Any) -> bool:
    """True for None, NaN, or ±inf."""
    if x is None:
        return True
    if isinstance(x, (float, np.floating)):
        return math.isnan(x) or math.isinf(x)
    if isinstance(x, pd._libs.tslibs.nattype.NaTType):
        return True
    return False

def to_clean_dict(obj: Any) -> Any:            # <-- single public helper
    """
    Recursively turn *anything* (dataclass, pydantic BaseModel,
    dict / list / tuple / deque / numpy scalar …) into a dict / list
    **with all null-like entries removed**.
       atr_series = atr(
            high       = data["high"],
            low        = data["low"],
            close      = data["close"],
            length     = atrlen
        
        )
    """
    # dataclass → dict first
    if is_dataclass(obj):
        obj = asdict(obj)

    # pydantic model → dict
    if hasattr(obj, "model_dump"):
        obj = obj.model_dump()

     

    # primitive
    if not isinstance(obj, (dict, list, tuple, set)):
        return None if _is_null(obj) else obj

    # dict
    if isinstance(obj, dict):
        cleaned: Dict[str, Any] = {}
        for k, v in obj.items():
            v_clean = to_clean_dict(v)
            if v_clean is not None:            # drop null-like keys
                cleaned[k] = v_clean
        return cleaned or None                 # drop empty dicts

    # list / tuple / set
    if isinstance(obj, (list, tuple, set)):
        cleaned_list: List[Any] = [
            v for v in (to_clean_dict(x) for x in obj) if v is not None
        ]
        return cleaned_list or None            # drop empty lists

    # fallback (never hit with usual types)
    return None

# Handle NaN values in the response to make it JSON compliant
def clean_nan(o):
    """Recursively replaces all NaN/inf values in a dict/list with None"""
    try:
        if isinstance(o, dict):
            is_dict = {k: clean_nan(v) for k, v in o.items()}
            #logger.debug((f"isinstance dict: {is_dict}")
            return {k: clean_nan(v) for k, v in o.items()}
        elif isinstance(o, list):
            #logger.debug((f"isinstance list: {[clean_nan(v) for v in o]}")
            return [clean_nan(v) for v in o]
        elif isinstance(o, float):
            if np.isnan(o) or np.isinf(o):
                return 0.0
            #logger.debug((f"isinstance float: {o}")
            return o
        elif isinstance(o, str):
            # Handle string representations of NaN/inf
            if o.lower() in ['nan', 'inf', '-inf', 'null']:
                return 0.0
            #logger.debug((f"isinstance str: {o}")
            return o
        elif o is None:
            return 0.0
        
        else:
            # Handle numpy types that might not be caught above
            try:
                if hasattr(o, 'dtype') and np.issubdtype(o.dtype, np.floating):
                    #logger.debug((f"hasattr o.dtype: {o}")
                    if np.isnan(o) or np.isinf(o):
                        return 0.0
            except (TypeError, ValueError):
                pass
            return o
    except Exception as e:  
        logger.error(f"Error cleaning NaN values: {e}")
        return None
    
async def vol_stop(data: pd.DataFrame, atrlen: int, atrfactor: float):
    """
    
       atr_series = atr(
            high       = data["high"],
            low        = data["low"],
            close      = data["close"],
            length     = atrlen
        
        )
    """
    
    try:
        # 1) average of high+low
        logger.debug(f"Calculating volatility stop for bars with atrlen={atrlen} and atrfactor={atrfactor}")
        
        atrM = None
        last_atr = None
        
        avg_hl = (data["high"] + data["low"]) / 2
        close_d = data["close"].values
        high_d, low_d = data["high"].values, data["low"].values
        atr_np = ta.ATR(high_d, low_d, close_d, timeperiod=atrlen)
        logger.debug(f"Calculating volatility stop for bars with atrlen={atrlen} and atrfactor={atrfactor}")
        
        atr_series= pd.Series(atr_np, index=data.index) if atr_np is not None else None
        
        
    
        

        # 2) compute ATR * factor, ask for full series
     
        last_atr = atr_series.iloc[-1] if not None else 0.0
        logger.debug(f"ATR series for {atrlen} bars: {last_atr}")
        

        # 3) guard against None or too little data
        if atr_series is None or not isinstance(atr_series, (pd.Series, pd.DataFrame)):
            logger.warning(
                f"ATR returned None or invalid for period {atrlen} "
                f"with only {len(data)} bars; defaulting ATR to zero."
            )
            atr_series = pd.Series(0.0, index=data.index)
        atrM = atr_series * atrfactor
        logger.debug(f"ATR: atrM= {atrM} and atr_series={atr_series} with atrfactor={atrfactor}")
        atrM = atrM.fillna(0)
        logger.debug(f"ATR: atrM= {atrM} and atr_series={atr_series} with atrfactor={atrfactor}")
            
        
        

        # 4) now use avg_hl as your 'close' for the volatility stop logic
        close = avg_hl
        max_price = close.copy()
        min_price = close.copy()
        vStop = pd.Series(np.nan, index=close.index)
        uptrend = pd.Series(True, index=close.index)

        for i in range(1, len(close)):
            max_price.iloc[i] = max(max_price.iloc[i - 1], close.iloc[i])
            min_price.iloc[i] = min(min_price.iloc[i - 1], close.iloc[i])
            # initialize vStop
            if np.isnan(vStop.iloc[i - 1]):
                vStop.iloc[i] = close.iloc[i]
            else:
                vStop.iloc[i] = vStop.iloc[i - 1]

            if uptrend.iloc[i - 1]:
                vStop.iloc[i] = max(vStop.iloc[i], max_price.iloc[i] - atrM.iloc[i])
            else:
                vStop.iloc[i] = min(vStop.iloc[i], min_price.iloc[i] + atrM.iloc[i])

            uptrend.iloc[i] = (close.iloc[i] - vStop.iloc[i]) >= 0

            # reset pivot on trend flip
            if uptrend.iloc[i] != uptrend.iloc[i - 1]:
                max_price.iloc[i] = close.iloc[i]
                min_price.iloc[i] = close.iloc[i]
                vStop.iloc[i] = (
                    max_price.iloc[i] - atrM.iloc[i]
                    if uptrend.iloc[i]
                    else min_price.iloc[i] + atrM.iloc[i]
                )
        return vStop, uptrend, atr_series
    except Exception as e:
        logger.error(f"Error calculating volatility stop: {e}")
        return None

async def debug_vstop_bars(symbol: str, df) -> List[Dict[str, Any]]:
    """
    Return the list of bars (with vStop & uptrend) for `symbol`, converting
    each timestamp to America/New_York and formatting as 'YYYY-MM-DD HH:MM:SS'.
    """
    # get your dataframe
    logger.debug(f"Debugging vStop bars for {symbol}...")
    if df is None or df.empty:
        return []

    # timezone objects
    tz_ny = pytz.timezone("America/New_York")
    tz_utc = pytz.utc

    bars: List[Dict[str, Any]] = []
    logger.debug(f"Processing {len(df)} bars for {symbol}...")
    for _, row in df.iterrows():
        ts = row["date"]
        # ensure it's timezone‐aware
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=tz_utc)
        # convert and format
        ts_ny = ts.astimezone(tz_ny)
        ts_str = ts_ny.strftime("%Y-%m-%d %H:%M:%S")

        bars.append({
            "time":    ts_str,
            "open":    row["open"],
            "high":    row["high"],
            "low":     row["low"],
            "close":   row["close"],
            "vStop":   row["vStop"],
            "uptrend": bool(row["uptrend"]),
        })
        logger.debug(f"Returning 2 {len(bars)} bars for {symbol}...")

    return bars

    


async def compute_volatility_series(high, low, yesterday_close, contract=None, ib=None) -> float:
    """
    Given a DataFrame of daily bars with columns ['high','low','close'], returns a Series
    of volatility (true range * 100 / |low|) for each bar.
    plot(ta.tr(true)*100/math.abs(low), title="Volatility.D")
    ta.tr= math.max(high - low, math.abs(high - close[1]), math.abs(low - close[1]))
    math.max(high - low, math.abs(high - close[1]), math.abs(low - close[1]))
    """
    try:
        symbol = contract.symbol if contract else None
        logger.debug(f"Computing volatility series for {symbol} ")
        tr = None
        vol = None
        barSizeSetting_web = timeframe_dict[symbol]
        if barSizeSetting_web is None:
            web_request= await get_tv_ticker_dict(symbol)
            barSizeSetting_web= web_request.barSizeSetting_web
        is_valid_timeframe = "secs" in barSizeSetting_web or "min" in barSizeSetting_web
        if is_valid_timeframe:
            barSizeSetting = barSizeSetting_web
        else:
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(barSizeSetting_web)
        if ((yesterday_close is None or isNan(yesterday_close)) or high is None or isNan(high)) and contract is not None:
            logger.warning(f"Yesterday's close is None or zero, cannot compute volatility: high: {high} and low: {low} and yesterday_close {yesterday_close} for {symbol}")
            await price_data_dict.add_ticker(ib.ticker(contract), barSizeSetting, post_log=False) 
            df =  await yesterday_close_bar(ib, contract)
            last_daily_bar_dict[symbol] = df.iloc[-1]
            
            y_close = df.iloc[-2]['close'] if len(df) >= 2 else None 
            last_daily_bar = last_daily_bar_dict[symbol]
            logger.debug(f"Yesterday's close for {symbol} is {y_close} and last daily bar is {last_daily_bar}")  
            high = last_daily_bar['high']
            low = last_daily_bar['low']
            yesterday_close = y_close
            

       
       
        
            
        
        
        tr = max(high - low, abs(high - yesterday_close), abs(low - yesterday_close))
        vol = tr *100 / math.fabs(low) if low != 0 else None
        await daily_volatility.set(symbol, vol)  # Store the computed volatility in the daily_volatility store
        logger.debug(f"yesterday_close: {yesterday_close} volatility: {vol} tr: {tr} d_high: {high} d_low: {low}")
        return round(vol, 2)
    except Exception as e:
        logger.error(f"Error computing volatility series: {e}")
        return 0.0

async def get_active_tickers(ib) -> list[dict]: # keep
        try:
            sym = None
            ticker = None
            active_ticker= None
            # Use the ib.tickers() method to get all active ticker objects
            tickers_list: Ticker = ib.tickers()
            for ticker in tickers_list:
                if ticker is None or not hasattr(ticker, 'contract'):
                    logger.warning(f"Invalid ticker found in list: {ticker}")
                    continue
                sym = ticker.contract.symbol
               
                    
                
                if active_tickers.get(sym) is None:
                    
                    active_tickers[sym] = ticker
                logger.debug(f"tickers_list tickers: {tickers_list}")
                active_ticker = active_tickers.get(sym)
                
                logger.debug(f"Active tickers: {active_ticker}")
                tckDict = await ticker_to_dict(active_ticker)
          
            
                for t in tckDict:
                    logger.debug(f"Ticker data: {t}")
                # Convert each ticker to a dictionary for JSON serialization
                return [await ticker_to_dict(t) for t in active_tickers.values()]
        except Exception as e:
            logger.error(f"Error getting active tickers: {e}")
            return []

async def ticker_to_dict(ticker) -> dict:
        try:
            logger.debug(f"Converting ticker to dict: {ticker}")

            def format_datetime(dt):
                if dt is None:
                    return None
                if isinstance(dt, datetime):
                    return dt.isoformat()
                return str(dt)

            # If ticker is already a dict, simply return it (or reformat if needed)
            if isinstance(ticker, dict):
                logger.warning("Ticker is already a dict; returning formatted version.")
                # Optionally reformat its time field if present.
                if "time" in ticker:
                    ticker["time"] = format_datetime(ticker["time"])
                return ticker

            # Ensure ticker has a contract attribute
            if not hasattr(ticker, "contract"):
                logger.warning("Ticker does not have a contract attribute.")
                return {}

            # Format the time attribute
            timestamp = format_datetime(getattr(ticker, "time", None))

            # Convert ticks if available
            tick_list = []
            if hasattr(ticker, "ticks") and ticker.ticks:
                logger.debug(
                    f"Converting {len(ticker.ticks)} ticks for {ticker.contract.symbol}"
                )
                for tick in ticker.ticks:
                    tick_list.append(
                        {
                            "time": format_datetime(getattr(tick, "time", None)),
                            "tickType": getattr(tick, "tickType", None),
                            "price": getattr(tick, "price", None),
                            "size": getattr(tick, "size", None),
                        }
                    )
                    logger.debug(f"Tick data: {tick_list[-1]}")

            result = {
                "conId": (
                    ticker.contract.conId if hasattr(ticker.contract, "conId") else None
                ),
                "symbol": (
                    ticker.contract.symbol
                    if hasattr(ticker.contract, "symbol")
                    else None
                ),
                "secType": (
                    ticker.contract.secType
                    if hasattr(ticker.contract, "secType")
                    else None
                ),
                "exchange": (
                    ticker.contract.exchange
                    if hasattr(ticker.contract, "exchange")
                    else None
                ),
                "primaryExchange": getattr(ticker.contract, "primaryExchange", None),
                "currency": (
                    ticker.contract.currency
                    if hasattr(ticker.contract, "currency")
                    else None
                ),
                "localSymbol": (
                    ticker.contract.localSymbol
                    if hasattr(ticker.contract, "localSymbol")
                    else None
                ),
                "tradingClass": (
                    ticker.contract.tradingClass
                    if hasattr(ticker.contract, "tradingClass")
                    else None
                ),
                "time": timestamp,
                "minTick": getattr(ticker, "minTick", None),
                "bid": getattr(ticker, "bid", None),
                "bidSize": getattr(ticker, "bidSize", None),
                "bidExchange": getattr(ticker, "bidExchange", None),
                "ask": getattr(ticker, "ask", None),
                "askSize": getattr(ticker, "askSize", None),
                "askExchange": getattr(ticker, "askExchange", None),
                "last": getattr(ticker, "last", None),
                "lastSize": getattr(ticker, "lastSize", None),
                "lastExchange": getattr(ticker, "lastExchange", None),
                "prevBidSize": getattr(ticker, "prevBidSize", None),
                "prevAsk": getattr(ticker, "prevAsk", None),
                "prevLastSize": getattr(ticker, "prevLastSize", None),
                "volume": getattr(ticker, "volume", None),
                "close": getattr(ticker, "close", None),
                "ticks": tick_list,
                "bboExchange": getattr(ticker, "bboExchange", None),
                "snapshotPermissions": getattr(ticker, "snapshotPermissions", None),
            }
            logger.debug(f"Converted ticker to dict: {result}")
            return result
        except Exception as e:
            logger.error(f"Error converting ticker to dict: {e}")
            return {}


def shortable_shares(symbol, action, quantity, shortableShares):
    try:
        required_shares = None
        shares = None
        if action == "SELL":
            shares = shortableShares
            if shares is None or isNan(shares):
                return False
    
            required_shares = quantity * 5
            if shares < required_shares:
                return False
    
            logger.debug(f"Short availability check passed for {symbol}. Required: {required_shares}, Available: {shares}")
            return True

    except Exception as e:

        logger.error(f"Error placing LIMIT order for {symbol}: {e}")
        return None
        

def ensure_utc(bars):
    for b in bars:
        if b.time.tzinfo is None:
            b.time = b.time.replace(tzinfo=timezone.utc)
        else:
            b.time = b.time.astimezone(timezone.utc)
    return bars


def floor_time_to_n(dt: datetime, n: int) -> datetime:
    """
    Floor `dt` down to the nearest multiple of `n` seconds,
    and force it to be UTC‐aware.
    """
    # 1) make dt UTC‐aware if naive, else convert to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # 2) floor seconds
    sec = (dt.second // n) * n
    return dt.replace(second=sec, microsecond=0)


def aggregate_5s_to_nsec(bars5s: List[RealTimeBar], n: int) -> List[RealTimeBar]:
    """
    Take a list of 5‑sec RealTimeBar objects (oldest→newest),
    and roll them up into N‑sec bars.

    Returns a new list of RealTimeBar (with bar.time floored to N‑sec).
    """
    agg: List[RealTimeBar] = []
    current: RealTimeBar = None

    for b in bars5s:
        #logger.debug("aggregate of 5s bars started")
        bucket = floor_time_to_n(b.time, n)

        if current and current.time == bucket:
            # extend the existing bucket
            current.high   = max(current.high,   b.high)
            current.low    = min(current.low,    b.low)
            current.close  = b.close
            current.volume += b.volume
        else:
            # start a new bucket
            current = RealTimeBar(
                time=bucket,
                endTime=-1,
                open_= b.open_,
                high  = b.high,
                low   = b.low,
                close = b.close,
                volume=b.volume,
                wap   = 0.0,
                count = 0
            )
            agg.append(current)

    return agg


def get_latest_csv(directory):
    """Retrieve the latest CSV file from the specified directory."""
    csv_files = list(Path(directory).glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV files found in directory")
    return str(max(csv_files, key=os.path.getctime))


def delete_orders_db():
    """
    Deletes the orders.db file if it exists.
    The file path is constructed relative to the directory of the current file.
    """
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "orders.db")
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            logger.debug(f"Deleted orders.db file at {db_path}")
        except Exception as e:
            logger.error(f"Failed to delete orders.db file at {db_path}: {e}")
    else:
        logger.debug(f"orders.db file does not exist at {db_path}; nothing to delete")


def is_bracket_exit_order(trade: Trade) -> bool:
    """
    Detects if the trade is a bracket child (exit order) based on ocaGroup naming pattern.
    Pattern: exit_<symbol>_<permId>_<reqId>
    """
    oca_group = getattr(trade.order, "ocaGroup", "")
    symbol = trade.contract.symbol
    perm_id = trade.order.orderId
    logger.debug(
        f"Checking if trade is a bracket exit order: {oca_group} for symbol: {symbol} and permId: {perm_id}"
    )

    # Match "exit_<symbol>_<permId>_<reqId>"
    pattern = rf"^exit_{re.escape(symbol)}_{perm_id}_[\w\d]+$"
    logger.debug(f"Regex pattern for exit order: {pattern}")
    true_or_false = bool(re.match(pattern, oca_group))
    logger.debug(f"Is bracket exit order: {true_or_false}")
    return bool(re.match(pattern, oca_group))


def extract_order_fields(parsed_data):
    """
    Extract important order fields from the parsed_data structure.

    Args:
        parsed_data (dict): The parsed data dictionary from order requests

    Returns:
        dict: Dictionary containing the extracted fields
    """
    try:
        # Navigate to the order data which is in the "0" key of request_data
        order_data = parsed_data.get("request_data", {}).get("0", {})

        # Handle case where order might be a list instead of a dictionary
        order_obj = order_data.get("order", {})
        symbol = None
        conId = None
        orderId = None
        permId = None
        account = None
        order = {}

        if isinstance(order_obj, list) and len(order_obj) > 0:
            # If order is a list, take the first item
            first_item = order_obj[0]
            symbol = first_item.get("contract", {}).get("symbol")
            conId = first_item.get("contract", {}).get("conId")
            # Use the nested order object if it exists
            order = first_item.get("order", {})
            orderId = order.get("orderId")
            permId = order.get("permId")
            account = order.get("account")
        else:
            # Extract fields normally if order is a dictionary
            symbol = order_data.get("contract", {}).get("symbol")
            conId = order_data.get("contract", {}).get("conId")
            orderId = order_data.get("orderId") or order_data.get("order", {}).get(
                "orderId"
            )
            permId = order_data.get("permId") or order_data.get("order", {}).get(
                "permId"
            )
            account = order_data.get("order", {}).get("account")
            order = order_data.get("order", {})

        # Return all extracted fields in a dictionary
        return {
            "symbol": symbol,
            "orderId": orderId,
            "conId": conId,
            "permId": permId,
            "account": account,
            "order": order,
        }
    except Exception as e:
        logger.error(f"Error extracting order fields: {e}")
        return {}

def is_float(x):
    if isinstance(x, np.ndarray):
        # array: True only if its dtype is a float subtype
        return np.issubdtype(x.dtype, np.floating)
    else:
        # scalar (or other): True if its type is a NumPy floating subtype
        return np.issubdtype(type(x), np.floating)
    

async def compute_position_size(
    entry_price,
    stop_loss_price,
    accountBalance,
    riskPercentage,
    rewardRiskRatio,
    orderAction,
    order_details
):
    """
    Compute the position size, take profit, and risk metrics.
    Only used when quantity is not provided or is 0.0.
    """
    brokerComish = 0.0
    quantity = 0.0
    if order_details.quantity is not None and order_details.quantity > 0:
        quantity= order_details.quantity

    # Validate inputs to prevent calculation errors
    if entry_price <= 0.0:
        raise ValueError(f"Invalid entry price: {entry_price}")

    if accountBalance <= 0.0:
        raise ValueError(f"Invalid account balance: {accountBalance}")

    # Calculate broker commission
    _brokerComish = max(round((accountBalance / entry_price) * 0.005), 1.0)

    # Calculate per-share risk with validation
    perShareRisk = abs(entry_price - stop_loss_price)

    # Ensure we have a reasonable risk per share
    if perShareRisk < 0.01:
        logger.debug(
            f"Per share risk ({perShareRisk}) is too small; defaulting to 1% of entry price"
        )
        perShareRisk = entry_price * 0.01

    # Calculate tolerated risk
    toleratedRisk = abs((riskPercentage / 100 * accountBalance) - _brokerComish)

    # Calculate quantity based on risk and max capital allocation
    if quantity == 0.0:
        quantity = round(
            min(
                toleratedRisk / perShareRisk,
                math.floor((accountBalance * 0.95) / entry_price),
            )
        )

        # Ensure valid quantity
    if quantity <= 0:
        logger.debug(
            f"Calculated quantity ({quantity}) is invalid; using minimum quantity of 1"
        )
        quantity = 0.0
    logger.debug(f"Calculated quantity: {quantity}")
    logger.debug(f"Broker commission: {_brokerComish}")
    logger.debug(f"Per share risk: {perShareRisk}")
    logger.debug(f"Tolerated risk: {toleratedRisk}")
    logger.debug(f"Account balance: {accountBalance}")
    logger.debug(f"Entry price: {entry_price}")
    logger.debug(f"Stop loss price: {stop_loss_price}")
    logger.debug(f"Risk percentage: {riskPercentage}")
    logger.debug(f"Reward risk ratio: {rewardRiskRatio}")
    logger.debug(f"Order action: {orderAction}")
    
    if quantity != 0:
    
        brokerComish = max(round(quantity * 0.005), 1.0)
    
    # Calculate take profit price
    if orderAction == "BUY":
        take_profit_price = round(entry_price + (perShareRisk * rewardRiskRatio), 2)
        logger.debug(f"Take profit price: {take_profit_price} with orderAction: {orderAction}")
    else:
        take_profit_price = round(entry_price - (perShareRisk * rewardRiskRatio), 2)
        logger.debug(f"Take profit price: {take_profit_price} with orderAction: {orderAction}")
    logger.info(f"compute_position_size={stop_loss_price}, limitPrice={entry_price},  quantity={quantity} take_profit_price={take_profit_price}, brokerComish={brokerComish}, perShareRisk={perShareRisk}, toleratedRisk={toleratedRisk}")
    return quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk

# Add this function to help debug dictionaries
def inspect_dict_values(dict_name, data_dict, symbol=None):
    """
    Helper function to safely inspect dictionary values and their types.
    
    Args:
        dict_name: String name of the dictionary for logging
        data_dict: The dictionary to inspect
        symbol: Optional specific symbol to check, or None to check all
    
    Returns:
        None - logs information about the dictionary values
    """
    try:
        if symbol:
            # Check only the specific symbol
            if symbol not in data_dict:
                logger.debug(f"{dict_name}[{symbol}] does not exist")
                return
                
            value = data_dict[symbol]
            logger.debug(f"{dict_name}[{symbol}] type: {type(value)}")
            
            # If it's a Series, show sample values
            if hasattr(value, 'iloc'):
                logger.debug(f"{dict_name}[{symbol}] last value: {value.iloc[-1]}")
                logger.debug(f"{dict_name}[{symbol}] type of last value: {type(value.iloc[-1])}")
                logger.debug(f"{dict_name}[{symbol}] sample (last 3): {value.iloc[-3:].values}")
            else:
                logger.debug(f"{dict_name}[{symbol}] value: {value}")
        else:
            # Check all keys in the dictionary
            logger.debug(f"{dict_name} keys: {list(data_dict.keys())}")
            for key, value in list(data_dict.items())[:5]:  # Show first 5 for brevity
                logger.debug(f"{dict_name}[{key}] type: {type(value)}")
                
                # If it's a Series, show sample values
                if hasattr(value, 'iloc'):
                    logger.debug(f"{dict_name}[{key}] last value: {value.iloc[-1]}")
                    logger.debug(f"{dict_name}[{key}] type of last value: {type(value.iloc[-1])}")
    except Exception as e:
        logger.error(f"Error inspecting {dict_name}: {e}")
def format_order_details_table(order_details):
    """
    Format order details as a nice pandas DataFrame table for terminal display.

    Args:
        order_details: Dictionary containing order details

    Returns:
        Formatted string representation of the table
    """
    # Convert dictionary to a DataFrame with 'Parameter' and 'Value' columns
    df = pd.DataFrame(
        {"Parameter": order_details.keys(), "Value": order_details.values()}
    )

    # Format numerical values
    for idx, param in enumerate(df["Parameter"]):
        value = df.loc[idx, "Value"]
        if isinstance(value, (int, float)):
            if "price" in param or "Balance" in param:
                df.loc[idx, "Value"] = f"${value:.2f}"
            elif "percentage" in param or "Ratio" in param:
                df.loc[idx, "Value"] = f"{value:.2f}"

    # Return formatted table string
    return f"\n{'='*50}\nORDER DETAILS\n{'='*50}\n{df.to_string(index=False)}\n{'='*50}"


def current_millis() -> int:
    return int(time.time_ns() // 1_000_000)


async def get_timestamp() -> str:
    # Adjust timestamp (subtracting 4 hours) if needed for
    ts = current_millis() - (4 * 60 * 60 * 1000)
    return str(ts)


def is_market_hours():
    ny_tz = pytz.timezone("America/New_York")
    now = datetime.now().astimezone(ny_tz)  # Convert server local time to New York time

    if now.weekday() >= 5:
        return False  # Weekend

    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open <= now < market_close


def is_weekend():
    ny_tz = pytz.timezone("America/New_York")
    now = datetime.now(ny_tz)
    # weekday() returns 0-6 where 0 is Monday and 6 is Sunday
    # So 5 is Saturday and 6 is Sunday
    return now.weekday() >= 5



# --- 1) Compute the bar‐start for a given dt, aligned to 9:30 AM ET ---
def floor_to_market_interval(dt: datetime, interval: int) -> datetime:
    """
    Given a timezone‐aware dt, floor it to the nearest 
    `interval`-second bucket measured from 9:30 AM America/New_York that day.
    """
    tz_ny = pytz.timezone("America/New_York")
    dt_ny = dt.astimezone(tz_ny)
    # market open today at 9:30
    mo = dt_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    # if before open, you could choose to skip or roll to previous day’s open
    if dt_ny < mo:
        mo -= timedelta(days=1)
    elapsed = (dt_ny - mo).total_seconds()
    bucket = math.floor(elapsed / interval)
    start_ny = mo + timedelta(seconds=bucket * interval)
    # return as UTC‐aware so all tables use UTC
    return start_ny.astimezone(pytz.utc)

# --- 2) Ensure each per‐interval table exists ---
async def ensure_agg_table(db: aiosqlite.Connection, interval: int):
    tbl = f"bars_{interval}s"
    await db.execute(f"""
    CREATE TABLE IF NOT EXISTS {tbl} (
      symbol   TEXT    NOT NULL,
      time     TIMESTAMP NOT NULL,
      open     REAL,
      high     REAL,
      low      REAL,
      close    REAL,
      volume   REAL,
      PRIMARY KEY(symbol, time)
    )""")
    await db.commit()

# --- 3) Upsert a single aggregated bar into its table ---
async def upsert_agg_bar(
    db: aiosqlite.Connection,
    symbol: str,
    interval: int,
    bucket_start: datetime,
    bar: dict
):
    """
    INSERT or UPDATE the bar for (symbol, bucket_start) in table bars_{interval}s.
    If it exists, we max the high, min the low, overwrite close, and sum volumes.
    """
    tbl = f"bars_{interval}s"
    await ensure_agg_table(db, interval)

    await db.execute(f"""
    INSERT INTO {tbl}(symbol, time, open, high, low, close, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(symbol, time) DO UPDATE SET
      high   = MAX(high, excluded.high),
      low    = MIN(low,  excluded.low),
      close  = excluded.close,
      volume = volume + excluded.volume
    """, (
      symbol,
      bucket_start.isoformat(sep=' '),
      bar["open"],
      bar["high"],
      bar["low"],
      bar["close"],
      bar["volume"],
    ))
    await db.commit()
   

# --- 4) Master routine: call this for each new 5s bar ---
async def aggregate_to_all_intervals(
    db: aiosqlite.Connection,
    symbol: str,
    confirmed_bar: dict
):
    """
    confirmed_bar is your dict from on_bar_update:
      {
        "time": datetime (tz‐aware),
        "open": float,
        "high": float,
        "low":  float,
        "close":float,
        "volume":float,
        … 
      }
    This will bucket it into each TARGET_INTERVAL and upsert.
    """
    TARGET_INTERVALS = [10, 15, 30, 60, 120, 300] 
    dt = confirmed_bar["time"]
    # ensure tz‐aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.utc)

    for interval in TARGET_INTERVALS:
        bucket = floor_to_market_interval(dt, interval)
        await upsert_agg_bar(db, symbol, interval, bucket, confirmed_bar)


async def fetch_agg_bars(
    db: Connection,
    symbol: str,
    barSizeSetting: str,
    limit: int = 100
) -> pd.DataFrame:
    """
    Pull the last `limit` bars for `symbol` at the barSizeSetting_web given by `barSizeSetting`
    from tables named bars_{N}s (e.g. bars_30s for a 30-second barSizeSetting_web).
    """
    # 1) turn "30 secs" or "1 min" into (barSizeString, N)
    _, interval = await convert_pine_timeframe_to_barsize(barSizeSetting, False)
    table = f"bars_{interval}s"

    # 2) figure out the current bucket (so we only pull closed bars)
    now = datetime.now(timezone.utc)
    bucket = floor_to_market_interval(now, interval).isoformat(sep=" ")

    # 3) query them
    sql = f"""
      SELECT time, open, high, low, close, volume
        FROM {table}
       WHERE symbol = ?
         AND time <= ?
    ORDER BY time DESC
       LIMIT ?
    """
    async with db.execute(sql, (symbol, bucket, limit)) as cur:
        rows = await cur.fetchall()

    # 4) turn into a DataFrame, sort ascending
    df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
    df["time"] = pd.to_datetime(df["time"])  # ISO strings → timestamps
    return df.sort_values("time").reset_index(drop=True)


async def update_ticker_data(contract: Contract, ib=None) -> bool:
    symbol = contract.symbol
    logger.debug(f"Updating ticker data for {contract.symbol}...")
    ticker = ib.ticker(contract)
    logger.debug(f"Ticker data for {contract.symbol}: {ticker}")
    await price_data_dict.add_ticker(ticker)
    snapshot = await price_data_dict.get_snapshot(symbol)
    logger.debug(f"Ticker data for {contract.symbol} updated successfully for  snapshot.bid: {snapshot.bid} and snapshot {snapshot}")
    
    if snapshot is None:
        logger.warning(f"No ticker data found for {contract.symbol}")
        return None
    if not isNan(ticker.bid):
        snapshot.bid = ticker.bid
    if not isNan(ticker.ask):
        snapshot.ask = ticker.ask
    # Update volume if the new volume is valid.
    if not isNan(ticker.volume):
        snapshot.volume = ticker.volume
    logger.debug(f"Snapshot for {contract.symbol}: {snapshot.bid}")
    # Always update the time (assumed to be non-NaN if provided)
    snapshot.time = ticker.time
    snapshot.halted = ticker.halted
    snapshot.markPrice = ticker.markPrice
    snapshot.shortableShares = ticker.shortableShares
    # Update the snapshot with the latest data
    if snapshot.bid is not None and not isNan(snapshot.bid):
        logger.debug(f"Ticker data for {contract.symbol} updated successfully for  snapshot.bid: {snapshot.bid} and snapshot {snapshot}")
        snapshot = snapshot
        
        if snapshot.bid > 1.1:
            logger.debug(f"Valid bid price for {contract.symbol}: {snapshot.bid}")
            return snapshot
        
        elif logger.warning(f"Invalid bid price for {contract.symbol}: {snapshot.bid}"):
            return None
        
    else:
        logger.warning(f"Invalid bid price for {contract.symbol}: {snapshot.bid}")
        return None


async def get_tv_ticker_dict(ticker: str, post_log=True) -> OrderRequest | None:
    try:
        if post_log:
            logger.debug(f"Retrieving ticker data for {ticker} from store data...")
     
        result = await tv_store_data.get(ticker)
        if result is None:
            return None
        if post_log:
            logger.debug(f"Retrieved ticker {ticker}: {result}")
        order_data = OrderRequest(**result)
        #logger.debug(f"1 Converted to OrderRequest: {order_data.uptrend} stopLoss: {order_data.stopLoss} entryPrice: {order_data.entryPrice}")

        

        return order_data
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        return None

async def get_dynamic_atr(symbol: str, BASE_ATR=None, vol=None) -> float:
    atrFactor = None
    if BASE_ATR is None:
        atrFactor = 1.5
    else:
        atrFactor = BASE_ATR
    vol = await daily_volatility.get(symbol)
    if vol is None or math.isnan(vol):
        return atrFactor
    if vol < 10.0:
        return atrFactor
    elif vol < 20.0:
        return 2.0
    elif vol < 50.0:
        return 2.5
    else:
        return 3.0
def format_what_if_details_table(order_details):
    """
    Format order details as a nice pandas DataFrame table for terminal display.

    Args:
        order_details: Dictionary containing order details

    Returns:
        Formatted string representation of the table
    """
    # Convert dictionary to a DataFrame with 'Parameter' and 'Value' columns
    df = pd.DataFrame(
        {"Parameter": order_details.keys(), "Value": order_details.values()}
    )

    # Format numerical values
    for idx, param in enumerate(df["Parameter"]):
        value = df.loc[idx, "Value"]
        if isinstance(value, (int, float)):
            if "price" in param or "Balance" in param:
                df.loc[idx, "Value"] = f"${value:.2f}"
            elif "percentage" in param or "Ratio" in param:
                df.loc[idx, "Value"] = f"{value:.2f}"

    # Return formatted table string
    return f"\n{'='*50}\nORDER DETAILS\n{'='*50}\n{df.to_string(index=False)}\n{'='*50}"