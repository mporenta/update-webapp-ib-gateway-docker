from asyncio import tasks
from collections import defaultdict, deque
from polygon import RESTClient
from dotenv import load_dotenv
import os
load_dotenv()
boof = os.getenv("HELLO_MSG")
api_key = os.getenv("POLYGON_API_KEY")
from typing import *
poly_client = RESTClient(api_key)

import asyncio
from datetime import datetime, timedelta, timezone  
from math import *
from ib_async.util import isNan
from ib_async import *

from tech_a import _aggs_to_df
from models import (

    PriceSnapshot,
    VolatilityStopData
)
from tv_ticker_price_data import  tv_store_data, price_data_dict, daily_volatility, yesterday_close_bar, yesterday_close, subscribed_contracts, logger
from my_util import last_daily_bar_dict, compute_volatility_series


reqId = {}


# --- FastAPI Setup ---



shutting_down = False
historical_data_timestamp = {}

vstop_data = defaultdict(VolatilityStopData)
ema_data = defaultdict(dict)
uptrend_data = defaultdict(VolatilityStopData)
open_orders = defaultdict(dict)
  # Store last daily bar for each symbol

# your existing outputs

historical_data = defaultdict(dict)
last_fetch = {}

# Add this global structure:
realtime_bar_buffer = defaultdict(
    lambda: deque(maxlen=1)
)  # Now we process each 5s bar directly
confirmed_5s_bar = defaultdict(
    lambda: deque(maxlen=720)
)  # 1 hour of 5s bars (720 bars)
confirmed_40s_bar = defaultdict(
    lambda: deque(maxlen=720)
)  # 1 hour of 40s bars (720 bars)

order_data = {}
order_details = {}
async def gpt_poly_min_prices(contract: Contract, barSizeSetting: str):
    try:
        logger.info(f"Got ticker: {contract.symbol} from gpt_poly")
        now = datetime.now(timezone.utc)
        # fetch 3 days of 1-min bars
        aggs_one_min = list(poly_client.list_aggs(
            ticker=contract.symbol,
            multiplier=1,
            timespan="minute",
            from_=now - timedelta(days=1),
            to=now,
            adjusted=True,
            sort="asc",
            limit=5000,
        ))
        

        if not aggs_one_min:
            raise ValueError(f"No data found for ticker {contract.symbol}")
        logger.info(f"Fetched {len(aggs_one_min)} 1-min bars for {contract.symbol}")
        df=_aggs_to_df(aggs_one_min)
        logger.info(f"Converted to DataFrame with rows for {contract.symbol}")
    
        open= df['open']
        close= df['close']
        high= df['high']
        low= df['low']
        volume= df['volume']
        time = df.index  
        Last_bar_json= {"open": open, "close": close, "high": high, "low": low, "volume": volume, "time": time}
        
                # Normalize into a plain dict
        logger.info(f"Last bar JSON for {contract.symbol}: {Last_bar_json}")
                    

                # Store it
        #await price_data_dict.add_bar(contract.symbol, barSizeSetting, Last_bar_json)

    
        
    except Exception as e:
        logger.error(f"Unexpected error in /gpt_poly: {e}")
        
async def handle_new_bar(contract, bar):
    try:
        symbol = contract.symbol
        logger.debug(f"Handling new bar for {symbol} with {bar} bars")
        
        

        if not bar:
            logger.warning(f"No IB history for {contract.symbol}")
            return None
        
        
        
        rt_dict = {
                "time":   bar.date,
                "open_":  bar.open,
                "high":   bar.high,
                "low":    bar.low,
                "close":  bar.close,
                "volume": float(bar.volume),
            }
        return rt_dict
        
       
       
    except Exception as e:
            logger.error(f"Error handling bar for {symbol}: {e}")
            raise e     

# --- helper: pull the last *n* 5‑second bars and seed price_data_dict ------------
async def _front_load_rt_bars(symbol: str, contract: Contract, ib: IB, n: int = 100):
    """Grab the most‑recent *n* 5‑second bars from IB and push them straight
    into ``price_data_dict.rt_bars`` and ``price_data_dict.bars`` so the
    BarAggregator has data immediately.

    Args:
        symbol: the stock symbol (same as ``contract.symbol``)
        contract: *qualified* IBKR contract
        ib: connected ``IB`` instance
        n: number of 5‑second bars wanted (default 100 ⇒ last ~8.3 min)
    """
    try:
        rt_dict={}
        seconds = n * 5                         # e.g. 100×5 = 500 seconds
        bars = await ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime="",                  # now
            durationStr=f"{seconds} S",     # e.g. "500 S"
            barSizeSetting="5 secs",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=1,
        )
        if not bars:
            logger.warning(f"No 5‑sec history returned for {symbol}")
            return
        for bar in bars:
            rt_dict=await handle_new_bar(contract, bar)
        logger.info(f"Front‑loading {symbol} with {len(rt_dict)} 5‑sec bars…")

        
        await price_data_dict.add_rt_bar(symbol, rt_dict)
    except Exception as e:
        logger.error(f"front‑load failed for {symbol}: {e}")

# ------------------------------------------------------------------------------
# add_new_contract – patched so that a brand‑new subscription automatically gets
# at least 100 historic 5‑second bars seeded into the in‑memory store.
# ------------------------------------------------------------------------------
async def add_new_contract(symbol, barSizeSetting: str, ib=None) -> PriceSnapshot:
    try:
        vol = await daily_volatility.get(symbol)
        logger.info(f"add_new_contract: {symbol} barSizeSetting: {barSizeSetting} vol: {vol} : ")

        contract: Contract | None = None
        if barSizeSetting is None:
            barSizeSetting = None

        already_subscribed = await subscribed_contracts.has(symbol)
        subscribe_needed = not already_subscribed
        logger.debug(
            f"add_new_contract: {symbol} barSizeSetting: {barSizeSetting} already_subscribed: {already_subscribed} : "
        )

        if subscribe_needed:
            # --- qualify + subscribe live market data -------------------------
            contract = await subscribed_contracts.get(symbol, barSizeSetting)
            ib.reqMktData(contract, genericTickList="221,236", snapshot=False)
            first_ticker=await price_data_dict.add_ticker(ib.ticker(contract))

         
            logger.debug(
                f"Subscribed {symbol}; first markPrice {first_ticker.markPrice:.4f} – front loading RT bars…"
            )

            # ---------- NEW --------------
            # pull ~8 minutes of 5‑sec history so the BarAggregator & indicators
            # have something to chew on before the first live bar arrives.
            logger.info(f"Front loading {symbol} with  5‑sec bars…")
            
            # ---------- /NEW -------------

        else:
            contract = await subscribed_contracts.get(symbol, barSizeSetting)

        # ensure daily volatility is in cache
        if vol is None or isNan(vol):
            logger.debug(f"Computing daily volatility for {symbol}…")
            df = await yesterday_close_bar(ib, contract)
            if df.empty:
                raise RuntimeError("No historical data for " + contract.symbol)
            last_daily_bar_dict[contract.symbol] = df.iloc[-1]

        if contract is None:
            raise RuntimeError(f"Contract for {symbol} not found or could not be created.")

        return contract

    except Exception as e:
        logger.error(f"Error in add_new_contract for {symbol}: {e}")
        raise

async def add_new_contract_old(symbol, barSizeSetting: str, ib=None) -> PriceSnapshot:
    try:
        vol=await daily_volatility.get(symbol)
        logger.info(f"add_new_contract: {symbol} barSizeSetting: {barSizeSetting} vol: {vol} : ")
       
        contract: Contract = None
        if barSizeSetting is None:
            barSizeSetting = await get_bar_size_setting(symbol)
        already_subscribed = await subscribed_contracts.has(symbol)
        subscribe_needed=not already_subscribed
        logger.debug(f"add_new_contract: {symbol} barSizeSetting: {barSizeSetting} already_subscribed: {already_subscribed} : ")
        if subscribe_needed:
            

            contract = await subscribed_contracts.get(symbol, barSizeSetting)
            
            
            ticker: Ticker = ib.reqMktData(contract, genericTickList = "221,236", snapshot=False)
            logger.debug(f"1 Adding contract {contract.symbol} with barSizeSetting {barSizeSetting}...")
            first_ticker = await price_data_dict.add_ticker(ib.ticker(contract))
            logger.debug(f"2 Adding contract {contract.symbol} with barSizeSetting {barSizeSetting}...and first_ticker markPrice: {first_ticker.markPrice}")
            logger.debug(f"3 Adding contract {contract.symbol} with barSizeSetting {barSizeSetting}...")
            
            
            
            
            
            
            #await first_get_historical_data(contract, barSizeSetting, vstopAtrFactor)  
        else:
            contract = await subscribed_contracts.get(symbol, barSizeSetting)
        if vol is None or isNan(vol):
            logger.debug(f"Computing daily volatility for {symbol}...")
            df= await yesterday_close_bar(ib, contract)
            
            
            if df.empty:
                raise RuntimeError("No historical data for " + contract.symbol)
        
            last_daily_bar_dict[contract.symbol] = df.iloc[-1]
            
            _yesterday_close =  yesterday_close[symbol]
            logger.debug(f"Subscribing market data for {symbol} with barSizeSetting: {barSizeSetting} _yesterday_close: {_yesterday_close}")
            # Compute daily volatility if not already set
            vol=await daily_volatility.get(symbol)
        if contract is None:
            raise RuntimeError(f"Contract for {symbol} not found or could not be created.")
        return contract
    except Exception as e:
        logger.error(f"Error in add_new_contract for {contract.symbol}: {e}")
        raise e
