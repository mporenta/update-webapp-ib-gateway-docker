
# app.py
from collections import defaultdict, deque


from typing import *
import os, asyncio, time
import re
import threading
from threading import Lock
from datetime import datetime, timedelta
from urllib import response
import pytz
import aiosqlite
import math
from math import *


import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Query, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import signal
import sys
from ib_async.util import isNan
from ib_async import *
from ib_async import (
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
from dotenv import load_dotenv
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
from pandas_ta.overlap import ema
from pandas_ta.volatility import atr

# from pnl import IBManager
from pnl import IBManager
from tv_scan import tv_volatility_scan_ib
import numpy as np
from timestamps import current_millis
from log_config import log_config, logger
from ticker_list import ticker_manager


from indicators import daily_volatility
from p_rvol import PreVol
from dev.break_change import VolatilityScanner  # Import our new scanner
from trade_dict import trade_to_dict
from my_util import *

from ib_db import (
    IBDBHandler,
    order_status_handler_fast_api,
    write_positions_to_db,
    clear_positions_table,
    delete_zero_positions,
    get_all_position_symbols,
    insert_trade,
    get_all_trades,
    delete_trade,
    DB_PATH,
)


df = pd.DataFrame()
global_close_prices = {}
ema9_dict = {}
# from hammer import HammerBacktest, TVOpeningVolatilityScan
load_dotenv()

# --- Global Variables --
forex_contract = Forex("EURUSD")
forex_order = MarketOrder("BUY", 1500)


# --- Configuration ---
PORT = int(os.getenv("FAST_API_PORT", "80"))
HOST = os.getenv("FAST_API_HOST", "localhost")
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = 7777
ib_host = '127.0.0.1'
ib_port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
boofMsg = os.getenv("HELLO_MSG")







reqId = {}

order_db = IBDBHandler("orders.db")
# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), ".env")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))

shutting_down = False
price_data_lock = Lock()
historical_data_timestamp = {}
subscribed_contracts = defaultdict(dict)
vstop_data = defaultdict(VolatilityStopData)
ema_data = defaultdict(dict)
uptrend_data = defaultdict(VolatilityStopData)
open_orders = defaultdict(dict)


last_fetch = {}

# Add this global structure:
realtime_bar_buffer = defaultdict(
    lambda: deque(maxlen=1)
)  # Now we process each 5s bar directly
confirmed_5s_bar = defaultdict(
    lambda: deque(maxlen=720)
)  # 1 hour of 5s bars (720 bars)


order_data = {}
order_details = {}

class VolStopPriceData:
    def __init__(self, ib: IB):
        self.ib = ib
        self.ib_connect = self.ib
        self.shutting_down = False
        self.entry_prices = (
            {}
        )  # key: symbol, value: {"entry_long": price, "entry_short": price, "timestamp": time}
        self.timestamp_id = {}
        self.client_id = client_id
        self.host = ib_host
        self.port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
        self.max_attempts = 300
        self.initial_delay = 1
        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        # price dicts:
        self.open_price = {}
        self.high_price = {}
        self.low_price = {}
        self.close_price = {}
        self.bid_price = {}
        self.ask_price = {}
        self.volume = {}
        self.atr_cache = {}
        self.nine_ema = {}
        self.twenty_ema = {}
        self.ticker = {}  
        self.daily_bars = {}  # key: symbol, value: DataFrame for daily historical data
        

        self.account_pnl: AccountPnL = None
        self.semaphore = asyncio.Semaphore(10)
        self.active_rtbars = {}  # Store active real-time bar subscriptions
        self.qualified_contract_cache = {}
        self.realtime_bar_buffer = defaultdict(
            lambda: deque(maxlen=1)
        )
        self.confirmed_5s_bar = defaultdict(
            lambda: deque(maxlen=720)
        )
        self.price_data = defaultdict(PriceSnapshot)
        self.realtime_bars = defaultdict(list)  # Store real-time bars for each contract
        self.historical_data = defaultdict(dict)
        self.snapshot = None  # Current price snapshot

    async def connect(self) -> bool:
        attempt = 0
        while attempt < self.max_attempts:
            try:
                logger.info(
                    f"Connecting to IB at {self.host}:{self.port} : client_id:{self.client_id}...with self.risk_amount {risk_amount}"
                )
                await self.ib.connectAsync(
                    host=self.host, port=self.port, clientId=self.client_id, timeout=40
                )
                if self.ib.isConnected():
                    logger.info("Connected to IB Gateway")

                    logger.info("Connecting to Database.")

                    await order_db.connect()
                    logger.info(
                        "Order database connection established, subscribing to events."
                    )
                    self.ib.errorEvent += self.error_code_handler
                    self.ib.disconnectedEvent += self.on_disconnected
                    
                    await self.add_contract(Stock("NVDA", "SMART", "USD"), "15 secs")
                    # self.ib.disconnectedEvent += self.on_disconnected
                    
                    # await self.ib_vol_data.process_web_order(web_order)
                    return True
            
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {e}")
            attempt += 1
            await asyncio.sleep(self.initial_delay)
            
        logger.error("Max reconnection attempts reached")
       

        return False
    async def on_disconnected(self):
        try:
            logger.warning("Disconnected from IB.")
            # Check the shutdown flag and do not attempt reconnect if shutting down.
            if self.shutting_down:
                logger.info("Shutting down; not attempting to reconnect.")
                return
            await asyncio.sleep(1)
            if not self.ib.isConnected():
                await self.connect()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected, shutting down...")
            self.shutting_down = True
            await self.graceful_disconnect()
        except Exception as e:

            logger.error(f"Error in on_disconnected: {e}")
            # Attempt to reconnect if not shutting down
    async def graceful_disconnect(self):
        try:
            self.shutting_down = True  # signal shutdown

            # Remove all event handlers
            if self.ib.isConnected():
                logger.info("Unsubscribing events and disconnecting from IB...")

                # Get all event attributes
                event_attrs = [attr for attr in dir(self.ib) if attr.endswith("Event")]

                # Clear all event handlers
                for event_attr in event_attrs:
                    try:
                        event = getattr(self.ib, event_attr)
                        if hasattr(event, "clear"):
                            event.clear()
                    except Exception as e:
                        logger.warning(f"Error clearing event {event_attr}: {e}")

                # Disconnect from IB
                self.ib.disconnect()
                logger.info("Disconnected successfully.")

            return True

        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
            return False  
    async def error_code_handler(
        self, reqId: int, errorCode: int, errorString: str, contract: Contract
    ):
        logger.warning(f"Error for reqId {reqId}: {errorCode} - {errorString}")

    async def first_get_historical_data(self, qualified_contract, barSizeSetting, vstopAtrFactor):
        try:
            contract = await self.qualify_contract(qualified_contract)
            logger.info(f"First qualified contract {contract.symbol} with barSizeSetting {barSizeSetting}...")
            
            df = pd.DataFrame()
            symbol = contract.symbol
            bar_sec_int = 0
            # parse to seconds (you already have this):
            barSizeString, bar_sec_int = await  convert_pine_timeframe_to_barsize(barSizeSetting)
            logger.info(f"First Bar size {barSizeString} and bar_sec_int {bar_sec_int} durationStr {bar_sec_int + 10} S")
        
            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting=barSizeString,
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1,
            )
            if not bars:
                logger.warning(f"First No historical data for {symbol}")
            if bars:        
                logger.info(f"First Got bars historical data for {symbol}...")
                df = util.df(bars)
                
            logger.info(f"First Got df historical data for {symbol}...")
            self.snapshot = self.price_data[symbol]
            logger.info(f"Got self.snapshot historical data for {symbol}...")

            vStop, uptrend = await self.vol_stop(df, 20, vstopAtrFactor)
            logger.info(f"Got vstop historical data for {symbol}...")

            if vStop is not None:
                df["vStop"] = vStop
                df["uptrend"] = uptrend
                vstop_data[symbol] = vStop  # full Series
                uptrend_data[symbol] = uptrend  # full Series
                uptrend_data[symbol] = uptrend
        
            self.historical_data[symbol] = df
            historical_data_timestamp[symbol] = datetime.now()
            logger.info(f"Got self.historical_data[] historical data for {symbol}...")

            self.snapshot.last = df["close"].iloc[-1]

            logger.info(
                f"{symbol} | Bid={self.snapshot.bid} Ask={self.snapshot.ask} "
                f"LastClose={self.snapshot.last} vStop={vstop_data[symbol].iloc[-1]} uptrend={uptrend_data[symbol].iloc[-1]}"
            )
            
            return bars, df

        except Exception as e:
            logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
            raise

    async def vol_stop(self, data: pd.DataFrame, atrlen: int, atrfactor: float):
        # 1) average of high+low
        avg_hl = (data["high"] + data["low"]) / 2

        # 2) compute ATR * factor, ask for full series
        atr_series = atr(
            high       = data["high"],
            low        = data["low"],
            close      = data["close"],
            length     = atrlen,
            sequential = True
        )

        # 3) guard against None or too little data
        if atr_series is None or not isinstance(atr_series, (pd.Series, pd.DataFrame)):
            logger.warning(
                f"ATR returned None or invalid for period {atrlen} "
                f"with only {len(data)} bars; defaulting ATR to zero."
            )
            atr_series = pd.Series(0.0, index=data.index)

        atrM = atr_series * atrfactor
        atrM = atrM.fillna(0)
        logger.info(f"ATR: atrM= {atrM} and atr_series={atr_series} with atrfactor={atrfactor}")

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

        return vStop, uptrend

    async def on_pending_tickers(self, tickers):
        logger.debug(f"Received pending tickers: {tickers}")

        for ticker in tickers:
            self.ticker = ticker
            symbol = self.ticker.contract.symbol
            # If no ticker instance exists yet for this symbol, initialize it.
            if symbol not in self.price_data:
                self.price_data[symbol] = self.ticker
            self.snapshot = self.price_data[symbol]

            # Only update tick_last if the incoming 'last' is valid.
            if not isNan(ticker.last):
                self.snapshot.tick_last = ticker.last
            # Update bid and ask only if the new values are valid.
            if not isNan(ticker.bid):
                self.snapshot.bid = ticker.bid
            if not isNan(ticker.ask):
                self.snapshot.ask = ticker.ask
            # Update volume if the new volume is valid.
            if not isNan(ticker.volume):
                self.snapshot.volume = ticker.volume
            # Always update the tick_time (assumed to be non-NaN if provided)
            self.snapshot.tick_time = ticker.time
            self.snapshot.halted = ticker.halted
            self.snapshot.markPrice = ticker.markPrice
            self.snapshot.shortableShares = ticker.shortableShares

            logger.debug(
                f"Updated ticker for {symbol}: tick_last={getattr(self.snapshot, 'tick_last', None)}, "
                f"Bid={self.snapshot.bid}, Ask={self.snapshot.ask}, Volume={self.snapshot.volume}, "
                f"tick_time={self.snapshot.tick_time}"
            )
           

    async def on_bar_update(self, bars, has_new_bar):
        if not bars or len(bars) == 0:
            return
        if not has_new_bar:
            return

        symbol = bars.contract.symbol
        last_bar = bars[-1]

        # Convert the real-time bars to a DataFrame and store in self.historical_data.
        df = util.df(bars)
        self.historical_data[symbol] = df
        last_historical_bar = df.iloc[-1]

        # Floor the bar time to a 5-second interval.
        time_ = self.floor_time_to_5s(last_bar.time)
        bar_size = getattr(bars, "_bar_size", 5)
        # Build the confirmed_bar dictionary.
        # Only update if the incoming value is not NaN; otherwise preserve (or leave as None).
        confirmed_bar = {
            "time": time_,
            "open": last_bar.open_ if not isNan(last_bar.open_) else None,
            "high": last_bar.high if not isNan(last_bar.high) else None,
            "low": last_bar.low if not isNan(last_bar.low) else None,
            "close": last_bar.close if not isNan(last_bar.close) else None,
            "volume": last_bar.volume if not isNan(last_bar.volume) else None,
            "vStop": None,
            "ema9": None,
            "uptrend": None,
        }

        # Add the latest raw bar to the realtime buffer.
        self.realtime_bar_buffer[symbol].append(last_bar)
        # Append the confirmed bar.
        self.confirmed_5s_bar[symbol].append(confirmed_bar)
        try:
            await order_db.insert_realtime_bar(symbol, confirmed_bar, bar_size)
        except Exception as e:
            logger.error(f"Failed to write realtime bar: {e}")

        # Only update the self.snapshot's bar_close if we have a valid close value.
        if confirmed_bar["close"] is not None and not isNan(confirmed_bar["close"]):
            self.price_data[symbol].bar_close = confirmed_bar["close"]
        # Also store the entire confirmed bar if needed.
        self.price_data[symbol].last_confirmed_bar = confirmed_bar

        logger.debug(
            f"Confirmed 5s bar for {symbol} at {time_}: {confirmed_bar}, "
            f"Last historical bar: {last_historical_bar}"
        )

    def floor_time_to_5s(self, dt: datetime):
        return dt.replace(second=(dt.second // 5) * 5, microsecond=0)

    async def qualify_contract(self, contract: Contract):
        try:
            symbol = contract.symbol
            logger.info(f"Qualifying contract: {symbol}...")

            # If we get here, the contract isn't in the cache, so qualify it
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                logger.error(f"Contract qualification failed for {symbol}")
                return None

            # Store in subscribed_contracts for future use
            subscribed_contracts[symbol] = qualified[0]

            logger.info(f"Qualified {symbol} and stored in cache")

            # Return the specific contract, not the entire dictionary
            return qualified[0]
        except Exception as e:
            logger.error(f"Error qualifying contract {contract.symbol}: {e}")
            return None

    async def convert_pine_timeframe_to_barsize(self, barSizeSetting):
        barSizeString = ""
        bar_sec_int = 0
        
        # Implementation of conversion logic here
        # This is a placeholder - you would need to implement the actual conversion logic
        
        return barSizeString, bar_sec_int

    async def add_contract(self, contract: Contract, barSizeSetting: str) -> PriceSnapshot:
        try:
            symbol = contract.symbol
            logger.info(f"Adding contract {symbol} with barSizeSetting {barSizeSetting}...")
        
            contract = Contract(
                symbol=symbol,
                exchange="SMART",
                secType="STK",
                currency="USD",
            )
            qualified_contract = await self.qualify_contract(contract)
            first_get_historical_data = await self.first_get_historical_data(
                qualified_contract, barSizeSetting, vstopAtrFactor=1.5
            )

            # Request market data and 5-second real-time bars
            self.ticker: Ticker = self.ib.reqMktData(contract, genericTickList = "221,236", snapshot=False)
            self.ib.pendingTickersEvent += self.on_pending_tickers
            bars: RealTimeBarList = self.ib.reqRealTimeBars(contract, 5, "TRADES", False)

            bars.updateEvent += self.on_bar_update
            await asyncio.sleep(3)  # Allow time for the ticker to update
            await self.get_historical_data(
                contract, barSizeSetting, vstopAtrFactor=1.5
            ) 

            # Initialize or update the PriceSnapshot entry

            # Wait briefly to allow market data stream to initialize
            await asyncio.sleep(3)
            self.price_data[symbol] = self.ticker
            self.snapshot = self.price_data[symbol]

            self.snapshot.ticker = self.ticker
            self.snapshot.halted = self.ticker.halted
            self.snapshot.markPrice = self.ticker.markPrice
            self.snapshot.shortableShares = self.ticker.shortableShares
            logger.info(f"Ticker data for {symbol}:shortableShares {self.snapshot.shortableShares} markPrice {self.snapshot.markPrice} halted {self.snapshot.halted}")

            self.snapshot.bars = bars
            logger.info(
                f"Added contract {symbol} with self.snapshot.ticker: {self.snapshot.ticker} and {self.snapshot.bars}"
            )
             
            return self.snapshot

        except Exception as e:
            logger.error(f"Error in add_contract for {contract.symbol}: {e}")
            raise e

    async def get_historical_data(self, contract, barSizeSetting, vstopAtrFactor):
        try:
            bars = []
            df = pd.DataFrame()
            symbol = contract.symbol
            bar_sec_int = 0
            # parse to seconds (you already have this):
            barSizeString, bar_sec_int = await  convert_pine_timeframe_to_barsize(barSizeSetting)
            logger.info(f"Bar size {barSizeString} and bar_sec_int {bar_sec_int}")
            
            # if it's anything other than raw 5‑sec, roll up:
            if bar_sec_int in (10, 15, 30, 60):
                logger.info(f"checked bar_sec_int {bar_sec_int}")
                # 1) fetch the raw 5s history from your DB
                raw5 = await order_db.fetch_realtime_bars(symbol, 5, limit=int(3000/5))
                # 2) roll it up into N‑sec bars
                agg_bars = self.aggregate_5s_to_nsec(raw5, bar_sec_int)
                # 3) build your DataFrame exactly as before
                df = pd.DataFrame([{
                    "date": b.time,
                    "open": b.open_,
                    "high": b.high,
                    "low": b.low,
                    "close": b.close,
                    "volume": b.volume
                } for b in agg_bars])
                self.historical_data[symbol] = df
                historical_data_timestamp[symbol] =datetime.now()
                if not agg_bars:
                    logger.warning(f"No aggregated bars available for {symbol} after aggregation bar_sec_int {bar_sec_int}.bar_sec_int+10 {bar_sec_int +10}")
                    return None

                # 4) compute your vStop as usual
                if len(df) < 21:
                    logger.warning(f"No data available for {symbol} after aggregation bar_sec_int {bar_sec_int}.bar_sec_int+10 {bar_sec_int +10}")
                    bars = await self.ib.reqHistoricalDataAsync(
                        contract,
                        endDateTime="",
                        durationStr="1 D",
                        barSizeSetting=barSizeString,
                        whatToShow="TRADES",
                        useRTH=False,
                        formatDate=1,
                    )
                    df = util.df(bars)
                    if not bars:
                        logger.warning(f"No historical data for {symbol}")
                logger.info(f"Got bars historical data for {symbol}...")
            
            logger.info(f"Got df historical data for {symbol}...")
            self.snapshot = self.price_data[symbol]
            logger.info(f"Got self.snapshot historical data for {symbol}...")
            if df.empty:
                logger.warning(f"No historical data available for {symbol} after aggregation bar_sec_int {bar_sec_int}.bar_sec_int+10 {bar_sec_int +10}")
                return None
                
            vStop, uptrend = await self.vol_stop(df, 20, vstopAtrFactor)
            logger.info(f"Got vstop historical data for {symbol}...")

            if vStop is not None:
                df["vStop"] = vStop
                df["uptrend"] = uptrend
                vstop_data[symbol] = vStop  # full Series
                uptrend_data[symbol] = uptrend  # full Series
                uptrend_data[symbol] = uptrend
            
            self.historical_data[symbol] = df
            historical_data_timestamp[symbol] = datetime.now()
            logger.info(f"Got self.historical_data[] historical data for {symbol}...")

            self.snapshot.last = df["close"].iloc[-1]

            logger.info(
                f"{symbol} | Bid={self.snapshot.bid} Ask={self.snapshot.ask} "
                f"LastClose={self.snapshot.last} vStop={vstop_data[symbol].iloc[-1]} uptrend={uptrend_data[symbol].iloc[-1]}"
            )
            
            last_rows = df.tail(5).reset_index()
            formatted_table = (
                "\n"
                + "=" * 80
                + "\n"
                + "VOLATILITY STOP ANALYSIS (Last 5 bars)\n"
                + "=" * 80
                + "\n"
                + f"{'Timestamp':<20} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'vStop':>8} {'Trend':>8}\n"
                + "-" * 80
                + "\n"
            )

            for _, row in last_rows.iterrows():
                trend = "⬆️ UP" if row["uptrend"] else "⬇️ DOWN"
                formatted_table += (
                    f"{row['date'].strftime('%Y-%m-%d %H:%M:%S'):<20} "
                    f"{row['open']:>8.2f} {row['high']:>8.2f} {row['low']:>8.2f} {row['close']:>8.2f} "
                    f"{row['vStop']:>8.2f} {trend:>8}\n"
                )
            formatted_table += "=" * 80
            logger.info("Volatility Stop calculated and added to DataFrame")
            logger.info(formatted_table)
            first_get_historical_data = await self.first_get_historical_data(
                contract, barSizeSetting, vstopAtrFactor=1.5
            )
        
            return bars, df

        except Exception as e:
            logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
            raise e
            
    def floor_time_to_n(self, dt: datetime, n: int) -> datetime:
        """
        Floor datetime `dt` down to the nearest multiple of `n` seconds.
        e.g. dt=12:00:17, n=10 → 12:00:10
        """
        sec = (dt.second // n) * n
        return dt.replace(second=sec, microsecond=0)

    def aggregate_5s_to_nsec(self, bars5s: List[RealTimeBar], n: int) -> List[RealTimeBar]:
        """
        Take a list of 5‑sec RealTimeBar objects (oldest→newest),
        and roll them up into N‑sec bars.

        Returns a new list of RealTimeBar (with bar.time floored to N‑sec).
        """
        agg: List[RealTimeBar] = []
        current: RealTimeBar = None

        for b in bars5s:
            #logger.info("aggregate of 5s bars started")
            bucket = self.floor_time_to_n(b.time, n)

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
  
if __name__ == "__main__":
    try:
        log_config.setup()
        ib_connect = VolStopPriceData(ib=IB())
        
        asyncio.run(ib_connect.connect())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected, shutting down...")
        asyncio.run(ib_connect.graceful_disconnect())