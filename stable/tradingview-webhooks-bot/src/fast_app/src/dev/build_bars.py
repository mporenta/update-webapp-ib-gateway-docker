# app.py
from collections import defaultdict, deque

from turtle import up
from typing import *
import os, asyncio, time
import re
import threading
from threading import Lock
from datetime import datetime, timedelta
from matplotlib.pyplot import bar
import pytz

import math
from math import *


import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import signal
import sys
from ib_async import *
from ib_async import IB, Ticker, Contract, Stock, LimitOrder, StopOrder, util, Trade, Order, BarDataList, BarData, MarketOrder
from dotenv import load_dotenv
from models import OrderRequest, AccountPnL, TickerResponse, QueryModel, WebhookRequest, TickerSub, TickerRefresh, TickerRequest, PriceSnapshot, VolatilityStopData
from pandas_ta.overlap import ema 
from pandas_ta.volatility import  atr
# from pnl import IBManager
from new_pnl import IBManager
import numpy as np
from timestamps import current_millis
from log_config import log_config, logger
from ticker_list import ticker_manager


from indicators import daily_volatility
from p_rvol import PreVol
from break_change import VolatilityScanner  # Import our new scanner
from trade_dict import trade_to_dict
from my_util import *
df = pd.DataFrame()
global_close_prices = {}
ema9_dict = {}
# from hammer import HammerBacktest, TVOpeningVolatilityScan
load_dotenv()

# --- Configuration ---
PORT = int(os.getenv("FAST_API_PORT", "80"))
HOST = os.getenv("FAST_API_HOST", "localhost")
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))

boofMsg = os.getenv("HELLO_MSG")

# Instantiate IB manager
ib = IB()
ib_manager = IBManager(ib)
from vol_stop_price import PriceDataNew
ib_vol_data = PriceDataNew(ib_manager)
from price import PriceData
ib_price_data = PriceData(ib_manager)

pre_rvol = PreVol(ib_manager)

reqId = {}


# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), ".env")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))



ib = IB()
price_data_lock = Lock()
historical_data_timestamp = {}
subscribed_contracts =  defaultdict(dict)
vstop_data = defaultdict(VolatilityStopData)
ema_data= defaultdict(dict)
uptrend_data =  defaultdict(VolatilityStopData)
open_orders = defaultdict(dict)
historical_data =  defaultdict(dict)
last_fetch = {}
price_data = defaultdict(PriceSnapshot)




async def vol_stop(data, atrlen, atrfactor):
    try:
        logger.info("Calculating Volatility Stop...")
        df= {}
        bars = None
            
                
            
        # Calculate average of high and low prices
        avg_hl = (data['high'] + data['low']) / 2
        logger.info(f"Average High-Low calculated: {avg_hl.tail(5)}")
        atr_pandas_ta = atr(data['high'], data['low'], data['close'], atrlen)
        nine_ema = ema(data['close'], length=9).tolist() 
        logger.info(f"ATR calculated for {atrlen} periods: {atr_pandas_ta.tail(5)}") 
            
        atrM = atr_pandas_ta * atrfactor
        atrM = atrM.fillna(0)  # Fill NaN values with 0
        #close = data['close']
        close=avg_hl
        

        logger.info("atrM: ", atrM.tail(5))  # Debugging line to check ATR values

        max_price = close.copy()
        min_price = close.copy()
        vStop = pd.Series(np.nan, index=close.index)
        uptrend = pd.Series(True, index=close.index)

        for i in range(1, len(close)):
            max_price.iloc[i] = max(max_price.iloc[i-1], close.iloc[i])
            min_price.iloc[i] = min(min_price.iloc[i-1], close.iloc[i])
            vStop.iloc[i] = vStop.iloc[i-1] if not np.isnan(vStop.iloc[i-1]) else close.iloc[i]

            if uptrend.iloc[i-1]:
                vStop.iloc[i] = max(vStop.iloc[i], max_price.iloc[i] - atrM.iloc[i])
            else:
                vStop.iloc[i] = min(vStop.iloc[i], min_price.iloc[i] + atrM.iloc[i])

            uptrend.iloc[i] = close.iloc[i] - vStop.iloc[i] >= 0

            if uptrend.iloc[i] != uptrend.iloc[i-1] and i > 1:
                max_price.iloc[i] = close.iloc[i]
                min_price.iloc[i] = close.iloc[i]
                vStop.iloc[i] = max_price.iloc[i] - atrM.iloc[i] if uptrend.iloc[i] else min_price.iloc[i] + atrM.iloc[i]

        return vStop, uptrend, nine_ema
    except Exception as e:
        logger.error(f"Error calculating Volatility Stop: {e}")
        raise e

# Handlers
async def on_pending_tickers(tickers):
    logger.debug(f"Received pending tickers: {tickers}")
    
    for ticker in tickers:
        
        
       
        symbol = ticker.contract.symbol
        price_data[symbol] = ticker
        snapshot = price_data[symbol]

        

        snapshot.last = ticker.last
        snapshot.bid = snapshot.bid
        snapshot.ask = snapshot.ask
        snapshot.volume = ticker.volume

        logger.debug(
            f"Updated ticker for {symbol}: "
            f"Last={snapshot.last}, Bid={snapshot.bid}, Ask={snapshot.ask}, Vol={snapshot.volume}"
        )
        logger.debug(f"Updated ticker for {symbol}: bid: {snapshot.bid } ask: {snapshot.ask} last: {snapshot.last}")



# Add this global structure:
realtime_bar_buffer = defaultdict(lambda: deque(maxlen=1))  # Now we process each 5s bar directly
confirmed_5s_bar = defaultdict(lambda: deque(maxlen=720))  # 1 hour of 5s bars (720 bars)

def floor_time_to_5s(dt: datetime):
    return dt.replace(second=(dt.second // 5) * 5, microsecond=0)

async def on_bar_update(bars, has_new_bar):
    if not bars or len(bars) == 0:
        return
    if not has_new_bar:
        return
    symbol = bars.contract.symbol
    last_bar = bars[-1]

    # Store DataFrame in the historical_data dictionary
    df = util.df(bars)
    historical_data[symbol] = df
    last_historical_bar = df.iloc[-1]

    # Each bar is already 5 seconds from IB, so we can use it directly
    time_ = floor_time_to_5s(last_bar.time)

    # Create a confirmed 5s bar directly from the real-time bar
    confirmed_bar = {
        "time": time_,
        "open": last_bar.open_,
        "high": last_bar.high,
        "low": last_bar.low,
        "close": last_bar.close,
        "volume": last_bar.volume,
        "vStop": None,
        "ema9": None,
        "uptrend": None
    }

    # Store in buffer (just for consistency with the rest of the code)
    realtime_bar_buffer[symbol].append(last_bar)

    # Add to confirmed bars collection
    confirmed_5s_bar[symbol].append(confirmed_bar)
    price_data[symbol].confirmed_5s_bar = confirmed_bar

    logger.debug(f"Confirmed 5s bar for {symbol} at {time_}: {confirmed_bar} last_historical_bar: {last_historical_bar}")
# Contract handler
async def add_contract(contract: Contract, barSizeSetting: str) -> PriceSnapshot:
    try:
        symbol = contract.symbol
        

        # Request market data and 5-second real-time bars
        ib.reqMktData(contract, snapshot=False)
        ib.pendingTickersEvent += on_pending_tickers
        bars: RealTimeBarList = ib.reqRealTimeBars(contract, 5, 'TRADES', False)
        

        bars.updateEvent += on_bar_update
        

        # Initialize or update the PriceSnapshot entry
        

        # Wait briefly to allow market data stream to initialize
        await asyncio.sleep(3)
        ticker: Ticker = ib.ticker(contract)
        
        snapshot = price_data[symbol]
        snapshot.ticker = ticker
        snapshot.bars = bars
        logger.info(f"Added contract {symbol} with snapshot.ticker: {snapshot.ticker} and {snapshot.bars}")
        # Fetch historical bars and store additional data
        await get_historical_data(contract, barSizeSetting, vstopAtrFactor=1.5)

        return snapshot

    except Exception as e:
        logger.error(f"Error in add_contract for {contract.symbol}: {e}")
        raise e
async def get_historical_data(contract, barSizeSetting, vstopAtrFactor):
    try:
        symbol = contract.symbol
        logger.info(f"Fetching historical data for {symbol}...")

        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',
            durationStr='1 D',
            barSizeSetting=barSizeSetting,
            whatToShow='TRADES',
            useRTH=False,
            formatDate=1
        )

        if not bars:
            logger.warning(f"No historical data for {symbol}")
            return None, None

        df = util.df(bars)
        snapshot = price_data[symbol]

        vStop, uptrend, nine_ema = await vol_stop(df, 20, vstopAtrFactor)

        if vStop is not None:
            df['vStop'] = vStop
            df['uptrend'] = uptrend
            vstop_data[symbol] = vStop  # full Series
            uptrend_data[symbol] = uptrend  # full Series

            ema_data[symbol] = nine_ema
            uptrend_data[symbol] = uptrend

        df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
        historical_data[symbol] = df
        historical_data_timestamp[symbol] = datetime.utcnow()

        snapshot.last = df['close'].iloc[-1]

        logger.info(
            f"{symbol} | Bid={snapshot.bid} Ask={snapshot.ask} "
            f"LastClose={snapshot.last} vStop={vstop_data[symbol].iloc[-1]} uptrend={uptrend_data[symbol].iloc[-1]}"
        )
        last_rows = df.tail(5).reset_index()
        formatted_table = (
            "\n" + "="*80 + "\n" +
            "VOLATILITY STOP ANALYSIS (Last 5 bars)\n" +
            "="*80 + "\n" +
            f"{'Timestamp':<20} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'vStop':>8} {'Trend':>8}\n" +
            "-"*80 + "\n"
        )

        for _, row in last_rows.iterrows():
            trend = "⬆️ UP" if row['uptrend'] else "⬇️ DOWN"
            formatted_table += (
                f"{row['date'].strftime('%Y-%m-%d %H:%M:%S'):<20} "
                f"{row['open']:>8.2f} {row['high']:>8.2f} {row['low']:>8.2f} {row['close']:>8.2f} "
                f"{row['vStop']:>8.2f} {trend:>8}\n"
            )
        formatted_table += "="*80
        logger.info("Volatility Stop calculated and added to DataFrame")
        logger.info(formatted_table)

        return bars, df

    except Exception as e:
        logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
        raise

    


    except Exception as e:
        logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
        raise e

    
async def _heartbeat():
    while True:
        await asyncio.sleep(60)


# FastAPI lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    await ib.connectAsync('127.0.0.1', 4002, clientId=33)
    
    logger.info(f"Starting application; connecting to IB...{risk_amount}")
    if not await ib_manager.connect():
            raise RuntimeError("Failed to connect to IB Gateway")
    

    logger.info("IB connected and listeners registered.")
    

    # Keep background tasks alive
    bg_task = asyncio.create_task(_heartbeat())
    try:
        yield
    finally:
        bg_task.cancel()
        ib.disconnect()
        logger.info("IB disconnected.")



app = FastAPI(lifespan=lifespan)
app.mount(
    "/static", StaticFiles(directory=os.path.join(CURRENT_DIR, "static")), name="static"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://portfolio.porenta.us", "https://tv.porenta.us"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    try:
        return templates.TemplateResponse("tbot_dashboard.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/refresh_mk_data")
async def subscribe_to_ticker(ticker: TickerRefresh):
    try:
        logger.info(f"Received subscription request for {ticker.ticker}")
        barSizeSetting = ticker.barSizeSetting
        if barSizeSetting is None:
            barSizeSetting = "1 min"
        vstopAtrFactor = ticker.vstopAtrFactor
        if vstopAtrFactor is None:
            vstopAtrFactor = 1.5
        symbol = ticker.ticker.upper()  # Ensure the ticker symbol is in uppercase
        contract = Stock(symbol, 'SMART', 'USD')
        await get_historical_data(contract, barSizeSetting, vstopAtrFactor)
       
        logger.info(f"Refreshed data to {symbol} with barSizeSetting: {barSizeSetting}")
        return {"status": f"Subscribed to {symbol} with barSizeSetting: {barSizeSetting}"}
    except Exception as e:
        logger.error(f"Error subscribing to ticker: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/boof")
async def sub_to_ticker(ticker: TickerSub):
    
    barSizeSetting = ticker.barSizeSetting
    logger.info(f"Subscribing to ticker: {ticker.ticker} with barSizeSetting: {barSizeSetting}")
    symbol = ticker.ticker 
    contract = Stock(ticker.ticker, 'SMART', 'USD')
    
    # Fix: Get the qualified contract (not the entire dictionary)
    qualified_contract = await qualify_contract(contract)
    
    # Store it in the subscribed_contracts dictionary
    if qualified_contract:
        subscribed_contracts[symbol] = qualified_contract
        
        # Pass the specific contract to add_contract, not the entire dictionary
        await add_contract(qualified_contract, barSizeSetting)
        
        #await asyncio.sleep(3)  # Give some time for the contract to be added and data to start flowing
        bar = price_data[symbol].bars
        t = price_data[symbol].ticker
        # Rest of the function...
    if not t:
        await sub_to_ticker(ticker)
        t = price_data[symbol].ticker
        bar = price_data[symbol].bars
    logger.info(f"Market data for {ticker.ticker}: t: {t} bar: {bar}")
           
           

    # If ticker is stored as a dictionary (e.g., {"TSLA": Ticker(...)}), get one ticker value.
    if t and isinstance(t, dict):
        t_value = next(iter(t.values()))
        bar_value = bar.get(0) if bar else None
        if not t_value:
            logger.error(f"No valid ticker data found for {symbol}")
            raise ValueError(f"No valid ticker data found for {symbol}")
        if not bar_value:
            logger.error(f"No valid bar data found for {symbol}")
            raise ValueError(f"No valid bar data found for {symbol}")
        
       

        logger.info(f"Ticker data: {t_value}")
        data = {
            "symbol": t_value.contract.symbol,
            "bidSize": t_value.bidSize,
            "bid": t_value.bid,
            "ask": t_value.ask,
            "askSize": t_value.askSize,
            "high": bar_value.high,
            "low": bar_value.low,
            "close": bar_value.close,
        }
        logger.info(f"Market data for {ticker.ticker}: {data}")
    logger.info(f"Subscribed to {contract} with barSizeSetting: {barSizeSetting}")
    return {"status": f"Subscribed to {ticker.ticker} with barSizeSetting{barSizeSetting}"}




@app.get("/active-tickers")
async def list_active_tickers():
    try:
        active_tickers = await ib_manager.get_active_tickers()

        # Handle NaN values in the response to make it JSON compliant
        def clean_nan(value):
            if isinstance(value, dict):
                return {k: clean_nan(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [clean_nan(item) for item in value]
            elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                return None
            else:
                return value

        cleaned_tickers = clean_nan(active_tickers)
        return JSONResponse(content={"status": "success", "tickers": cleaned_tickers})
    except Exception as e:
        logger.error(f"Error retrieving tickers: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})
@app.get("/tickers/")
async def get_all_tickers():
    cur = ticker_manager.conn.cursor()
    cur.execute("SELECT * FROM tickers")
    rows = cur.fetchall()
    tickers = [dict(row) for row in rows]
    return JSONResponse(content=tickers)


@app.get("/api/pnl-data")
async def get_current_pnl():
    try:
        logger.info(f"boof message {boofMsg}")
        completed_order = []
        new_get_position_update = await ib_manager.get_position_update()
        logger.info(f"New get_position_update: {new_get_position_update}")

        # Unpack the values from app_pnl_event()
        orders, position_json, pnl, close_trades = await ib_manager.app_pnl_event()
        net_liquidation = ib_manager.net_liquidation
        buying_power = ib_manager.buying_power
        get_positions_from_db = await ib_manager.get_positions_from_db()
        for close_trade in close_trades:
            if close_trade.orderStatus.status != "Cancelled":
                completed_order = close_trade
       

        data = {
            "get_positions_from_db": jsonable_encoder(get_positions_from_db),
            "trades": jsonable_encoder(orders),
            "completed_orders": jsonable_encoder(completed_order),
            "pnl": jsonable_encoder(pnl),
            "positions": jsonable_encoder(position_json),
            
            "net_liquidation": net_liquidation,
            "buying_power": buying_power
            
        }
        
        logger.debug(f"Got them for jengo - get_current_pnl {data}")
        logger.info(f"Got them for jengo - positions {jsonable_encoder(position_json)}")
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error fetching pnl data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def qualify_contract(contract: Contract):
    try:
        symbol = contract.symbol
        logger.info(f"Qualifying contract: {symbol}...")
        
        # If we get here, the contract isn't in the cache, so qualify it
        qualified = await ib.qualifyContractsAsync(contract)
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


@app.get("/price", response_model=TickerResponse)
async def get_ticker_price(ticker: str = Query(..., description="Stock ticker symbol")):
    global subscribed_contracts  # Declare it as global to avoid the UnboundLocalError
    symbol = ticker  # Define symbol first using the function parameter
    snapshot = price_data[symbol]  # Now it's safe to use
    bid_price = snapshot.bid
    last = snapshot.last
    confirmed_bar = snapshot.confirmed_5s_bar
    tick = snapshot.ticker
    # Check if ticker exists in subscribed_contracts
    contract = subscribed_contracts.get(ticker)
    
    logger.info(f"Processing webhook request for {ticker} asynchronously.")
    symbol = ticker
    
    # Ensure the ticker symbol is in uppercase
    if symbol not in subscribed_contracts:
        logger.info(f"Ticker {symbol} not found in subscribed contracts")
        contract_data = {
            "symbol": symbol,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract_data)
        # Get the qualified contract, don't replace the entire dictionary
        qualified_contract = await qualify_contract(ib_contract)
        await add_contract(qualified_contract, barSizeSetting='1 min')
        contract = qualified_contract  # Use the qualified contract
    
    if not contract:
        raise HTTPException(status_code=404, detail=f"Contract for ticker {symbol} not found")
    
  
    
    # Access the ticker data from your price_data instance
    if symbol not in ib_vol_data.ticker:
        raise HTTPException(status_code=404, detail=f"Ticker {symbol} not found or not subscribed")
    
    ticker_obj = price_data[symbol].ticker
    last = snapshot.last
    logger.info(f"Order prices for {symbol}: {last}")
    
    # Safe extraction of values with default
    def safe_float(value, default=0.0):
        if value is None or math.isnan(value) or not math.isfinite(value):
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    # Extract relevant data from the ticker object with safety checks
    response = {
        "symbol": symbol,
        "bid": safe_float(getattr(ticker_obj, 'bid', None)),
        "ask": safe_float(getattr(ticker_obj, 'ask', None)),
        "last": safe_float(getattr(ticker_obj, 'last', None)),
        "high": safe_float(getattr(ticker_obj, 'high', None)),
        "low": safe_float(getattr(ticker_obj, 'low', None)),
        "close": safe_float(getattr(last, 'close', None)),
        "tick_close": safe_float(getattr(ticker_obj, 'close', None)),
        "volume": safe_float(getattr(ticker_obj, 'volume', None), 0),
        "timestamp": str(ticker_obj.time) if hasattr(ticker_obj, "time") and ticker_obj.time else datetime.datetime.now().isoformat()
    }
    
    return response
if __name__ == "__main__":
    log_config.setup()

    # Create a force exit mechanism
    def force_exit():
        logger.warning("Forcing application exit after timeout")
        # Use os._exit instead of sys.exit to force termination
        os._exit(0)

    # Add signal handlers that include a force-exit failsafe
    def handle_exit(sig, frame):
        logger.info(f"Received signal {sig} - initiating graceful shutdown")

        # Set a timeout to force exit if needed
        timer = threading.Timer(1.0, force_exit)
        timer.daemon = True  # Make sure the timer doesn't prevent exit
        timer.start()

        # Let the normal shutdown process continue

    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # Run the application
    uvicorn.run("larry:app", host=HOST, port=PORT, log_level="info")