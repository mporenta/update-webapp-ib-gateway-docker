# main.py
import os
from collections import defaultdict, deque
from fastapi import APIRouter
from pathlib import Path
from typing import Any, Dict
import math
import json
from dataclasses import dataclass
import pandas as pd
import time
import numpy as np
from datetime import datetime

from pandas_ta.volatility import atr, true_range
from fastapi import FastAPI, HTTPException, Request, Response, Query, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from ib_async.util import isNan
from ib_async import *
from dotenv import load_dotenv
import asyncio
from log_config import log_config, logger
from dev.break_change import VolatilityScanner  # Import our new scanner
from ticker_list import ticker_manager 
load_dotenv()

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
price_data = defaultdict(PriceSnapshot)
yesterday_close: dict[str, float] = {}  
# --- Configuration ---
PORT = int(os.getenv("TVWB_HTTPS_PORT", "80"))
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))

IB_HOST = '127.0.0.1'
subscribed_contracts: dict[str, Contract] = {}

@dataclass
class AccountPnL:
    unrealized_pnl: float
    realized_pnl: float
    total_pnl: float
    

class QueryModel(BaseModel):
    query: str

# Request cache to help filter duplicates if desired
class WebhookMetric(BaseModel):
    name: str
    value: float

class WebhookRequest(BaseModel):
    timestamp: int
    ticker: str  
    currency: str
    timeframe: str
    clientId: int
    key: str
    contract: str
    orderRef: str
    direction: str
    metrics: list[WebhookMetric]
class BarData(BaseModel):
    ticker: str
    durationStr: str
    barSizeSetting: str

class TickerSub(BaseModel): 
    ticker: str  

# Request model for our endpoint
class OrderRequest(BaseModel):
    rewardRiskRatio: float
    riskPercentage: float
    atrFactor: float
    vstopAtrFactor: float  
    kcAtrFactor: float     
    accountBalance: float
    ticker: str
    orderAction: str  
    stopType: str
    meanReversion: bool
    stopLoss: float
    submit_cmd: bool

# Request model for our endpoint
class tvOrderRequest(BaseModel):
    entryPrice: float
    rewardRiskRatio: float
    riskPercentage: float
    kcAtrFactor: float
    atrFactor: float
    vstopAtrFactor: float
    accountBalance: float
    ticker: str
    orderAction: str  # "BUY" for long, "SELL" for short
    stopType: str
    meanReversion: bool
    stopLoss: float
    submit_cmd: bool   

# For simplicity, our duplicate-check cache is a deque
from dataclasses import dataclass
@dataclass
class WebhookCacheEntry:
    ticker: str
    direction: str
    order_ref: str
    timestamp: int
request_cache = deque(maxlen=1000)
def is_duplicate(data: WebhookRequest) -> bool:
    current_time = data.timestamp
    while request_cache and (current_time - request_cache[0].timestamp) > 120000:
        request_cache.popleft()
    for entry in request_cache:
        if (entry.ticker == data.ticker and entry.direction == data.direction and
            entry.order_ref == data.orderRef and abs(entry.timestamp - current_time) <= 15000):
            logger.info(f"Duplicate webhook for {data.ticker}")
            return True
    request_cache.append(WebhookCacheEntry(data.ticker, data.direction, data.orderRef, current_time))
    return False

# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), '.env')
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))

ib = IB()



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Delete the orders.db file before connecting to IB
    logger.info("Deleting orders.db...")
   
  
 

    try:
        if not ib.isConnected():
            logger.info("IB connection established, subsrcribing to events...")
            await connect()
        yield
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected, shutting down...")
    finally:

        logger.info("Cancelling background task...")
        await graceful_disconnect()
        logger.info("IB disconnected.")


async def connect() -> bool:
    initial_delay = 1
    max_attempts = 300
    attempt = 0
    while attempt < max_attempts:
        try:
            logger.info(
                f"Connecting to IB at {IB_HOST}:{4002} : 187:{187}...with risk_amount {risk_amount}"
            )
            await ib.connectAsync(
                host=IB_HOST, port=4002, clientId=187, timeout=40
            )
            if ib.isConnected():
                logger.info("Connected to IB Gateway")

                await subscribe_to_events()

                return True
        except Exception as e:
            logger.error(f"Connection attempt {attempt+1} failed: {e}")
        attempt += 1
        await asyncio.sleep(initial_delay)
    logger.error("Max reconnection attempts reached")
    return False


async def on_disconnected():
    try:
        global shutting_down
        logger.warning("Disconnected from IB.")
        # Check the shutdown flag and do not attempt reconnect if shutting down.
        if shutting_down:
            logger.info("Shutting down; not attempting to reconnect.")
            return
        await asyncio.sleep(1)
        if not ib.isConnected():
            await connect()
    except Exception as e:

        logger.error(f"Error in on_disconnected: {e}")
        # Attempt to reconnect if not shutting down


async def graceful_disconnect():
    try:
        global shutting_down
        shutting_down = True  # signal shutdown


        # Remove all event handlers
        if ib.isConnected():
            logger.info("Unsubscribing events and disconnecting from IB...")

            # Get all event attributes
            event_attrs = [attr for attr in dir(ib) if attr.endswith("Event")]

            # Clear all event handlers
            for event_attr in event_attrs:
                try:
                    event = getattr(ib, event_attr)
                    if hasattr(event, "clear"):
                        event.clear()
                except Exception as e:
                    logger.warning(f"Error clearing event {event_attr}: {e}")

            # Disconnect from IB
            ib.disconnect()
            logger.info("Disconnected successfully.")

        return True

    except Exception as e:
        logger.error(f"Error during disconnect: {e}")
        return False


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
async def vol_stop(data: pd.DataFrame, atrlen: int, atrfactor: float):
    # 1) average of high+low
    
    atrM = None
    atr_series = {}
    avg_hl = (data["high"] + data["low"]) / 2

    # 2) compute ATR * factor, ask for full series
    atr_series = atr(
        high       = data["high"],
        low        = data["low"],
        close      = data["close"],
        length     = 20
       
    )
    logger.info(f"ATR series for {atrlen} bars: {atr_series}")
    

    # 3) guard against None or too little data
    if atr_series is None or not isinstance(atr_series, (pd.Series, pd.DataFrame)):
        logger.warning(
            f"ATR returned None or invalid for period {atrlen} "
            f"with only {len(data)} bars; defaulting ATR to zero."
        )
        atr_series = pd.Series(0.0, index=data.index)

    atrM = atr_series * atrfactor
    logger.info(f"ATR: atrM= {atrM} and atr_series={atr_series} with atrfactor={atrfactor}")
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
async def subscribe_to_events():
    try:
        if not ib.isConnected():
            logger.warning("IB is not connected, cannot subscribe to events.")
            await connect()
        contract_data = {
            "symbol": "NVDA",
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract_data)

        

    

        qualified_contract = await qualify_contract(ib_contract)
        symbol= qualified_contract.symbol if qualified_contract else ib_contract.symbol

        logger.info("Subscribing to IB events...")
        # 1) Figure out the exact bar size string and interval seconds
        yesterdayBar = await ib.reqHistoricalDataAsync(
            contract=qualified_contract,
            endDateTime="",
            durationStr="2 D",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        
        

        await asyncio.sleep(0.1)  

        df= util.df(yesterdayBar)
        if len(df) >= 2:
            d_high = df.iloc[-1]['high']
            d_low = df.iloc[-1]['low']
            price_data[symbol].yesterday_close_price = df.iloc[-2]['close']
            yesterday_close[symbol]=price_data[symbol].yesterday_close_price
            _yesterday_close =  yesterday_close[symbol]
           
            tr = max(d_high - d_low, abs(d_high - _yesterday_close), abs(d_low - _yesterday_close))
        else:
            tr = None
        vol = tr *100 / math.fabs(d_low) if d_low != 0 else None
        
        logger.info(f"Yesterday's close price for {symbol}: volatility: {vol} tr: {tr} and yesterday close: {_yesterday_close}")
      
        logger.info(f"Last bar high: {d_high}, low: {d_low} for {symbol}")   
        yesterday_close[symbol]=price_data[symbol].yesterday_close_price
        
        
        _yesterday_close =  yesterday_close[symbol]
        logger.info(f"Yesterday's close for {symbol} is {_yesterday_close}") 
      
        
        
        
        logger.info(f"yesterday_close: {yesterday_close} volatility: {vol} tr: {tr} d_high: {d_high} d_low: {d_low}")

        logger.info(f"Fetched {len(df)} historical bars for {symbol} with bar size 5 secs")
        print(df)
        
        last_historical_bar = df.iloc[-1]
        logger.info(f"Last historical bar for {symbol}: {last_historical_bar}")
        await add_contract(qualified_contract, barSizeSetting = "1 min")
        await asyncio.sleep(6)
        ticker = ib.ticker(qualified_contract)  # give it a moment to fetch
        await asyncio.sleep(1)  # give it a moment to fetch
        open_hist = last_historical_bar['open']
        open_price = ticker.open 
        high_price = ticker.high
        low_price = ticker.low 
        close_price = ticker.close 
        volume = ticker.volume 
        logger.info(f"Open: {open_price}, High: {high_price}, Low: {low_price}, Close: {close_price}, Volume: {volume} open_hist: {open_hist}")

        logger.info(f"Ticker data for {symbol}: {ticker}")
        # Ticker(contract=Contract(secType='STK', conId=4815747, symbol='NVDA', exchange='SMART', primaryExchange='NASDAQ', currency='USD', localSymbol='NVDA', tradingClass='NMS'), time=datetime.datetime(2025, 4, 25, 20, 12, 45, 739218, tzinfo=datetime.timezone.utc), minTick=0.01, bidSize=300.0, bidExchange='MZ', last=106.96, lastSize=300.0, lastExchange='D', volume=266835.0, open=106.85, high=107.72, low=105.73, close=106.43, markPrice=110.98999786, halted=0.0, bboExchange='9c0001', snapshotPermissions=3)




    
        if df.empty or len(df) < 1:
            logger.warning(f"No historical  bars for {symbol}")



    except Exception as e:
        logger.error(f"Error subscribing to events: {e}")
        raise e


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
        return templates.TemplateResponse("backtrader.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        
@app.post("/get_contracts")
async def get_contracts(contract: Request):
    raw_data = await contract.json()
    symbol = raw_data.get("ticker")
    logger.info(f"getting raw_data for: {symbol}")
    web_request = {"symbol": raw_data.get("ticker"), "exchange": "SMART", "secType": "STK", "currency": "USD"}
    logger.info(f"Canceling order web_request: {web_request}")
    
    ib_contract = Contract(**web_request)
    qualified_contract = await ib.qualify_contract(ib_contract)
    logger.info(f"qualified_contract: {qualified_contract}")
    json_qualified_contract = {
        "symbol": qualified_contract.symbol,
        
        "exchange": qualified_contract.primaryExchange
        
    } 
   
    return JSONResponse(content=json_qualified_contract)
        
@app.post("/tvscan_ib")
async def tv_volatility_scan_ib(ticker: Request, barSizeSetting: str = "1 min"):
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
    raw_data = await ticker.json()
    logger.info(f"Canceling order raw_data: {raw_data}")
    symbol = raw_data.get("ticker")
    contract = {"symbol": symbol, "exchange": "SMART", "secType": "STK", "currency": "USD"}
    try:
        logger.info(f"Processing TWS market data for {symbol} with bar size {barSizeSetting}")
        
        # Validate bar size setting
        valid_bar_sizes = [
            '1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
            '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
            '20 mins', '30 mins', '1 hour', '2 hours', '3 hours', 
            '4 hours', '8 hours', '1 day', '1 week', '1 month'
        ]
        
        if barSizeSetting not in valid_bar_sizes:
            logger.error(f"Invalid bar size setting: {barSizeSetting}")
            return {"error": f"Invalid bar size setting. Must be one of: {', '.join(valid_bar_sizes)}"}
        
        # Create a stock contract for the requested ticker
        contract = Stock(symbol, 'SMART', 'USD')
        
        # Determine durations based on bar size to capture enough data
        # for 9:30-9:36 AM analysis
        if 'secs' in barSizeSetting:
            duration_str = '1 D'  # 1 day for seconds data
        elif 'min' in barSizeSetting:
            duration_str = '5 D'  # 5 days for minute data
        elif 'hour' in barSizeSetting:
            duration_str = '2 W'  # 2 weeks for hourly data
        else:
            duration_str = '1 M'  # 1 month for daily or larger
        
        # Request historical data from TWS
        start_time = time.time()
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',  # Current time
            durationStr=duration_str,
            barSizeSetting=barSizeSetting,
            whatToShow='TRADES',
            useRTH=True
        )
        logger.info(f"Data fetch took {time.time() - start_time:.2f} seconds for {len(bars) if bars else 0} bars")
        
        if not bars or len(bars) < 2:
            logger.error(f"Insufficient data received for {ticker}")
            return {"error": f"Insufficient data received for {ticker}"}
        
        # Convert IB bars to pandas DataFrame with proper timezone
        df = pd.DataFrame({
            'datetime': [pd.to_datetime(bar.date) for bar in bars],
            'open': [bar.open for bar in bars],
            'high': [bar.high for bar in bars],
            'close': [bar.close for bar in bars],
            'low': [bar.low for bar in bars],
            'volume': [bar.volume for bar in bars]
        })
        
        # Use our scanner to analyze the data directly
        scanner = VolatilityScanner()
        start_time = time.time()
        matches = scanner.scan_dataframe(df)
        logger.info(f"Scan took {time.time() - start_time:.2f} seconds, found {len(matches)} matches")
        
        # Prepare result data
        match_data = []
        for match in matches:
            # Handle different datetime formats
            try:
                dt_str = match["datetime"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(match["datetime"], "strftime") else str(match["datetime"])
                
                match_data.append({
                    "datetime": dt_str,
                    "open": float(match["open"]),
                    "close": float(match["close"]),
                    "change_pct": float(match["change_pct"]),
                    "volume": float(match["volume"])
                })
                logger.info(f"Added match: {dt_str}, change: {match['change_pct']}%, volume: {match['volume']}")
            except Exception as e:
                logger.error(f"Error formatting match data: {e}")
                # Continue processing other matches even if one fails
        
        # Build the response with useful information
        response = {
            "ticker": symbol,
            "bar_size": barSizeSetting,
            "data_points": len(df),
            "matches_found": len(matches),
            "matches": match_data
        }
        
        # Add time range info if available
        if not df.empty:
            response["time_range"] = {
                "start": df['datetime'].min().strftime("%Y-%m-%d %H:%M:%S"),
                "end": df['datetime'].max().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        return response
    except Exception as e:
        logger.error(f"Error in tv_volatility_scan_ib: {e}")
        return {"error": str(e), "type": str(type(e).__name__)}
async def on_pending_tickers(tickers):
    logger.debug(f"Received pending tickers: {tickers}")

    for ticker in tickers:
        symbol = ticker.contract.symbol
        # If no ticker instance exists yet for this symbol, initialize it.
        if symbol not in price_data:
            price_data[symbol] = ticker
        snapshot = price_data[symbol]

        # Only update tick_last if the incoming 'last' is valid.
        if not isNan(ticker.last):
            snapshot.tick_last = ticker.last
        # Update bid and ask only if the new values are valid.
        if not isNan(ticker.bid):
            snapshot.bid = ticker.bid
        if not isNan(ticker.ask):
            snapshot.ask = ticker.ask
        # Update volume if the new volume is valid.
        if not isNan(ticker.volume):
            snapshot.volume = ticker.volume
        # Always update the tick_time (assumed to be non-NaN if provided)
        snapshot.tick_time = ticker.time
        snapshot.halted = ticker.halted
        snapshot.markPrice = ticker.markPrice
        snapshot.shortableShares = ticker.shortableShares
        


        logger.debug(
            f"Updated ticker for {symbol}: tick_last={getattr(snapshot, 'tick_last', None)}, "
            f"Bid={snapshot.bid}, Ask={snapshot.ask}, Volume={snapshot.volume}, "
            f"tick_time={snapshot.tick_time}"
        )

async def add_contract(contract: Contract, barSizeSetting: str) -> PriceSnapshot:
    try:
        symbol = contract.symbol

        # Request market data and 5-second real-time bars
        

        ticker: Ticker = ib.reqMktData(contract, genericTickList = "221,236", snapshot=False)
        ib.pendingTickersEvent += on_pending_tickers
        bars: RealTimeBarList = ib.reqRealTimeBars(contract, 5, "TRADES", False)

        #bars.updateEvent += on_bar_update

        # Initialize or update the PriceSnapshot entry

        # Wait briefly to allow market data stream to initialize
        #await asyncio.sleep(3)
        price_data[symbol] = ticker
        snapshot = price_data[symbol]

       
        snapshot.ticker = ticker
        snapshot.halted = ticker.halted
        snapshot.markPrice = ticker.markPrice
        snapshot.shortableShares = ticker.shortableShares
        logger.info(f"Ticker data for {symbol}:shortableShares {snapshot.shortableShares} markPrice {snapshot.markPrice} halted {snapshot.halted}")

        snapshot.bars = bars
        logger.info(
            f"Added contract {symbol} with snapshot.ticker: {snapshot.ticker} and {snapshot.bars}"
        )
        # Fetch historical bars and store additional data

        return snapshot

    except Exception as e:
        logger.error(f"Error in add_contract for {contract.symbol}: {e}")
        raise e
@app.post("/tvscan")
async def tv_csv_volatility_scan():
    try:
        directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
        csv_files = list(Path(directory).glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError("No CSV files found in the directory.")

        latest_csv = str(max(csv_files, key=os.path.getctime))
        logger.info(f"Processing TradingView CSV: {latest_csv}")

        # Use our new scanner instead of backtrader
        scanner = VolatilityScanner()
        matches = scanner.scan_csv(latest_csv)

        return {
            "message": f"TV CSV scan completed: {os.path.basename(latest_csv)}",
            "matches": [
                {
                    "datetime": match["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
                    "open": match["open"],
                    "close": match["close"],
                    "change_pct": match["change_pct"]
                } for match in matches
            ]
        }
    except Exception as e:
        logger.error(f"Error in tv_csv_volatility_scan: {e}")
        return {"error": str(e)}
@app.get("/autocomplete/")
async def autocomplete(query: str = Query(...)):
    results = ticker_manager.query_ticker_data(query)
    return JSONResponse(content=results)
@app.get("/tickers/")
async def get_all_tickers():
    cur = ticker_manager.conn.cursor()
    cur.execute("SELECT * FROM tickers")
    rows = cur.fetchall()
    tickers = [dict(row) for row in rows]
    return JSONResponse(content=tickers)


@app.get("/query-tickers/")
async def query_tickers(query_model: QueryModel = Depends()):
    results = ticker_manager.query_ticker_data(query_model.query)
    return JSONResponse(content=results)
if __name__ == "__main__":
    log_config.setup()
    uvicorn.run("main:app", host="localhost", port=5011, log_level="info")