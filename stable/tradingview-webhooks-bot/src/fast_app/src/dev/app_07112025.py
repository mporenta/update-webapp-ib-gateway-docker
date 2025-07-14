
# app.py	
from asyncio import tasks
from collections import defaultdict, deque
import jsonpickle

from typing import *
import os, asyncio
from dotenv import load_dotenv
from datetime import time as dt_time
import threading
import signal
from threading import Lock
from datetime import datetime, timedelta, timezone

from math import *
import httpx

import talib as ta  
import pandas as pd
from fastapi import FastAPI, Depends, HTTPException, Request, BackgroundTasks, Security, Query
from fastapi.security.api_key import APIKeyHeader, APIKey
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pandas_ta.volatility import atr
from polygon_src.main_wsh_rvol import start_rvol
import sys
from ib_async.util import isNan
from ib_async import *



ib = IB()
from models import (
    OrderRequest,
    AccountPnL,
    PriceSnapshot,
    
    TickerSub,
    TickerRequest,

   
   
)
from pnl import  IBManager, get_orders
from add_contract import add_new_contract
from pnl_close import update_pnl
from tv_ticker_price_data import volatility_stop_data, tv_store_data, price_data_dict, daily_volatility, yesterday_close, timeframe_dict, order_placed, subscribed_contracts, ib_open_orders, parent_ids, yesterday_close_bar, agg_bars
from account_values import account_metrics_store
# from pnl import IBManager 
account_pnl: dict[str, dict[int, PnL]] = defaultdict(dict)
from tv_scan import tv_volatility_scan_ib
import numpy as np
from log_config import log_config, logger
from ticker_list import ticker_manager
from check_orders import  process_web_order_check, ck_bk_ib
from bar_size import convert_pine_timeframe_to_barsize
from stop_order import stop_loss_order, market_stop_loss_order, market_bracket_order, limit_bracket_order, new_trade_update_asyncio
from tech_a import gpt_data_poly, get_ib_pivots, ema_check
from my_util import is_float, clean_nan, get_bar_size_setting, vol_stop, get_timestamp, ticker_to_dict, get_active_tickers, floor_to_market_interval, is_market_hours, shortable_shares, aggregate_5s_to_nsec, update_ticker_data,  fetch_agg_bars, ensure_utc, floor_time_to_n, get_dynamic_atr, upsert_agg_bar, get_tv_ticker_dict, compute_position_size, get_latest_csv, delete_orders_db, format_order_details_table
from ib_db import IBDBHandler
from fill_data import fill_data


from ib_bracket import ib_bracket_order, bk_ib, bracket_trade_update,new_trade_update

reqId = {}

from sqlalchemy import (
    create_engine, Column, String, Integer, Float, BigInteger,
    PrimaryKeyConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
load_dotenv()
boof = os.getenv("HELLO_MSG")
api_key = os.getenv("POLYGON_API_KEY")
from polygon import RESTClient

from polygon.rest.models import TickerDetails
from polygon.rest.reference  import *
from polygon.exceptions import BadResponse
API_KEY   = os.getenv("POLYGON_API_KEY")
poly_bad_response = BadResponse()
poly_client = RESTClient(api_key)
order_db = IBDBHandler("orders.db")
# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), ".env")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))

shutting_down = False
price_data_lock = Lock()
historical_data_timestamp = {}

ema_data = defaultdict(dict)
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

order_details = {}
orders_dict: Dict[str, Trade] = {}

# --- Configuration ---
PORT = int(os.getenv("FAST_API_PORT", "5011"))
HOST = os.getenv("FAST_API_HOST", "127.0.0.1")
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv("FAST_API_CLIENT_ID", "2222"))
ib_host = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
ib_port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
boofMsg = os.getenv("HELLO_MSG")

ib_manager = IBManager(None)
subscribed_contracts.set_ib(ib)
bk_ib.set_ib(ib)
ck_bk_ib.set_ib(ib)
# default ATR factor
raw_key = os.getenv("ENV_MY_GPT_API_KEY")
if not raw_key:
    raise RuntimeError("ENV_MY_GPT_API_KEY not set in .env")
MY_GPT_API_KEY = f"{raw_key}{datetime.now().strftime('%Y%m%d')}"
print(f"MY_GPT_API_KEY: {MY_GPT_API_KEY}")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def get_api_key(request: Request = None, api_key_header: str = Security(api_key_header), apikey: str = Query(None, description="API key as query parameter")
) -> APIKey:
    """
    Check for API key in both header (X-API-Key) and query parameter (apikey).
    Priority: header first, then query parameter.
    Skip API key for routes that serve HTML templates and dashboard endpoints.
    """
    allowed_paths = [
        "/", 
        "/db_view", 
        "/static", 
        "/favicon.ico",
        "/api/cancel-order",
        "/api/cancel-all-orders",
        "/api/pnl-data",
        "/tvscan",
        "/tvscan_ib",
        "/get_contracts",
        "/close_positions",
        "/unsubscribe_from_ticker",
        "/rvol",
        "/active-symbols",
        "/check_order",
        "/place_order",
        "/webapp-post-data",
        "/symbols"
    ]
    if request and any(request.url.path.startswith(path) for path in allowed_paths):
        return None  # No API key required for these routes

    api_key = api_key_header or apikey
    if api_key == MY_GPT_API_KEY:
        return api_key

    raise HTTPException(
        status_code=401,
        detail="Invalid or missing API key. Provide via X-API-Key header or apikey query parameter",
        headers={"WWW-Authenticate": "API Key"},
    )


async def on_pending_tickers(tickers):
    try:
        barSizeSetting = None
        
        new_ticker= None
        contract: Contract = None
        symbol=None

        for ticker in tickers:
            new_ticker= ticker
            contract = ticker.contract
            symbol = ticker.contract.symbol
            barSizeSetting = await get_bar_size_setting(symbol, False)
            if not barSizeSetting:
                barSizeSetting = "15 secs"  # default to 15 seconds if not found
            
            
            await price_data_dict.add_ticker(new_ticker, barSizeSetting, False)
            #logger.debug(f" ticker symbol is {symbol}; for pending tickers barSizeSetting is {barSizeSetting}")
            
            
            


        #logger.debug(f" ticker symbol is {symbol}; for pending tickers barSizeSetting is {barSizeSetting}")
        

    except Exception as e:
        logger.error(f"Error in on_pending_tickers: {e}")
        raise e

def floor_time_to_5s(dt: datetime):
    return dt.replace(second=(dt.second // 5) * 5, microsecond=0) 

async def on_bar_update(bars, has_new_bar):
    try:
        if not bars or len(bars) == 0:
            return
        if not has_new_bar:
            return

        symbol = bars.contract.symbol
        contract= bars.contract
        
        
        

        # Convert the real-time bars to a DataFrame and store in historical_data.
        df = util.df(bars)
        historical_data[symbol] = df
        

        # Floor the bar time to a 5-second interval.
        last = bars[-1]
        time_ = floor_time_to_5s(last.time)
        bar_size = getattr(bars, "_bar_size", 5)
        # Build the bar_dict dictionary.
        # Only update if the incoming value is not NaN; otherwise preserve (or leave as None).
        rt_bar_dict = {
            "time": getattr(last, "date", getattr(last, "time", None)),
            "open_": last.open_,
            "high": last.high,
            "low": last.low,
            "close": last.close,
            "volume": getattr(last, "volume", None),
        }
        
        logger.debug(rt_bar_dict)
        price_data=await price_data_dict.add_rt_bar(symbol, rt_bar_dict)
        barSnap = await price_data_dict.get_snapshot(symbol)
        logger.debug(f"Realtime bar for {symbol} added")
        logger.debug(barSnap.rt_bar)
        await agg_bars.update(symbol)
        
     

        
    except Exception as e:
        logger.error(f"Failed to write realtime bar: {e}")

async def handle_bar(contract, bars):
    try:
        symbol = contract.symbol
        contract= contract
        barSizeSetting = await get_bar_size_setting(symbol) # default to 1 min if not found
        
        

        if not bars:
            logger.warning(f"No IB history for {contract.symbol}")
            return None
        df = util.df(bars)
        logger.debug(df)
        
        open= df['open'][0]
        close= df['close'][0]
        high= df['high'][0]
        low= df['low'][0]
        volume= df['volume'][0]
        time= df['date'][0]
        Last_bar_json= {"open": open, "close": close, "high": high, "low": low, "volume": volume, "time": time}
        
                # Normalize into a plain dict
        logger.debug('Last_bar_json')
                    

                # Store it
        await price_data_dict.add_bar(contract.symbol, barSizeSetting, Last_bar_json)
    except Exception as e:
            logger.error(f"Error handling bar for {symbol}: {e}")
            raise e     



def calculate_market_price(snapshot):
    """
    Calculate market_price using the snapshot's bid, ask, and bar_close values.
    Here we:
      - Check if bid and ask are valid (non-zero and non-NaN).
      - Compute the midpoint of bid and ask.
      - Average the midpoint with bar_close to smooth transient fluctuations.

    Args:
        snapshot: An object from price_data[symbol] that has attributes:
            - bid (live bid price)
            - ask (live ask price)
            - bar_close (the close value from the latest confirmed bar)

    Returns:
        A float representing the calculated market price.
    """
    # Ensure bid, ask, and bar_close are available and valid.
    bid = snapshot.bid
    ask = snapshot.ask
    bar_close = snapshot.bar_close

    # Check basic validity of bid and ask (adjust your conditions as needed)
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        midpoint = (bid + ask) / 2
    else:
        # Fallback: Use the last bar's close when bid/ask are missing or invalid.
        midpoint = bar_close

    # Combine the midpoint with the bar_close to get a more representative market price.
    # You could also use different weights if desired.
    market_price = (midpoint + bar_close) / 2
    return market_price


# Usage inside an order function:
# snapshot = await price_data_dict.get_snapshot(symbol)  # Assume this has been updated by our event handlers
# market_price = calculate_market_price(snapshot)
# logger.debug(f"Calculated market_price for {contract.symbol}: {market_price}")

# make sure this sits below where historical_data is defined




async def get_positions():
    try:
        current_position= None
        
        current_position = await ib.reqPositionsAsync()
        if current_position is not None and len(current_position) > 0:
            await order_db.insert_positions(current_position)
            ib_data = None
            logger.warning("positions found in IB.")
        

        return None

    except Exception as e:
        logger.error(f"Error fetching positions: {e}")
        return None


 



async def error_code_handler(
    reqId: int, errorCode: int, errorString: str, contract: Contract
):
    logger.warning(f"Error for reqId {reqId}: {errorCode} - {errorString}")




async def cancel_open_orders(position: Position):
    try:
        openOrders= None
        parent_id= None
        sym = position.contract.symbol 
        if sym:
            parent_id=parent_ids[sym]
        # Only proceed if this symbol is one we traded.
        if position.position == 0 and parent_id is not None:
            openOrders=ib.openOrders()
            
            # Iterate open orders; cancel any StopOrder whose orderId == parent_id
            for o in openOrders:
                # Confirm it’s a StopOrder (orderType = "STP") and matches our orderId.
                if o.orderId == parent_id or o.orderId == parent_id or o.permId == parent_id:
                    try:
                        ib.cancelOrder(o)
                        logger.info(f"[{sym}] canceled Stop‑Loss(orderId={o.orderId}) after manual close position.")
                    except Exception as cancel_err:
                        logger.error(f"[{sym}] error canceling orderId={o.orderId}: {cancel_err}")
    except Exception as e:
        logger.error(f"Error canceling open orders for {position.contract.symbol}: {e}")
        return False
#=================================FastAPI Setup=================================   
# 
# This FastAPI application connects to Interactive Brokers (IB) and manages trading operations.
# It handles real-time bar updates, order placements, and position management.
#   
#===============================  
@asynccontextmanager
async def lifespan(app: FastAPI):
    log_config.setup()
    # Delete the orders.db file before connecting to IB
    logger.info("Deleting orders.db...")
    logger.info(f"boof message66 {boofMsg} with Fast API port {PORT} and Fast API host {HOST}")
    delete_orders_db()
    

    
    try:
        #await ib_manager.connect()
        if not ib.isConnected():
            logger.info("Connecting to IB...")
            await order_db.connect()
            logger.info(
                "Order database connection established, subscribing to events."
            )
            
            await connect()
            

        yield
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected, shutting down...")
    finally:

        logger.info("Cancelling background task...")
        if ib.isConnected():
            await graceful_disconnect()
            logger.info("IB disconnected.")


async def connect() -> bool:
    initial_delay = 1
    max_attempts = 300
    attempt = 0
    while attempt < max_attempts:
        try:
            logger.info(
                f"Connecting to IB at {ib_host}:{ib_port} : client_id:{client_id}...with risk_amount {risk_amount}"
            )
            await ib.connectAsync(
                host=ib_host, port=ib_port, clientId=client_id, timeout=40
            )
            logger.info("IB connection established, subsrcribing to events...")

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


async def subscribe_to_events():
    try:
        RUN_WSH = int(os.getenv("RUN_WSH", 0))

        run_wsh=RUN_WSH
        logger.info(f"RUN_WSH is set to {run_wsh}")
        account = (
            ib.managedAccounts()[0] if ib.managedAccounts() else None
        )
        if not account:
            return
        ib.reqPnL(account)
        await asyncio.sleep(1)  # Allow time for PnL request to process
        logger.info("Subscribing to IB events...")
        pnl= ib.pnl(account)
        
        
        
        # ib.execDetailsEvent += on_trade_fill_event
        ib.newOrderEvent += new_trade_update_asyncio
        #ib.orderStatusEvent += ib_open_orders.on_order_status_event

        ib.orderStatusEvent += new_trade_update_asyncio

        ib.pendingTickersEvent += on_pending_tickers
        ib.pnlEvent += on_pnl_event
        
       
        portfolio_items = ib.portfolio()
        await order_db.insert_portfolio_items(portfolio_items)
        for items in portfolio_items:
            symbol= items.contract.symbol
            position = items.position
            barSizeSetting = "1 min"  # default bar size for portfolio items
            if position != 0:
                logger.info(f"Portfolio item {symbol} has position {position}, checking for existing contract...")
                contract = await add_new_contract(symbol, barSizeSetting, ib)
            



        ib.disconnectedEvent += on_disconnected
        ib.errorEvent += error_code_handler
        rvolFactor = 0.2
        await get_positions()
        
            
        if run_wsh==11111:
            rvol=await start_rvol(rvolFactor, True)
            if rvol:
                logger.info(f"rvol started with factor {rvolFactor}")
        await account_metrics_store.ib_account(account)
        logger.info(f"Account account_metrics_store {account_metrics_store.account}.")
       
        account_pnl=await account_metrics_store.update_pnl_dict_init(pnl)
        logger.info(f"Initial PnL for account {account}: {account_pnl}")

    except Exception as e:
        logger.error(f"Error subscribing to events: {e}")
        raise e
    
async def on_pnl_event(pnl):
    if not ib.isConnected():
        await connect()
    #account_pnl=await account_metrics_store.update_pnl_dict(pnl)
    portfolio_items = []
    logger.debug(f"on_pnl_event: Received PnL event for with pnl as: {pnl}")
    account_pnl = AccountPnL(
        unrealizedPnL=0.0, realizedPnL=0.0, dailyPnL=0.0
    )
    trades = []
    if account_pnl.dailyPnL>0:
        updatePnl = await update_pnl(account_pnl, ib)
        logger.info(f"first Updated PnL: {updatePnl}")
    else:
        account_pnl=await account_metrics_store.update_pnl_dict(pnl)
    

    
    trades =  ib.openTrades()
   
    for trade in trades: 
        ib_orders=await order_db.insert_trade(trade)
        
        logger.debug(f"Open order for {trade.contract.symbol}: {trade}")
        
    logger.debug(f"PnL event received: {pnl}")

    if account_pnl:
        

        updatePnl = await update_pnl(account_pnl, ib)
        logger.debug(f"Updated PnL: {updatePnl}")
        
   

    

app = FastAPI(lifespan=lifespan, dependencies=[Depends(get_api_key)])
app.mount(
    "/static", StaticFiles(directory=os.path.join(CURRENT_DIR, "static")), name="static"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://nyc.porenta.us", "https://zt.porenta.us", "https://porenta.us"],
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



@app.get("/old_gpt_poly/{symbols}/" )
async def gpt_poly_prices(ticker: str):
    try:
        logger.info(f"Got ticker: {ticker} from gpt_poly")
        now = datetime.now(timezone.utc)
        # fetch 3 days of 1-min bars
        aggs_one_min = list(poly_client.list_aggs(
            ticker=ticker,
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
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=now - timedelta(days=366),
            to=now,
            adjusted=True,
            sort="asc",
            limit=5000,
        ))

        if not aggs_one_min or not aggs_daily:
            raise HTTPException(status_code=404,
                detail="Insufficient historical data")

        data = await gpt_data_poly(ticker, aggs_daily, aggs_one_min)
        return JSONResponse(content=jsonable_encoder(data))

    except HTTPException as he:
        logger.error(f"Data fetch error for {ticker}: {he.detail}")
        return JSONResponse(status_code=he.status_code, content={"error": he.detail})
    except Exception as e:
        logger.error(f"Unexpected error in /gpt_poly: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
async def gpt_ib_data(contract: Contract) -> dict:
    """
    Pull daily + 1-min history, enrich with TA-Lib indicators, and return JSON-ready dict.
    """
    try:
        logger.info(f"gpt_data for {contract.symbol} started, will return data asynchronously for contract : {contract}")
        
        # -------- download history -------------------------------------------------
        daily_bars = await ib.reqHistoricalDataAsync(
            contract=contract, 
            endDateTime="", 
            durationStr="1 Y",
            barSizeSetting="1 day", 
            whatToShow="TRADES", 
            useRTH=True, 
            formatDate=1,
        )
        minute_bars = await ib.reqHistoricalDataAsync(
            contract=contract, 
            endDateTime="", 
            durationStr="3 D",
            barSizeSetting="1 min", 
            whatToShow="TRADES", 
            useRTH=False, 
            formatDate=1,
        )
       

        # Fixed: Use util.df correctly from ib_insync
        df_d = util.df(daily_bars)
        df_m = util.df(minute_bars)
        
        if df_d.empty or df_m.empty:
            logger.warning(f"Empty dataframes for {contract.symbol}")
            return {"error": "No historical data available"}

        

        return df_d, df_m, daily_bars, minute_bars
          

    except Exception as e:
        logger.error(f"Error in gpt_data for {contract.symbol}: {e}")
        return {"error": str(e)}
    
@app.get("/gpt_poly/{ticker}/" )
async def gpt_prices(ticker: str):
    try:
        symbol = ticker
        logger.info(f"Got ticker: {symbol} from gpt")
    
        barSizeSetting = "1 min"  # default bar size setting

        contract: Contract = None
  
        logger.info(f"Received TV post data for ticker: {symbol} bar_test: barSizeSetting: {barSizeSetting}")
        c = Contract(symbol=symbol, exchange="SMART", secType="STK", currency="USD")
        logger.info(f"Qualifying contract for {symbol}…")
        qualified = await ib.qualifyContractsAsync(c)
        if not qualified:
            logger.error(f"Failed to qualify {symbol}")
            return JSONResponse(content={"error": f"Failed to qualify {symbol}"}, status_code=400)
        
        contract = qualified[0]
        already_subscribed = await subscribed_contracts.has(symbol)
        subscribe_needed=not already_subscribed
        if subscribe_needed:
            logger.info(f"Subscribing to ticker {symbol} with barSizeSetting: {barSizeSetting}")    
            ib.reqMktData(contract, genericTickList = "221,236", snapshot=False)
            await asyncio.sleep(0.1)  # Allow time for ticker data to be fetched
            ticker_data = ib.ticker(contract)
            await price_data_dict.add_ticker(ticker_data, barSizeSetting)
        logger.info(f"Ticker contract {contract} subscribe_needed? {subscribe_needed}")
        
        
        # --- inside gpt_prices() -----------------------------------------------
        snapshot = await price_data_dict.get_snapshot(symbol)
        daily_df, minute_df, daily_bars, minute_bars = await gpt_ib_data(contract)
        analysis  = await gpt_data_poly(symbol, daily_df, minute_df, ibData=True)


        result_raw = {
            "ticker_snapshot": snapshot.to_dict(),   # PriceSnapshot already cleans itself
            "analysis":        analysis
        }

        clean_payload = clean_nan(result_raw)       # <-- critical line
        logger.info(f"gpt_data for {symbol} started, will return data asynchronously for contract : {contract}" ) 
        logger.info(f"gpt_data for {symbol} returned ticker snapshot: {snapshot}")
        logger.info(f"gpt_data for {symbol} returned ticker clean_payload: {clean_payload}")
        return JSONResponse(content=jsonable_encoder(clean_payload), status_code=200)
        
      
    except Exception as e:
        logger.error(f"Error processing gpt request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ema/{ticker}/" )
async def get_ema(ticker: str, request: Request = None, barSizeSetting: str = Query(None, description="barSizeSetting")):
    try:
        symbol = ticker
        logger.info(f"Got ticker: {symbol} from ema with request {request} and barSizeSetting: {barSizeSetting} from query parameter")
        if barSizeSetting is None:
            barSizeSetting = "1 min"
    

        contract: Contract = None
  
        logger.info(f"Received TV post data for ticker: {symbol} bar_test: barSizeSetting: {barSizeSetting}")
        c = Contract(symbol=symbol, exchange="SMART", secType="STK", currency="USD")
        logger.info(f"Qualifying contract for {symbol}…")
        qualified = await ib.qualifyContractsAsync(c)
        if not qualified:
            logger.error(f"Failed to qualify {symbol}")
            return JSONResponse(content={"error": f"Failed to qualify {symbol}"}, status_code=400)
        
        contract = qualified[0]
        already_subscribed = await subscribed_contracts.has(symbol)
        subscribe_needed=not already_subscribed
        if subscribe_needed:
            logger.info(f"Subscribing to ticker {symbol} with barSizeSetting: {barSizeSetting}")    
            ib.reqMktData(contract, genericTickList = "221,236", snapshot=False)
            await asyncio.sleep(0.1)  # Allow time for ticker data to be fetched
            ticker_data = ib.ticker(contract)
            await price_data_dict.add_ticker(ticker_data, barSizeSetting)
        logger.info(f"Ticker contract {contract} subscribe_needed? {subscribe_needed}")
       
        
        
        # --- inside ema_prices() -----------------------------------------------
        emaCheck =await ema_check(contract, ib, barSizeSetting)  # Ensure EMA is checked for the contract
        ema, raw_ticker_json_cleaned = emaCheck
        logger.info(f"EMA for {symbol} is {ema}")
        response= {
            "symbol": symbol,
            "ema": ema,
            "barSizeSetting": barSizeSetting,
            "raw_ticker_json_cleaned": raw_ticker_json_cleaned
          
        }
        response=clean_nan(response)
        

        
        return JSONResponse(content=jsonable_encoder(response), status_code=200)
        
      
    except Exception as e:
        logger.error(f"Error processing ema request: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/rvol", response_class=HTMLResponse)
async def get_rvol(rvolFactor: float, getTicker=None):
    try:
        logger.info(f"Getting rvol for factor: {rvolFactor} and ticker: {getTicker}")
        await start_rvol(rvolFactor, getTicker)
        
       
    except Exception as e:
        logger.error(f"Error getting rvol: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/db_view", response_class=HTMLResponse)
async def home(request: Request):
    try:
        return templates.TemplateResponse("backtrader.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/get_contracts")
async def get_contracts(ticker: TickerSub):
    symbol = ticker.ticker
    barSizeSetting = ticker.barSizeSetting

    
    logger.info(f"getting ticker for BT Ticker Tradingview: {symbol}")
 

    qualified_contract = await add_new_contract(symbol, barSizeSetting, ib)

    # Store it in the subscribed_contracts dictionary
    
    logger.info(f"qualified_contract: {qualified_contract}")
    json_qualified_contract = {
        "symbol": qualified_contract.symbol,
        "exchange": qualified_contract.primaryExchange,
    }
    

    return JSONResponse(content=json_qualified_contract)


@app.post("/tvscan_ib")
async def tv_volatility(ticker: TickerSub, barSizeSetting: str = "1 min", ib=ib):
    try:
        symbol = ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker

        
        barSizeSetting = ticker.barSizeSetting
       
        logger.info(f"Received request for ticker: {symbol}")

        contract_data = {
            "symbol": symbol,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract_data)
        qualified = await ib.qualifyContractsAsync(ib_contract)
        if not qualified:
            logger.error(f"Contract qualification failed for {symbol}")
            return None
        ib_contract=qualified[0]
        # Store in subscribed_contracts for future use
      

        #
        response = await tv_volatility_scan_ib(
            ib_contract, barSizeSetting, ib
        )
        # async def tv_volatility_scan_ib(contract, barSizeSetting: str = "1 min", ib=None):

        # Perform volatility scan logic here
        # For example, you can calculate the volatility based on the historical data

        # Return the results as JSON
        return JSONResponse(content={"status": "success", "data": response})
    except Exception as e:
        logger.error(f"Error in tv_volatility: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )




@app.post("/unsubscribe_from_symbol")
async def unsubscribe_to_ticker(ticker: TickerRequest):
    try:
        
        symbol = ticker.ticker
        logger.info(f"Unsubscribing from ticker: {symbol}")

        
        qualified_contract = await subscribed_contracts.get(symbol)
        if qualified_contract:
            cancelMktData=ib.cancelMktData(qualified_contract)
            await asyncio.sleep(2)
            if cancelMktData:
                await subscribed_contracts.delete(symbol)
                logger.info(f"Unsubscribed from {symbol}")
            return JSONResponse(content={"status": f"Unsubscribed from {symbol}"})
        else:
            logger.warning(f"No active subscription found for {symbol}")
            return JSONResponse(content={"status": f"No active subscription found for {symbol}"})
    except Exception as e:
        logger.error(f"Error unsubscribing from ticker: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
  

@app.get("/graceful-disconnect")
async def get_graceful_disconnect():
    try:
        if ib.isConnected():
            await graceful_disconnect()
            await ib_manager.graceful_disconnect()
        
        return JSONResponse(content={"status": "success", "ib.isConnected()": ib.isConnected()})
    except Exception as e:
        logger.error(f"Error ib.isConnected(): {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})
@app.get("/active-symbols")
async def list_active_tickers():
    try:
        tickers_list: list[Ticker] = ib.tickers()
        result = []

        for ticker in tickers_list:
            tckDict = await ticker_to_dict(ticker)
            cleaned = clean_nan(tckDict)
            result.append(cleaned)

        logger.debug(f"Active tickers: {result}")
        return JSONResponse(content={"status": "success", "tickers": result})
    except Exception as e:
        logger.error(f"Error retrieving tickers: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})



@app.get("/symbols/")
async def get_all_tickers():
    cur = ticker_manager.conn.cursor()
    cur.execute("SELECT * FROM tickers")
    rows = cur.fetchall()
    tickers = [dict(row) for row in rows]
    return JSONResponse(content=tickers)


@app.get("/orders")
async def fetch_all_orders():
    try:
        trades= None
        account_value_json=await account_metrics_store.to_json(ib)
        logger.info(f"Account value data: {account_value_json}")
        account_value_json=jsonpickle.encode(account_value_json, unpicklable=False)
        logger.info(f"Account value jsonpickle: {account_value_json}")

       
        open_trades = await ib.reqAllOpenOrdersAsync()
        for trade in open_trades:
            symbol = trade.contract.symbol
            logger.info(f"Trade: {symbol}")
            ib_orders=await order_db.insert_trade(trade)

        await asyncio.sleep(0.5)  # Give some time for the orders to be fetched
        trades = await order_db.get_all_trades()
        db_portfolio_items=await order_db.fetch_portfolio_list()
        logger.info(f"JSON Fetched portfolio items from db: {jsonpickle.encode(db_portfolio_items, unpicklable=True)}")
        logger.info(f"Fetched trades from db: {trades}")
        logger.info(f"JSON Fetched trades from db: {jsonable_encoder(trades)}")
        portfolio_items = ib.portfolio()
        logger.info(f"Fetched portfolio items from IB: {portfolio_items}")

        db_position= await order_db.fetch_positions_list()
         
        get_current_portfolio_summary=await order_db.fetch_portfolio_list()
        logger.info(f"Fetched posistions from db: {get_current_portfolio_summary}")
        logger.info(f"JSON Fetched posistions from db: {jsonable_encoder(get_current_portfolio_summary)}")
        data = {
            "old_close_trade": jsonable_encoder(portfolio_items),
            "account_pnl": jsonpickle.encode(account_pnl, unpicklable=False),
            
            
            
            "trades": jsonable_encoder(trades),
            "db_position": jsonable_encoder(db_position),
            "get_current_portfolio_summary": jsonable_encoder(get_current_portfolio_summary)
        }
        logger.info(f"data db: {data}")
        


        

        return {"orders": data}
    except Exception as e:
        logger.error(f"Failed to fetch orders: {e}")
        raise HTTPException(status_code=500, detail="Error fetching orders")


@app.get("/api/pnl-data")
async def get_current_pnl():
    try:
        ib_data = None
        await account_metrics_store.update_summary(ib)
        
        logger.debug(f"boof message1 {boofMsg}")
        completed_order = []
        # Unpack the values from 
        old_close_trades  = []
        try:
            completed_order = ib.trades()
            
            ib_data= await account_metrics_store.ib_order_trade_data(order_db, ib)
            logger.debug(f"2boof message2 {boofMsg}")
            
            if ib_data is not None:
                orders, old_close_trades = ib_data
            
       
        except Exception as e:
            logger.error(f"Error fetching PnL data: {e}")
            raise HTTPException(status_code=500, detail="Error fetching PnL data")
        
        await get_positions()
        account_pnl = AccountPnL(
            unrealizedPnL=0.0, realizedPnL=0.0, dailyPnL=0.0
        )

        account_value_json = {}
        account = account_metrics_store.account
        if account is None:
            account = ""
        pnl=ib.pnl(account)
        if pnl is not None:
            for pnl_item in pnl:
                if pnl_item.account == account:

                            
                    account_pnl = AccountPnL(
                        unrealizedPnL=float(0.0 if isNan(pnl_item.unrealizedPnL) else pnl_item.unrealizedPnL),
                        realizedPnL=float(0.0 if isNan(pnl_item.realizedPnL) else pnl_item.realizedPnL),
                        dailyPnL=float(0.0 if isNan(pnl_item.dailyPnL) else pnl_item.dailyPnL),
                            
                    )
                    logger.info(f"Account PnL for {account}: {account_pnl}")
        account_value_json=jsonable_encoder(account_metrics_store.pnl_data)
        await on_pnl_event(account_pnl)
        logger.debug(f"Account PnL for {account}: {account_value_json}")
        portfolio_items = ib.portfolio()
        await order_db.insert_portfolio_items(portfolio_items)
       
        current_position = None
        current_position: Position = await ib.reqPositionsAsync()
        if current_position is not None and len(current_position) > 0:
            logger.warning("Boof found positions in IB.")
        portfolio_db=await order_db.fetch_portfolio_list()

    
        try:
            db_posistions = await order_db.fetch_positions_list()
            
            
        except Exception as e:
            logger.error(f"Error fetching PnL data: {e}")
        data = {
            "pnl": jsonable_encoder(account_value_json),
            "net_liquidation": account_metrics_store.net_liquidation,
            "buying_power": account_metrics_store.buying_power,
            "settled_cash": account_metrics_store.settled_cash,
            "portfolio_db": jsonable_encoder(portfolio_db),
            "portfolio_items": jsonable_encoder(portfolio_items),
            "positions": jsonable_encoder(db_posistions),
            "old_close_trade": jsonable_encoder(old_close_trades),
            "orders": jsonable_encoder(old_close_trades),
            "trades": jsonable_encoder(completed_order),
            "completed_orders": jsonable_encoder(orders),
            "account_summary": jsonable_encoder(account_metrics_store.account_summary),
            #"account_values": jsonable_encoder(account_metrics_store.account_values),
        }
        
        #logger.info("\n" + account_value_json.to_table())
        logger.info(f"account_value_json: {account_value_json},")

        logger.debug(f"Got them for jengo - get_current_pnl {jsonable_encoder(data)}")
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error fetching pnl data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/api/all-orders")
async def all_orders():
    try:
       
        ib_data = None
        
        logger.info(f"boof message1 {boofMsg}")
        completed_order = []
        # Unpack the values from 
        old_close_trades  = []
        try:
            completed_order = ib.trades()
            
            ib_data= await account_metrics_store.ib_order_trade_data(order_db, ib)
            logger.info(f"2boof message2 {boofMsg}")
            
            if ib_data is not None:
                orders, old_close_trades = ib_data
            
       
        except Exception as e:
            logger.error(f"Error fetching PnL data: {e}")
            raise HTTPException(status_code=500, detail="Error fetching PnL data")

        data = {
                    "old_close_trade": jsonable_encoder(old_close_trades),
                    "orders": jsonable_encoder(old_close_trades),
                    "trades": jsonable_encoder(completed_order),
                    "completed_orders": jsonable_encoder(orders),
                    
                }
        """""
        async def ib_order_trade_data(self, order_db, ib=None):
       
            openTrades = ib.openTrades()
            CompletedOrdersAsync = await ib.reqCompletedOrdersAsync(False)
            for CompletedOrder in CompletedOrdersAsync:
                await order_db.insert_trade(CompletedOrder)
       
    

            return openTrades, CompletedOrdersAsync
        """""
        return data
    except Exception as e:
        logger.error(f"Error fetching pnl data: {e}")
        raise HTTPException(status_code=500, detail=str(e))     

@app.get("/api/cancel-all-orders")
async def cancel_all_orders():
    openTrades = None
    ib.reqGlobalCancel()
    logger.info("reqGlobalCancel Canceling all orders...")

    

    openTrades =  ib.openTrades()
    if openTrades is None or len(openTrades) == 0:
        logger.info("No open trades to cancel.")
        return JSONResponse(content={"status": "success", "message": "No open trades to cancel."}, status_code=200)
    status = await on_cancel_order(openTrades)
    status_json = jsonable_encoder(status)

    return JSONResponse(content=status_json, status_code=200)
    
   


async def on_cancel_order(openTrades) -> bool:
    canceled_trades = None
    canceled= None
    
    for trade in openTrades:

        symbol= trade.contract.symbol
        if trade.orderStatus.status != "Cancelled":
            
            canceled=ib.cancelOrder(trade.order)
            canceled_trades=canceled
            
            logger.info(f"Canceling orders: {trade.orderStatus.status}")
            await asyncio.sleep(1)
            if canceled:
                return canceled_trades
    


@app.post("/api/cancel-order")
async def cancel_orders(request: Request):
    try:
        # Get the raw JSON data from the request
        raw_data = await request.json()
        logger.debug(f"Canceling order raw_data: {raw_data}")

        # Extract the order data
        order_data = raw_data["order"]

        # Create a minimal Order object with the required fields
        order = Order()
        order.clientId = order_data["clientId"]
        order.orderId = order_data["orderId"]
        order.permId = order_data["permId"]

        # Optional but might be needed for some validation in the cancelOrder method
        order.transmit = order_data.get("transmit", True)

        # Now call cancelOrder with this order object
        result = ib.cancelOrder(order)
        if result:
            logger.info(f"Order {order.orderId} cancelled successfully")
            

        return {"status": "success", "message": f"Order {order.orderId} cancelled"}
    except Exception as e:
        logger.error(f"Error cancelling order: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/sub")
async def sub(request: Request):
    try:
        raw_data = await request.json()
        barSizeSetting = raw_data["barSizeSetting"]
        
        
        
        if barSizeSetting is None:
            logger.warning(f"Missing bar size {barSizeSetting}")
            return JSONResponse(content="", status_code=401)

        logger.info(f"Starting ticker sub from fastapi")
        asyncio.create_task(start_sub(barSizeSetting))
        return JSONResponse(content="", status_code=200)
      

      
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def start_sub(barSizeSetting: str):
    directory = os.path.dirname(os.path.abspath(__file__))
    try:

        logger.info("Starting pre-market volume analysis...")
        latest_csv = get_latest_csv(directory)
        await process_csv(latest_csv, barSizeSetting)
    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")


async def process_csv(file_path, barSizeSetting: str):
    logger.info(f"Processing file: {file_path}")
    df = pd.read_csv(file_path)
    sub_data = {}

    # Ensure required columns exist
    required_columns = ["Symbol", "Exchange"]
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            raise KeyError(f"Missing required column: {col}")

    result = df[required_columns].copy()
    qualified_contract = None
    contract = None

    for symbol in result["Symbol"]:
        try:

            # Create contract once - used in all request types
            if symbol:
                contract = Contract(
                    symbol=symbol,
                    exchange="SMART",
                    secType="STK",
                    currency="USD",
                )
                qualified_contract = await subscribed_contracts.get(contract.symbol)
            logger.debug(f"Fetching historical get_contract data for {contract.symbol}...")
            
            sub_data = await add_new_contract(symbol, barSizeSetting, ib)
            await asyncio.sleep(0.2)

        except Exception as e:
            logger.error(f"Failed to get data for {contract.symbol}: {e}")

        # Increased delay for better rate limiting
        

    return sub_data

@app.post("/close_positions")
async def webapp_close_positions_post(ticker: OrderRequest, background_tasks: BackgroundTasks):
    try:
        if not ticker:
            return Response(status_code=403, content="No ticker object provided")
        limit_price = 0.0
        contract: Contract = None
        totalQuantity = 0.0
        action = None
        order: Order = None
        placeOrder = False
        trade: Trade = None
        ib_ticker = None
        snapshot = None
        parent_id = None
        reqId = None
        
        # Given a string like "NASDAQ:NVDA":
        symbol = ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker
        if symbol is None or symbol == "":
            symbol = ticker.ticker
        logger.info(f"Received close_positions webhook")

        if not ticker:
            return JSONResponse(content={"status": "error", "message": "No ticker object provided"}, status_code=403)
        
        await tv_store_data.set(symbol, ticker.model_dump())
        barSizeSetting = await get_bar_size_setting(symbol)
        if is_market_hours():

            # If you also need the exchange:
            c = Contract(symbol=symbol, exchange="SMART", secType="STK", currency="USD")
            logger.debug(f"Qualifying contract for {symbol}…")
            qualified = await ib.qualifyContractsAsync(c)
            if not qualified:
                logger.error(f"Failed to qualify {symbol}")
                return None
            contract = qualified[0]

        
        else:
            contract = await add_new_contract(symbol, barSizeSetting, ib)
            await asyncio.sleep(2)
        logger.debug(
                f"Processing close_all request for {symbol}"
            )
          # Allow time for contract to be fetched

            
            
        
        if contract is None:
            return JSONResponse(content={"status": "error", "message": "No contract"}, status_code=403)
        parent_id = parent_ids[symbol]
        openOrders =  ib.openOrders()
        for openOrder in openOrders:
            logger.info(f"Parent ID for {contract.symbol} is openOrder.orderId: {openOrder.orderId} reqId for parent_ids :{parent_ids[symbol]}")
            if openOrder.orderId:
                parent_ids[symbol] =openOrder.orderId
                logger.info(f"Parent ID for {contract.symbol} is openOrder.orderId: {openOrder.orderId} reqId for parent_ids :{parent_ids[symbol]}")

            if openOrder.orderId == parent_id:
                open_order: Order = openOrder
                logger.info(f"Open order found for {symbol} with action {openOrder.action} and quantity {openOrder.totalQuantity} and open_order {open_order}")
                result = ib.cancelOrder(open_order)
                if result:
                    logger.info(f"Order {open_order.orderId} cancelled successfully for open_order {open_order}")
        positions = await ib.reqPositionsAsync()
        for pos in positions:
            if (pos.position > 0 or pos.position < 0) and (pos.contract.symbol == symbol):
                placeOrder = True
                totalQuantity = abs(pos.position)
                await cancel_open_orders(pos)
            
            
                action = "SELL" if pos.position > 0 else "BUY"
                logger.info(
                    f"Closing position for {contract.symbol} with action {action} and quantity {totalQuantity}"
                )
                    
                    
                    

        reqId = ib.client.getReqId()
        parent_ids[contract.symbol] = reqId
        logger.info(f"Parent ID for {contract.symbol} is {parent_ids[contract.symbol]} reqId for symbol {parent_ids[symbol]}")
        if is_market_hours():
            logger.info(f"Market is open, using market order for {contract.symbol} with action {action} and quantity {totalQuantity}")
            order = MarketOrder(action, totalQuantity, orderId=reqId)
        else:
            logger.info(f"Market is closed, using limit order for {contract.symbol} with action {action} and quantity {totalQuantity}")
            snapshot = await price_data_dict.get_snapshot(symbol)

            if snapshot is None:
                ib_ticker = ib.ticker(contract)
                if ib_ticker:
                    snapshot = await price_data_dict.add_ticker(ib_ticker)
            if snapshot.ask > 2.0 and snapshot.bid > 2.0:
                limit_price= snapshot.ask if action == "SELL" else snapshot.bid
            if limit_price is None or limit_price == 0.0:
                limit_price= ib_ticker.markPrice()

            order = LimitOrder(
                action,
                totalQuantity=totalQuantity,
                lmtPrice=round(limit_price, 2),
                tif="GTC",
                outsideRth=True,
                transmit=True,
            
                orderRef=f"Web close_postions - {contract.symbol}",
            )
            if action == "SELL":
                logger.info(f"Limit price for {contract.symbol} is {limit_price} with ticker.ask {snapshot.ask}") 
            else:
                logger.info(f"Limit price for {contract.symbol} is {limit_price} with ticker.bid {snapshot.bid}")
        if placeOrder:
            trade = ib.placeOrder(contract, order)
    

        if trade:
            await asyncio.sleep(1)
            logger.info(f"Order ID for {contract.symbol}")
            order_json = {
                "ticker": contract.symbol,
                "orderAction": action,
                "limit_price": 0,
                "quantity": totalQuantity,
                "rewardRiskRatio": ticker.rewardRiskRatio,
                "riskPercentage": ticker.riskPercentage,
                "accountBalance": ticker.accountBalance,
                "stopType": ticker.stopType,
                "atrFactor": ticker.atrFactor,
            }
            logger.info(format_order_details_table(order_json))
            web_request_json =jsonable_encoder(order_json)
            await order_db.insert_order(trade)
            

            logger.info(
                f"Order Entry for {contract.symbol} successfully placed with order"
            )
            await zapier_relay(ticker, snapshot)
            return JSONResponse(content=web_request_json)  

            
        
            
           
                
        
    except Exception as e:
        logger.error(f"Error processing close_positions: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)

async def process_close_positions_post(ticker: OrderRequest):
    """
    Webhook endpoint that receives order event notifications from Fast API Webapp 

    This endpoint immediately returns a 200 OK response to Fast API Webapp  and then processes
    the webhook payload asynchronously to fetch order details, log events, and forward
    the data to ib_async functions.

    Args:
        webhook (OrderRequest): The validated webhook payload received from Fast API Webapp .

    Returns:
        JSONResponse: An empty response with status code 200.
    """
    # Immediately return a 200 response and process the webhook asynchronously.
    try:
        symbol = ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker

        
        logger.info(f"Received webhook request for : {contract.symbol}")
        order =[]
        totalQuantity = 0.0
        trade = {}
        action = None
        barSize  = "1 min"
        is_valid_timeframe = "secs" in barSize or "min" in barSize
        if is_valid_timeframe:
            barSizeSetting = barSize
        else:
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(barSize)
        
        timeframe_dict[symbol] = barSizeSetting
        bar_size_dict= timeframe_dict[symbol]

        
        already_subscribed = await subscribed_contracts.has(symbol)
        logger.info(f"Already subscribed to {contract.symbol}: {already_subscribed}")
        contract = await add_new_contract(symbol, barSizeSetting, ib)
        logger.info(f"Contract for {contract.symbol}: {contract}")
        
        

        
        
        limit_price = None
        
        qty = ticker.quantity
        positions = await ib.reqPositionsAsync()
        for pos in positions:
            if (pos.position > 0 or pos.position < 0) and (pos.contract.symbol == ticker.ticker):
                totalQuantity = abs(pos.position)
                   
                  
                action = "SELL" if pos.position > 0 else "BUY"
                logger.info(
                    f"Closing position for {contract.symbol} with action {action} and quantity {totalQuantity}"
                )
        
        if is_market_hours():
            
            logger.info(f"Market is open, using market order for {contract.symbol} with action {action} and quantity {totalQuantity}")
            order = MarketOrder(action, totalQuantity)
        

        if not is_market_hours():
            barSizeSetting = ticker.barSizeSetting_tv
            limit_price=await get_limit_price(contract, action)
            logger.info(f"Processing webhook request for {contract.symbol} with limit_price {limit_price}")
            
            logger.info(
                f"New Order close_all request for {contract.symbol} with jengo and uptrend"
            )
            positions = await ib.reqPositionsAsync()
            for pos in positions:
                if (pos.position > 0 or pos.position < 0) and (pos.contract.symbol == ticker.ticker):
                    totalQuantity = abs(pos.position)
                   
                  
                    action = "SELL" if pos.position > 0 else "BUY"
                    logger.info(
                        f"Closing position for {contract.symbol} with action {action} and quantity {totalQuantity}"
                    )

            order = LimitOrder(
                action,
                totalQuantity=totalQuantity,
                lmtPrice=limit_price,
                tif="GTC",
                outsideRth=True,
                transmit=True,
               
                orderRef=f"Web close_postions - {contract.symbol}",
            )
                  

                 

        
        trade =ib.placeOrder(contract, order)
        
        if trade:
            logger.info(f"Placing order for {trade.contract.symbol} with action {action} and quantity {totalQuantity}")
            #trade.fillEvent += on_trade_filled_event

            return JSONResponse(content="", status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook request: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )

async def get_limit_price(contract: Contract, action: str) -> float:
    """Fetch the limit price for a given contract and action."""
    try:
        ticker = ib.ticker(contract)
        if ticker:
            if action == "BUY":
                return ticker[0].ask  # Use ask price for buy orders
            elif action == "SELL":
                return ticker[0].bid  # Use bid price for sell orders
        return 0.0  # Default to 0 if no ticker data is available
    except Exception as e:
        logger.error(f"Error fetching limit price for {contract.symbol}: {e}")
        return 0.0


@app.post("/shorts")
async def fetch_single_stock_data(ticker: TickerRequest):
    """Fetch short availability for a single stock"""
    try:
        symbol = ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker

        
        contract = Contract(symbol=symbol, secType='STK', exchange='SMART', currency='USD')
        qualified_contracts = await ib.qualifyContractsAsync(contract)
        
        if not qualified_contracts:
            logger.warning(f"Could not qualify contract for {contract.symbol}")
            return None
        
        # Create a proper ScannerSubscription object
        subscription = ScannerSubscription(
            instrument="STK",
            locationCode="STK.US.MAJOR",
            scanCode="TOP_PERC_LOSE",
            abovePrice=0,
            marketCapAbove=0
        )
        
        # Set up scanner subscription options
        scannerSubscriptionOptions = [
            TagValue("includeLocals", "no"),
            TagValue("stockTypeFilter", "ALL")
        ]
        
        # Request scanner data using the proper method signature
        short_rates = await ib.reqScannerDataAsync(
            subscription,
            scannerSubscriptionOptions
        )
        
        # Process the scan results
        for item in short_rates:
            if item.contractDetails.contract.symbol == ticker.ticker:
                available = item.get('shortableShares', 0)
                fee_rate = item.get('shortableRate', 0.0)
                
                return {
                    'symbol': symbol,
                    'available_shares': available,
                    'fee_rate': fee_rate,
                    'updated_at': datetime.now(timezone.utc)
                }
        
        # Rest of the method remains the same...
    except Exception as e:
        logger.error(f"Error fetching short data for {contract.symbol}: {e}")
        return None


async def get_vstop(ticker: OrderRequest, contract: Contract):
    try:
        symbol = contract.symbol
        already_subscribed = await subscribed_contracts.has(symbol)
        barSizeSetting = await get_bar_size_setting(symbol)
        logger.debug(f"Already subscribed to {contract.symbol}: {already_subscribed} with barSizeSetting: {barSizeSetting}")
        
        subscribe_needed=not already_subscribed

        # subscribe to live market data only once
        
        logger.debug(f"Fetching historical bars for subscribed contracts")
        df = pd.DataFrame()
        bars = None
        last_bar = None
        snapshot=await price_data_dict.get_snapshot(symbol)
        logger.info(f"2 Retrieved vStop and uptrend for {symbol}")
        hasBidAsk = snapshot.hasBidAsk if snapshot else None
        
        
       
        
        if subscribe_needed or contract is None or not hasBidAsk:
            logger.debug(f"2.5 Retrieved vStop and uptrend for {symbol}")
            logger.debug(f"Subscribing market data for {contract.symbol} with barSizeSetting: {barSizeSetting}")

            contract= await add_new_contract(symbol, barSizeSetting, ib)
            
            
            logger.debug(f"Market data subscription active for {contract.symbol}")
        vol= await daily_volatility.get(symbol)
        if vol is None:
            logger.warning(f"No volatility data found for {ticker.ticker}")
          
            await yesterday_close_bar(ib, contract)
            vol= await daily_volatility.get(symbol)
        
        
        logger.debug(f"Processing TV post data for {contract.symbol}")
       
        
        vstopAtrFactor = ticker.vstopAtrFactor if ticker.vstopAtrFactor else 1.5
        atrlen= 20
        # determine bar size string
        logger.debug(f"1 Retrieved vStop and uptrend for {symbol}")
        
        
        
        
         
        
        
        logger.debug(f"3 Retrieved vStop and uptrend for {symbol}")
        atrFactorDyn = await get_dynamic_atr(symbol, BASE_ATR=None, vol=vol)
        if atrFactorDyn is not None:
                vstopAtrFactor = atrFactorDyn
        if vstopAtrFactor > 2.0:
            atrlen=3
        #dict_bars = util.df(df_dict_bars.bars) if df_dict_bars is not None else pd.DataFrame()
        #last_dict_bar= dict_bars.iloc[-1] if not dict_bars.empty else None
        df = await agg_bars.get_df(symbol, barSizeSetting)
        logger.info(f"2 Retrieved vStop and uptrend for {symbol}  agg_bars length is {len(df)}")
        if len(df) < 25:
            logger.info(f"Fetching reqHistoricalDataAsync data for {contract.symbol} with barSizeSetting: {barSizeSetting} because agg_bars length is {len(df)}")

            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr="1500 S",
                barSizeSetting=barSizeSetting,
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1
            )
            if not bars:
                logger.warning(f"No IB history for {contract.symbol}")
                return None
            
            df = util.df(bars)
            if df.empty:
                logger.warning(f"No  data found for {contract.symbol} with barSizeSetting: {barSizeSetting}" )
            asyncio.create_task(handle_bar(contract, bars))
        logger.debug(f"4 Retrieved vStop and uptrend for {symbol}")
        
        

        vStop, uptrend, atr = await vol_stop(df, atrlen, vstopAtrFactor)
        
        
        logger.debug(f"5 Retrieved vStop and uptrend for {symbol}")
        df["vStop"], df["uptrend"],  df["uptrend"] = vStop, uptrend, atr
        if vStop is not None:
            logger.debug(f"vStop float for {contract.symbol}: {vStop}, uptrend: {uptrend}")
            await volatility_stop_data.set(symbol, vStop, uptrend, atr, vstopAtrFactor)
            vol_stop_data = await volatility_stop_data.get(symbol)
            vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float=vol_stop_data
        
            
            await price_data_dict.add_vol_stop(symbol, vStop_float, uptrend_bool, vstopAtrFactor, atr_float)
            vStop_float_dict, uptrend_dict=await price_data_dict.get_vol_stop(symbol)
            
            logger.info(f"Retrieved vStop and uptrend for {ticker}: vStop: {vStop_float_dict}, uptrend: {uptrend_dict} with vstopAtrFactor: {vstopAtrFactor_float} and atr_float: {atr_float} and atr length {atrlen} for {symbol}")
            

        
        
        response = {
            "symbol": contract.symbol
            
            
            
        }
        
        logger.debug(f"vStop data for {contract.symbol}: {snapshot.vStop}, uptrend: {snapshot.uptrend}")
        return JSONResponse(content=response, status_code=200)
        #logger.info(f"Fetched {len(df)} historical bars for {contract.symbol} at 15 secs interval last_bar.time: {last_bar.date}")
    except Exception as e:
        logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
        raise

 
@app.post("/check_order")
async def check_web_order(ticker: OrderRequest, background_tasks: BackgroundTasks):
    
    """
    Webhook endpoint that receives order event notifications.

    Processes different order types (close_all, boof, or regular orders)
    and handles them asynchronously while returning an immediate response.

    Args:
        ticker (OrderRequest): The validated webhook payload.

    Returns:
        JSONResponse: Status 200 for successful receipt, 500 for errors.
    """
    
    try:
        symbol = ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker

        
        await tv_store_data.set(symbol, ticker.model_dump())
        
       
        barSizeSetting = ""
        action = ticker.orderAction
        entryPrice = ticker.entryPrice
        contract = None
        quantity = ticker.quantity
        qualified_contract = None
        is_valid_timeframe = "secs" in ticker.barSizeSetting_tv or "min" in ticker.barSizeSetting_tv  or "mins" in ticker.barSizeSetting_tv
              
      
        # Create contract once - used in all request types
        if is_valid_timeframe:
            barSizeSetting = ticker.barSizeSetting_tv
            
        else:
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(ticker.barSizeSetting_tv)
            
        if ticker.ticker:
            
            qualified_contract = await subscribed_contracts.get(symbol)
           

        # Handle regular order requests
        logger.info(f"Processing regular order request for {contract.symbol}")
        asyncio.create_task(process_web_order_check(ticker, qualified_contract, barSizeSetting))
        return JSONResponse(content="", status_code=200)

    except Exception as e:
        logger.error(f"Error processing order request: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )
    
@app.post("/webapp-post-data")
async def webapp_post_data(ticker: OrderRequest, background_tasks: BackgroundTasks):
    try:
        symbol = ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker
        if symbol is None:
            symbol = ticker.ticker
        await tv_store_data.set(symbol, ticker.model_dump())
        
        logger.info(f"Received webapp post data for ticker: {symbol} unixtime {ticker.unixtime} ticker.ticker {ticker.ticker} ticker.barSizeSetting_tv {ticker.barSizeSetting_tv}")
        contract: Contract = None
        barSizeSetting = await get_bar_size_setting(symbol)  
        snapshot = None
        
        uptrend = None
        hasBidAsk = None
        response = {
                "status": "boof1 failed"}
        new_response_json=jsonable_encoder(response)
        
        contract, orderPlace,data_set = await asyncio.gather(
            add_new_contract(symbol, barSizeSetting, ib),
            
            order_placed.get(symbol),
            tv_store_data.set(symbol, ticker.model_dump())
        )
        sub= await get_vstop(ticker, contract)
        if not await price_data_dict.add_first_rt_bar(symbol):
            logger.info(f"Adding bar for {contract.symbol} to add_first_rt_bar")
            bars: RealTimeBarList = ib.reqRealTimeBars(contract, 5, "TRADES", False)
            bars.updateEvent += on_bar_update
                

            first_rt_bar=await price_data_dict.add_first_rt_bar(symbol)
            logger.info(f"First real-time bar added for {contract.symbol}: {first_rt_bar}")
        #ib_reqTicker = ib.ticker(contract)
        snapshot = await price_data_dict.get_snapshot(symbol)
        hasBidAsk = snapshot.hasBidAsk if snapshot else None
        logger.debug(f"Snapshot for {symbol}  snapshot: {snapshot} and 'bid'={snapshot.bid} and 'ask'={snapshot.ask} and 'close'={snapshot.close} and 'halted'={snapshot.halted} and 'hasBidAsk'={snapshot.hasBidAsk} and 'shortableShares'={snapshot.shortableShares}")
       
        if orderPlace:
            logger.info(f"Order already placed for {symbol}, skipping processing.")
            return JSONResponse(content="OK", status_code=200)
        logger.debug(f"Received TV post data for ticker: {symbol} data_set {data_set} ")
        
        logger.debug(f"Snapshot for {symbol} after vstop: {snapshot.hasBidAsk}")

        #vStop_value, uptrend_value = await price_data_dict.get_vol_stop(symbol)
        vol_stop_data = await volatility_stop_data.get(symbol)
        vStop_value,uptrend_value, atr_float, vstopAtrFactor_float=vol_stop_data
        logger.debug(f"vStop_value: {vStop_value} and uptrend_value: {uptrend_value} for {symbol}")
        vStop_float = 0.0 if vStop_value is None or isNan(vStop_value) else vStop_value
        uptrend = "False" if uptrend_value is None else uptrend_value
        if snapshot.hasBidAsk:
            hasBidAsk = "True"
        elif not snapshot.hasBidAsk:
            hasBidAsk = "False"
        if uptrend_value:
            uptrend = "True"
        elif not uptrend_value:
            uptrend = "False"
        
       
     
        response = {
            "status": "success",
            "message": f"Subscribed to ticker boof2 {symbol} for vStop calculation",
            "ticker_info": {
                "symbol": symbol,
                "timeframe": ticker.barSizeSetting_tv or (snapshot.barSizeSetting if snapshot else ""),
                "accountBalance": 0.0 if ticker.accountBalance is None or isNan(ticker.accountBalance) else ticker.accountBalance,
                "rewardRiskRatio": 0 if ticker.rewardRiskRatio is None or isNan(ticker.rewardRiskRatio) else ticker.rewardRiskRatio,
                "response_number": 1
    
            },
            "snapshot": {}
        }
        

            # Only add snapshot data if we have a valid snapshot
        if snapshot.hasBidAsk:
            response= {
    
                
                "bid": 0.0 if isNan(snapshot.bid) else snapshot.bid,
                "ask": 0.0 if isNan(snapshot.ask) else snapshot.ask,
                "vStop": 0.0 if isNan(vStop_float) else round(vStop_float, 2),
                "close": 0.0 if isNan(snapshot.close) else snapshot.close,
                "uptrend": "" if uptrend is None else uptrend,
                "halted": 0.0 if isNan(snapshot.halted) else snapshot.halted,
                "hasBidAsk": "" if hasBidAsk is None else hasBidAsk,
                "shortableShares": 0.0 if isNan(snapshot.shortableShares) else snapshot.shortableShares,
                "markPrice": 0.0 if isNan(snapshot.markPrice) else round(snapshot.markPrice, 2),
                "response_number": 2
            }
            
            
        else:
            # Create an empty snapshot structure filled with default values
            response["snapshot"] = {
                "last": 0.0, 
                "bid": 0.0, "ask": 0.0, "close": 0.0,
                "last_bar_close": 0.0, "halted": 0.0, "hasBidAsk": False,
                "shortableShares": 0.0, "markPrice": 0.0,
                "response_number": 3
            }
        
     
        new_response_json=jsonable_encoder(response)
       
       
        
        if hasBidAsk:
            
           
            logger.debug(f"hasBidAsk is not None: Snapshot for {symbol} hasBidAsk: {hasBidAsk} vStop_float: {vStop_float} uptrend: {uptrend}")
            return JSONResponse(content=new_response_json, status_code=200)

        else:
            
            return JSONResponse(content=new_response_json, status_code=200)


    except Exception as e:
        logger.error(f"Error processing TV post data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/place_order")
async def place_web_order(ticker: OrderRequest, background_tasks: BackgroundTasks):
    
    """
    Webhook endpoint that receives order event notifications.

    Processes different order types (close_all, boof, or regular orders)
    and handles them asynchronously while returning an immediate response.

    Args:
        ticker (OrderRequest): The validated webhook payload.

    Returns:
        JSONResponse: Status 200 for successful receipt, 500 for errors.
    """
    
    try:
        symbol = ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker

        await tv_store_data.set(symbol, ticker.model_dump())
        
       
        barSizeSetting = ""
        action = ticker.orderAction

        
        entryPrice = ticker.entryPrice
        quantity = ticker.quantity
        qualified_contract = None
        is_valid_timeframe = "secs" in ticker.barSizeSetting_tv or "min" in ticker.barSizeSetting_tv  or "mins" in ticker.barSizeSetting_tv
          
      
        # Create contract once - used in all request types
        if is_valid_timeframe:
            barSizeSetting = ticker.barSizeSetting_tv
            
        else:
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(ticker.barSizeSetting_tv)

        already_subscribed = await subscribed_contracts.has(symbol)
        if not already_subscribed:
            logger.info(f"Not subscribed to {symbol}, adding to subscribed_contracts")
            await subscribed_contracts.get_w_ib(symbol, barSizeSetting, ib)
            await asyncio.sleep(1)
            
            
        if ticker.ticker:
            
            qualified_contract = await add_new_contract(symbol, barSizeSetting, ib)
            await price_data_dict.get_snapshot(symbol)
            if not await price_data_dict.has_bar(symbol):
                logger.info(f"Adding bar for {qualified_contract.symbol} to price_data_dict")
                bars: RealTimeBarList = ib.reqRealTimeBars(qualified_contract, 5, "TRADES", False)
                bars.updateEvent += on_bar_update

            
        
        
        # Handle close_all requests
        if action == "close_all" or quantity < 0:
            logger.info(
                f"Processing close_all request for {qualified_contract.symbol} with quantity {quantity}"
            )
            asyncio.create_task(process_close_all(qualified_contract, ticker, barSizeSetting))
            return JSONResponse(content="", status_code=200)

   
        # Handle regular order requests
        logger.info(f"Processing regular order request for {qualified_contract.symbol}")
        asyncio.create_task(process_web_order(action, ticker, qualified_contract, barSizeSetting))
        return JSONResponse(content="", status_code=200)

    except Exception as e:
        logger.error(f"Error processing order request: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )


async def process_web_order(action: str, ticker: OrderRequest, contract: Contract, barSizeSetting=None, order=None ):
    try:
        set_market_order=ticker.set_market_order
        takeProfitBool = ticker.takeProfitBool
        web_request_json = None
        order= None
        entryPrice=None
        stopLoss = None
        symbol= None
        trade = None
        snapshot = None
        uptrend = None
        shortableShares = -1.0
        snapshot = await price_data_dict.get_snapshot(contract.symbol)
        
        bracket = {}
        hasBidAsk = snapshot.hasBidAsk if snapshot else None
        is_valid_timeframe = "secs" in ticker.barSizeSetting_tv or "min" in ticker.barSizeSetting_tv  or "mins" in ticker.barSizeSetting_tv
        await get_vstop(ticker, contract)
        if hasBidAsk:
            limit_price = snapshot.last          # IB Tick-by-tick “Last” price

            market_price = (
                snapshot.markPrice
                if snapshot.markPrice             # non-zero / non-NaN?
                else (snapshot.bid + snapshot.ask) * 0.5
            )

            last_bar_close = (
                snapshot.last_bar_close           # set by add_bar() / add_rt_bar()
                or snapshot.close                 # same value – both safe
            )

            yesterday_price = snapshot.yesterday_close_price
        
        shortableShares = snapshot.shortableShares if snapshot.shortableShares is not None else 0.0
        
        
        logger.info(f"Snapshot for {contract.symbol} after vstop: stopLoss: {stopLoss}  - {snapshot.hasBidAsk} and shortableShares: {shortableShares} with bid {snapshot.bid} and ask {snapshot.ask} and close {snapshot.close} and markPrice {snapshot.markPrice}")
        ema =await ema_check(contract, ib, barSizeSetting)  # Ensure EMA is checked for the contract
        emaCheck, _ = ema
        if emaCheck is not None:
            action = "BUY" if emaCheck =="LONG" else "SELL"
            logger.info(f"emaCheck order Action for {contract.symbol} is {action} and entryPrice {ticker.entryPrice} and emaCheck {emaCheck}"
            )
        if not hasBidAsk or shortableShares is None or (action=="SELL" and shortableShares == -1.0):
            logger.info(f"Fetching reqTickersAsync for {contract.symbol}...")
            ib_ticker = ib.ticker(contract)
            snapshot = await price_data_dict.add_ticker(ib_ticker)
        entryPrice = snapshot.ask if action == "BUY" else snapshot.bid
        if entryPrice is None or entryPrice < 2.0:
            logger.warning(f"Entry price for {contract.symbol} is None or too low, using default value of 1.0")
            entryPrice = snapshot.markPrice
        

        if barSizeSetting is None:
            if is_valid_timeframe:
                barSizeSetting = ticker.barSizeSetting_tv
            
            else:
                barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(ticker.barSizeSetting_tv)
            
                barSizeSetting = ticker.barSizeSetting_tv
        logger.info(f"Processing webhook request for {ticker} ")
        # ticker= TickerSub(ticker=ticker.ticker)
        symbol = contract.symbol
        
        get_symbol_dict= await get_tv_ticker_dict(symbol)
        if stopLoss is None or stopLoss == 0.0 :
            vstop_values= await price_data_dict.get_snapshot(symbol)
            if vstop_values is not None:
                stopLoss = vstop_values.vStop
                uptrend = vstop_values.uptrend
               
                logger.info(f"Using vstop values for {contract.symbol}: stopLoss={stopLoss}, entryPrice={entryPrice}, uptrend={uptrend}")
            else:
                logger.warning(f"No vstop values found for {contract.symbol}, using defaults.")
                stopLoss = get_symbol_dict.stopLoss
                entryPrice = get_symbol_dict.entryPrice
                uptrend =get_symbol_dict.uptrend
        atrFactor = get_symbol_dict.atrFactor
        vstopAtrFactor =get_symbol_dict.vstopAtrFactor
        if vstopAtrFactor is None or vstopAtrFactor == 0.0:
            vstopAtrFactor = atrFactor
        
        stop_diff = stopLoss - entryPrice if stopLoss and entryPrice else None
        logger.info(f"stopLoss: {stopLoss} and order entryPrice: {entryPrice} and stop_diff: {stop_diff} and uptrend: {uptrend} for {contract.symbol}")
        if not is_market_hours():
            set_market_order= False
            

        
        
        if (ticker.entryPrice is not None and ticker.entryPrice != 0) and  (
             ticker.stopLoss is not None and  ticker.stopLoss != 0
        ):
            logger.info(f"Using entryPrice from webhook: {entryPrice}")
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(
                ticker.barSizeSetting_tv
            )
            entryPrice = ticker.entryPrice
            stopLoss = ticker.stopLoss
            

        logger.info(f"New Order request for {contract.symbol} with jengo and uptrend")
        

        # uptrend_orderAction = "BUY" if uptrend  else "SELL"
        logger.info(
            f"Uptrend order Action for {contract.symbol} is {action} and entryPrice {ticker.entryPrice}"
        )
        order = OrderRequest(
            entryPrice=entryPrice,  # Use the latest market price as entry price
            rewardRiskRatio=ticker.rewardRiskRatio,
            quantity=ticker.quantity,  # Use the quantity from the webhook
            riskPercentage=ticker.riskPercentage,
            vstopAtrFactor=vstopAtrFactor,
            timeframe=barSizeSetting,
            kcAtrFactor=vstopAtrFactor,
            atrFactor=atrFactor,
            accountBalance=ticker.accountBalance,
            ticker=symbol,
            orderAction=action,
            stopType=ticker.stopType,
            set_market_order=set_market_order,
            takeProfitBool=ticker.takeProfitBool,
            stopLoss=stopLoss,
            uptrend=uptrend,
        )
        await tv_store_data.set(symbol, order.model_dump()) 
        logger.warning(f"order  is {order}")
        logger.warning(
            f"barSizeSetting  is {barSizeSetting} for  {contract.symbol} and vStop type {ticker.stopType}"
        )
        

        # (contract: Contract, ib: IB=None, barSizeSetting=None)
        quantity = ticker.quantity
        

        logger.info(
            f"New Order request for  subscribed_contracts {contract} with stopLoss {stopLoss} and ticker.stopLoss {ticker.stopLoss} and stopLoss+5 {stopLoss}"
        )
        vol_stop_data = await volatility_stop_data.get(contract.symbol)
        vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float=vol_stop_data
        
        
        shortable= False
        if action == "SELL" and  emaCheck =="SHORT":
            logger.info(f"Checking if {contract.symbol} is shortable...")
            is_shortable=shortable_shares(contract.symbol, action, quantity, shortableShares)
            logger.info(f"{contract.symbol} is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")

            if shortable_shares:
                logger.info(f"{contract.symbol} is shortable, is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")
                shortable = True

            if not shortable:
                order = OrderRequest(
                    entryPrice=entryPrice,  # Use the latest market price as entry price
                    rewardRiskRatio=ticker.rewardRiskRatio,
                    quantity=ticker.quantity,  # Use the quantity from the webhook
                    riskPercentage=ticker.riskPercentage,
                    vstopAtrFactor=vstopAtrFactor,
                    timeframe=barSizeSetting,
                    kcAtrFactor=vstopAtrFactor,
                    atrFactor=atrFactor,
                    accountBalance=ticker.accountBalance,
                    ticker=symbol,
                    orderAction=action,
                    stopType=ticker.stopType,
                    set_market_order=set_market_order,
                    takeProfitBool=ticker.takeProfitBool,
                    stopLoss=stopLoss,
                    uptrend=uptrend,
                    notes="not shortable shares"
                )
                await tv_store_data.set(symbol, order.model_dump())
                
                logger.warning(f"{contract.symbol} is not shortable, cannot place SELL order, is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")
                
                return JSONResponse(content={"status": "Not shortable", "message": f"{contract.symbol} is not shortable, cannot place SELL order."}, status_code=403)
        if stopLoss > 0: 
            orderPlace=await order_placed.set(symbol)
            
          
            if set_market_order and not takeProfitBool and is_market_hours():
                stopLoss= vStop_float
                

                logger.info(f"takeProfitBool {takeProfitBool} Placing stop loss order for {contract.symbol} with stopLoss: {ticker.stopLoss} and entryPrice: {ticker.entryPrice}")
                trade= await market_stop_loss_order(contract,  order, action, quantity,round(stopLoss, 2), snapshot, ib, set_market_order)
                
            logger.info(f"Order being placed for {contract.symbol} orderPlace {orderPlace} ")
            if not set_market_order and not takeProfitBool:
                stopLoss= vStop_float
               

                logger.info(f"takeProfitBool {takeProfitBool} Placing stop loss order for {contract.symbol} with stopLoss: {ticker.stopLoss} and entryPrice: {ticker.entryPrice}")
                trade= await stop_loss_order(contract,  order, action, quantity, round(entryPrice, 2),round(stopLoss, 2), snapshot, ib, set_market_order)
            elif takeProfitBool and not set_market_order:
                logger.info(f"Bracket Order - takeProfitBool {takeProfitBool} Placing bracket loss order for {contract.symbol} with stopLoss: {stopLoss} and entryPrice: {entryPrice}")
                trade= await limit_bracket_order(contract,  order, action, quantity, round(entryPrice, 2), round(stopLoss, 2), snapshot, ib)
            elif takeProfitBool and set_market_order and  is_market_hours():
                logger.info(f"Bracket Order - takeProfitBool {takeProfitBool} Placing bracket loss order for {contract.symbol} with stopLoss: {stopLoss} and entryPrice: {entryPrice}")
                trade = await market_bracket_order(contract,  order, action, quantity,round(stopLoss, 2), snapshot, ib)
        
            if trade:
                orderPlace=await order_placed.delete(symbol)
                logger.info(f"Order placed for {contract.symbol} orderPlace {orderPlace} canceling market data")
                ib.cancelMktData(contract)
                await zapier_relay(order, snapshot)
                web_request_json =jsonable_encoder(trade)
                logger.debug(f"Order Entry for {contract.symbol} successfully placed with order {web_request_json}")
            

        
           
            
            return web_request_json
        else:
            logger.info(f"Bracket order not placed for {contract.symbol} ")

            return {"status": "Nothing"}
           
       
      
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")





async def process_close_all(contract: Contract, ticker: OrderRequest, barSizeSetting= None):
    try:
        symbol = contract.symbol 
        snapshot = await price_data_dict.get_snapshot(symbol)
        

        
        if barSizeSetting is None:
            is_valid_timeframe = "secs" in ticker.barSizeSetting_tv or "min" in ticker.barSizeSetting_tv
            if is_valid_timeframe:
                barSizeSetting = ticker.barSizeSetting_tv
            else:
                barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(ticker.barSizeSetting_tv)

        
        logger.info(f"Processing webhook request for {contract.symbol} ")
        first_ticker = None
        totalQuantity = 0.0
        trade = {}
        order = {}
        action = None
        limit_price = 0.0

        logger.info(
            f"New Order close_all request for {contract.symbol} with jengo and uptrend"
        )
        
        
        logger.info(
                    f"first_ticker.ask: {first_ticker.ask}, first_ticker.bid: {first_ticker.bid}, first_ticker.markPrice: {first_ticker.markPrice}"
                )
        
        placeOrder=False
        
        positions = await ib.reqPositionsAsync()
        
        parent_id = parent_ids[symbol]
        openOrders =  ib.openOrders()
        for openOrder in openOrders:
            if openOrder.orderId:
                parent_ids[symbol] =openOrder.orderId

            if openOrder.orderId == parent_id:
                open_order: Order = openOrder
                logger.info(f"Open order found for {symbol} with action {openOrder.action} and quantity {openOrder.totalQuantity}")
                result = ib.cancelOrder(open_order)
                if result:
                    logger.info(f"Order {open_order.orderId} cancelled successfully for open_order {open_order}")
                
                  
        for pos in positions:
            
            if pos.position == 0.0:
                continue
            await cancel_open_orders(pos)

            if pos.contract.symbol == symbol:
                placeOrder=True
                
                logger.info(
                    f"Position: {pos.contract.symbol} - {pos.position} shares at {pos.avgCost} and first_ticker.ask: {first_ticker.ask}, first_ticker.bid: {first_ticker.bid}, first_ticker.markPrice: {first_ticker.markPrice}"
                )
                action = "SELL" if pos.position > 0 else "BUY"
                totalQuantity = abs(pos.position)
                if is_market_hours:
                    logger.info(
                        f"Market is open, closing position for {contract.symbol} with action {action} and quantity {totalQuantity}"
                    )
                    order = MarketOrder(action, totalQuantity)
                if not is_market_hours():
                    
                    
                    first_ticker= await price_data_dict.get_snapshot(symbol)
                    
                    logger.info(f"Is not market hours, using limit order for {contract.symbol} with action {action} and quantity {totalQuantity}")

                    limit_price = first_ticker.ask if action == "BUY" else first_ticker.bid
                    if limit_price <= 2.0 or limit_price is None:
                        logger.warning(f"Limit price for {contract.symbol} is None or 0, using default value of 1.0")
                        limit_price = first_ticker.markPrice
                    
                    order = LimitOrder(
                        action,
                        totalQuantity=totalQuantity,
                        lmtPrice=limit_price,
                        tif="GTC",
                        outsideRth=True,
                        transmit=True,
                        
                        orderRef=f"Web close_postions - {contract.symbol}",
                    )
                  
              
        if placeOrder:
            trade = ib.placeOrder(contract, order)
        

        if trade:
            logger.info(f"Order ID for {contract.symbol}")
            order_json = {
                "ticker": contract.symbol,
                "orderAction": action,
                "limit_price": round(limit_price, 2),
                
            
                "quantity": totalQuantity,
                "rewardRiskRatio": ticker.rewardRiskRatio,
                "riskPercentage": ticker.riskPercentage,
                "accountBalance": ticker.accountBalance,
                "stopType": ticker.stopType,
                "atrFactor": ticker.atrFactor,
            }
            logger.info(format_order_details_table(order_json))
            web_request_json =jsonable_encoder(order_json)
            await order_db.insert_order(trade)
            await asyncio.sleep(1)
            await cancel_open_orders(pos)

            logger.info(
                f"Order Entry for {contract.symbol} successfully placed with order"
            )
            await zapier_relay(ticker, snapshot)
            return JSONResponse(content={"status": "success"})
        else:
            logger.info(
                f"No position found for {contract.symbol} in IB. Cannot close position."
            )
            await zapier_relay(ticker, snapshot)
            return JSONResponse(
                content={
                    "status": f"No position found for {contract.symbol} in IB. Cannot close position."
                }
            )
        
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")
    
async def zapier_forward(ticker: OrderRequest, snapshot=None):
    
    try:
        symbol= ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker
        if symbol is None:
            symbol = ticker.ticker
        form=await tv_store_data.get(symbol)
        
        logger.debug(f"Zapier forward form data for {symbol}: {form}")
        client = httpx.Client()

        reqUrl = "https://hooks.zapier.com/hooks/catch/10447300/2n8ncic/"

        headersList = {
        
        "Content-Type": "application/json",
        "Accept": "application/json" 
        }
        if form is None:
            form=ticker.model_dump()
        #test_payload=jsonpickle.encode(ticker, unpicklable=False)
        #logger.debug(f"Zapier test payload with ticker: {test_payload}")
        result_raw = {
            "OrderRequest": form,   
            "snapshot":        snapshot
        }
        payload=jsonable_encoder(result_raw)
        payload = clean_nan(payload)

        #payload = jsonable_encoder(form)
        logger.debug(f"Zapier payload: {payload}")
        
        

        data = client.post(reqUrl, data=payload, headers=headersList)

        logger.debug(data.text)
        return None
    except Exception as e:
        logger.error(f"Error forwarding to Zapier: {e}")
        return None
    
async def zapier_relay(ticker: OrderRequest, snapshot=None):
    
    try:
        asyncio.create_task(zapier_forward(ticker,snapshot))
        return JSONResponse(content="", status_code=200)
        
    except Exception as e:
        logger.error(f"Error forwarding to Zapier: {e}")
        return None
@app.post("/tv-post-data")
async def tv_post_data(ticker: OrderRequest, background_tasks: BackgroundTasks):
    try:
        takeProfitBool = ticker.takeProfitBool
        logger.info(f"Received TV post data for ticker: {ticker.ticker} with takeProfitBool: {takeProfitBool}")
        
        symbol = ticker.ticker.split(":", 1)[1] if ":" in ticker.ticker else ticker.ticker
        logger.info(f"Received TV post data for ticker: {symbol} unixtime {ticker.unixtime} ticker.ticker {ticker.ticker} ticker.barSizeSetting_tv {ticker.barSizeSetting_tv}")
        snapshot = None
        

        
        contract: Contract = None
        tv_data= await tv_store_data.set(symbol, ticker.model_dump()),
        action = ticker.orderAction
        barSizeSetting = await get_bar_size_setting(symbol)
        
        if action == "close_all":
            contract = await add_new_contract(symbol, barSizeSetting, ib)
            logger.debug(
                f"Processing close_all request for {symbol}"
            )
            asyncio.create_task(process_close_all(contract, ticker, barSizeSetting))
            return JSONResponse(content="", status_code=200)
        
        
        logger.debug(f"Received TV post data for ticker: {symbol} unixtime {ticker.barSizeSetting_tv} bar_test: barSizeSetting: {barSizeSetting}")
        contract, orderPlace = await asyncio.gather(
            add_new_contract(symbol, barSizeSetting, ib),
            
            order_placed.get(symbol)
        )
        if not await price_data_dict.add_first_rt_bar(symbol):
                logger.info(f"Adding bar for {contract.symbol} to add_first_rt_bar")
                bars: RealTimeBarList = ib.reqRealTimeBars(contract, 5, "TRADES", False)
                bars.updateEvent += on_bar_update
                

                first_rt_bar=await price_data_dict.add_first_rt_bar(symbol)
                logger.info(f"First real-time bar added for {contract.symbol}: {first_rt_bar}")
        snapshot= await price_data_dict.get_snapshot(symbol)
        
        
        if orderPlace:
            logger.info(f"Order already placed for {symbol}, skipping processing.")
            return JSONResponse(content="OK", status_code=200)
        logger.debug(f"Received TV post data for ticker: {symbol} unixtime {ticker.unixtime} ")

        if ticker.entryPrice == 0  or isNan(ticker.entryPrice) or ticker.orderAction not in ["BUY", "SELL", "close_all"]:
            sub=get_vstop(ticker, contract)

            asyncio.create_task(sub)
            if sub:
                if snapshot is not None:
                    logger.debug(f"Snapshot for {symbol}: markPrice: {snapshot.markPrice}")
                else:
                    logger.debug(f"Subscribing to ticker {symbol} for vStop calculation no snapshot for boof")
                response = {
                    "status": "success",
                    "message": f"Subscribed to ticker boof3 {symbol} for vStop calculation"
                    
                }
  
                return JSONResponse(content=response, status_code=200)
        if ticker.entryPrice != 0 and ticker.orderAction in ["BUY", "SELL"]:
            logger.info(f"Processing TV post data for {symbol} with entryPrice {ticker.entryPrice} and orderAction {ticker.orderAction}")
            trade= await process_tv_post_data(contract, ticker, barSizeSetting,  snapshot)
            #trade=background_tasks.add_task(
                    #process_tv_post_data, contract, ticker, barSizeSetting, snapshot 
                #)
            if trade:
                await zapier_relay(ticker, snapshot)
                fill=await fill_data.get_trade(symbol)
                logger.info(f"fill data for {symbol} processed successfully: {fill}")
            # return immediately
            return JSONResponse(content=trade, status_code=200)
    except Exception as e:
        logger.error(f"Error processing TV post data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
async def process_tv_post_data(contract: Contract, ticker: OrderRequest, barSizeSetting,  snapshot):
    try:
        sub= await get_vstop(ticker, contract)
        uptrend = None
        
        orderAction=None
        quantity = 0.0
        web_request_json = {}
        entryPrice = None
        stopLoss = ticker.stopLoss
        barSizeSetting = await get_bar_size_setting(symbol)
        logger.debug(f"Processing TV post data for {contract.symbol}")
        takeProfitBool= ticker.takeProfitBool
        logger.info(f"Processing takeProfitBool {takeProfitBool} for TV post data for {ticker} with entryPrice {ticker.entryPrice} and orderAction {ticker.orderAction}")
        if snapshot is None:
            snapshot= await price_data_dict.get_snapshot(contract.symbol)
        entryPrice =None
        emaCheck= None
        ema =await ema_check(contract, ib, barSizeSetting)  # Ensure EMA is checked for the contract
        emaCheck, _ = ema
        if emaCheck is not None:
            orderAction = "BUY" if emaCheck =="LONG" else "SELL"
            logger.info(f"emaCheck order Action for {contract.symbol} is {orderAction} and entryPrice {ticker.entryPrice} and emaCheck {emaCheck}"
            )
        else:
            orderAction = ticker.orderAction
        action = orderAction
        set_market_order=ticker.set_market_order
        order= []
        symbol = contract.symbol
        if float(ticker.entryPrice) == 0.0 or isNan(ticker.entryPrice) or ticker.entryPrice is None or ticker.stopLoss == 0:
            logger.warning(f"Invalid entryPrice for {contract.symbol}, skipping processing. entryPrice: {ticker.entryPrice}")
            entryPrice = snapshot.ask if action == "BUY" else snapshot.bid
            if entryPrice is None or entryPrice < 2.0:
                logger.warning(f"Entry price for {contract.symbol} is None or too low, using default value of 1.0")
                entryPrice = snapshot.markPrice
        
        
        
        

        
        if (ticker.entryPrice is not None and ticker.entryPrice != 0 and not is_float(ticker.entryPrice)) and (
            ticker.stopLoss is not None and  ticker.stopLoss != 0 and not is_float(ticker.stopLoss)
       ):
           logger.info(f"Using entryPrice from webhook: {entryPrice}")
           barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(
               ticker.barSizeSetting_tv
           )
           entryPrice = ticker.entryPrice
           stopLoss = ticker.stopLoss
        # determine bar size string
        
        already_subscribed = await subscribed_contracts.has(symbol)
        subscribe_needed=not already_subscribed
        logger.debug(f"Already subscribed to {symbol}: {already_subscribed}")
        logger.debug(f"Contract for {symbol}: {contract}")
        
       
        await price_data_dict.add_vol_stop(symbol, stopLoss, ticker.uptrend, ticker.vstopAtrFactor, atr_float=None)

        
        sub= None
        # subscribe to live market data only once
        if subscribe_needed:
            logger.debug(f"Subscribing market data for {symbol} with barSizeSetting: {barSizeSetting} ")

            contract = await add_new_contract(symbol, barSizeSetting, ib)
            

        bk_vol=await daily_volatility.get(symbol)
        if bk_vol is not None:
            vol=bk_vol
            
        
        get_symbol_dict= await get_tv_ticker_dict(symbol)
        if stopLoss is None or stopLoss == 0.0 :
            vstop_values= await price_data_dict.get_snapshot(symbol)
            if vstop_values is not None:
                stopLoss = vstop_values.vStop
                uptrend = vstop_values.uptrend
               
                logger.info(f"Using vstop values for {contract.symbol}: stopLoss={stopLoss}, entryPrice={entryPrice}, uptrend={uptrend}")
            
        atrFactor = get_symbol_dict.atrFactor
        vstopAtrFactor =get_symbol_dict.vstopAtrFactor
        if vstopAtrFactor is None or vstopAtrFactor == 0.0:
            vstopAtrFactor = atrFactor
        
        entryPrice = ticker.entryPrice
        if not is_market_hours():
            set_market_order= False
        # uptrend_orderAction = "BUY" if uptrend  else "SELL"
        logger.info(
            f"Uptrend order Action for {contract.symbol} is {orderAction} and entryPrice {ticker.entryPrice}"
        )
        order = OrderRequest(
            entryPrice=entryPrice,  # Use the latest market price as entry price
            rewardRiskRatio=ticker.rewardRiskRatio,
            quantity=ticker.quantity,  # Use the quantity from the webhook
            riskPercentage=ticker.riskPercentage,
            vstopAtrFactor=vstopAtrFactor,
            timeframe=barSizeSetting,
            kcAtrFactor=vstopAtrFactor,
            atrFactor=atrFactor,
            accountBalance=ticker.accountBalance,
            ticker=symbol,
            orderAction=orderAction,
            stopType=ticker.stopType,
            set_market_order=set_market_order,
            takeProfitBool=ticker.takeProfitBool,
            stopLoss=stopLoss,
            uptrend=uptrend,
            notes="process_tv_post_data"
        )
        await tv_store_data.set(symbol, order.model_dump())
        logger.warning(f"order  is {order}")
        logger.warning(
            f"barSizeSetting  is {barSizeSetting} for  {contract.symbol} and vStop type {ticker.stopType}"
        )
        takeProfitBool = ticker.takeProfitBool
        
        
    
        if ticker.quantity is not None and ticker.quantity > 0:
            quantity = ticker.quantity
        

        logger.info(
            f"New Order request for  subscribed_contracts {contract} with stopLoss {stopLoss} and ticker.stopLoss {ticker.stopLoss} and stopLoss+5 {stopLoss+5}"
        )
        trade = None
        shortableShares = snapshot.shortableShares
        shortable= False
        if action == "SELL" and emaCheck =="SHORT":
            logger.info(f"Checking if {contract.symbol} is shortable...")
            is_shortable=shortable_shares(contract.symbol, action, quantity, shortableShares)
            logger.info(f"{contract.symbol} is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")

            if shortable_shares:
                logger.info(f"{contract.symbol} is shortable, is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")
                shortable = True

            if not shortable:
                order = OrderRequest(
                    entryPrice=entryPrice,  # Use the latest market price as entry price
                    rewardRiskRatio=ticker.rewardRiskRatio,
                    quantity=ticker.quantity,  # Use the quantity from the webhook
                    riskPercentage=ticker.riskPercentage,
                    vstopAtrFactor=vstopAtrFactor,
                    timeframe=barSizeSetting,
                    kcAtrFactor=vstopAtrFactor,
                    atrFactor=atrFactor,
                    accountBalance=ticker.accountBalance,
                    ticker=symbol,
                    orderAction=orderAction,
                    stopType=ticker.stopType,
                    set_market_order=set_market_order,
                    takeProfitBool=ticker.takeProfitBool,
                    stopLoss=stopLoss,
                    uptrend=uptrend,
                    notes="not shortable shares"
                )
                await tv_store_data.set(symbol, order.model_dump())
                logger.warning(f"{contract.symbol} is not shortable, cannot place SELL order, is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")
                
                return JSONResponse(content={"status": "Not shortable", "message": f"{contract.symbol} is not shortable, cannot place SELL order."}, status_code=403) 
        

        if stopLoss > 0:
            order_details=order 

            
        
            orderPlace=await order_placed.set(symbol)
           
            if  not takeProfitBool and set_market_order and is_market_hours():
                

                logger.info(f"takeProfitBool {takeProfitBool} Placing stop loss order for {contract.symbol} with stopLoss: {ticker.stopLoss} and entryPrice: {ticker.entryPrice}")
                trade= await market_stop_loss_order(contract,  order, action, quantity,round(ticker.stopLoss, 2), snapshot, ib, set_market_order)
                
                logger.info(f"Order being placed for {contract.symbol} orderPlace {orderPlace} ")
            elif takeProfitBool and (not set_market_order or not is_market_hours()):
                logger.info(f"Bracket Order - takeProfitBool {takeProfitBool} Placing bracket loss order for {contract.symbol} with stopLoss: {stopLoss} and entryPrice: {entryPrice}")
                trade= await limit_bracket_order(contract,  order, action, quantity, round(entryPrice, 2), round(stopLoss, 2), snapshot, ib)
            
            ###############################################
            elif takeProfitBool and symbol=="FOOOOOOOOOOOOOO" and (not set_market_order or not is_market_hours()):
                logger.info(f"Bracket Order - takeProfitBool {takeProfitBool} Placing bracket loss order for {contract.symbol} with stopLoss: {stopLoss} and entryPrice: {ticker.entryPrice}")
                trade = await ib_bracket_order(contract, action, quantity, order_details=order, vol=vol, stopLoss=ticker.stopLoss)
            ###################################################

            elif takeProfitBool and set_market_order and  is_market_hours():
                logger.info(f"Bracket Order - takeProfitBool {takeProfitBool} Placing bracket loss order for {contract.symbol} with stopLoss: {stopLoss} and entryPrice: {ticker.entryPrice}")
                trade = await market_bracket_order(contract,  order, action, quantity,round(ticker.stopLoss, 2), snapshot, ib)
        
            
            elif not set_market_order and (not takeProfitBool or not is_market_hours()):
                entryPrice = snapshot.ask if action == "BUY" else snapshot.bid
                if entryPrice is None or entryPrice < 2.0:
                    logger.warning(f"Entry price for {contract.symbol} is None or too low, using default value of 1.0")
                    entryPrice = snapshot.markPrice

                logger.info(f"takeProfitBool {takeProfitBool} Placing stop loss order for {contract.symbol} with stopLoss: {ticker.stopLoss} and entryPrice: {ticker.entryPrice}")
                trade= await stop_loss_order(contract,  order, action, quantity, round(ticker.entryPrice, 2),round(ticker.stopLoss, 2), snapshot, ib, set_market_order)
            
           
       
            if trade:
                orderPlace=await order_placed.delete(symbol)
                logger.info(f"Order placed for {contract.symbol} orderPlace {orderPlace} canceling market data")
                await asyncio.sleep(1)
                fill=await fill_data.get_trade(contract.symbol)
                ib.cancelMktData(contract)
                await zapier_relay(order, snapshot)
                web_request_json =jsonable_encoder(trade)
                logger.info(f"Order Entry for {contract.symbol} successfully placed with order boof fill {fill}")
           
            
            return web_request_json
        else:
            logger.info(f"Bracket order not placed for {contract.symbol} ")
            orderPlace=await order_placed.delete(symbol)

            return {"status": "Nothing",
                    "ticker": contract.symbol,
                    }
           
       
      
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")
       



@app.get("/get-ticker/{ticker}")
async def read_ticker_dict(ticker: str):
    snapshot = await price_data_dict.get_snapshot(ticker)
    if snapshot is None:
        raise RuntimeError(f"No live data for {ticker}")

    limit_price = snapshot.last          # IB Tick-by-tick “Last” price

    market_price = (
        snapshot.markPrice
        if snapshot.markPrice             # non-zero / non-NaN?
        else (snapshot.bid + snapshot.ask) * 0.5
    )

    last_bar_close = (
        snapshot.last_bar_close           # set by add_bar() / add_rt_bar()
        or snapshot.close                 # same value – both safe
    )

    yesterday_price = snapshot.yesterday_close_price
    logger.info(f"Retrieved ticker {ticker} with yesterday_price: {yesterday_price}, limit_price: {limit_price}, market_price: {market_price}, last_bar_close: {last_bar_close}")

    result = await tv_store_data.get(ticker)
    if result is None:
        raise HTTPException(status_code=404, detail="Key not found")
    contract = await subscribed_contracts.get(ticker)
    logger.info(f"Retrieved ticker {ticker}: {result}")
    ib_ticker = ib.ticker(contract)
    logger.info(f"Retrieved IB ticker for {ticker}: {ib_ticker}")
    logger.info(f"Retrieved snapshot for {ticker}: {snapshot}")
    logger.info(f"Retrieved snapshot for {ticker}: bid: {snapshot.bid}, ask: {snapshot.ask}, last: {snapshot.last}")
    order_data = OrderRequest(**result)
    vStop_float, uptrend=await price_data_dict.get_vol_stop(ticker)
    logger.info(f"Retrieved vStop and uptrend for {ticker}: vStop: {vStop_float}, uptrend: {uptrend}")
    logger.info(f"Converted to OrderRequest: {order_data.uptrend} stopLoss: {order_data.stopLoss} entryPrice: {order_data.entryPrice}")
    vol_stop_data = await volatility_stop_data.get(ticker)
    vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float=vol_stop_data
    logger.info(f"Retrieved vStop float and uptrend_bool for {ticker}: vStop_float: {vStop_float}, uptrend_bool: {uptrend_bool}")
    json_response = {
        "order_data.uptrend": order_data.uptrend,
        "uptrend": uptrend_bool,
        "stopLoss": order_data.stopLoss,
        "entryPrice": order_data.entryPrice,
        "vStop": vStop_float,
        "ticker": ticker,
        "bid": snapshot.bid,
        "ask": snapshot.ask,
        "last": snapshot.last
    }
    snapshot_json=jsonpickle.encode(snapshot, unpicklable=False)
    fast_snapshot_json=jsonable_encoder(snapshot_json)
    logger.info(f"Snapshot JSON for {ticker}: fast_snapshot_json: {fast_snapshot_json}")
    logger.info(f"Returning JSON response for ticker {ticker}: {json_response}")
    return JSONResponse(content=snapshot_json, status_code=200)
@app.get("/all-symbols")
async def get_all_tickers_dict():
    result = await tv_store_data.all()
    if result is None:
        raise HTTPException(status_code=404, detail="Key not found")
    logger.info(f"Retrieved tickers  {result}")
    order_data = OrderRequest(**result)
    all_symbols= await price_data_dict.all_snapshots()
    ticker_json = jsonable_encoder(order_data)
    snap_json  = jsonable_encoder(all_symbols)
    reponse = {
        "uptrend": order_data.uptrend,
        "stopLoss": order_data.stopLoss,
        "entryPrice": order_data.entryPrice,
        "all_symbols": snap_json,
        "ticker_json": ticker_json
    }
    logger.info(f"Converted to OrderRequest: {order_data.uptrend} stopLoss: {order_data.stopLoss} entryPrice: {order_data.entryPrice} all_symbols: {snap_json} ticker_json: {ticker_json}")
    return JSONResponse(content=clean_nan(snap_json), status_code=200)



if __name__ == "__main__":
    log_config.setup()
    load_dotenv()

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
    # uvicorn.run("app:app", host=HOST, port=PORT, log_level="info")

    uvicorn.run("app:app", host=HOST, port=PORT, log_level="info")

