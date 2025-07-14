
# apy.py
import os, json, asyncio, pytz, time
import re
from typing import Any, Dict
import math
from dataclasses import dataclass
import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime
from collections import deque
from fastapi import FastAPI, HTTPException, Request, Response, Query, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from ib_async import *
from dotenv import load_dotenv
from pnl import IBManager
from timestamps import current_millis, get_timestamp, is_market_hours
from log_config import log_config, logger
from ticker_list import ticker_manager 
from price import PriceData
import talib as ta
from indicators import daily_volatility
from p_rvol import PreVol
from break_change import PriceChangeFilter
from hammer import HammerBacktest, TVOpeningVolatilityScan
load_dotenv()

# --- Configuration ---
PORT = int(os.getenv("TVWB_HTTPS_PORT", "80"))
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv('FASTAPI_IB_CLIENT_ID', '1111'))
IB_HOST = os.getenv('IB_GATEWAY_HOST', '127.0.0.1')
subscribed_contracts: dict[str, Contract] = {}
# Instantiate IB manager
ib = IB()
ib_manager = IBManager(ib) 

ib_price_data = PriceData(ib_manager)
pre_rvol = PreVol(ib_manager)
price_change_filter = PriceChangeFilter(ib_manager)


@dataclass
class AccountPnL:
    unrealized_pnl: float
    realized_pnl: float
    total_pnl: float
    

class QueryModel(BaseModel):
    query: str

# Request cache to help filter duplicates if desired boof
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



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to IB
    logger.info(f"Starting application; connecting to IB...{risk_amount}")
    if not await ib_manager.connect():
        raise RuntimeError("Failed to connect to IB Gateway")
    yield  # Application runs during this yield
    # Shutdown: Gracefully disconnect
    await ib_manager.graceful_disconnect()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(CURRENT_DIR, "static")), name="static")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://portfolio.porenta.us", "https://tv.porenta.us"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
async def get_ohlc_vol(contract, durationStr, barSizeSetting, return_df):
        try:
            if return_df:
                df=await ib_price_data.ohlc_vol(
                contract=contract,
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow='TRADES',
                useRTH=True,
                return_df=return_df
            )
                logger.info(f"return_df Got them for jengo - get_ohlc_vol ")
                return df

            if not return_df:
                ohlc4, hlc3, hl2, open_prices, high, low, close, vol = await ib_price_data.ohlc_vol(
                    contract=contract,
                    durationStr=durationStr,
                    barSizeSetting=barSizeSetting,
                    whatToShow='TRADES',
                    useRTH=True,
                    return_df=return_df
                )
                logger.info(f"ohlc4, hlc3, hl2, open_prices, high, low, close, vol Got them for jengo - get_ohlc_vol ")
                return ohlc4, hlc3, hl2, open_prices, high, low, close, vol
        except Exception as e:
            logger.error(f"Error in get_ohlc_vol: {e}")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    try:
        return templates.TemplateResponse("tbot_dashboard.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/rvol", response_class=HTMLResponse)
async def rvol(rvol: float):
    try:
        rvol_percent = float(rvol / 10)
        logger.info(f"Starting rvol from fastapi")
        await pre_rvol.start(rvol_percent)

        return JSONResponse(content={"status": "success", "message": f"rvol started with {rvol_percent}"})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/change", response_class=HTMLResponse)
async def chnage(percent: float):
    try:
        change_percent = float(percent)
        logger.info(f"Starting percent change from fastapi")
        await price_change_filter.start(change_percent)

        return JSONResponse(content={"status": "success", "message": f"percent change started with {change_percent}"})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
def safe_float(val) -> float:
    try:
        f = float(val)
        return f if not math.isnan(f) else 0.0
    except Exception:
        return 0.0

@app.get("/api/pnl-data")
async def get_current_pnl():
    try:
        pnl = await ib_manager.app_pnl_event()
        logger.info(f"pnl-data endpoint P&L: {pnl}")
        positions = await ib_manager.get_position_update()
        net_liquidation = ib_manager.net_liquidation
        buying_power = ib_manager.buying_power
        logger.info(f"pnl-data endpoint Net Liquidation: {net_liquidation} and buying power: {buying_power}")
        orders = ib_manager.open_orders
        trades = ib_manager.all_trades

        data = {
            "positions": list(positions.values()),
            "pnl": [{
                "unrealized_pnl": safe_float(pnl.unrealized_pnl),
                "realized_pnl": safe_float(pnl.realized_pnl),
                "total_pnl": safe_float(pnl.total_pnl),
                "net_liquidation": safe_float(net_liquidation) if safe_float(net_liquidation) > 0 else 0.0,
                "buying_power": safe_float(buying_power) if safe_float(buying_power) > 0 else 0.0,
            }],
            "orders": orders,
            "trades": trades
        }
        return data
    except Exception as e:
        logger.error(f"Error fetching pnl data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/proxy/webhook")
async def proxy_webhook(webhook_data: WebhookRequest):
    outcome = ""
    try:
        logger.info(f"Webhook received for {webhook_data.ticker}")
        if not ib_manager.ib.isConnected():
            outcome = "IB Gateway not connected"
            logger.error(outcome)
            return JSONResponse(content={"status": "error", "message": outcome})
        if is_duplicate(webhook_data):
            outcome = "Duplicate request"
            logger.info(outcome)
            return JSONResponse(content={"status": "rejected", "message": outcome})
        risk_amount = float(os.getenv('WEBHOOK_PNL_THRESHOLD', '-300'))
        if ib_manager.account_pnl and ib_manager.account_pnl.total_pnl <= risk_amount:
            outcome = f"PnL {ib_manager.account_pnl.total_pnl} below threshold {risk_amount}. Blocking Webhook order."
            logger.warning(outcome)
            return JSONResponse(content={"status": "rejected", "message": outcome})
        price = next((m.value for m in webhook_data.metrics if m.name=="price"), None)
        entry_limit = next((m.value for m in webhook_data.metrics if m.name=="entry.limit"), None)
        qty = next((m.value for m in webhook_data.metrics if m.name=="qty"), None)
        orderRef = webhook_data.orderRef
        direction = webhook_data.direction
        action = "BUY" if direction=="strategy.entrylong" else "SELL"
        exit_stop = next((m.value for m in webhook_data.metrics if m.name=="exit.stop"), None)
        exit_limit = next((m.value for m in webhook_data.metrics if m.name=="exit.limit"), None)
        isBracket = exit_stop > 0

        contract = {"symbol": webhook_data.ticker, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract)

        trade = None
        if is_market_hours() and (entry_limit is None or entry_limit < 0.01) and not isBracket:
            order = MarketOrder(action, totalQuantity=qty, tif='GTC', orderRef=orderRef)
            logger.info(f"Placing market order for {webhook_data.ticker} at price {entry_limit} during market hours")
            trade = await ib_manager.place_order(ib_contract, order, direction, price=price or 0.0)

        if is_market_hours() and (entry_limit is None or entry_limit < 0.01) and isBracket:
            logger.info(f"Placing bracket order for {webhook_data.ticker}")
            reqId = await ib_manager.get_req_id()
            order = MarketOrder(action, totalQuantity=qty, tif='GTC', orderRef=orderRef, orderId=reqId, transmit=False, outsideRth=False)
            logger.info(f"Market order for {webhook_data.ticker} at price {entry_limit}: {order}")
            trade = await ib_manager.place_bracket_market_order(ib_contract, order, direction, action, exit_stop or 0.0, exit_limit or 0.0)
        
        # Limit order with Bracket
        if is_market_hours() and entry_limit > 0.01 and isBracket:
            logger.info(f"Placing bracket order for limit order {webhook_data.ticker}")
            reqId = await ib_manager.get_req_id()
            order = LimitOrder(action, totalQuantity=qty, lmtPrice=round(entry_limit,2), tif='GTC', outsideRth=False, orderRef=orderRef, transmit=False)
            logger.info(f"Market order for {webhook_data.ticker} at price {entry_limit}: {order}")
            trade = await ib_manager.place_bracket_market_order(ib_contract, order, direction, action, exit_stop or 0.0, exit_limit or 0.0)

        if not is_market_hours():
            last_price = await ib_manager.get_last_price(ib_contract, direction, action)
            use_price = entry_limit if entry_limit and entry_limit >= 10.0 else last_price
            order = LimitOrder(action, totalQuantity=qty, lmtPrice=round(use_price,2), tif='GTC', outsideRth=True, orderRef=orderRef)
            price = use_price
            logger.info(f"Placing limit order for {webhook_data.ticker} at price {price} (last price: {last_price}, use_price: {use_price}) during non-market hours")
            trade = await ib_manager.place_order(ib_contract, order, direction, price=price or 0.0)

        if trade:
            outcome = f"Order placed for {webhook_data.ticker} with price {price} during market hours: {is_market_hours()}"
            logger.info(outcome)
        else:
            outcome = f"Order placement failed for {webhook_data.ticker}"
            logger.error(outcome)
        return JSONResponse(content={"status": "completed", "message": outcome})
    except Exception as e:
        outcome = f"Error in webhook processing: {str(e)}"
        logger.error(outcome)
        return JSONResponse(content={"status": "error", "message": outcome})
    


@app.post("/close_positions")
async def close_positions(webhook_data: WebhookRequest):
    try:
        qty = next((m.value for m in webhook_data.metrics if m.name=="qty"), None)
        action =webhook_data.direction
        symbol= webhook_data.ticker
        logger.info(f"Closing position for {symbol}")
        # This endpoint is used for closing positions (triggered by webhook alerts)
        contract_data = {"symbol": symbol, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract_data) 
        
      
        
        trade=await ib_price_data.create_close_order(ib_contract, action, qty)
        if trade:
            return JSONResponse(content={"status": "success", "message": f"Close order placed for {symbol}", "trade": trade})
        else:
            return JSONResponse(content={"status": "no action", "message": f"No active position found for {symbol}"})
    except Exception as e:
        logger.error(f"Error closing position: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hard-data")
async def get_hard_data():
    data = await ib_manager.fast_get_ib_data()
    if data is None:
        data = {"positions": [], "pnl": [{"unrealized_pnl": 0.0, "realized_pnl": 0.0,
                                            "total_pnl": 0.0, "net_liquidation": 0.0}], "orders": [], "trades": []}
    return JSONResponse(content=data)

@app.get("/hello")
async def hello():
    unique_ts = str(current_millis() - (4 * 60 * 60 * 1000))
    return {"message": f"Hello World time:{unique_ts}"}


# Add these imports to your existing imports section
# Add this new model class after your existing model definitions
class GenericJSONRequest(BaseModel):
    data: Dict[str, Any]
    
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            # Add any custom encoders if needed
            datetime: lambda v: v.isoformat()
        }

# Add this new endpoint to your existing FastAPI app
@app.post("/api/parse-json")
async def parse_json(request: Request):
    try:
        # Get raw JSON data from request
        raw_data = await request.json()
        
        # Create a standardized response format
        parsed_data = {
            "timestamp": current_millis(),
            "request_data": raw_data,
            "metadata": {
                "content_type": request.headers.get("content-type"),
                "content_length": request.headers.get("content-length"),
                "endpoint": str(request.url),
                "method": request.method
            },
            "parsed_fields": {
                "top_level_keys": list(raw_data.keys()) if isinstance(raw_data, dict) else [],
                "data_types": {
                    key: str(type(value).__name__) 
                    for key, value in raw_data.items()
                } if isinstance(raw_data, dict) else {}
            }
        }
        
        logger.info(f"Successfully parsed JSON data: {json.dumps(parsed_data, indent=2)}")
        prase=await process_parsed_json(parsed_data) 
        logger.info(f"prase: {prase}")
        return JSONResponse(content=parsed_data)
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid JSON format: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing JSON request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

# Example usage in another function:
async def process_parsed_json(parsed_data: Dict[str, Any]):
    """
    Example function showing how to use the parsed JSON data
    """
    try:
        # Access the original request data
        request_data = parsed_data.get("request_data", {})
        
        # Access metadata
        timestamp = parsed_data.get("timestamp")
        content_type = parsed_data.get("metadata", {}).get("content_type")
        
        # Access parsed field
        top_level_keys = parsed_data.get("parsed_fields", {}).get("top_level_keys", [])
        data_types = parsed_data.get("parsed_fields", {}).get("data_types", {})
        
        # Process the data as needed
        result = {
            "processed_at": current_millis(),
            "original_timestamp": timestamp,
            "processed_fields": top_level_keys,
            # Add any additional processing results
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing parsed JSON: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing parsed JSON: {str(e)}")
@app.post("/boof")
async def sub_to_ticker(ticker: TickerSub):
    try:
        logger.info(f"Subscribing to ticker: {ticker.ticker}")
        contract_data = {"symbol": ticker.ticker, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract_data) 
        # Store the contract instance for later cancellation
        subscribed_contracts[ticker.ticker] = ib_contract
        price = await ib_price_data.req_mkt_data(ib_contract)
        return JSONResponse(content={"status": "success", "contract": contract_data, "price": price})
    except Exception as e:
        logger.error(f"Error in subscribing to ticker: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})

@app.post("/un_boof")
async def unsub_from_ticker(ticker: TickerSub):
    try:
        # Retrieve the original contract instance
        ib_contract = subscribed_contracts.get(ticker.ticker)
        logger.info(f"Unsubscribing from ticker: {ib_contract}")
        if not ib_contract:
            raise HTTPException(status_code=404, detail="Ticker not subscribed")
        await ib_price_data.unsub_mkt_data(ib_contract)
        # Optionally remove the contract from the cache
        del subscribed_contracts[ticker.ticker]
        return JSONResponse(content={"status": "success", "contract": {"symbol": ib_contract}})
    except Exception as e:
        logger.error(f"Error in unsubscribing from ticker: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})
@app.post("/bars")
async def get_contract_bars(ticker: TickerSub, action: str):
    try:
        contract = {"symbol": ticker.ticker, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract)
        bars = await ib_price_data.get_bars(ib_contract, action)
        return JSONResponse(content={"status": "success", "bars": bars})
    except Exception as e:
        logger.error(f"Error in get_contract_bars: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})
@app.post("/rt-bars")
async def get_contract_rt_bars(ticker: TickerSub):
    try:
        contract = {"symbol": ticker.ticker, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract)
        bars = await ib_price_data.get_rt_bars(ib_contract)
        return JSONResponse(content={"status": "success", "rt_bars": bars})
    except Exception as e:
        logger.error(f"Error in get_contract_bars: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})
@app.post("/ticks")
async def get_contract_ticks(ticker: TickerSub):
    try:
        contract = {"symbol": ticker.ticker, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract)
        ticks = await ib_price_data.get_ticks(ib_contract)
        return JSONResponse(content={"status": "success", "bars": ticks})
    except Exception as e:
        logger.error(f"Error in get_contract_ticks: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})
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



@app.post("/place_order")
async def place_web_order(web_request: OrderRequest):
    try:
        order = web_request
        action = web_request.orderAction
        stopType = web_request.stopType
        meanReversion = web_request.meanReversion
        stopLoss = float(web_request.stopLoss)
        logger.info(f"New Order request for {web_request.ticker} with stopLoss {stopLoss} and web_request.stopLoss {web_request.stopLoss} and stopLoss+5 {stopLoss+5}")
        # You can now use these fields that were in your payload
        vstopAtrFactor = web_request.vstopAtrFactor
        kcAtrFactor = web_request.kcAtrFactor
        
        if action not in ["BUY", "SELL"]:
            raise ValueError("Order Action is None")
            
        # Step 1: Connect to IB and retrieve market data/entry price
        contract = {"symbol": web_request.ticker, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract)
        logger.info(f"New Order request for {web_request.ticker}")
        
        # Step 2: Calculate position details
        durationStr='35 D'
        barSizeSetting='1 day'
        ohlc4, hlc3, hl2, open_prices, high, low, close, vol = await get_ohlc_vol(ib_contract, durationStr, barSizeSetting, return_df=False)
        
        # Pass the additional parameters to your functions if needed
        df, orderEntry = await asyncio.gather(
            daily_volatility(ib_manager.ib, contract, high, low, close, stopType),
            ib_price_data.create_order(order, ib_contract, True, meanReversion, stopLoss)
        )
        
        logger.info(f"Order Entry for {web_request.ticker}: {orderEntry}")
        return JSONResponse(content={"status": "success", "order_details": orderEntry})
        
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail=f"Error placing order: {str(e)}")


# --- Main Endpoint ---
@app.post("/place_tv_order")
async def place_tv_web_order(web_request: tvOrderRequest):
    try:
        ema = None
        
        if web_request.entryPrice <= 0:
            logger.error(f"entryPrice is Less than or equal to 0")
            raise ValueError("entryPrice is Less than or equal to 0")
        if web_request.entryPrice is None:
            logger.error(f"entryPrice is None")
            raise ValueError("entryPrice is None")
        
        # Step 1: Connect to IB and retrieve market data/entry price
        contract = {"symbol": web_request.ticker, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract)
        logger.info(f"New Order request for  for {web_request.ticker}")
        # Step 2: Calculate position details
        durationStr='2 D'
        barSizeSetting='1 min'
        df_1 = await get_ohlc_vol(ib_contract, durationStr, barSizeSetting, return_df=True)
        df_1['ema'] = ta.EMA(df_1['close'].values, timeperiod=9)
        ema = df_1.iloc[-1]['ema']
        ema_orderAction = "BUY" if ema < float(web_request.entryPrice) else "SELL"
        logger.info(f"ema_orderAction for {web_request.ticker}: {ema_orderAction} with ema {ema} and entryPrice {web_request.entryPrice}")
        order =tvOrderRequest(
            entryPrice=web_request.entryPrice,
            rewardRiskRatio=web_request.rewardRiskRatio,
            riskPercentage=web_request.riskPercentage,
            vstopAtrFactor=web_request.vstopAtrFactor,
            kcAtrFactor=web_request.kcAtrFactor,
            atrFactor=web_request.atrFactor,
            accountBalance=web_request.accountBalance,
            ticker=web_request.ticker,
            orderAction=ema_orderAction,
            stopType=web_request.stopType,
            submit_cmd = web_request.submit_cmd,
            meanReversion=web_request.meanReversion,
            stopLoss=float(web_request.stopLoss),
            
        )
        submit_cmd = web_request.submit_cmd
        meanReversion=web_request.meanReversion
        stopLoss=float(web_request.stopLoss)
        logger.info(f"New Order request for  for {web_request.ticker} with stopLoss {stopLoss} and web_request.stopLoss {web_request.stopLoss} and stopLoss+5 {stopLoss+5}")

        orderEntry =await ib_price_data.create_tv_entry_order(order, ib_contract, submit_cmd, meanReversion, stopLoss)
        
        
        logger.info(f"Order Entry for {web_request.ticker}: {orderEntry}")
       
      
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")
    
@app.post("/check_order")
async def check_web_order(web_request: OrderRequest):
    try:

        order = web_request
        action = web_request.orderAction
        stopType = web_request.stopType
        meanReversion = web_request.meanReversion
        stopLoss = web_request.stopLoss

        if action not in ["BUY", "SELL"]:
            raise ValueError("Order Action is None")
        # Step 1: Connect to IB and retrieve market data/entry price
        contract = {"symbol": web_request.ticker, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract)
        logger.info(f"New Order check for {web_request.ticker}")
        # Step 2: Calculate position details
        durationStr='35 D'
        barSizeSetting='1 day'
        ohlc4, hlc3, hl2, open_prices, high, low, close, vol = await get_ohlc_vol(ib_contract,  durationStr, barSizeSetting, return_df=False)
        df, orderEntry =await asyncio.gather(
            daily_volatility(ib_manager.ib, contract, high, low, close, stopType),
            ib_price_data.create_order(order, ib_contract, False, meanReversion, stopLoss)
        )
        
        logger.info(f"Order Check for {web_request.ticker}: {orderEntry}")
       
        
    except Exception as e:
        logger.error(f"Error checking order: {e}")
        raise HTTPException(status_code=500, detail="Error checking order")
@app.post("/tv_close")
async def tv_close(ticker: TickerSub):
    try:
        qty = None
        action =None
        symbol= ticker.ticker
        logger.info(f"Closing position for {symbol}")
        # This endpoint is used for closing positions (triggered by webhook alerts)
        contract_data = {"symbol": symbol, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract_data) 
        
      
        
        trade=await ib_price_data.create_close_order(ib_contract, action=None, qty=None)
        if trade:
            return JSONResponse(content={"status": "success", "message": f"Close order placed for {symbol}", "trade": trade})
        else:
            return JSONResponse(content={"status": "no action", "message": f"No active position found for {symbol}"})
    except Exception as e:
        logger.error(f"Error closing position: {e}")
        raise HTTPException(status_code=500, detail=str(e))
       
 
@app.post("/backtest")
async def backtest_strategy(request: Request):
    try:
        body = await request.json()
        symbol = body.get("symbol", "").upper()
        strat_type = body.get("strat_type", "").upper()
        bar_size = body.get("barSize", "1 min")
        window_minutes = int(body.get("windowMinutes", 30))
        end_time_str = body.get("endTime")

        if not symbol:
            return JSONResponse(status_code=400, content={"error": "Missing 'symbol'"})

        # IB bar size limits
        max_duration_by_bar_size = {
            "15 secs": 2 * 24 * 60,
            "30 secs": 3 * 24 * 60,
            "1 min": 7 * 24 * 60,
            "5 mins": 30 * 24 * 60,
            "15 mins": 60 * 24 * 60,
            "30 mins": 120 * 24 * 60,
            "1 hour": 180 * 24 * 60,
            "1 day": 365 * 24 * 60,
        }

        max_minutes = max_duration_by_bar_size.get(bar_size.lower(), 60)
        if window_minutes > max_minutes:
            logger.warning(f"Requested window {window_minutes}m exceeds IB limit for '{bar_size}', clipping to {max_minutes}m.")
            window_minutes = max_minutes

        end_time = pd.to_datetime(end_time_str) if end_time_str else datetime.datetime.now()
        end_time = end_time.tz_localize("UTC")
        duration_str = f"{window_minutes} M"

        logger.info(f"Backtest request | Symbol: {symbol} | Bar Size: {bar_size} | Duration: {duration_str} | End Time: {end_time}")

        
        contract = {"symbol": symbol, "exchange": "SMART", "secType": "STK", "currency": "USD"}
        ib_contract = Contract(**contract)
        qualified = await ib_manager.ib.qualifyContractsAsync(ib_contract)
        if not qualified:
            logger.error(f"Contract qualification failed for {symbol}")
            return None
        # Use the qualified contract instance directly
        qualified_contract = qualified[0]

        bars = await ib_manager.ib.reqHistoricalDataAsync(
            contract=qualified_contract,
            endDateTime='',
            durationStr='1 D',
            barSizeSetting=bar_size,
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )

        if not bars:
            logger.error(f"No historical data returned for {symbol}")
            return JSONResponse(status_code=404, content={"error": "No historical data returned"})

        df = pd.DataFrame([{
            'datetime': bar.date,
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        } for bar in bars])
        df.set_index('datetime', inplace=True)
        strategyName = HammerBacktest if strat_type == 'hammer' else TVOpeningVolatilityScan

        data = bt.feeds.PandasData(dataname=df)
        cerebro = bt.Cerebro()
        cerebro.adddata(data)
        cerebro.addstrategy(strategyName)
        cerebro.run()

        return JSONResponse(content={
            "status": "success",
            "message": f"Backtest completed for {symbol} over last {window_minutes} minutes at '{bar_size}' bars."
        })

    except Exception as e:
        logger.error(f"Exception in backtest_strategy: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

    
if __name__ == "__main__":
    log_config.setup()
    uvicorn.run("app:app", host="localhost", port=5001, log_level="info")
