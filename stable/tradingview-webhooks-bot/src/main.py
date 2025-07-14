
# app.py	
from collections import defaultdict, deque

from typing import *
import os, asyncio
from dotenv import load_dotenv
import threading
import signal
from threading import Lock
from datetime import datetime

from fastapi import FastAPI, Depends, HTTPException, Request, BackgroundTasks, Security, Query
from fastapi.security.api_key import APIKeyHeader, APIKey
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from matplotlib.pyplot import bar
import uvicorn
from fast_app.src.polygon_src.main_wsh_rvol import start_rvol
from ib_async.util import isNan
from ib_async import *






from fast_app.src.models import (
    OrderRequest,
    AccountPnL,
    PriceSnapshot,
   
   
   )
from fast_app.src.app_ib_conn import IBConnection
from fast_app.src.pnl import  IBManager
from fast_app.src.pnl_close import update_pnl
from fast_app.src.tv_ticker_price_data import  tv_store_data, price_data_dict, daily_volatility, yesterday_close, timeframe_dict, order_placed, subscribed_contracts, ib_open_orders, parent_ids, yesterday_close_bar, agg_bars,barSizeSetting_dict
from fast_app.src.account_values import account_metrics_store
# from pnl import IBManager 
account_pnl: dict[str, dict[int, PnL]] = defaultdict(dict)
from log_config import log_config, logger
from fast_app.src.ticker_list import ticker_manager
from fast_app.src.check_orders import  process_web_order_check, ck_bk_ib
from fast_app.src.bar_size import convert_pine_timeframe_to_barsize
from fast_app.src.stop_order import stop_loss_order, market_stop_loss_order, market_bracket_order, limit_bracket_order
from tech_a import gpt_data_poly, get_ib_pivots, ema_check
from my_util import  clean_nan, ticker_to_dict,  is_market_hours, delete_orders_db, format_order_details_table
from ib_db import order_db
from fill_data import fill_data

#################################
# New Imports
from helpers.resolve_contract import resolve_contract, add_new_contract
from helpers.choose_action import choose_action
from helpers.derive_stop_loss import derive_stop_loss
from helpers.risk import compute_risk_position_size
from helpers.ib_stop_order import ib_stop_order, new_trade_update_asyncio
from helpers.zapier import zapier_relay
from gpt.gpt import gpt_data
####################################
from ib_bracket import  bk_ib

reqId = {}




api_key = os.getenv("POLYGON_API_KEY")
from polygon import RESTClient

from polygon.rest.models import TickerDetails
from polygon.rest.reference  import *
from polygon.exceptions import BadResponse
API_KEY   = os.getenv("POLYGON_API_KEY")
poly_bad_response = BadResponse()
poly_client = RESTClient(api_key)
# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), ".env")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))

shutting_down = False


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



# --- Configuration ---
PORT = int(os.getenv("FAST_API_PORT", "5011"))
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv("FAST_API_CLIENT_ID", "2222"))
ib_host = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
ib_port = int(os.getenv("TBOT_IBKR_PORT", "4002"))

ib_connection = IBConnection(None)
ib= ib_connection.ib
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

# main.py
import os
import uvicorn
import logging
from contextlib import asynccontextmanager
import signal
from dotenv import load_dotenv
import threading
from log_config import log_config
import tbot
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates

from commons import VERSION_NUMBER, LOG_LOCATION
from components.actions.base.action import am
from components.events.base.event import em
from components.logs.log_event import LogEvent
from components.schemas.trading import Order, Position
from utils.log import get_logger
from utils.register import register_action, register_event, register_link
from settings import REGISTERED_ACTIONS, REGISTERED_EVENTS, REGISTERED_LINKS
# Set the default path to the .env file in the user's home directory
log_config.setup()
logger = get_logger(__name__)
# Load the environment variables from the chosen .env file
HOST = os.getenv("FAST_API_HOST", "127.0.0.1")
boof = os.getenv("HELLO_MSG")
logger.info(f"Hello {boof} from FastAPI!")

# ───── paths / templates ───────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates   = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))

# ───── logging ────────────────────────────────────────────────────────────────

# ───── registration ───────────────────────────────────────────────────────────
registered_actions = [register_action(a) for a in REGISTERED_ACTIONS]
registered_events  = [register_event(e) for e in REGISTERED_EVENTS]
registered_links   = [register_link(l, em, am) for l in REGISTERED_LINKS]

schema_list = {"order": Order().as_json(), "position": Position().as_json()}

# ───── lifespan (startup / shutdown) ──────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting TBOT API…")
    # (e.g.) open DB connection / preload caches / warm-up
    yield
    logger.info("Shutting down TBOT API…")
    tbot.close_connection(None)

# ───── app instance ───────────────────────────────────────────────────────────
app = FastAPI(lifespan=lifespan)

# static files
app.mount("/static", StaticFiles(directory=os.path.join(CURRENT_DIR, "static")), name="static")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://nyc.porenta.us",
        "https://zt.porenta.us",
        "https://porenta.us",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ───── routes mapped to existing tbot views ───────────────────────────────────
# These should match exactly with Flask app.add_url_rule() calls

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    tbot.set_request_context(request)
    return tbot.get_main()

@app.get("/orders", response_class=HTMLResponse)
def orders(request: Request):
    tbot.set_request_context(request)
    return tbot.get_orders()

@app.get("/alerts", response_class=HTMLResponse)
def alerts(request: Request):
    tbot.set_request_context(request)
    return tbot.get_alerts()

@app.get("/ngrok")
def ngrok():
    return tbot.get_ngrok()

@app.get("/errors", response_class=HTMLResponse)
def errors(request: Request):
    tbot.set_request_context(request)
    return tbot.get_errors()

@app.get("/tbot", response_class=HTMLResponse)
def tbot_page(request: Request):
    tbot.set_request_context(request)
    return tbot.get_tbot()

# data endpoints (JSON) - no request context needed
@app.get("/orders/data")
def orders_data():
    return tbot.get_orders_data()

@app.get("/alerts/data")
def alerts_data():
    return tbot.get_alerts_data()

@app.get("/errors/data")
def errors_data():
    return tbot.get_errors_data()

@app.get("/tbot/data")
def tbot_data():
    return tbot.get_tbot_data()


# ───── custom endpoints previously in Flask ───────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, guiKey: str | None = None):
    try:
        with open(".gui_key") as f:
            if f.read().strip() != guiKey:
                return PlainTextResponse("Access Denied", status_code=401)
    except FileNotFoundError:
        logger.warning("GUI key file not found. Open GUI mode detected.")

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "schema_list": schema_list,
            "action_list": am.get_all(),
            "event_list": registered_events,
            "version": VERSION_NUMBER,
        },
    )

@app.post("/webhook")
async def webhook(request: Request):
    payload = await request.json()
    if not payload or "key" not in payload:
        logger.warning(f"Bad payload: {payload}")
        return Response(status_code=415)

    logger.debug(f"Request Data: {payload}")
    fired = []
    for e in em.get_all():
        if e.webhook and e.key == payload["key"]:
            logger.info(f"Triggered events: and e.key {e.key} ")
            e.trigger(data=payload)
            fired.append(e.name)

    if not fired:
        logger.warning(f"No events triggered for webhook request {payload}")
    else:
        logger.info(f"Triggered events: {fired}")
        logger.info(f"client IP: {request.client.host}")
        ip = request.headers.get("X-Forwarded-For", request.client.host)
        logger.info(f"client IP: {ip}")


    return Response(status_code=200)
@app.get("/fast_dashboard", response_class=HTMLResponse)
async def fast_dashboard(request: Request):
    try:
        return templates.TemplateResponse("fast_api_dashboard.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/logs")
def get_logs():
    with open(LOG_LOCATION) as f:
        logs = [LogEvent().from_line(line).as_json() for line in f]
    return logs

@app.post("/event/active")
def activate_event(event: str | None = None, active: str = "true"):
    if not event:
        raise HTTPException(status_code=404, detail="Event name cannot be empty")
    try:
        evt = em.get(event)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Cannot find event '{event}'")

    evt.active = (active == "true")
    logger.info(f"Event {evt.name} active set to {evt.active}")
    return {"active": evt.active}
@app.get("/gpt_poly/{symbol}/" )
async def gpt_prices(symbol: str):
    try:
        logger.info(f"Got symbol: {symbol} from gpt")

        barSizeSetting = "1 min"  # default bar size setting

        contract: Contract = None
        contract,snapshot = await add_new_contract(symbol, barSizeSetting, ib)

        # Check if the contract was successfully created
        if not contract:
            logger.error(f"Failed to create contract for {symbol}")
            return JSONResponse(content={"error": f"Failed to create contract for {symbol}"}, status_code=400)

        # --- inside gpt_prices() -----------------------------------------------
        analysis = await gpt_data(contract, ib, polyData=False)


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


 



async def process_close_all(contract: Contract, req: OrderRequest, barSizeSetting: str, snapshot: PriceSnapshot = None) -> JSONResponse   :
    try:
        symbol = contract.symbol 
        snapshot = await price_data_dict.get_snapshot(symbol)
        
        logger.info(f"Processing webhook request for {contract.symbol} ")
        totalQuantity = 0.0
        trade = []
        order = []
        action = None
        limit_price = 0.0

        logger.info(
            f"New Order close_all request for {contract.symbol} with jengo and uptrend"
        )
        
        
        logger.info(
                    f"snapshot.ask: {snapshot.ask}, snapshot.bid: {snapshot.bid}, snapshot.markPrice: {snapshot.markPrice}"
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
                logger.debug(f"No position for {pos.contract.symbol}, skipping...")
                continue

            if pos.contract.symbol == symbol:
                placeOrder=True
                
                logger.info(
                    f"Position: {pos.contract.symbol} - {pos.position} shares at {pos.avgCost} and snapshot.ask: {snapshot.ask}, snapshot.bid: {snapshot.bid}, snapshot.markPrice: {snapshot.markPrice}"
                )
                action = "SELL" if pos.position > 0 else "BUY"
                totalQuantity = abs(pos.position)
                if is_market_hours:
                    logger.info(
                        f"Market is open, closing position for {contract.symbol} with action {action} and quantity {totalQuantity}"
                    )
                    order = MarketOrder(action, totalQuantity)
                if not is_market_hours():
                    
                    
                    logger.info(f"Is not market hours, using limit order for {contract.symbol} with action {action} and quantity {totalQuantity}")
                    if req.limit_price is  None or req.limit_price == 0.0 or isNan(req.limit_price):
                        limit_price = snapshot.markPrice if snapshot.markPrice else snapshot.ask if action == "BUY" else snapshot.bid
                        if limit_price <= 2.0 or limit_price is None:
                            logger.warning(f"Limit price for {contract.symbol} is None or 0, using default value of 1.0")
                            limit_price = 1.0

                    limit_price = req.limit_price
                    if limit_price <= 2.0 or limit_price is None:
                        logger.warning(f"Limit price for {contract.symbol} is None or 0, using default value of 1.0")
                        limit_price = snapshot.markPrice
                    
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
                "symbol": contract.symbol,
                "orderAction": action,
                "limit_price": round(limit_price, 2),
                
            
                "quantity": totalQuantity,
                "rewardRiskRatio": symbol.rewardRiskRatio,
                "riskPercentage": symbol.riskPercentage,
                "accountBalance": symbol.accountBalance,
                "stopType": symbol.stopType,
                "atrFactor": symbol.atrFactor,
            }
            logger.info(format_order_details_table(order_json))
            web_request_json =jsonable_encoder(order_json)
            await order_db.insert_order(trade)
            await asyncio.sleep(1)

            logger.info(
                f"Order Entry for {contract.symbol} successfully placed with order"
            )
            return JSONResponse(content={"status": "success"})
        else:
            logger.info(
                f"No position found for {contract.symbol} in IB. Cannot close position."
            )
            return JSONResponse(
                content={
                    "status": f"No position found for {contract.symbol} in IB. Cannot close position."
                }
            )
        
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")

@app.get("/symbols/")
async def get_all_tickers():
    cur = ticker_manager.conn.cursor()
    cur.execute("SELECT * FROM tickers")
    rows = cur.fetchall()
    tickers = [dict(row) for row in rows]
    return JSONResponse(content=tickers)

@app.get("/active-symbols")
async def list_active_tickers():
    try:
        tickers_list: list[Ticker] = ib.tickers()
        result = []

        for req in tickers_list:
            tckDict = await ticker_to_dict(req)
            cleaned = clean_nan(tckDict)
            result.append(cleaned)

        logger.debug(f"Active tickers: {result}")
        return JSONResponse(content={"status": "success", "tickers": result})
    except Exception as e:
        logger.error(f"Error retrieving tickers: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})

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
        await ib_connection.on_pnl_event(account_pnl)
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

        #logger.debug(f"Got them for jengo - get_current_pnl {jsonable_encoder(data)}")
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error fetching pnl data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/tv-post-data")
async def tv_post_data(req: OrderRequest, background_tasks: BackgroundTasks):
    try:
        logger.info(f"Received TV post data for req: {req.symbol} with takeProfitBool: {req.takeProfitBool}")
        symbol = req.symbol
        logger.debug(f"Received TV post data for req: {symbol} unixtime {req.unixtime} req.symbol {req.symbol} req.barSizeSetting_tv {req.barSizeSetting_tv}")
        req_dict=await tv_store_data.set(symbol, req, tv_hook=True)
        barSizeSetting =barSizeSetting_dict[symbol] 
        logger.debug(f"Received TV post data for req: {symbol} barSizeSetting {barSizeSetting} from req_dict {req_dict}")
        stop_loss = req.stop_loss
        takeProfitBool = req.takeProfitBool
        logger.debug(f"Received TV post data for req: {req.symbol} with takeProfitBool: {takeProfitBool}")
        
        logger.debug(f"Received TV post data for req: {symbol} unixtime {req.unixtime} req.symbol {req.symbol} req.barSizeSetting_tv {req.barSizeSetting_tv}")
        snapshot = None
        contract: Contract = None
         # ---- 1. Resolve contract + snapshot ------------------------------
        contract, snapshot = await resolve_contract(req, barSizeSetting, ib,tv_hook=True)
        logger.debug(f"Resolved contract: {contract.symbol} with barSizeSetting: {barSizeSetting}") 
        logger.debug(f"No stop_loss provided in request for {symbol}, deriving stop_loss...")
        stop_loss_data = await derive_stop_loss(req, contract, barSizeSetting, snapshot,ib)
        logger.debug(f"Derived stop_loss data for {symbol}: {stop_loss_data}")
        stop_loss, uptrend =stop_loss_data
        logger.debug(f"Derived stop_loss for {symbol} is {stop_loss} with uptrend {uptrend}")
        action = req.orderAction
        orderPlace = await  order_placed.get(symbol)
        logger.debug(f"Order already placed for {symbol}: {orderPlace}")
        if req.limit_price == 0  or isNan(req.limit_price) or req.orderAction not in ["BUY", "SELL", "close_all"]:
                logger.debug(f"No limit_price provided in request for {symbol}, using market price...")
                response = {
                    "status": "success",
                    "message": f"Subscribed to ticker boof3 {symbol} for vStop calculation"
                    
                }
                logger.debug(f"No limit_price provided in request for {symbol}, using market price...")
           
                return JSONResponse(content=response, status_code=200)
        
        
        if action == "close_all":
            logger.debug(f"Received close_all action for {symbol}, processing...")
          
            asyncio.create_task(process_close_all(contract, req, barSizeSetting, snapshot))
            return JSONResponse(content="", status_code=200)
        

        logger.debug(f"Processing TV post data for {symbol} with action {action} and limit_price {req.limit_price}")

       
        snapshot= await price_data_dict.get_snapshot(symbol)
        
        
        if orderPlace:
            logger.info(f"Order already placed for {symbol}, skipping processing.")
            return JSONResponse(content="OK", status_code=200)
        logger.debug(f"Received TV post data for req: {symbol} unixtime {req.unixtime} ")

        
        if req.limit_price != 0 and req.orderAction in ["BUY", "SELL"]:
            logger.info(f"Processing TV post data for {symbol} with limit_price {req.limit_price} and orderAction {req.orderAction}")
            # vStop, ATR, etc.
        
            logger.info(f"stop_loss for {contract.symbol} is {stop_loss} with action {action} and req.stop_loss {req.stop_loss} and req.vstopAtrFactor {req.vstopAtrFactor}")   
            quantity, limit_price, take_profit_price = (await compute_risk_position_size(
                contract,
                stop_loss,
                req.accountBalance,
                req.riskPercentage,
                req.rewardRiskRatio,
                action,
                req,
                snapshot,
                ib
            ))
        
            brokerComish = max(round(quantity * 0.005), 1.0)
            toleratedRisk = abs((req.riskPercentage / 100 * req.accountBalance) - (max(round((req.accountBalance / limit_price) * 0.005), 1.0)))
            perShareRisk = abs(limit_price - stop_loss)
            logger.debug(f"brokerComish: {brokerComish}, perShareRisk: {perShareRisk}, toleratedRisk: {toleratedRisk}")

            # ---- Build the order object -----------------------------------
        
            trade = await ib_stop_order(contract, req, action, quantity, limit_price, stop_loss, snapshot, barSizeSetting, req.takeProfitBool, ib)

            # ---- Submit & persist -----------------------------------------
            if trade is None:
                logger.error(f"Trade could not be placed for {contract.symbol} with action {action} and quantity {quantity}")
                raise HTTPException(status_code=500, detail="Trade could not be placed")
            logger.debug(f"Trade placed for {contract.symbol} with action {action} and quantity {quantity}")
            orderPlace=await order_placed.delete(contract.symbol)
            logger.debug(f"Order placed for {contract.symbol} with action {action} and quantity {quantity}, orderPlace status: {orderPlace}")
            await zapier_relay(req, snapshot)
            logger.debug(f"Zapier relay for {symbol} completed successfully")
            fill=await fill_data.get_trade(symbol)
            logger.info(f"fill data for {symbol} processed successfully: {fill}")
            
            ib.cancelMktData(contract)
            order_json = {
                "req": contract.symbol,
                "orderAction": action,
                "limit_price": round(limit_price, 2),
                "stop_loss": round(stop_loss,2),
                "take_profit_price": take_profit_price,
                "brokerComish": brokerComish,   
                "perShareRisk": perShareRisk,
                "toleratedRisk": toleratedRisk,
                "req.limit_price": req.limit_price,
                "req.stop_loss": req.stop_loss,
                "uptrend": snapshot.uptrend,
                    
                    
                "quantity": quantity,
                "rewardRiskRatio": req.rewardRiskRatio,
                "riskPercentage": req.riskPercentage,
                "accountBalance": req.accountBalance,
                "stopType": req.stopType,
                "atrFactor": req.atrFactor,
            }
            web_request_json = jsonable_encoder(order_json)
                
            #logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss {stop_loss} and web_request_json: {web_request_json} with order_json: {order_json}")
            logger.info(format_order_details_table(order_json))

            # ---- Respond ---------------------------------------------------
            return JSONResponse({
                "web_request_json": web_request_json,
                "status":   "placed",
                "symbol":   contract.symbol,
                "action":   action,
                "qty":      quantity,
                "entry":    snapshot.markPrice,
                "stopLoss": stop_loss,
                "orderId":  trade.order.orderId if trade and trade.order else None
            })
          
            
           
            
    except Exception as e:
        logger.error(f"Error processing TV post data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
ib_instance: Dict[str, IB] = defaultdict(IB)     
ib_instance["ib"] = ib
@app.post("/webapp-post-data")
async def webapp_post_data(req: OrderRequest, background_tasks: BackgroundTasks):
    try:
        ib_=ib_instance["ib"]
      
        orderPlace = await order_placed.get(req.symbol)
        logger.info(f"Received webapp post data for ib_: {ib_} with ib_connection.ib: {ib_connection.ib}")
       
        
        if orderPlace:
            logger.info(f"Order already placed for {req.symbol}, skipping processing.")
            return JSONResponse(content="OK", status_code=200)
        await tv_store_data.set(req.symbol, req)
        barSizeSetting =barSizeSetting_dict[req.symbol]
        contract, snapshot = await resolve_contract(req, barSizeSetting, ib_)
        
        new_response_json=jsonable_encoder(snapshot)
       
        snapshot_json_cleaned = clean_nan(new_response_json)
       
        if snapshot.hasBidAsk:
            
            return JSONResponse(content=snapshot_json_cleaned, status_code=200)

        else:
            
            return JSONResponse(content=snapshot_json_cleaned, status_code=200)


    except Exception as e:
        logger.error(f"Error processing TV post data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/place_order")
async def place_order(req: OrderRequest):
    try:
        
        await tv_store_data.set(req.symbol, req)

        
        # ---- 1. Resolve contract + snapshot ------------------------------
        contract, snapshot = await resolve_contract(req, ib)
        barSizeSetting = barSizeSetting_dict.get(contract.symbol)  # default to 15 seconds if not found
        logger.debug(f"Resolved contract: {contract.symbol} with barSizeSetting: {barSizeSetting}")

        # ---- 2. Stop-loss (vStop) & qty ----------------------------------

        stop_loss_data = await derive_stop_loss(req, contract, barSizeSetting, snapshot,ib)
        stop_loss, uptrend =stop_loss_data

        # ---- 3. Trend / direction logic ----------------------------------
        action = await choose_action(req, contract,snapshot,uptrend, barSizeSetting, ib)              # returns "BUY" or "SELL"

    
        # vStop, ATR, etc.
        
        logger.info(f"stop_loss for {contract.symbol} is {stop_loss} with action {action} and req.stop_loss {req.stop_loss} and req.vstopAtrFactor {req.vstopAtrFactor}")   
        quantity, limit_price, take_profit_price = (await compute_risk_position_size(
            contract,
            stop_loss,
            req.accountBalance,
            req.riskPercentage,
            req.rewardRiskRatio,
            action,
            req,
            snapshot,
            ib
        ))
        
        brokerComish = max(round(quantity * 0.005), 1.0)
        toleratedRisk = abs((req.riskPercentage / 100 * req.accountBalance) - (max(round((req.accountBalance / limit_price) * 0.005), 1.0)))
        perShareRisk = abs(limit_price - stop_loss)

        # ---- 4. Build the order object -----------------------------------
        
        trade = await ib_stop_order(contract, req, action, quantity, limit_price, stop_loss, snapshot, barSizeSetting, req.takeProfitBool, ib)

        # ---- 5. Submit & persist -----------------------------------------
        if trade is None:
            logger.error(f"Trade could not be placed for {contract.symbol} with action {action} and quantity {quantity}")
            raise HTTPException(status_code=500, detail="Trade could not be placed")
        orderPlace=await order_placed.delete(contract.symbol)
            
        ib.cancelMktData(contract)
        order_json = {
            "req": contract.symbol,
            "orderAction": action,
            "limit_price": round(limit_price, 2),
            "stop_loss": round(stop_loss,2),
            "take_profit_price": take_profit_price,
            "brokerComish": brokerComish,   
            "perShareRisk": perShareRisk,
            "toleratedRisk": toleratedRisk,
            "req.limit_price": req.limit_price,
            "req.stop_loss": req.stop_loss,
            "uptrend": snapshot.uptrend,
                    
                    
            "quantity": quantity,
            "rewardRiskRatio": req.rewardRiskRatio,
            "riskPercentage": req.riskPercentage,
            "accountBalance": req.accountBalance,
            "stopType": req.stopType,
            "atrFactor": req.atrFactor,
        }
        web_request_json = jsonable_encoder(order_json)
                
        #logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss {stop_loss} and web_request_json: {web_request_json} with order_json: {order_json}")
        logger.info(format_order_details_table(order_json))

        # ---- 6. Respond ---------------------------------------------------
        return JSONResponse({
            "web_request_json": web_request_json,
            "status":   "placed",
            "symbol":   contract.symbol,
            "action":   action,
            "qty":      quantity,
            "entry":    snapshot.markPrice,
            "stopLoss": stop_loss,
            "orderId":  trade.order.orderId if trade and trade.order else None
        })

    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")
# ───── entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log_config.setup()
    port = 5000
    logger.info(f"1 Starting FastAPI with log level 'info' and HOST={HOST} and port={port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
