
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
from polygon_src.main_wsh_rvol import start_rvol
from ib_async.util import isNan
from ib_async import *





ib = IB()
from models import (
    OrderRequest,
    AccountPnL,
    PriceSnapshot,
   
   
   
)
from pnl import  IBManager
from add_contract import add_new_contract
from pnl_close import update_pnl
from tv_ticker_price_data import  tv_store_data, price_data_dict, daily_volatility, yesterday_close, timeframe_dict, order_placed, subscribed_contracts, ib_open_orders, parent_ids, yesterday_close_bar, agg_bars,barSizeSetting_dict
from account_values import account_metrics_store
# from pnl import IBManager 
account_pnl: dict[str, dict[int, PnL]] = defaultdict(dict)
from log_config import log_config, logger
from ticker_list import ticker_manager
from check_orders import  process_web_order_check, ck_bk_ib
from bar_size import convert_pine_timeframe_to_barsize
from stop_order import stop_loss_order, market_stop_loss_order, market_bracket_order, limit_bracket_order
from tech_a import gpt_data_poly, get_ib_pivots, ema_check
from my_util import  clean_nan, ticker_to_dict,  is_market_hours, delete_orders_db, format_order_details_table
from ib_db import order_db
from fill_data import fill_data

#################################
# New Imports
from helpers.resolve_contract import resolve_contract
from helpers.choose_action import choose_action
from helpers.derive_stop_loss import derive_stop_loss
from helpers.risk import compute_risk_position_size
from helpers.ib_stop_order import ib_stop_order, new_trade_update_asyncio
from helpers.zapier import zapier_relay
####################################
from ib_bracket import  bk_ib

reqId = {}




boof = os.getenv("HELLO_MSG")
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
        "/active-tickers",
        "/check_order",
        "/place_order_old",
        "/place_order",
        "/webapp-post-data",
        "/tickers"
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
            barSizeSetting= barSizeSetting_dict[symbol]
            if not barSizeSetting:
                barSizeSetting = "15 secs"  # default to 15 seconds if not found
            
            
            await price_data_dict.add_ticker(new_ticker, barSizeSetting, False)
            #logger.debug(f" symbol symbol is {symbol}; for pending tickers barSizeSetting is {barSizeSetting}")
            
            
            


        #logger.debug(f" symbol symbol is {symbol}; for pending tickers barSizeSetting is {barSizeSetting}")
        

    except Exception as e:
        logger.error(f"Error in on_pending_tickers: {e}")
        raise e



#=================================FastAPI Setup=================================   
# 
# This FastAPI application connects to Interactive Brokers (IB) and manages trading operations.
# It handles real-time bar updates, order placements, and position management.
#   
#===============================  
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_dotenv()
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
        if is_market_hours():
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
        if is_market_hours():
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
async def error_code_handler(
    reqId: int, errorCode: int, errorString: str, contract: Contract
):
    logger.warning(f"Error for reqId {reqId}: {errorCode} - {errorString}")



   
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
       
@app.post("/webapp-post-data")
async def webapp_post_data(req: OrderRequest, background_tasks: BackgroundTasks):
    try:
        orderPlace = await order_placed.get(req.symbol)
        
        if orderPlace:
            logger.info(f"Order already placed for {req.symbol}, skipping processing.")
            return JSONResponse(content="OK", status_code=200)
        await tv_store_data.set(req.symbol, req)
        barSizeSetting =barSizeSetting_dict[req.symbol]
        contract, snapshot = await resolve_contract(req, ib)
        
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
async def place_order_new(req: OrderRequest):
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
    
if __name__ == "__main__":
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

    uvicorn.run("app_dev:app", host=HOST, port=PORT, log_level="info")
