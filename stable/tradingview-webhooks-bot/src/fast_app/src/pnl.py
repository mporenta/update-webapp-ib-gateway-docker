from asyncio import tasks
from collections import defaultdict, deque

from httpx import delete, get

from typing import *
import os, asyncio, time
import re
import threading
import signal
from threading import Lock
from datetime import datetime, timedelta, timezone
import random
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
from pandas_ta.overlap import ema
from pandas_ta.volatility import atr, true_range
import json
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
from ib_db import (
    IBDBHandler,
    order_status_handler
)

from models import (
    OrderRequest,
    AccountPnL,
    PriceSnapshot,
    
    VolatilityStopData
    
)

from tv_ticker_price_data import volatility_stop_data, tv_store_data, price_data_dict, daily_volatility, yesterday_close_bar, yesterday_close, daily_true_range, timeframe_dict, barSizeSetting_dict, order_placed, subscribed_contracts, ib_open_orders, reconcile_open_orders, parent_ids
from rt_bar import SimulatedIB
ib_sim = SimulatedIB()
import numpy as np
from log_config import log_config, logger
from bar_size import convert_pine_timeframe_to_barsize

from my_util import get_timestamp, get_tv_ticker_dict, get_dynamic_atr,is_market_hours, last_daily_bar_dict, compute_volatility_series, format_order_details_table, vol_stop

from super import supertrend, supertrend_last


reqId = {}

order_db = IBDBHandler("orders.db")
# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), ".env")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))

shutting_down = False
price_data_lock = Lock()
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

# --- Configuration ---
PORT = int(os.getenv("FAST_API_PORT", "5011"))
HOST = os.getenv("FAST_API_HOST", "127.0.0.1")
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv("FAST_API_CLIENT_ID", "2222"))
ib_host = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
ib_port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
boofMsg = os.getenv("HELLO_MSG")




async def generate_random_int(min_value: int = 2, max_value: int = 1_000_000, exclude: int = 1111) -> int:
    """
    Generate a random integer between min_value and max_value (inclusive),
    but never return the exclude value.
    """
    while True:
        n = random.randint(min_value, max_value)
        if n != exclude:
            print(f"Generated random int: {n}")
            return n




class IBManager:
    def __init__(self, ib=None):
        if ib is None:
            ib = IB()

        # self.db_handler = db_handler  # Use the global db_handler instance
        self.ib = ib
        # self.ib_vol_data = PriceDataNew()
        self.client_id = None
        self.host = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
        self.port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
        self.risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300.00"))
        self.max_attempts = 300
        self.initial_delay = 1
        self.account = None
        self.shutting_down = False

        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        self.all_trades = {}
        self.orders_dict: Dict[str, Trade] = {}
        self.orders_dict_test: Dict[str, Trade] = defaultdict(dict)

        self.orders_dict_oca_id: Dict[str, Trade] = {}
        self.orders_dict_oca_symbol: Dict[str, Trade] = {}
        self.account_pnl: AccountPnL = None
        self.semaphore = asyncio.Semaphore(10)
        self.wrapper = Wrapper(self)
        self.client = Client(self.wrapper)
        
        self.net_liquidation = {}
        self.settled_cash = {}
        self.buying_power = {}
        self.order_db = order_db

    async def connect(self) -> bool:
        attempt = 0
        while attempt < self.max_attempts:
            try:
                if self.client_id is None:
                    
                    self.client_id = int(await generate_random_int())
                    logger.info(f"Generating random client_id..{self.client_id}")
                logger.info(
                    f"Connecting to IB at {self.host}:{self.port} : client_id:{self.client_id}...with self.risk_amount {self.risk_amount}"
                )
                await self.ib.connectAsync(
                    host=self.host, port=self.port, clientId=self.client_id, timeout=40
                )
                if self.ib.isConnected():
                    self.ib.errorEvent += self.error_code_handler
                    logger.info("Connected to IB Gateway")

                    logger.info("Connecting to Database.")
                    asyncio.create_task(self.subscribe_events())

                   
                    #await self.subscribe_events()
                    # self.ib.disconnectedEvent += self.on_disconnected
                    
                   
                    return True
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {e}")
            attempt += 1
            await asyncio.sleep(self.initial_delay)
        logger.error("Max reconnection attempts reached")
        return False

    async def subscribe_events(self):
        try:
            await self.order_db.connect()
            logger.info(
                "Order database connection established, subscribing to events."
            )
            logger.info("Jengo getting to IB data...")
            self.account = (
                self.ib.managedAccounts()[0] if self.ib.managedAccounts() else None
            )
            if not self.account:
                return
            self.ib.reqPnL(self.account)
            logger.info(f"Jengo getting to reqPnL data... {self.account}")
            
           
            
           
            # Subscribe to key IB events. (Assumes event attributes exist.)
            

           
            self.ib.disconnectedEvent += self.on_disconnected

           
            logger.info("Subscribed to IB events")
           
            await asyncio.sleep(1)  # Allow time for initial data to be processed
            
            positions = await self.ib.reqPositionsAsync()
            
        except Exception as e:
            logger.error(f"Error subscribing to events: {e}")

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
        except Exception as e:

            logger.error(f"Error in on_disconnected: {e}")
            # Attempt to reconnect if not shutting down

  

    async def init_get_pnl_event(self, pnl: PnL):
        """Handle aggregate PnL updates"""
        if not self.ib.isConnected():
            return
        try:
            await asyncio.sleep(1)
            self.account = (
                self.ib.managedAccounts()[0] if self.ib.managedAccounts() else None
            )
            if not self.account:
                return

            positions: Position = []
            trades: Trade = self.ib.openOrders()
            if trades:
                for trade in trades:
                    await ib_open_orders.set(trade.contract.symbol,  trade)
                    

            
            open_orders_list, positions, account_values = await asyncio.gather(
                self.ib.reqAllOpenOrdersAsync(),
                self.ib.reqPositionsAsync(),
                self.ib.accountSummaryAsync(),
            )
            
            for open_order in open_orders_list:
                

                self.orders_dict = {
                    order.contract.symbol: order for order in open_orders_list
                }
                symbol = open_order.contract.symbol
                self.open_orders[symbol] = {
                    "symbol": symbol,
                    "order_id": open_order.order.orderId,
                    "status": open_order.orderStatus.status,
                    "timestamp": await get_timestamp(),
                }
            logger.info("Getting initial account values")

            if account_values:
                for account_value in account_values:
                    if account_value.tag == "NetLiquidation":
                        self.net_liquidation = float(account_value.value)
                        logger.info(
                            f"NetLiquidation updated: ${self.net_liquidation:,.2f}"
                        )
                    elif account_value.tag == "SettledCash":
                        self.settled_cash = float(account_value.value)
                        logger.info(f"SettledCash updated: ${self.settled_cash:,.2f}")
                    elif account_value.tag == "BuyingPower":
                        self.buying_power = float(account_value.value)
                        logger.info(f"BuyingPower updated: ${self.buying_power:,.2f}")

            # Store PnL data
            logger.info(f"PnL data updated pnl arg: {pnl}")
            for pnl_val in pnl:
                if pnl_val.account == self.account:
                    logger.info(
                        f"PnL data updated for account {pnl_val.account}: {pnl_val}"
                    )
                self.account_pnl = AccountPnL(
                    unrealizedPnL=float(pnl_val.unrealizedPnL or 0.0),
                    realizedPnL=float(pnl_val.realizedPnL or 0.0),
                    dailyPnL=float(pnl_val.dailyPnL or 0.0),
                    
                )

            logger.debug(f"update pnl to self.account_pnl db: {self.account_pnl}")

            new_positions = {}
            for pos in positions:
                if pos.position != 0:
                    symbol = pos.contract.symbol
                    new_positions[symbol] = {
                        "symbol": symbol,
                        "position": pos.position,
                        "average_cost": pos.avgCost,
                        "timestamp": await get_timestamp(),
                    }

            self.current_positions = new_positions
            logger.debug(f"Positions updated: {list(self.current_positions.keys())}")
              # Allow time for PnL data to be fetched

            return (
                self.account_pnl,
                self.current_positions,
                self.net_liquidation,
                self.settled_cash,
                self.buying_power,
            )

        except Exception as e:
            logger.error(f"Error updating initial account values: {str(e)}")

  



    

    async def on_order_status_event(self, trade: Trade):
        try:
            logger.debug(f"Order status event received for {trade.contract.symbol}")
            await order_status_handler(self, self.order_db, trade)

            symbol = trade.contract.symbol
            self.open_orders[symbol] = {
                "symbol": symbol,
                "order_id": trade.order.orderId,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp(),
            }
            self.orders_dict[symbol] = (
                trade  # Update the orders_dict with the latest trade
            )
            portfolio_item = []

            # portfolio_item = self.ib.portfolio()

            # Synchronous call, no await needed fill.execution.avgPrice
            if portfolio_item:
                for pos in portfolio_item:
                    logger.debug(
                        f"Position for: {pos.contract.symbol} at shares of {pos.position}"
                    )
                    if pos.contract.symbol == symbol and pos.position == 0:
                        logger.debug(
                            f"Updating position for  == 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if pos.contract.symbol == symbol and pos.position != 0:
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if pos.contract.symbol == symbol:
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
            logger.debug(
                f"Order status updated for {symbol}: {trade.orderStatus.status}"
            )
        except Exception as e:
            logger.error(f"Error in order status event: {e}")
   
    async def on_pnl_event(self, pnl):
        if not self.ib.isConnected():
            return
        portfolio_item = []
        trades = []
        portfolio_item = await self.ib.reqPositionsAsync()
        for port_item in portfolio_item:
            
            if port_item.position == 0:
                logger.debug(
                    f"Position for: {port_item.contract.symbol} at shares of {port_item.position}"
                )
                return None  # Skip positions with zero shares
        trades = await self.ib.reqAllOpenOrdersAsync()
        for trade in trades:
            self.orders_dict = {order.contract.symbol: order for order in trades}
            symbol = trade.contract.symbol
            self.open_orders[symbol] = {
                "symbol": symbol,
                "order_id": trade.order.orderId,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp(),
            }
            # Synchronous call, no await needed fill.execution.avgPrice
            if portfolio_item:
                
                for pos in portfolio_item:
                    

                    logger.debug(
                        f"Position for: {pos.contract.symbol} at shares of {pos.position}"
                    )
                    if (
                        pos.contract.symbol == trade.contract.symbol
                        and pos.position == 0
                    ):
                        logger.debug(
                            f"Updating position for  == 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if (
                        pos.contract.symbol == trade.contract.symbol
                        and pos.position != 0
                    ):
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if pos.contract.symbol == trade.contract.symbol:
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
        logger.debug(f"PnL event received: {pnl}")
        logger.debug(f"self.account_pnl event received: {self.account_pnl}")

        if pnl:
            logger.debug(f" on pnl event for jengo Received PnL data: {pnl}")
            self.account_pnl = AccountPnL(
                unrealizedPnL=float(
                    pnl.unrealizedPnL or 0.0
                ),  # Changed attribute name
                realizedPnL=float(pnl.realizedPnL or 0.0),  # Changed attribute name
                dailyPnL=float(
                    pnl.dailyPnL or 0.0
                ),  # Changed attribute name if needed
                
            )

        trades, update_pnl = await asyncio.gather(
            self.ib.reqAllOpenOrdersAsync(),
            self.update_pnl(self.account_pnl),
        )

        for trade in trades:

            self.orders_dict = {order.contract.symbol: order for order in trades}

            symbol = trade.contract.symbol
            self.open_orders[symbol] = {
                "symbol": symbol,
                "order_id": trade.order.orderId,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp(),
            }
            logger.debug(
                f"Order status updated for {symbol}: {trade.orderStatus.status}"
            )

    async def update_pnl(self, pnl: AccountPnL):
        try:

            if pnl:
                logger.debug(f"2 PnL updated: {pnl}")
                self.account_pnl = AccountPnL(
                    unrealizedPnL=float(pnl.unrealizedPnL or 0.0),
                    realizedPnL=float(pnl.realizedPnL or 0.0),
                    dailyPnL=float(pnl.dailyPnL or 0.0),
                    
                )
                pnl_calc = self.account_pnl.dailyPnL - self.account_pnl.unrealizedPnL
            logger.debug(
                f"3 PnL updated: self.account_pnl.dailyPnL {self.account_pnl.dailyPnL} + self.unrealizedPnL {self.account_pnl.unrealizedPnL} = {pnl_calc}"
            )
            # Check risk threshold and trigger closing positions if needed.
            
            await self.check_pnl_threshold()
            return self.account_pnl
        except Exception as e:
            logger.error(f"Error updating PnL: {e}")

    async def check_pnl_threshold(self):
        contract: Contract = None
        symbol = None
        open_order_status = False
        qty = None
        action = None
        marketPrice = 0.0
        try:
            if self.account_pnl and self.account_pnl.dailyPnL <= self.risk_amount:
                logger.warning(
                    f"PnL {self.account_pnl.dailyPnL} below threshold {self.risk_amount}. Initiating position closure."
                )

            
                
                
                latest_positions = await self.ib.reqPositionsAsync()
                # Early exit if no positions to close
                if not latest_positions:
                    logger.debug("No positions found to close.")
                    return None
                for latest_position in latest_positions:
                    symbol = latest_position.contract.symbol
                    contract = latest_position.contract
                    action = "SELL" if latest_position.position > 0 else "BUY"
                    qty = abs(latest_position.position)
                    marketPrice = latest_position.marketPrice if hasattr(latest_position, 'marketPrice') else 0.0
                
                    logger.debug(
                        f"Latest position: {latest_position.contract.symbol} - {latest_position.position} shares at avg cost {latest_position.avgCost}"
                    )
                    if latest_position.position == 0:
                        logger.debug(
                            f"Skipping position {latest_position.contract.symbol} with zero shares."
                        )
                        await self.cancel_open_orders(latest_position)
                        return None  # Skip positions with zero shares
                if symbol is not None:
                    open_order_status=await reconcile_open_orders(symbol, self.ib)

                
                tasks = []
                if not open_order_status:
                    q_contract= await self.ib.qualifyContractsAsync(contract)

                    
                    
                    tasks.append(self.process_close_all_main(q_contract, action, qty, marketPrice))

                else:
                    logger.debug(
                        f"Skipping {symbol}: already has a pending order/trade."
                    )

                # Execute all close orders simultaneously
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    # Process results to check for errors
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            logger.error(f"Error executing close order: {result}")
                    logger.info(
                        f"Close orders have been placed for {len(tasks)} positions"
                    )
                else:
                    logger.info(
                        "No positions eligible for closure; pending orders/trades exist for all positions."
                    )
            else:
                logger.debug(
                    f"PnL {self.account_pnl.dailyPnL} is above threshold {self.risk_amount}. No action taken."
                )
                return None  # No action needed if PnL is above threshold
        except Exception as e:
            logger.error(f"Error in check_pnl_threshold: {e}", exc_info=True)

    async def process_close_positions_post(self, contract: Contract, action: str, qty: int, marketPrice: float = 0.0):
        
        try:
            if not self.ib.isConnected():
                logger.info(
                    f"IB not connected in create_order, attempting to reconnect"
                )
                await self.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
            symbol = contract.symbol
            
            logger.info(f"Received webhook request for : {symbol}")
            order =[]
            totalQuantity = 0.0
            trade = {}
            
            snapshot, web_request,  already_subscribed = await asyncio.gather(price_data_dict.get_snapshot(symbol), get_tv_ticker_dict(symbol), subscribed_contracts.has(symbol))
            timeframe= web_request.timeframe
            if timeframe is None:
                timeframe = "1 min"
            barSize  = timeframe
            barSizeSetting= barSizeSetting_dict[symbol]
            if barSizeSetting is None:
                barSizeSetting = barSize
            
            is_valid_timeframe = "secs" in barSizeSetting or "min" in barSizeSetting
            if not is_valid_timeframe:
                barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(barSizeSetting)
            
            
            logger.info(f"Already subscribed to {symbol}: {already_subscribed}")
            
            logger.info(f"Contract for {symbol}: {contract}")
            subscribe_needed=not already_subscribed
           
            if subscribe_needed:
                logger.info(f"Subscribing market data for {symbol} with barSizeSetting: {barSizeSetting}")
                await self.add_new_contract_pnl(contract.symbol, barSizeSetting)
            
                logger.info(f"Market data subscription active for {symbol}")
            else:
                logger.info(f"Market data for {symbol} already subscribed, skipping.")

        
        
            limit_price = marketPrice
          
        
            if is_market_hours():
                totalQuantity = abs(qty)
                logger.info(f"Market is open, using market order for {symbol} with action {action} and quantity {totalQuantity}")
                order = MarketOrder(action, totalQuantity)
        

            if not is_market_hours():
                ask_price = 0.0
                bid_price = 0.0
                markPrice = 0.0
                last = 0.0
                take_profit_price = 0.0
                price_source =''
                #logger.info(f"Last close for {symbol}: {await price_data_dict.get_snapshot(symbol)}")
                
                if snapshot.hasBidAsk:
                    ask_price = snapshot.ask if (snapshot.ask is not None and not isNan(snapshot.ask) and snapshot.ask > 1.1) else 0.0
                    bid_price = snapshot.bid if (snapshot.bid is not None and not isNan(snapshot.bid) and snapshot.bid > 1.1) else 0.0
                    last = snapshot.last if (snapshot.last is not None and not isNan(snapshot.last) and snapshot.last >1.1) else 0.0
                    if action == "BUY" and ask_price > 2.0:
                        limit_price = round(ask_price, 2)
                        price_source ="snapshot.ask"
                    elif action == "SELL" and bid_price > 2.0:
                        limit_price = round(bid_price, 2)
                        price_source ="snapshot.bid"
                    elif not isNan(snapshot.last):
                        limit_price = round(snapshot.last, 2)
                        price_source = "snapshot.last"
                    elif not isNan(snapshot.markPrice) and snapshot.markPrice > 0.0:
                        limit_price = round(snapshot.markPrice, 2)   
                        price_source       
                    else:
                        limit_price = marketPrice
                        price_source = "None 0.0"
                    logger.info(f"Bracket OCA order for {symbol}  hasBidAsk:...")
                if snapshot.markPrice and (snapshot.markPrice is not None and not isNan(snapshot.markPrice) and snapshot.markPrice > 0):
                    markPrice = snapshot.markPrice
                logger.info(f"Processing webhook request for {web_request.ticker} ")
            
                logger.info(
                    f"New Order close_all request for {web_request.ticker} with jengo and uptrend"
                )
                

                order = LimitOrder(
                    action,
                    totalQuantity=1,
                    lmtPrice=limit_price,
                    tif="GTC",
                    outsideRth=True,
                    transmit=True,
               
                    orderRef=f"close price_source {price_source} - {contract.symbol}",
                )
                  

                 

        
            trade =self.ib.placeOrder(contract, order)
        
            if trade:
                logger.info(f"Placing order for {trade.contract.symbol} with action {action} and quantity {totalQuantity}")
                trade.fillEvent += self.on_fill_update

                return trade
        except Exception as e:
            logger.error(f"Error processing webhook request: {e}")
            return None
        
    async def process_close_all_main(self, contract: Contract, action: str, qty: float, marketPrice: float): 
        try:
            symbol=contract.symbol
        

        
        

            logger.info(f"Boof Processing PnL event - process_close_all_main request for {symbol} with a qty of {qty}")
            first_ticker = None
            totalQuantity = 0.0
            trade = {}
            order = {}
            action = None
            limit_price = 0.0

            logger.info(
                f"New Order close_all request for {symbol} with jengo and uptrend"
            )
        
            already_subscribed = await subscribed_contracts.has(symbol)
            subscribe_needed=not already_subscribed
            if subscribe_needed:

                first_ticker= await price_data_dict.get_snapshot(symbol)
                    
            first_ticker= await price_data_dict.get_snapshot(symbol)
            logger.info(
                        f"first_ticker.ask: {first_ticker.ask}, first_ticker.bid: {first_ticker.bid}, first_ticker.markPrice: {first_ticker.markPrice}"
                    )
        
            placeOrder=False
            parent_id = parent_ids[symbol]
        
        
            openOrders =  self.ib.openOrders()
            for openOrder in openOrders:
                if openOrder.parentId == parent_id:
                    open_order: Order = openOrder
                    logger.info(f"Open order found for {symbol} with action {openOrder.action} and quantity {openOrder.totalQuantity}")
                    result = self.ib.cancelOrder(open_order)
                    if result:
                        logger.info(f"Order {open_order.orderId} cancelled successfully for open_order {open_order}")
                
                  
           
            if is_market_hours:
                logger.info(
                    f"Market is open, closing position for {contract.symbol} with action {action} and quantity {totalQuantity}"
                )
                order = MarketOrder(action, totalQuantity)
            if not is_market_hours():
            
                logger.info(f"Is not market hours, using limit order for {contract.symbol} with action {action} and quantity {totalQuantity}")
                if marketPrice > 0.0:
                    limit_price = marketPrice

                elif limit_price == 0.0 or limit_price is None:
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
                trade = self.ib.placeOrder(contract, order)
        

            if trade:
                logger.info(f"Order ID for {contract.symbol}")
                order_json = {
                    "ticker": contract.symbol,
                    "orderAction": action,
                    "limit_price": round(limit_price, 2),
                
            
                    "quantity": totalQuantity,
                    
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
        
    async def add_new_contract_pnl(self, symbol: str, barSizeSetting: str) -> PriceSnapshot:
        try:
            contract = Contract()
            contract.symbol = symbol
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
       
            df= await yesterday_close_bar(self.ib, contract)
            vol=await daily_volatility.get(symbol)
        
            if df.empty:
                raise RuntimeError("No historical data for " + contract.symbol)
       
            last_daily_bar_dict[contract.symbol] = df.iloc[-1]
        
            _yesterday_close =  yesterday_close[symbol]
        
        
            ticker: Ticker = self.ib.reqMktData(contract, genericTickList = "221,236", snapshot=False)
            logger.info(f"1 Adding contract {contract.symbol} with barSizeSetting {barSizeSetting}...")
            await asyncio.sleep(1)  # Allow time for the ticker to be set up
            first_ticker = await price_data_dict.add_ticker(self.ib.ticker(contract))
            logger.info(f"2 Adding contract {contract.symbol} with barSizeSetting {barSizeSetting}...and first_ticker markPrice: {first_ticker.markPrice}")
            #await asyncio.sleep(2)
            logger.info(f"3 Adding contract {contract.symbol} with barSizeSetting {barSizeSetting}...")
           
        
        
        
            if vol is None or isNan(vol):
                logger.info(f"Computing daily volatility for {symbol}...")
                # Compute daily volatility if not already set
                vol=await compute_volatility_series(ticker.high , ticker.low , _yesterday_close, contract,self.ib)
            #await asyncio.sleep(2)
            #await first_get_historical_data(contract, barSizeSetting, vstopAtrFactor)  2025-05-24 18:47:31.499   | 2025-05-24 18:47:18.025 | INFO     | app:tv_post_data:2177 | Received TV post data for ticker: NVDA unixtime 1748112420000 
         

        except Exception as e:
            logger.error(f"Error in add_new_contract for {contract.symbol}: {e}")
            raise e
    async def cancel_open_orders(self, position: Position):
        try:
            parent_id= None
            sym = position.contract.symbol if (position.position != 0 or  position.position == 0)  else None
            if sym:
                parent_id=parent_ids[sym]
            # Only proceed if this symbol is one we traded.
            if position.position == 0 and parent_id is not None:
                openOrders=self.ib.openOrders()
            
                # Iterate open orders; cancel any StopOrder whose parentId == parent_id
                for o in openOrders:
                    # Confirm it’s a StopOrder (orderType = "STP") and matches our parentId.
                    if o.parentId == parent_id or o.orderId == parent_id or o.permId == parent_id:
                        try:
                            self.ib.cancelOrder(o)
                            logger.info(f"[{sym}] canceled Stop‑Loss(orderId={o.orderId}) after manual close.")
                        except Exception as cancel_err:
                            logger.error(f"[{sym}] error canceling orderId={o.orderId}: {cancel_err}")
        except Exception as e:
            logger.error(f"Error canceling open orders for {position.contract.symbol}: {e}")
            return False  
    async def place_close_order(self, pos):
        try:
            logger.info(
                f"place_close_order Placing close order for {pos.contract.symbol} with position {pos.position}"
            )
            symbol = pos.contract.symbol
            # Qualify the contract first
            qualified_contracts = await subscribed_contracts.get(symbol)
            if not qualified_contracts:
                logger.error(f"Could not qualify contract for {pos.contract.symbol}")
                return None

            qualified_contract = qualified_contracts[0]
            logger.info(f"place_close_order qualified_contract: {qualified_contract}")
            action = "SELL" if pos.position > 0 else "BUY"
            quantity = abs(pos.position)
            orderRef = f"close_{symbol}_{await get_timestamp()}"
            # Choose order type based on market hours.
            if is_market_hours():
                order = MarketOrder(
                    action, totalQuantity=1, tif="GTC", orderRef=orderRef
                )
            else:
                # For limit orders, get last price and add a small offset
                last_price = await self.ib.reqHistoricalDataAsync(
                    qualified_contract,
                    endDateTime="",
                    durationStr="300 S",
                    barSizeSetting="1 min",
                    whatToShow="BID" if action == "BUY" else "ASK",
                    useRTH=False,
                )
                if not last_price:
                    logger.error(f"Price data unavailable for {symbol}")
                    return
                tick_offset = -0.01 if action == "SELL" else 0.01
                limit_price = round(last_price[-1].close + tick_offset, 2)
                order = LimitOrder(
                    action,
                    totalQuantity=1,
                    lmtPrice=limit_price,
                    tif="GTC",
                    outsideRth=True,
                    orderRef=orderRef,
                )
            trade: Trade = self.ib.placeOrder(qualified_contract, order)
            if trade:
                logger.info(f"Placed close order for {symbol}")
                trade.fillEvent += self.on_fill_update
                return trade
        except Exception as e:
            logger.error(f"Error placing close order for {symbol}: {e}")

    

    

    async def on_fill_update(self, trade: Trade, fill):
        try:
            if trade is None:
                logger.debug("Trade is None in on_fill_update, skipping update.")
                return None
            if fill is None:
                logger.debug(f"Fill is None for trade {trade}")
                orders = await self.ib.reqAllOpenOrdersAsync()
                if orders:
                    self.orders_dict = {
                        order.contract.symbol: order for order in orders
                    }
                    symbol = trade.contract.symbol  # Safe since trade is not None
                    self.open_orders[symbol] = {
                        "symbol": symbol,
                        "order_id": trade.order.orderId,
                        "status": trade.orderStatus.status,
                        "timestamp": await get_timestamp(),
                    }
                return None
            if fill:
                await ib_open_orders.delete(trade.contract.symbol,  trade)
                logger.info(
                    f"Fill received for fill {fill.contract.symbol}. jengo Updating positions..."
                )

        except Exception as e:
            logger.error(f"Error updating positions on fill: {e}")

   

   

    async def app_pnl_event(self):
        portfolio = self.ib.portfolio()

        # Convert each PortfolioItem to a dict
        portfolio_data = [
            {
                "symbol": item.contract.symbol,
                "secType": item.contract.secType,
                "exchange": item.contract.exchange,
                "currency": item.contract.currency,
                "position": item.position,
                "marketPrice": item.marketPrice,
                "marketValue": item.marketValue,
                "averageCost": item.averageCost,
                "unrealizedPNL": item.unrealizedPNL,
                "realizedPNL": item.realizedPNL,
                "account": item.account,
            }
            for item in portfolio
        ]

        logger.info(f"Current portfolio_data: {portfolio_data}")

        orders, close_trades = await asyncio.gather(
            self.ib.reqAllOpenOrdersAsync(),
            self.ib.reqCompletedOrdersAsync(False),
        )
        open_orders_event = await self.open_orders_event(orders)
        logger.info(f"Open orders updated: {open_orders_event}")

        pnl = self.ib.pnl(self.account)
        logger.info(f"PnL data: {pnl}")

        for pnl_val in pnl:
            if pnl_val.account == self.account:
                logger.debug(
                    f"PnL data updated for account {pnl_val.account}: {pnl_val}"
                )
            self.account_pnl = AccountPnL(
                unrealizedPnL=float(pnl_val.unrealizedPnL or 0.0),
                realizedPnL=float(pnl_val.realizedPnL or 0.0),
                dailyPnL=float(pnl_val.dailyPnL or 0.0),
                
            )

        return orders, portfolio_data, self.account_pnl, close_trades
     

    async def open_orders_event(self, orders: Trade):
        try:
            logger.debug("Updating open orders...")
            # In the app_pnl_event method
            if self.client_id == 1111:
                for order in orders:
                    symbol = order.contract.symbol
                    if (
                        symbol in self.open_orders
                        and self.open_orders[symbol]["order_id"] != order.order.orderId
                    ):
                        logger.debug(f"Stale order detected for {symbol}, updating...")
                        del self.open_orders[symbol]  # Remove stale order entry
                    elif symbol not in self.open_orders:
                        # Initialize the entry for new symbols
                        logger.debug(
                            f"New order detected for {symbol}, adding to tracking..."
                        )
                        self.open_orders[symbol] = {
                            "symbol": symbol,
                            "order_id": order.order.orderId,
                            "status": order.orderStatus.status,
                            "timestamp": await get_timestamp(),
                        }

            return self.open_orders  # Return the updated open orders dictionary
        except Exception as e:

            logger.error(f"Error in open_orders_event: {e}")
            return self.open_orders


    async def error_code_handler(
        self, reqId: int, errorCode: int, errorString: str, contract: Contract
    ):
        logger.warning(f"Error for reqId {reqId}: {errorCode} - {errorString}")
ib_manager = IBManager(None)

async def get_orders(trades: Trade, symbol: str, ib=None) -> list:
    try:
        
        open_trades = []
        order_id = None
        db_rows = None
        db_keys = None
        ib_keys = set()

        # Pull active open orders from IB
        if trades is not None:
            # Handle case when a single Trade object is passed
            if not isinstance(trades, list):
                # Convert single trade to list
                open_trades = trades

                # Process the single trade
               
                status = trades.orderStatus
                order = trades.order

                # Resolve orderId with fallback to permId
                raw_order_id = status.orderId or order.orderId
                fallback_id = order.permId or status.permId
                order_id = (
                    raw_order_id if raw_order_id and raw_order_id > 0 else fallback_id
                )

                ib_keys.add((order_id, symbol))
                await order_db.insert_trade(trades)
                logger.info(
                    f"Order ID: {order_id}, Symbol: {symbol}, Status: {status.status}"
                )
                logger.info(f"Received trade: {trades}")
            else:
                # Process multiple trades
                open_trades = trades
                for trade in open_trades:
                   
                    status = trade.orderStatus
                    order = trade.order

                    raw_order_id = status.orderId or order.orderId
                    fallback_id = order.permId or status.permId
                    order_id = (
                        raw_order_id
                        if raw_order_id and raw_order_id > 0
                        else fallback_id
                    )

                    ib_keys.add((order_id, symbol))
                    await order_db.insert_trade(trade)  # Insert each trade individually
                    logger.info(
                        f"Order ID: {order_id}, Symbol: {symbol}, Status: {status.status}"
                    )
        else:
            # Get all open orders from IB
            open_trades: Trade = await  ib.reqAllOpenOrdersAsync()

            # Process open trades
            for open_trade in open_trades:
                try:
                    logger.info(f"Open trade symbol: {symbol}")
                    
                    status = open_trade.orderStatus
                    order = open_trade.order

                    # Resolve orderId with fallback to permId
                    raw_order_id = status.orderId or order.orderId
                    fallback_id = order.permId or status.permId
                    order_id = (
                        raw_order_id
                        if raw_order_id and raw_order_id > 0
                        else fallback_id
                    )

                    ib_keys.add((order_id, symbol))
                    await insert_trade(open_trade)  # Insert each trade individually
                    logger.info(
                        f"Order ID: {order_id}, Symbol: {symbol}, Status: {status.status}"
                    )
                except Exception as e:
                    logger.error(f"Error processing trade: {e}, trade: {open_trade}")
                    continue

        logger.debug(f"Open trades from IB: {open_trades}")

        # Fetch current DB orders
        db_rows = await order_db.get_all_trades()

        # Build a set of (orderId, symbol) from DB
        db_keys = set(
            (row[2], row[1]) for row in db_rows
        )  # orderId = index 2, symbol = index 1

        # Find stale orders (present in DB but missing from IB)
        stale_keys = db_keys - ib_keys

        for order_id, symbol in stale_keys:
            logger.info(f"Removing stale order: orderId={order_id}, symbol={symbol}")
            await order_db.delete_trade(order_id, symbol)

        return db_rows

    except Exception as e:
        logger.error(f"Error fetching or reconciling orders: {e}")







    

 
async def get_ib_bars(
    contract,
    barSizeSetting,
    ib: IB = None
) -> Optional[pd.DataFrame]:
    """
    Pull historical bars from IB and stash the last one in hist_bar_data.
    Returns a DataFrame of all bars, or None on failure.
    """
    if ib is None:
        logger.error("You must pass an IB client instance as `ib`")
        return None

    # IB expects a unit on durationStr
    duration = "7200 S"  # 7200 seconds = 2 hours

    try:
        vstopAtrFactor= None
        symbol = contract.symbol
        vol= await daily_volatility.get(symbol)
        atrFactorDyn, bars = await asyncio.gather(get_dynamic_atr(symbol, BASE_ATR=None, vol=vol),
                                                  ib.reqHistoricalDataAsync(
                                                    contract=contract,
                                                    endDateTime="",
                                                    durationStr=duration,
                                                    barSizeSetting=barSizeSetting,
                                                    whatToShow="TRADES",
                                                    useRTH=False,
                                                    formatDate=1)
        )
      

        if not bars:
            logger.warning(f"No IB history for {contract.symbol}")
            return None

        df = util.df(bars)
        if df.empty:
            logger.warning(f"No historical data for {contract.symbol} at {barSizeSetting}")
            return None
        if atrFactorDyn is not None:
            vstopAtrFactor = atrFactorDyn
        else:
            vstopAtrFactor = 1.5  # default value if not provided
        
        vStop, uptrend = await vol_stop(df, 20, vstopAtrFactor)
        df["vStop"], df["uptrend"] = vStop, uptrend
        if vStop is not None:
        
            vstop_data[symbol] = vStop  # full Series
            uptrend_data[symbol] = uptrend  # full Series

        
            uptrend_data[symbol] = uptrend
            vStop_float = vstop_data[symbol].iloc[-1]
            uptrend_bool = uptrend_data[symbol].iloc[-1]
            await price_data_dict.add_vol_stop(symbol, vStop_float, uptrend_bool)
            vStop_float_dict, uptrend_dict=await price_data_dict.get_vol_stop(symbol)
            logger.debug(f"vStop float for {contract.symbol}: {vStop_float}, uptrend: {uptrend_bool}")

        last: BarData = bars[-1]
        # Normalize into a plain dict
        bar_dict = {
            "time": getattr(last, "date", getattr(last, "time", None)),
            "open": last.open,
            "high": last.high,
            "low": last.low,
            "close": last.close,
            "volume": getattr(last, "volume", None),
        }

        # Store it
        await price_data_dict.add_bar(contract.symbol, barSizeSetting, bar_dict)
        return df

    except Exception as e:
        logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
        raise

async def p_tick_get_ib_bars(contract, barSizeSetting= None, ib=None):
    try:
        bar_sec_int = None
        if barSizeSetting is None:
            barSizeSetting = "15 secs"
        _, bar_sec_int=convert_pine_timeframe_to_barsize(barSizeSetting)
        if bar_sec_int is None:
            bar_sec_int = 15
        asyncio.sleep(bar_sec_int)
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",
            durationStr="7200",
            barSizeSetting=barSizeSetting,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1
        )
        if not bars:
            logger.warning(f"No IB history for {contract.symbol}")
            return None
                
        df = util.df(bars)
        last_bar = bars[-1]
        return df
        #logger.info(f"Fetched {len(df)} historical bars for {contract.symbol} at 15 secs interval last_bar.time: {last_bar.date}")
    except Exception as e:
        logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
        raise

    
async def process_bar_data(contract, ib=None, barSizeSetting=None) -> None:
    try:
        #await asyncio.sleep(2)
        snapshot: PriceSnapshot = None
        
        symbol = contract.symbol
        vol, df, tvData, tick = await asyncio.gather(daily_volatility.get(symbol), yesterday_close_bar(ib, contract), get_tv_ticker_dict(symbol), price_data_dict.get_snapshot(symbol))
        vstopAtrFactor = tvData.vstopAtrFactor if tvData else 1.5
        timeframe = tvData.timeframe
        if barSizeSetting is None:
            barSizeSetting = barSizeSetting_dict.get(symbol, None)
        logger.debug(f"Processing bar data for {symbol} with timeframe: {timeframe} and vstopAtrFactor: {vstopAtrFactor} and barSizeSetting: {barSizeSetting}")
        if timeframe is None:

            return None
        is_valid_timeframe = "secs" in timeframe or "min" in timeframe
        if is_valid_timeframe and barSizeSetting is None:
            logger.info(f"Using timeframe {timeframe} as barSizeSetting for {symbol}")
            barSizeSetting = timeframe
        else:
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(timeframe)
        
        logger.debug(f"Processing bar data for {symbol} with barSizeSetting: {barSizeSetting}")
        
    
        if df.empty:
            raise RuntimeError("No historical data for " + contract.symbol)
       
        price_data_dict.last_daily_bar_dict[symbol] = df.iloc[-1]
        
        _yesterday_close =  yesterday_close[symbol]

       
        
        timeframe = tvData.timeframe if tvData else "15 secs"
        vstopAtrFactor = tvData.vstopAtrFactor if tvData else 1.5
        
        
        vol_df,  atrFactorDyn = await asyncio.gather(get_ib_bars(contract, barSizeSetting, ib), get_dynamic_atr(symbol, BASE_ATR=None, vol=vol))
        if vol_df.empty:
            logger.error(f"No historical data for {symbol} at {barSizeSetting}")
            raise RuntimeError(f"No historical data for {symbol} at {barSizeSetting}")
        logger.debug(f"Historical data for {symbol} at {barSizeSetting} fetched successfully")
        vstopAtrFactor = atrFactorDyn
          
      

        
        last_value, valid = supertrend_last(vol_df, atr_period=20, atr_multiplier=atrFactorDyn)
        logger.debug(f"Supertrend last value for {symbol} is {last_value} with valid {valid} and atrFactorDyn {atrFactorDyn}")

        
        logger.debug(f"Dynamic ATR factor for {symbol} is {vstopAtrFactor} with daily_volatility {vol} ")
        vol_stop_data, snapshot = await asyncio.gather(vol_stop(vol_df, 20, vstopAtrFactor), price_data_dict.get_snapshot(symbol))
        vStop, uptrend = vol_stop_data
        vol_df["vStop"], vol_df["uptrend"] = vStop, uptrend
        if vStop is not None:
            
           
            vstop_data[symbol] = vStop  # full Series
            uptrend_data[symbol] = uptrend  # full Series

           
            uptrend_data[symbol] = uptrend
            vStop_float = vstop_data[symbol].iloc[-1]
            uptrend_bool = uptrend_data[symbol].iloc[-1]
            await price_data_dict.add_vol_stop(symbol, vStop_float, uptrend_bool)
            logger.debug(f"vStop for {symbol} is {vStop_float} with uptrend {uptrend_bool}")
        
  
        logger.debug(f"Getting daily volatility for {symbol} volatility {vol} yesterday_close {_yesterday_close}")
        logger.debug("\n" + snapshot.to_table())
        logger.debug(
            f"{symbol} vStop={vStop.iloc[-1]} uptrend={uptrend.iloc[-1]}"
        )
        
        
        
        logger.debug(f"Ticker data for {symbol}:shortableShares {snapshot.shortableShares} markPrice {snapshot.markPrice} halted {snapshot.halted} dayly_volatility {vol} yesterday_close {_yesterday_close} snapshot.hasBidAsk {snapshot.hasBidAsk}") 
        
        return snapshot  
       

    except Exception as e:
        logger.error(f"Error processing bar data for {symbol}: {e}")


    
async def insert_rt_bar(symbol, rt_bar_dict, bar_size, last_bar):
    """
    Insert a confirmed real-time bar into the database.
    This is used to store the 5s bars in the realtime_bars table.
    """

    if order_db.conn is None:
        await order_db.connect()           # just in case
    #await aggregate_to_all_intervals(order_db.conn, symbol, bar_dict)

    # Add the latest raw bar to the realtime buffer.
    realtime_bar_buffer[symbol].append(last_bar)
    # Append the bar_dict.
    confirmed_5s_bar[symbol].append(rt_bar_dict)
    try:
        await order_db.insert_realtime_bar(symbol, rt_bar_dict, bar_size)
        tvData = await get_tv_ticker_dict(symbol)
        vstopAtrFactor = tvData.vstopAtrFactor if tvData else 1.5
        timeframe = tvData.timeframe
        if timeframe is None:

            return None
        barSizeSetting=barSizeSetting_dict[symbol]
        #await get_historical_data(bars.contract, barSizeSetting, vstopAtrFactor)
    except Exception as e:
        logger.error(f"Failed to write realtime bar: {e}")


    
async def process_rt_bar(ticker: OrderRequest):
    try:
        logger.info(f"Processing TV post data for {ticker.ticker}")
        symbol = ticker.ticker
        
        already_subscribed = await subscribed_contracts.has(symbol)
        logger.info(f"Already subscribed to {symbol}: {already_subscribed}")
        contract = await subscribed_contracts.get(symbol)
        logger.info(f"Contract for {symbol}: {contract}")
        
        
        subscribe_needed=not already_subscribed
        await tv_store_data.set(symbol, ticker.model_dump())
        if subscribe_needed:
            logger.info(f"Subscribing to real time sim bars for {symbol} with barSizeSetting: {ticker.timeframe}")
            
            bars=await ib_sim.start_rt_bars(contract)
            
            logger.info(f"Real time sim bars subscription active for {symbol} with barSizeSetting: {ticker.timeframe}")
            logger.info(f"Real time sim bars for {symbol} started with barSizeSetting: {ticker.timeframe}")
        else:
            logger.info(f" real time sim bars {symbol} already subscribed, skipping.")

       
        return

    except Exception as e:
        logger.error(f"Error processing TV post data for {ticker.ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def floor_time_to_5s(dt: datetime):
    return dt.replace(second=(dt.second // 5) * 5, microsecond=0)

