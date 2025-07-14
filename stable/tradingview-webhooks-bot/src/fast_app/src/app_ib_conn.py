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
from helpers.price_data_dict import barSizeSetting_dict, price_data_dict, bar_sec_int_dict
from pnl_close import update_pnl

from models import AccountPnL
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
from log_config import logger
from ib_db import order_db
from helpers.ib_stop_order import ib_stop_order, new_trade_update_asyncio
from account_values import account_metrics_store
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))

class IBConnection:
    def __init__(self, ib=None):
        load_dotenv()
        client_id = int(os.getenv("FAST_API_CLIENT_ID", "1111"))
        ib_host = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
        ib_port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
        self.boofMsg = os.getenv("HELLO_MSG")
        if ib is None:
            ib = IB()

        self.ib = ib
        self.client_id = client_id
        self.ib_host = ib_host
        self.ib_port = ib_port
        
        
        self.risk_amount = risk_amount
        self.max_attempts = 300
        self.initial_delay = 1
        self.account = []
        self.shutting_down = False

        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        self.all_trades = {}
        self.orders_dict: Dict[str, Trade] = {}
        self.orders_dict_test: Dict[str, Trade] = defaultdict(dict)

        self.orders_dict_oca_id: Dict[str, Trade] = {}
        self.orders_dict_oca_symbol: Dict[str, Trade] = {}
        self.semaphore = asyncio.Semaphore(10)
        self.wrapper = Wrapper(self)
        self.client = Client(self.wrapper)
        
        self.net_liquidation = {}
        self.settled_cash = {}
        self.buying_power = {}
        self.order_db = order_db

    async def connect(self) -> bool:
        initial_delay = 1
        max_attempts = 300
        attempt = 0
        while attempt < max_attempts:
            try:
                logger.info(
                    f"Connecting to IB at {self.ib_host}:{self.ib_port} : client_id:{self.client_id}...with risk_amount {self.risk_amount} and boofMsg: {self.boofMsg}"
                )
                await self.ib.connectAsync(
                    host=self.ib_host, port=self.ib_port, clientId=self.client_id, timeout=40
                )
                
                logger.info("IB connection established, subsrcribing to events...")

                await self.subscribe_to_events()

                return True
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {e}")
            attempt += 1
            await asyncio.sleep(initial_delay)
        logger.error("Max reconnection attempts reached")
        return False


    async def on_disconnected(self):
        try:
            global shutting_down
            logger.warning("Disconnected from IB.")
            # Check the shutdown flag and do not attempt reconnect if shutting down.
            if shutting_down:
                logger.info("Shutting down; not attempting to reconnect.")
                return
            await asyncio.sleep(1)
            if not self.ib.isConnected():
                await self.connect()
        except Exception as e:

            logger.error(f"Error in on_disconnected: {e}")
            # Attempt to reconnect if not shutting down


    async def graceful_disconnect(self):
        try:
            global shutting_down
            shutting_down = True  # signal shutdown


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


    async def subscribe_to_events(self):
        try:
            RUN_WSH = int(os.getenv("RUN_WSH", 0))

            run_wsh=RUN_WSH
            logger.info(f"RUN_WSH is set to {run_wsh}")
            self.account = (
                self.ib.managedAccounts()[0] if self.ib.managedAccounts() else None
            )
            if not self.account:
                return
            self.ib.reqPnL(self.account)
            await asyncio.sleep(1)  # Allow time for PnL request to process
            logger.info("Subscribing to IB events...")
            pnl= self.ib.pnl(self.account)


        
            # self.ib.execDetailsEvent += on_trade_fill_event
            self.ib.newOrderEvent += new_trade_update_asyncio
            #self.ib.orderStatusEvent += ib_open_orders.on_order_status_event

            self.ib.orderStatusEvent += new_trade_update_asyncio

            self.ib.pendingTickersEvent += self.on_pending_tickers
            self.ib.pnlEvent += self.on_pnl_event


           
            



            self.ib.disconnectedEvent += self.on_disconnected
            self.ib.errorEvent += self.error_code_handler
            rvolFactor = 0.2
            
            
            
           

        except Exception as e:
            logger.error(f"Error subscribing to events: {e}")
            raise e
    async def error_code_handler(self,
        reqId: int, errorCode: int, errorString: str, contract: Contract
    ):
        logger.warning(f"Error for reqId {reqId}: {errorCode} - {errorString}")




    async def on_pending_tickers(self, tickers):
        try:

            new_ticker= None

            for ticker in tickers:
                new_ticker= ticker
                await price_data_dict.add_ticker(ticker=new_ticker, post_log=False)
          
        except Exception as e:
            logger.error(f"Error in on_pending_tickers: {e}")
            raise e

    async def on_pnl_event(self, pnl):
        if not self.ib.isConnected():
            await self.connect()
        #account_pnl=await account_metrics_store.update_pnl_dict(pnl)
        portfolio_items = []
        logger.debug(f"on_pnl_event: Received PnL event for with pnl as: {pnl}")
        account_pnl = AccountPnL(
            unrealizedPnL=0.0, realizedPnL=0.0, dailyPnL=0.0
        )
        trades = []
        if account_pnl.dailyPnL>0:
            updatePnl = await update_pnl(account_pnl, self.ib)
            logger.info(f"first Updated PnL: {updatePnl}")
        else:
            account_pnl=await account_metrics_store.update_pnl_dict(pnl)
    

    
        trades =  self.ib.openTrades()
   
        for trade in trades: 
            ib_orders=await order_db.insert_trade(trade)
        
            logger.debug(f"Open order for {trade.contract.symbol}: {trade}")
        
        logger.debug(f"PnL event received: {pnl}")

        if account_pnl:
        

            updatePnl = await update_pnl(account_pnl, self.ib)
            logger.debug(f"Updated PnL: {updatePnl}")
ib_instance: Dict[str, IB] = defaultdict(IB)
  
ib_connection = IBConnection(None) 
logger.info(f"Connecting to IB with client_id: {ib_connection.client_id}, host: {ib_connection.ib_host}, port: {ib_connection.ib_port} and ib_connection {ib_connection}")
ib_instance["ib"] = ib_connection.ib
logger.info(f"ib_instance['ib'] set to: {ib_instance['ib']}")
def is_market_hours():
    ny_tz = pytz.timezone("America/New_York")
    now = datetime.now().astimezone(ny_tz)  # Convert server local time to New York time

    if now.weekday() >= 5:
        return False  # Weekend

    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open <= now < market_close