# ib_con.py
from hmac import new
import math
import re
import asyncio, os, time, pytz
from datetime import datetime
from dataclasses import dataclass
from ib_async import IB, Contract, MarketOrder, LimitOrder, PortfolioItem, StopOrder, TagValue, Trade, PnL, Client, Wrapper, Ticker, Order, Position, util, Fill
from log_config import log_config, logger
from typing import Dict, List
import pandas as pd
import numpy as np
from timestamps import current_millis, get_timestamp, is_market_hours
from price import PriceData

# Setup logging and environment
log_config.setup()


@dataclass
class AccountPnL:
    unrealized_pnl: float
    realized_pnl: float
    total_pnl: float
    timestamp: str

class IBManager:
    def __init__(self, ib: IB):
        self.ib = ib
        self.client_id  = 1111
        self.host = '127.0.0.1'
        self.port = int(os.getenv('TBOT_IBKR_PORT', '4002'))
        self.max_attempts = 300
        self.initial_delay = 1
        self.account = None
        
        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        self.all_trades={} 
        self.orders_dict: Dict[str, Trade] = {}
        self.orders_dict_oca_id: Dict[str, Trade] = {}
        self.orders_dict_oca_symbol: Dict[str, Trade] = {}
        self.account_pnl: AccountPnL = None
        self.semaphore = asyncio.Semaphore(10)
        self.wrapper = Wrapper(self)
        self.client = Client(self.wrapper)
        self.active_ticker_to_dict = {}
        self.active_tickers: Dict[str, Ticker] = {}
        self.net_liquidation = {}
        self.settled_cash =  {}
        self.buying_power =  {}

    async def connect(self) -> bool:
        attempt = 0
        while attempt < self.max_attempts:
            try:
                logger.info(f"Connecting to IB at {self.host}:{self.port} : client_id:{self.client_id}...")
                await self.ib.connectAsync(host=self.host, port=self.port, clientId=self.client_id, timeout=40)
                if self.ib.isConnected():
                    logger.info("Connected to IB Gateway")
                    #await self.subscribe_events()
                    self.ib.disconnectedEvent += self.on_disconnected
                    self.ib.errorEvent += self.error_code_handler
                    return True
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {e}")
            attempt += 1
            await asyncio.sleep(self.initial_delay)
        logger.error("Max reconnection attempts reached")
        return False

    async def subscribe_events(self):
        try:
            logger.info("Jengo getting to IB data...")
            self.account = self.ib.managedAccounts()[0] if self.ib.managedAccounts() else None
            if not self.account:
                return
            self.ib.reqPnL(self.account)
            logger.info(f"Jengo getting to reqPnL data... {self.account}")
            await asyncio.sleep(3)  # Allow time for PnL data to be fetched
            logger.info(f"Jengo getting to pnl data...")
            pnl= self.ib.pnl(self.account)
            logger.info(f"Jengo received to pnl data... {pnl}")
           
            # Subscribe to key IB events. (Assumes event attributes exist.)
            
            logger.info("Subscribed to IB events")
        except Exception as e:
            logger.error(f"Error subscribing to events: {e}")
    async def error_code_handler(self, reqId: int, errorCode: int, errorString: str, contract: Contract):

        logger.error(f"Error for reqId {reqId}: {errorCode} - {errorString}")

    async def graceful_disconnect(self):
        try:
            if self.ib.isConnected():
                logger.info("Unsubscribing events and disconnecting from IB...")
                
                self.ib.disconnect()
                logger.info("Disconnected successfully.")
                return True
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
            return False
    async def on_disconnected(self):
        logger.warning("Disconnected from IB. Reconnecting...")
        await asyncio.sleep(1)
        if not self.ib.isConnected():
            await self.connect()

    async def qualify_contract(self, contract: Contract):
        qualified = await self.ib.qualifyContractsAsync(contract)
        if not qualified:
            logger.error(f"Contract qualification failed for {contract.symbol}")
            return None
        # Use the qualified contract instance directly
        qualified_contract = qualified[0]
        logger.info(f"Qualified contract for: {qualified_contract.symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}")
        return qualified_contract
         