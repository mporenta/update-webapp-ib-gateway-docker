import asyncio
import json
from collections import defaultdict
from models import AccountPnL
from ib_async.util import isNan
from tv_ticker_price_data import  tv_store_data, price_data_dict, daily_volatility, yesterday_close_bar, yesterday_close, subscribed_contracts, logger

from typing import Dict, Optional, Any
from ib_async import *

class AccountMetricsStore:
    def __init__(self):
        self._lock = asyncio.Lock()
        self.pnl_data = None
        self.account_summary: Dict[str, float] = defaultdict(AccountValue)
        self.account= None
        self.account_values: Optional[list[AccountValue]] = None
        self.buying_power: float = 0.0
        self.net_liquidation: float = 0.0
        self.settled_cash: float = 0.0
        
            

            
    async def ib_account(self, account: str):
        async with self._lock:
            logger.debug(f"Setting account to  {account} in account_metrics_store")
            self.account = account
            
            
    async def update_pnl_dict_init(self, pnl: PnL):
        async with self._lock:
            try:
                
                self.pnl_data = AccountPnL(
                    unrealizedPnL=0.0, realizedPnL=0.0, dailyPnL=0.0
                )
                
            

                if pnl is not None:
                    for pnl_item in pnl:
                        if pnl_item.account == self.account:

                            
                            self.pnl_data = AccountPnL(
                                unrealizedPnL=float(0.0 if isNan(pnl_item.unrealizedPnL) else pnl_item.unrealizedPnL),
                                realizedPnL=float(0.0 if isNan(pnl_item.realizedPnL) else pnl_item.realizedPnL),
                                dailyPnL=float(0.0 if isNan(pnl_item.dailyPnL) else pnl_item.dailyPnL),
                            
                            )
                   
                return self.pnl_data
            except Exception as e:
                logger.error(f"Error in update_pnl_dict_init: {e}")
                self.pnl_data = AccountPnL(unrealizedPnL=0.0, realizedPnL=0.0, dailyPnL=0.0)
                return self.pnl_data
            
            
    async def update_pnl_dict(self, pnl: PnL)-> AccountPnL | None: 
        async with self._lock:
            
            self.pnl_data = AccountPnL(
                unrealizedPnL=0.0, realizedPnL=0.0, dailyPnL=0.0
            )
            

            if pnl:
                self.pnl_data = AccountPnL(
                    unrealizedPnL=float(0.0 if isNan(pnl.unrealizedPnL) else pnl.unrealizedPnL),
                    realizedPnL=float(0.0 if isNan(pnl.realizedPnL) else pnl.realizedPnL),
                    dailyPnL=float(0.0 if isNan(pnl.dailyPnL) else pnl.dailyPnL),
                    
                )
            return self.pnl_data

    async def update_summary(self, ib=None):
        try:
            async with self._lock:
                account_values: AccountValue = await ib.accountSummaryAsync()
                logger.debug(f"AccountMetricsStore update_summary called with account_values")
                
            
                if account_values:
                    for account_value in account_values:
                        self.account_values = account_values
                        self.account_summary[account_value.tag]=account_value.value
                        if account_value.tag == "NetLiquidation":
                            self.net_liquidation = float(account_value.value)
                        
                        elif account_value.tag == "TotalCashValue":
                            self.settled_cash = float(account_value.value)
                        
                        elif account_value.tag == "BuyingPower":
                            self.buying_power = float(account_value.value)
                logger.debug(f"AccountMetricsStore update_summary: account_summary")
                #logger.debug(self.account_summary)
                #logger.debug(self.account_values)
        except Exception as e:
            logger.error(f"Error in update_summary: {e}")
            self.account_summary = defaultdict(AccountValue)
            self.net_liquidation = 0.0
            self.settled_cash = 0.0
            self.buying_power = 0.0         

    

    async def ib_order_trade_data(self, order_db, ib=None):
       
        openTrades = ib.openTrades()
        CompletedOrdersAsync = await ib.reqCompletedOrdersAsync(False)
        for CompletedOrder in CompletedOrdersAsync:
            await order_db.insert_trade(CompletedOrder)
       
    

        return openTrades, CompletedOrdersAsync
account_metrics_store = AccountMetricsStore()  
