# price.py
from tracemalloc import stop
from ib_async import (
    Contract,
    Order,
    Trade,
    LimitOrder,
    StopOrder,
    TagValue,
    util,
    IB,
    MarketOrder,
)
from dataclasses import dataclass
from log_config import log_config, logger
from typing import *
import pandas as pd
import os, asyncio, pytz, time
import pandas as pd
import numpy as np
from fastapi.responses import JSONResponse, HTMLResponse

from fastapi import FastAPI, HTTPException
from datetime import datetime
from collections import deque
import math
import pandas_ta as ta
from pandas_ta.volatility import  atr
#import talib as ta

from pydantic import BaseModel

from indicators import (
    calculate_keltner_channels_ta_lib,
    volatility_stop,
    get_ema,
    detect_pivot_points,
    daily_volatility,
)
from timestamps import current_millis, get_timestamp, is_market_hours
from dotenv import load_dotenv

load_dotenv()
# --- Configuration ---
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv("FASTAPI_IB_CLIENT_ID", "1111"))
IB_HOST = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
# Instantiate IB manager


class QueryModel(BaseModel):
    query: str


# Request cache to help filter duplicates if desired
class WebhookMetric(BaseModel):
    name: str
    value: float


# Request model for our endpoint
class tvOrderRequest(BaseModel):
    entryPrice: float
    quantity: float  # Quantity to buy/sell
    rewardRiskRatio: float
    timeframe: str  
    riskPercentage: float
    kcAtrFactor: float
    atrFactor: float
    accountBalance: float
    ticker: str
    orderAction: str  # "BUY" for long, "SELL" for short
    stopType: str
    meanReversion: bool
    stopLoss: float
    submit_cmd: bool


class WebhookRequest(BaseModel):
    timestamp: int
    ticker: str
    currency: str
    timeframe_old: str
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
    timeframe: str  # e.g., "1 D", "60 S"
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


# For simplicity, our duplicate-check cache is a deque
from dataclasses import dataclass


@dataclass
class WebhookCacheEntry:
    ticker: str
    direction: str
    order_ref: str
    timestamp: int


# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), ".env")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Setup logging and environment
log_config.setup()


@dataclass
class AccountPnL:
    unrealized_pnl: float
    realized_pnl: float
    total_pnl: float
    timestamp: str


class PriceData:
    def __init__(self, ib_manager=None):
        self.ib_manager = ib_manager
        self.ib = ib_manager.ib if ib_manager else None
        self.entry_prices = (
            {}
        )  # key: symbol, value: {"entry_long": price, "entry_short": price, "timestamp": time}
        cached_atr_instance=PriceCachedATR(length=10)
        self.atr =cached_atr_instance 
        self.timestamp_id = {}
        self.client_id = int(os.getenv("FASTAPI_IB_CLIENT_ID", "1111"))
        self.host = os.getenv("WEBHOOK_IB_HOST", "172.23.49.114")
        self.port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
        self.max_attempts = 300
        self.initial_delay = 1
        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        #price dicts:
        self.open_price = {}
        self.high_price = {}
        self.low_price = {}
        self.close_price = {}
        self.bid_price = {}
        self.ask_price = {}
        self.volume = {}
        self.atr_cache={}
        self.nine_ema = {}
        self.twenty_ema = {}
        self.ticker = {}  
        self.daily_bars = {}  # key: symbol, value: DataFrame for daily historical data
        
        self.historical_df_day = {}  # key: symbol, value: DataFrame for daily historical data
        self.historical_df_one_min = {}  # key: symbol, value: DataFrame for 1-minute historical data
        self.triggered_bracket_orders= {}  # Store triggered bracket orders to avoid duplicates

        self.account_pnl: AccountPnL = None
        self.semaphore = asyncio.Semaphore(10)
        self.active_rtbars = {}  # Store active real-time bar subscriptions
        self.qualified_contract_cache = {}
        
    async def format_ticker_details_table(self, ticker_details):
        """
        Format order details as a nice pandas DataFrame table for terminal display.

        Args:
            order_details: Dictionary containing order details

        Returns:
            Formatted string representation of the table
        """
        # Convert dictionary to a DataFrame with 'Parameter' and 'Value' columns
        df = pd.DataFrame(
            {"Parameter": ticker_details.keys(), "Value": ticker_details.values()}
        )

        # Format numerical values
        for idx, param in enumerate(df["Parameter"]):
            value = df.loc[idx, "Value"]
            if isinstance(value, (int, float)):
                if "price" in param or "Balance" in param:
                    df.loc[idx, "Value"] = f"${value:.2f}"
                elif "percentage" in param or "Ratio" in param:
                    df.loc[idx, "Value"] = f"{value:.2f}"

        # Return formatted table string
        return f"\n{'='*50}\nTICKER DETAILS\n{'='*50}\n{df.to_string(index=False)}\n{'='*50}"
    
    def format_order_details_table(self, order_details):
        """
        Format order details as a nice pandas DataFrame table for terminal display.

        Args:
            order_details: Dictionary containing order details

        Returns:
            Formatted string representation of the table
        """
        # Convert dictionary to a DataFrame with 'Parameter' and 'Value' columns
        df = pd.DataFrame(
            {"Parameter": order_details.keys(), "Value": order_details.values()}
        )

        # Format numerical values
        for idx, param in enumerate(df["Parameter"]):
            value = df.loc[idx, "Value"]
            if isinstance(value, (int, float)):
                if "price" in param or "Balance" in param:
                    df.loc[idx, "Value"] = f"${value:.2f}"
                elif "percentage" in param or "Ratio" in param:
                    df.loc[idx, "Value"] = f"{value:.2f}"

        # Return formatted table string
        return f"\n{'='*50}\nORDER DETAILS\n{'='*50}\n{df.to_string(index=False)}\n{'='*50}"

    async def ohlc_vol(
        self, contract, durationStr, barSizeSetting, whatToShow, useRTH, return_df=False
    ):
        """
        Get historical OHLCV data from Interactive Brokers.

        Args:
            contract: The IB contract
            durationStr: Duration string for historical data (e.g., '1 D', '60 S')
            barSizeSetting: Bar size for historical data (e.g., '1 min', '5 secs')
            whatToShow: Type of data (e.g., 'TRADES', 'BID', 'ASK')
            useRTH: Whether to use regular trading hours data only
            return_df: If True, returns the raw DataFrame instead of processed values

        Returns:
            If return_df is True: DataFrame with OHLCV data
            Otherwise: Tuple of (ohlc4, hlc3, hl2, open, high, low, close, volume)
            None if no data is available
        """
        try:
            logger.info(
                f"Getting historical data for {contract.symbol} with duration {durationStr}, bar size {barSizeSetting}, and data type {whatToShow}"
            )
            # Get historical data for indicator calculations
            bars = await self.ib.reqHistoricalDataAsync(
                contract=contract,
                endDateTime="",
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow=whatToShow,
                useRTH=useRTH,
            )

            if not bars:
                logger.error(f"No historical data available for {contract.symbol}")
                return None

            # Convert bars to DataFrame
            data = [bar.__dict__ for bar in bars]
            df = pd.DataFrame(data)
           #logger.info(f"bars: {bars}")
           #logger.info(f"bar dict: {data}")
           #logger.info(f"DataFrame: {df}")
            
           #logger.info(f"DataFrame created with {len(df)} rows for {contract.symbol}")
           #logger.info(f"DataFrame columns: {df.columns}")
           #logger.info(f"DataFrame head: {df.head()}")
           #logger.info(f"DataFrame tail: {df.tail()}")
           #logger.info(f"DataFrame dtypes: {df.dtypes}")
           #logger.info(f"DataFrame info: {df.info()}")
           #logger.info(f"DataFrame describe: {df.describe()}")
           #logger.info(f"DataFrame shape: {df.shape}")
            
            
        

            # Return raw DataFrame if requested
            if return_df:
                return df

            # Check if we have enough data
            if len(df) < 10:
                logger.warning(
                    f"Not enough data to calculate OHLC averages. Need at least 10 bars, but got {len(df)}"
                )
                return None

            # Replace lines 296-304 with:
            open_prices = df["open"]
            high = df["high"]
            low = df["low"]
            close = df["close"]
            vol = df["volume"]
        except Exception as e:
            logger.error(f"Error calculating OHLC averages for {contract.symbol}: {e}")
            return None
        try:
            # Calculate derived values as Series
            hl2 = (high + low) / 2
            hlc3 = (high + low + close) / 3
            ohlc4 = (open_prices + high + low + close) / 4
        
            # Log just the most recent value or a summary statistic instead of the entire Series
            last_ohlc4 = ohlc4.iloc[-1] if len(ohlc4) > 0 else None
            logger.info(f"Calculated OHLCV data for {contract.symbol}, last ohlc4 = {last_ohlc4}")
        

            return ohlc4, hlc3, hl2, open_prices, high, low, close, vol
        
            
        except Exception as e:
            
            logger.error(f"Error getting historical data for {contract.symbol}: {e}")
            return None
        

    async def create_order(
        self,
        web_request: OrderRequest,
        contract: Contract,
        submit_cmd: bool,
        meanReversion: bool,
        stopLoss: float,
    ):
        try:
            entry_price= None
            logger.warning( f"web_request  is {web_request}")
            barSizeSetting=web_request.timeframe
            logger.warning( f"barSizeSetting  is {barSizeSetting} for  {web_request.ticker} and stop type {web_request.stopType}")
            
            
            entry_price = self.entry_prices[contract.symbol]["entry_long"] if web_request.orderAction == "BUY" else self.entry_prices[contract.symbol]["entry_short"]
            if entry_price is None:
                logger.warning(f"Entry price is None for {contract.symbol}. calling get_rt_price().")
                entry_price = await self.get_rt_price(contract, web_request.orderAction)
            order=[]
            qualified_contract = None
            if not self.ib.isConnected():
                logger.info(
                    f"IB not connected in create_order, attempting to reconnect"
                )
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
            logger.info(
                f"Creating order for {web_request.ticker} with action {web_request.orderAction} and stop type {web_request.stopType}"
            )
            if barSizeSetting is None:
                barSizeSetting = '1 min'
            else:
                barSizeSetting=web_request.timeframe
            stop_loss_price=None
            symbol = web_request.ticker
            take_profit_price = None
            stopType = web_request.stopType
            kcAtrFactor = web_request.kcAtrFactor
            vstopAtrFactor = web_request.vstopAtrFactor
            atrFactor = vstopAtrFactor if stopType == "vStop" else kcAtrFactor
            action = web_request.orderAction
            accountBalance = web_request.accountBalance
            riskPercentage = web_request.riskPercentage
            rewardRiskRatio = web_request.rewardRiskRatio
            stopLoss = web_request.stopLoss
            self.timestamp_id[symbol] = {
                "unixTimestamp": str(current_millis()),
                "stopType": stopType,
                "action": action,
                "riskPercentage": riskPercentage,
                "atrFactor": atrFactor,
                "rewardRiskRatio": rewardRiskRatio,
                "accountBalance": accountBalance,
                "stopLoss": stopLoss,
                "barSizeSetting": barSizeSetting,  
            }
            if action not in ["BUY", "SELL"]:
                raise ValueError("Order Action is None")
            
            timestamp_data = self.timestamp_id[symbol]
            logger.warning(f"Timestamp data for {symbol}: {timestamp_data}")
            # Step 1: Connect to IB and retrieve market data/entry price
            qualified, reqId = await asyncio.gather(
            self.ib.qualifyContractsAsync(contract),
            self.ib_manager.get_req_id()
            )
            if not qualified:
                logger.error(f"Contract qualification failed for {contract.symbol}")
                return None
            # Use the qualified contract instance directly
            qualified_contract = qualified[0]
            logger.info(
                f"Qualified contract for: {qualified_contract.symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}"
            )
            

            logger.info(f"Entry price for {qualified_contract.symbol}: {entry_price}")
            # In price.py, around line 425-430
            pos = await self.calculate_position(
                qualified_contract,
                entry_price,
                action,
                stopType,
                atrFactor,
                riskPercentage,
                accountBalance,
                rewardRiskRatio,
                stopLoss,
                durationStr = '1 D', 
                barSizeSetting= barSizeSetting,
            )

            if pos is None:
                logger.error(f"Failed to calculate position for {qualified_contract.symbol}")
                pos = {
                    "stop_loss_price": entry_price * 0.99 if action == "BUY" else entry_price * 1.01,
                    "quantity": 1,
                    "take_profit_price": entry_price * 1.02 if action == "BUY" else entry_price * 0.98,
                    "ema": self.twenty_ema.get(symbol, None),
                    "stopType": stopType
                }
                logger.warning(f"Using fallback position values for {qualified_contract.symbol}")

            ema = pos.get("ema")
            stopType = pos.get("stopType", web_request.stopType)  # Default to original if missing
            ema = pos.get("ema")
            stopType = pos.get("stopType", web_request.stopType)  # Default to original if missing
        
            logger.info(f"Calculated position for {qualified_contract.symbol}: entry={entry_price}, EMA= {ema} stop_loss={pos.get('stop_loss_price')}, take_profit={pos.get('take_profit_price')}, quantity={pos.get('quantity')}")

            stop_loss_price = pos.get("stop_loss_price")
            quantity = pos.get("quantity")
            take_profit_price = pos.get("take_profit_price")
        
            # Validate critical values before proceeding
            if take_profit_price is None:
                logger.error(f"Take profit price is None for {symbol}")
                raise ValueError("Take profit price is None")
            if stop_loss_price is None:
                logger.error(f"Stop loss price is None for {symbol}")
                raise ValueError("Stop loss price is None")
            if quantity is None:
                logger.error(f"Quantity is None for {symbol}")
                raise ValueError("Quantity is None")
            logger.info(
                f"Calculated position for {symbol}: entry={entry_price}, stop_loss={stop_loss_price}, take_profit={take_profit_price}, quantity={quantity}"
            )
            
            
            algo_order = LimitOrder(
               action=action,
               totalQuantity=quantity,
               lmtPrice=round(entry_price, 2),
               tif="GTC",
               transmit=True,
               algoStrategy="Adaptive",
               outsideRth=False,
               orderRef=f"Adaptive - {symbol}",
               algoParams=[TagValue("adaptivePriority", "Urgent")],
           )

            # Step 3: Place the bracket order
            limit_order = LimitOrder(
                    action,
                    totalQuantity=quantity,
                    lmtPrice=round(entry_price, 2),
                    tif="GTC",
                    outsideRth=True,
                    transmit=True,
                    orderId=reqId,
                )
            logger.info(
                    f"Afterhours limit order for {symbol} at price {entry_price}"
                )
            
            
            ################################################################################################ 1
            market_hours = is_market_hours()
            if market_hours:
                logger.info(f"Market Hours Algo Limit order for {symbol} at price {entry_price}")
                order = algo_order
            else:
                logger.info(f"Afterhours limit order for {symbol} at price {entry_price}: {limit_order}")
                order = limit_order


            
            logger.info(
                f"Placing order with algoStrategy {order.algoStrategy} action: {order.action} order: {order.totalQuantity} @ {entry_price}"
            )
           
            order_details = {}
            if submit_cmd:
                p_ord_id = order.orderId
                logger.debug(f"jengo bracket order webhook p_ord_id: {p_ord_id}")

                # Determine the type of parent order based on market hours

                logger.debug(
                    f"Market hours - creating MarketOrder for{contract.symbol}"
                )
                parent = order
                logger.debug(f"Parent Order - creating MarketOrder for{parent}")

                # Place the orders

                trade = self.ib.placeOrder(qualified_contract, parent)
                if trade:
                   
                    logger.info(
                        f"Parent Order placed for {qualified_contract.symbol}: {parent}"
                    )
                    trade.fillEvent += self.parent_filled
                    order_details = {
                        "ticker": qualified_contract.symbol,
                        "order Action": action,
                        "entry_price": order.lmtPrice,
                        
                        "quantity": order.totalQuantity,
                        "stopType": stopType,
                    }
                    logger.info(self.format_order_details_table(order_details))

                else:
                    logger.error(
                        f"Order placement failed for {contract.symbol}: {order}"
                    )

                trade_check = await self.ib.whatIfOrderAsync(qualified_contract, order)
                logger.debug(
                    f"Order details only for {symbol} with action {action} and stop type {stopType } trade_check= {trade_check}"
                )
                return trade

                
            if not submit_cmd:
                
                order_details = {
                    "ticker": qualified_contract.symbol,
                    "orderAction": web_request.orderAction,
                    "entry_price": entry_price,
                    "stop_loss_price": stop_loss_price,
                    "take_profit_price": take_profit_price,
                    "quantity": quantity,
                    "rewardRiskRatio": web_request.rewardRiskRatio,
                    "riskPercentage": web_request.riskPercentage,
                    "accountBalance": web_request.accountBalance,
                    "stopType": stopType,
                    "atrFactor": atrFactor,
                }
                logger.info(self.format_order_details_table(order_details))

                return {"status": "order details only", "order_details": order_details}

        except Exception as e:
            logger.error(f"Error creating order: {e}")
    

    async def create_tv_entry_order(
        self,
        web_request: tvOrderRequest,
        contract: Contract,
        submit_cmd: bool,
        meanReversion: bool,
        stopLoss: float,
    ):
        try:
            barSizeSetting=web_request.timeframe
            order=[]
            qualified_contract = None
            if not self.ib.isConnected():
                logger.info(
                    f"IB not connected in create_order, attempting to reconnect"
                )
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
            logger.info(
                f"Creating order for {web_request.ticker} with action {web_request.orderAction} and stop type {web_request.stopType}"
            )
            stop_loss_price=None
            symbol = web_request.ticker
            entry_price = float(web_request.entryPrice)
            stopType = web_request.stopType
            kcAtrFactor = web_request.kcAtrFactor
            vstopAtrFactor = web_request.vstopAtrFactor
            atrFactor = vstopAtrFactor if stopType == "vStop" else kcAtrFactor
            action = web_request.orderAction
            accountBalance = web_request.accountBalance
            riskPercentage = web_request.riskPercentage
            rewardRiskRatio = web_request.rewardRiskRatio
            stopLoss = float(web_request.stopLoss)
            quantity = float(web_request.quantity) 
            self.timestamp_id[symbol] = {
                "unixTimestamp": str(current_millis()),
                "stopType": stopType,
                "action": action,
                "riskPercentage": riskPercentage,
                "atrFactor": atrFactor,
                "rewardRiskRatio": rewardRiskRatio,
                "accountBalance": accountBalance,
                "stopLoss": stopLoss,
                "barSizeSetting": barSizeSetting,  # Store the bar size setting for reference
            }
            if action not in ["BUY", "SELL"]:
                raise ValueError("Order Action is None")
            # Step 1: Connect to IB and retrieve market data/entry price
            qualified, reqId = await asyncio.gather(
            self.ib.qualifyContractsAsync(contract),
            self.ib_manager.get_req_id()
            )
            if not qualified:
                logger.error(f"Contract qualification failed for {contract.symbol}")
                return None
            # Use the qualified contract instance directly
            qualified_contract = qualified[0]
            logger.info(
                f"Qualified contract for: {qualified_contract.symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}"
            )
            

            

           
        
           
            logger.info(
                f"Calculated position for {qualified_contract.symbol}: entry={entry_price},stop_loss={stop_loss_price}, quantity={quantity}"
            )

            stop_loss_price = stopLoss
           
            
            if quantity is None:
                logger.error(f"quantity is None for {symbol}")
                raise ValueError("Take profit price is None")
            if stop_loss_price is None:
                logger.error(f"Stop loss price is None for {symbol}")
                raise ValueError("Stop loss price is None")
            
            logger.info(
                f"Calculated position for {symbol}: entry={entry_price}, stop_loss={stop_loss_price},  quantity={quantity}"
            )
            
            
            algo_order = LimitOrder(
               action=action,
               totalQuantity=quantity,
               lmtPrice=round(entry_price, 2),
               tif="GTC",
               transmit=True,
               algoStrategy="Adaptive",
               outsideRth=False,
               orderRef=f"Adaptive - {symbol}",
               algoParams=[TagValue("adaptivePriority", "Urgent")],
           )

            # Step 3: Place the bracket order
            limit_order = LimitOrder(
                    action,
                    totalQuantity=quantity,
                    lmtPrice=round(entry_price, 2),
                    tif="GTC",
                    outsideRth=True,
                    transmit=True,
                    orderId=reqId,
                )
           
            logger.info(
                    f"Afterhours limit order for {symbol} at price {entry_price}"
                )
            ################################################################################################ 1
            market_hours = is_market_hours()
            if market_hours:
                logger.info(f"Market Hours Algo Limit order for {symbol} at price {entry_price}")
                order = algo_order
            else:
                logger.info(f"Afterhours limit order for {symbol} at price {entry_price}: {limit_order}")
                order = limit_order


            
            logger.info(
                f"Placing order with algoStrategy {order.algoStrategy} action: {order.action} order: {order.totalQuantity} @ {entry_price}"
            )
           
            order_details = {}
            if submit_cmd:
                p_ord_id = order.orderId
                logger.debug(f"jengo bracket order webhook p_ord_id: {p_ord_id}")

                # Determine the type of parent order based on market hours

                logger.debug(
                    f"Market hours - creating MarketOrder for{contract.symbol}"
                )
                parent = order
                logger.debug(f"Parent Order - creating MarketOrder for{parent}")

                # Place the orders

                trade = self.ib.placeOrder(qualified_contract, parent)
                if trade:
                    #await self.test_parent_filled(trade)  # Ensure the fill event is handled
                   
                    logger.info(
                        f"Parent Order placed for {qualified_contract.symbol}: {parent}"
                    )
                    trade.fillEvent += self.parent_filled
                    order_details = {
                        "ticker": qualified_contract.symbol,
                        "order Action": action,
                        "entry_price": order.lmtPrice,
                        
                        "quantity": order.totalQuantity,
                        "stopType": stopType,
                    }
                    logger.info(self.format_order_details_table(order_details))

                else:
                    logger.error(
                        f"Order placement failed for {contract.symbol}: {order}"
                    )

                trade_check = await self.ib.whatIfOrderAsync(qualified_contract, order)
                logger.debug(
                    f"Order details only for {symbol} with action {action} and stop type {stopType } trade_check= {trade_check}"
                )
                return trade

                
            if not submit_cmd:
                
                order_details = {
                    "ticker": qualified_contract.symbol,
                    "orderAction": web_request.orderAction,
                    "entry_price": entry_price,
                    "stop_loss_price": stop_loss_price,
                   
                    "quantity": quantity,
                    "rewardRiskRatio": web_request.rewardRiskRatio,
                    "riskPercentage": web_request.riskPercentage,
                    "accountBalance": web_request.accountBalance,
                    "stopType": stopType,
                    "atrFactor": atrFactor,
                }
                logger.info(self.format_order_details_table(order_details))

                return {"status": "order details only", "order_details": order_details}

        except Exception as e:
            logger.error(f"Error creating order: {e}")

    async def create_close_order(self, contract: Contract, action: str, qty: int):
        try:
            if not self.ib.isConnected():
                logger.info(
                    f"IB not connected in create_order, attempting to reconnect"
                )
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
            df = None
            bars = None
            order = None
            trade = None
            positions = None
            totalQuantity = None
            qualified_contract = None
            qualified_contract_final = None
            entry_price = None
            symbol = contract.symbol
            bid_price=self.bid_price[symbol]
            ask_price=self.ask_price[symbol]
            logger.info(
                f"Creating close order for {contract} wtih qty {qty} and action {action} ask_price: {ask_price}, bid_price: {bid_price}"
            )
            if bid_price is not None and ask_price is not None:
                entry_price = entry_price if action == "BUY" else bid_price
                
                
            
            whatToShow = "ASK" if action == "BUY" else "BID"

            # Use asyncio.gather with ohlc_vol instead of direct reqHistoricalDataAsync
            positions, qualified_contracts, bars = await asyncio.gather(
                self.ib.reqPositionsAsync(),
                self.ib.qualifyContractsAsync(contract),
                self.ohlc_vol(
                    contract=contract,
                    durationStr="300 S",
                    barSizeSetting="5 secs",
                    whatToShow=whatToShow,
                    useRTH=False,
                    return_df=False,
                ),
            )
            if bars is None:
                logger.error(f"No extended historical data for {contract.symbol}")
                return None
            # Cache the result for future calls
            if not hasattr(self, 'daily_bars'):
                self.daily_bars = {}
            self.daily_bars[symbol] = bars
            ohlc4_dv, hlc3_dv, hl2_dv, open_prices_dv, high_dv, low_dv, close_dv, vol_dv = bars
            
            df = util.df(bars) 

            logger.debug(
                f"Async data received for {symbol} with action {action} and qty {qty} close_dv: {close_dv}"
            )

            # Check if contract was qualified successfully
            if not qualified_contracts:
                logger.error(f"Could not qualify contract for {symbol}")
                return None

            # Initialize qualified_contract BEFORE using it
            qualified_contract = qualified_contracts[0]

            for pos in positions:
                if bid_price is not None and ask_price is not None:
                    entry_price = float(bid_price) if pos.position < 0 else float(ask_price)
                

                logger.info(
                    f"Checking position for: {pos.contract.symbol} and qualified_contract.symbol: {qualified_contract.symbol} with action {action} and qty {qty} and pos.position {pos.position}"
                )

                if (
                    qualified_contract.symbol == pos.contract.symbol
                    and pos.position != 0
                    and abs(pos.position) == qty
                ):
                    logger.info(
                        f"Position found for {symbol}: {pos.position} @ {pos.avgCost}"
                    )
                    totalQuantity = abs(pos.position)

                    if df is not None and not df.empty and entry_price is None:
                        entry_price = float(df.close.iloc[-1])

                    qualified_contract_final = qualified_contract

                    if not is_market_hours():
                        logger.info(f"Afterhours limit order for {symbol}")
                        order = LimitOrder(
                            action,
                            totalQuantity,
                            lmtPrice=entry_price,
                            tif="GTC",
                            outsideRth=True,
                            orderRef=f"Close Position - {symbol}",
                        )
                    else:
                        logger.info(
                            f"Market Hours Close order for {symbol} at price {entry_price}"
                        )
                        order = MarketOrder(action, totalQuantity)

                    logger.info(
                        f"Placing order with limit order for {symbol} at price {entry_price}"
                    )

                    trade = self.ib.placeOrder(qualified_contract_final, order)
                    if trade:
                        logger.info(f"Placed order for {symbol} with action {action}")
                        trade.fillEvent += self.ib_manager.on_fill_update
                        logger.info(f"Placed close order for {symbol}")
                        order_details = {
                            "ticker": symbol,
                            "action": action,
                            "entry_price": entry_price,
                            "quantity": pos.position,
                        }
                        logger.info(self.format_order_details_table(order_details))
                        return {"order_details": order_details}
                    else:
                        logger.error(f"Failed to place order for {symbol}")
                        logger.error(
                            f"Did shares match for {symbol}? : {pos.position != 0 and pos.contract == qty}"
                        )
                        return None

        except Exception as e:
            logger.error(f"Error creating order: {e}")
            return None

    
     # call get_rt_price() with fastAPI
    async def get_rt_price(
        self, contract, action: str, max_wait_seconds=5, keep_subscription=True, whatToShow="TRADES"):
        symbol = contract.symbol
        logger.info(f"Getting price from real-time bars for {symbol}")
        result_container = {"price": None, "source": None}

        # Get or create the real-time bar subscription.
        bars = await self.new_get_rt_bars(contract, symbol, keep_subscription, action, whatToShow)

        # If this is a new subscription (i.e. not stored already), attach the update callback.
        if symbol not in self.active_rtbars:
            logger.info(f"Attaching update event for {symbol}")
            bars.updateEvent += self.new_on_bar_update

        # Wait for a price update (via callback or direct check) up to max_wait_seconds.
        if symbol not in self.entry_prices.get(symbol, {}) and symbol in self.active_rtbars:
            logger.info(f"Entry price not found in cache for {symbol}, calling get_rt_price()")
            await self._wait_for_price(result_container, bars, symbol, action, max_wait_seconds)
      
        # Fallback: if no price yet and we're not keeping the subscription persistently.
        if result_container["price"] is None and not keep_subscription:
            logger.warning(
                f"Failed to get real-time price for {symbol}; using fallback"
            )
            fallback_price, source = await self._fallback_price(
                contract, symbol, action
            )
            result_container["price"] = fallback_price
            result_container["source"] = source

        # Cancel the temporary subscription if needed.
        if not keep_subscription and symbol not in self.active_rtbars and bars:
            self.ib.cancelRealTimeBars(bars)
            logger.info(f"Cancelled temporary real-time bar subscription for {symbol}")

        logger.info(
            f"Final entry price for {symbol}: {result_container['price']} (source: {result_container['source']})"
        )
        return result_container["price"]
    
    async def _wait_for_price(
        self, result_container, rtBars, symbol, action, max_wait_seconds
    ):
        start_time = time.time()
        while (
            result_container["price"] is None
            and time.time() - start_time < max_wait_seconds
        ):
            await asyncio.sleep(0.02)
            try:
                df = util.df(rtBars)
                if (
                    df is not None
                    and not df.empty
                    and "open_" in df.columns
                    and "close" in df.columns
                ):
                    logger.debug(f"Got dataframe with {len(df)} rows for {symbol}")
                    entry_long, entry_short = await self.calculate_entry_prices(df)
                    self.entry_prices[symbol] = {
                        "entry_long": entry_long,
                        "entry_short": entry_short,
                        "timestamp": datetime.now().isoformat(),
                    }
                    result_container["price"] = (
                        entry_long if action == "BUY" else entry_short
                    )
                    result_container["source"] = "direct"
                    logger.info(
                        f"Got self.entry_prices directly for {symbol}: {result_container['price']}"
                    )
                    break
            except Exception as e:
                logger.warning(f"Error getting data from rtBars for {symbol}: {e}")

    async def _fallback_price(self, contract, symbol, action):
        """
        Provides a fallback price using cached entry prices (if available) or historical data.
        Returns a tuple: (price, source)
        """
        # Use cached prices if available.
        try:
            cached_prices = None
            cached_prices_check = self.entry_prices.get(symbol)
            cached_prices = cached_prices_check if cached_prices_check is not None else None
            if cached_prices is None:
                logger.warning(f"Using cached entry price for {symbol} is none, calling ohlc_vol")
                try:
                    whatToShow = "ASK" if action == "BUY" else "BID"
                    df = await self.ohlc_vol(
                        contract=contract,
                        durationStr="60 S",
                        barSizeSetting="5 secs",
                        whatToShow=whatToShow,
                        useRTH=False,
                        return_df=True,
                    )
                    if df is not None and not df.empty:
                        last_price = float(df.close.iloc[-1])
                        entry_long = last_price
                        entry_short = last_price
                        cached_prices = last_price
                        
                        logger.warning(
                            f"Using historical fallback price for {symbol}: {last_price}"
                        )
                        return (
                            entry_long if action == "BUY" else entry_short,
                            "historical_fallback",
                        )
                    elif cached_prices is None:
                        logger.warning(f"No historical data available for {symbol}")
                        return (None, "no_data")
                    else:
                        logger.error(f"No historical data available for {symbol}")
                except Exception as e:
                    logger.error(f"Error getting historical fallback for {symbol}: {e}")
            else:
                return (
                    cached_prices["entry_long"] if action == "BUY" else cached_prices["entry_short"],
                    "cached",
                )         
               
            return (None, None)
        except Exception as e:
            logger.error(f"Error in fallback price logic for {symbol}: {e}")
            return (None, None)
    async def _get_initial_ema(self, contract):
        """
        Get initial 2-D historical data (1-min bars) and compute EMA.
        Returns the DataFrame (df_1) and the latest EMA.
        """
        durationStr = "2 D"
        barSizeSetting = "1 min"
        df_1 = await self.ohlc_vol(
            contract=contract,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow="TRADES",
            useRTH=False,
            return_df=True,
        )
        if df_1 is None:
            logger.error(f"No historical data available for {contract.symbol}")
            return None, None
        close=df_1["close"]
        df_1["ema"] = ta.ema(close, 20)
        ema = df_1.iloc[-1]["ema"]
        logger.info(f"Calculated EMA for {contract.symbol}: {ema}")
        return df_1, ema
    
    async def _compute_stop_loss(
        self,
        contract,
        orderAction,
        stopType,
        atrFactor,
        entry_price,
        provided_stopLoss,
        df_1,
        durationStr, 
        barSizeSetting, 
    ):
        """
        Compute the stop loss price if a valid one was not provided.
        Returns a tuple: (stop_loss_price, lower, upper, updated_stopType)
        """
        vstop_val = None
        uptrend = None
        symbol = contract.symbol
        stop_loss_price = None
        lower = None
        upper = None
        updated_stopType = stopType
        keltner = None
        vstop_df = None
        pivot_points = None

        # Use provided stop loss if greater than zero.
        # Only use the provided stop loss for manual orders.
        if stopType == "manual" and provided_stopLoss > 0:
            return round(provided_stopLoss, 2), None, None, stopType


        # --- Use Cached Extended (Daily) Data First ---
        if hasattr(self, 'daily_bars') and symbol in self.daily_bars:
            ohlc_data = self.daily_bars[symbol]
            logger.info(f"Using cached daily bars for {symbol}")
        else:
            
            ohlc_data = await self.ib.reqHistoricalDataAsync(
                contract=contract,
                endDateTime="",
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow="TRADES",
                useRTH=False,
            )
            
            if ohlc_data is None:
                logger.error(f"No extended historical data for {contract.symbol}")
                return None, None, None, stopType
            # Cache the result for future calls
            df_1 = pd.DataFrame(ohlc_data)
            
            self.daily_bars[symbol] = ohlc_data
            

      

        # Ensure contract is a proper Contract instance.
        if not hasattr(contract, "symbol"):
            logger.warning("Contract is not a Contract instance; attempting to convert it.")
            try:
                contract = Contract(**contract)
            except Exception as e:
                logger.error(f"Failed to convert contract dict to Contract: {e}")
                return None, None, None, stopType

       

        # --- Get indicator values based on stopType ---
        if stopType == "kcStop":
            keltner = await self.kc_bands(contract=contract, multiplier=atrFactor)
        if stopType == "vStop":
            vstop_val, uptrend = await volatility_stop(ohlc_data, length=20, atr_multiplier=atrFactor)
        if stopType == "pivot":
            pivot_points = await detect_pivot_points(df_1, left_bars=5, right_bars=5)

        # Process Keltner Channels if available.
        if keltner is not None:
            # If df_1 is a DataFrame, add channel columns.
            if isinstance(df_1, pd.DataFrame):
                df_1["KC_Middle"] = keltner["middle_band"]
                df_1["KC_Upper"] = keltner["upper_band"]
                df_1["KC_Lower"] = keltner["lower_band"]

                latest = df_1.iloc[-1]
                lower = latest["KC_Lower"]
                upper = latest["KC_Upper"]
            else:
                # If not a DataFrame (e.g. a float), use the keltner values directly.
                lower = keltner["lower_band"]
                upper = keltner["upper_band"]

            stop_loss_price = lower if orderAction == "BUY" else upper
            logger.info(f"Keltner Channels for {contract.symbol}: lower: {lower} - upper {upper}")

        # Determine stop loss from volatility stop.
        if vstop_val is not None:
            logger.info(f"Volatility Stop DataFrame for {contract.symbol}: {vstop_val}")
            
            if stopType == "vStop":
                if orderAction == "BUY" and uptrend:
                    stop_loss_price = vstop_val
                elif orderAction == "SELL" and not uptrend:
                    stop_loss_price = vstop_val
                elif (orderAction == "SELL" and uptrend) or (orderAction == "BUY" and not uptrend):
                    updated_stopType = "kcStop"
                    if keltner is None:
                        keltner = await self.kc_bands(contract=contract, multiplier=atrFactor)
                        if keltner is not None:
                            lower = keltner["lower_band"]
                            upper = keltner["upper_band"]
                    stop_loss_price = lower if orderAction == "BUY" else upper

        # Adjust using pivot points if selected.
        if pivot_points is not None:
            high_pivots = pivot_points.get("high_pivots", [])
            low_pivots = pivot_points.get("low_pivots", [])
            logger.info(f"Pivot Points for {contract.symbol}: high_pivots: {high_pivots} - low_pivots {low_pivots}")
            if orderAction == "BUY" and updated_stopType == "pivot" and stop_loss_price is None:
                if low_pivots:
                    stop_loss_price = low_pivots[0]["value"] - 0.02
                else:
                    stop_loss_price = entry_price * 0.98
            elif orderAction == "SELL" and updated_stopType == "pivot" and stop_loss_price is None:
                if high_pivots:
                    stop_loss_price = high_pivots[0]["value"] + 0.02

        # Fallback if still undefined.
        if stop_loss_price is None:
            stop_loss_price = entry_price * 0.99 if orderAction == "BUY" else entry_price * 1.01

        stop_loss_price = round(float(stop_loss_price), 2)
        return stop_loss_price, lower, upper, updated_stopType



    def _compute_position_size(
        self,
        entry_price,
        stop_loss_price,
        accountBalance,
        riskPercentage,
        rewardRiskRatio,
        orderAction,
    ):
        """
        Compute the broker commission, per-share risk, tolerated risk, quantity and take profit price.
        Note: rewardRiskRatio is used only to calculate the take profit.
        """
        brokerComish = max(round((accountBalance / entry_price) * 0.005), 1.0)
        perShareRisk = abs(entry_price - stop_loss_price)
        if perShareRisk < 0.01:
            logger.warning(
                f"Per share risk ({perShareRisk}) is too small; defaulting to 1% of entry price"
            )
            perShareRisk = entry_price * 0.01

        toleratedRisk = abs((riskPercentage / 100 * accountBalance) - brokerComish)
        quantity = round(
            min(
                toleratedRisk / perShareRisk,
                math.floor((accountBalance * 0.95) / entry_price),
            )
        )
        if quantity <= 0:
            logger.warning(
                f"Calculated quantity ({quantity}) is invalid; using minimum quantity of 1"
            )
            quantity = 1

        # Use rewardRiskRatio only to calculate take profit
        if orderAction == "BUY":
            take_profit_price = round(entry_price + (perShareRisk * rewardRiskRatio), 2)
        else:
            take_profit_price = round(entry_price - (perShareRisk * rewardRiskRatio), 2)

        return quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk

    async def calculate_position(
        self,
        contract,
        entry_price: float,
        orderAction: str,
        stopType: str,
        atrFactor: float,
        riskPercentage: float,
        accountBalance: float,
        rewardRiskRatio: float,
        stopLoss: float,
        durationStr, 
        barSizeSetting,
        historical_df=None  # Make this parameter optional
    ):
        try:
            ema = {}
            symbol = contract.symbol
    
            # Validate historical data
            df_to_use = historical_df if historical_df is not None else self.historical_df_one_min.get(symbol)
            if df_to_use is None or (hasattr(df_to_use, 'empty') and df_to_use.empty):
                logger.error(f"No historical data available for {symbol}")
                return {
                    "stop_loss_price": entry_price * 0.99 if orderAction == "BUY" else entry_price * 1.01,
                    "quantity": 1,
                    "take_profit_price": entry_price * 1.02 if orderAction == "BUY" else entry_price * 0.98,
                    "ema": self.twenty_ema.get(symbol, None),
                    "upper": None,
                    "lower": None,
                    "stopType": stopType,
                }
        
        
            # Make sure arrays have data before indexing
            
            #logger.warning(f"Checking historical data for {symbol}: df_to_use: {df_to_use} ")
            computed_stop_loss, lower, upper, updated_stopType = await self._compute_stop_loss(
                contract, orderAction, stopType, atrFactor, entry_price, stopLoss, df_to_use, durationStr, barSizeSetting
            )
        
            if computed_stop_loss is None:
                computed_stop_loss = entry_price * 0.99 if orderAction == "BUY" else entry_price * 1.01
                logger.warning("Failed to compute stop loss, using default value")
               

            # Compute the position size as before.
            quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = (
                self._compute_position_size(
                    entry_price,
                    computed_stop_loss,
                    accountBalance,
                    riskPercentage,
                    rewardRiskRatio,
                    orderAction,
                )
            )
            # Get cached EMA if available
            if self.twenty_ema.get(symbol, None) is not None:
                ema = self.twenty_ema.get(symbol, None)

            logger.info(
                f"Position for {contract.symbol}: entry={entry_price}, stop_loss={computed_stop_loss}, "
                f"take_profit={take_profit_price}, quantity={quantity}, brokerComish={brokerComish}, "
                f"toleratedRisk={toleratedRisk}, perShareRisk={perShareRisk}, ema={ema}"
            )

            return {
                "stop_loss_price": computed_stop_loss,
                "quantity": quantity,
                "take_profit_price": take_profit_price,
                "ema": ema,
                "upper": upper,
                "lower": lower,
                "stopType": updated_stopType,
            }
        except Exception as e:
            logger.error(f"Error calculating position: {e}", exc_info=True)
            return None
        
    async def get_qualified_contract(self, contract: Contract):
        try:
         
            qualified_contract = self.qualified_contract_cache.get(contract.symbol)
            if not qualified_contract:
                qualified = await self.ib.qualifyContractsAsync(contract)
                if not qualified:
                    logger.error(f"Contract qualification failed for {contract.symbol}")
                    return None
                qualified_contract = qualified[0]
                self.qualified_contract_cache[contract.symbol] = qualified_contract
            symbol=qualified_contract.symbol
            return qualified_contract
        except Exception as e:
            logger.error(f"Error qualifying contract for {contract.symbol}: {e}", exc_info=True)
            return None
    async def onPendingTickers(self, df, tickers):
        for t in tickers:
            logger.info(f"Processing pending ticker for {t.contract.symbol} self.entry_prices: { self.close_price[t.contract.symbol] if t.contract.symbol in self.close_price else 'not set'}")
            row_data = {
                'bidSize': t.bidSize,
                'bid': t.bid,
                'ask': t.ask,
                'askSize': t.askSize,
                'high': t.high,
                'low': t.low,
                'close': t.close
            }
            self.bid_price[t.contract.symbol] = t.bid
            self.ask_price[t.contract.symbol] = t.ask
            logger.debug(await self.format_ticker_details_table(row_data))
            row_data_series = pd.Series(row_data)
            if not row_data_series.isna().all():  # Only assign if not all values are NA
                df.loc[t.contract.symbol] = row_data
            self.ticker[t.contract.symbol] = t  # Store the ticker object for further use.
            #logger.info(f"Pending ticker update for {t.contract.symbol}: {self.ticker[t.contract.symbol]}")
            
    async def req_mkt_data(self, contract: Contract):
        try:
            self.ib.newOrderEvent += self.on_new_order_event
            # Ensure IB is connected and qualify contract.
            if not self.ib.isConnected():
                logger.info("IB not connected in req_mkt_data, attempting to reconnect")
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
            qualified_contract = await self.get_qualified_contract(contract)
            if qualified_contract is None:
                logger.error(f"Failed to qualify contract for {contract.symbol}")
                return None
            symbol = qualified_contract.symbol
            logger.info(f"Getting market data for {symbol}")

            # Request market data and attach ticker updates.
            tickers=self.ib.reqMktData(qualified_contract, "", True, False)
            self.ticker[symbol] = self.ib.ticker(qualified_contract)
            logger.debug(f"Market data requested for {symbol}, sleeping 2 seconds to allow population...")
            
        
            df = pd.DataFrame(index=[symbol], columns=['bidSize', 'bid', 'ask', 'askSize', 'high', 'low', 'close'])
            tickers.updateEvent += lambda tickers: asyncio.create_task(
                self.onPendingTickers(df, tickers)
            )
        
            # Start subscriptions for historical data (for indicator calculations)
            historical, df_ema = await asyncio.gather(
                self.ohlc_vol(
                    qualified_contract,
                    durationStr="35 D",
                    barSizeSetting="1 day",
                    whatToShow="TRADES",
                    useRTH=True,
                    return_df=True,
                ),
                self.ohlc_vol(
                    qualified_contract,
                    durationStr="2 D",
                    barSizeSetting="1 min",
                    whatToShow="TRADES",
                    useRTH=False,
                    return_df=True,
                )
            )
            self.historical_df_day[symbol] = historical
            self.historical_df_one_min[symbol] = df_ema
            logger.info(f"Historical data received for {symbol}")
            if historical is None or historical.empty:
                logger.error(f"No historical data available for {symbol}")
                return {"error": "No historical data available"}
        
            # Convert historical 1-min bars to a DataFrame and update prices.
            df = pd.DataFrame(df_ema)
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_localize(None)
            elif 'date' in df.columns:
                df['time'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
            else:
                df['time'] = pd.Timestamp.now()

            df['minute'] = df['time'].dt.floor('min')
            logger.debug(f"DataFrame after processing time column: {df.head()}")
        
            # Determine which OHLC columns to use.
            open_col = 'open' if 'open' in df.columns else ('open_' if 'open_' in df.columns else None)
            high_col = 'high' if 'high' in df.columns else ('high_' if 'high_' in df.columns else None)
            low_col = 'low' if 'low' in df.columns else ('low_' if 'low_' in df.columns else None)
            close_col = 'close' if 'close' in df.columns else None
            volume_col = 'volume' if 'volume' in df.columns else None
            if None in (open_col, high_col, low_col, close_col, volume_col):
                raise KeyError("Missing one or more required OHLC or volume columns in historical data")
            logger.debug(f"Using columns: open_col={open_col}, high_col={high_col}, low_col={low_col}, close_col={close_col}, volume_col={volume_col}")
            row = df.iloc[-1]
            self.open_price[symbol] = row.get("open_") or row.get("open")
            self.high_price[symbol] = row.get("high_") or row.get("high")
            self.low_price[symbol] = row.get("low_") or row.get("low")
            self.close_price[symbol] = row.get("close")
            self.volume[symbol] = row.get("volume")
        
            # Aggregate the 5-sec bars (here using the historical 1-min df for initialization).
            agg_df = df.groupby('minute').agg({
                open_col: 'first',
                high_col: 'max',
                low_col: 'min',
                close_col: 'last',
                volume_col: 'sum'
            }).reset_index().rename(columns={
                open_col: 'open',
                high_col: 'high',
                low_col: 'low',
                close_col: 'close',
                volume_col: 'volume'
            })
            if not hasattr(self, 'minute_bars'):
                self.minute_bars = {}
            if symbol not in self.minute_bars:
                self.minute_bars[symbol] = agg_df
            else:
                self.minute_bars[symbol] = pd.concat([self.minute_bars[symbol], agg_df]).drop_duplicates(subset='minute')
                self.minute_bars[symbol] = self.minute_bars[symbol].sort_values(by='minute')
            minute_df = self.minute_bars[symbol].tail(9)
            logger.debug(f"Aggregated 1-min bars for {symbol}: {minute_df}")
            try:
                # Compute indicator values (EMA, ATR, etc.).
                df_20 = pd.DataFrame(df_ema)
                close=df_20["close"]
                df_20["ema"] = ta.ema(close, 20)
                self.twenty_ema[symbol] = df_20.iloc[-1]["ema"]
                # Compute EMA9 using the aggregated 1-minute bars (minute_df)
                ema_values = ta.ema(close, 9)
                logger.debug(f"EMA9 values for {symbol}: {ema_values}")
                if ema_values is None:
                    self.nine_ema[symbol] = None
                else:
                    self.nine_ema[symbol] = ema_values.iloc[-1]
                ema_current = self.nine_ema[symbol]

                logger.info(f"EMA9 for {symbol}: {ema_current} self.nine_ema[symbol] {self.nine_ema[symbol]}")
            except Exception as e:
                logger.error(f"Error computing indicators: {e}")
                return {"error": "Failed to compute indicators"}
            # Compute entry prices using the last aggregated 1-min bar.
            latest_bar = minute_df.iloc[-1].to_dict()
            latest_close = latest_bar['close']
            ohlc4 = (latest_bar['open'] + latest_bar['high'] + latest_bar['low'] + latest_bar['close']) / 4
            buffer = 0.00015 * ohlc4
            entry_long = latest_close + buffer
            entry_short = latest_close - buffer
        
            self.entry_prices[symbol] = {
                "entry_long": entry_long,
                "entry_short": entry_short,
                "timestamp": datetime.now().isoformat(),
                "ema9": ema_current,
                "ema20": self.twenty_ema[symbol],
                "minute_bars": minute_df.to_dict(orient='records')
            }
        
            # Build and return contract JSON.
            contract_json = {
                "symbol": symbol,
                "secType": qualified_contract.secType,
                "currency": qualified_contract.currency,
                "exchange": qualified_contract.exchange,
                "localSymbol": qualified_contract.localSymbol,
                "primaryExchange": qualified_contract.primaryExchange,
                "conId": qualified_contract.conId,
                "lastTradeDateOrContractMonth": qualified_contract.lastTradeDateOrContractMonth,
                "entry_long": self.entry_prices[symbol]["entry_long"],
                "entry_short": self.entry_prices[symbol]["entry_short"],
                "timestamp": self.entry_prices[symbol]["timestamp"],
            }
            return contract_json

        except Exception as e:
            logger.error(f"Error in req_mkt_data: {e}")
            if not self.ib.isConnected():
                logger.info("Reconnecting to IB...")
                await self.ib_manager.connect()
            return {"error": str(e)}

    async def on_new_order_event(self, trade: Trade):
        try:
            logger.debug(f"Order status event received for {trade.contract.symbol}")
            order_details = {
                "order_id": trade.contract.symbol if hasattr(trade.contract, 'symbol') else 'unknown',
                "order_action": trade.order.action,
                "order_type": trade.order.orderType,
                "algoStrategy": trade.order.algoStrategy,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp()
            }
            
            logger.info(self.format_order_details_table(order_details))

           
           
        except Exception as e:
            logger.error(f"Error in order status event: {e}")
    async def get_entry_prices(self, symbol):
        """
        Get the latest entry prices for a given symbol.
        Returns a dictionary with entry_long and entry_short, or None if not available.
        """
        if symbol in self.entry_prices:
            return self.entry_prices[symbol]
        return None

    # Improved code with fallbacks
    async def calculate_entry_prices(self, df_all: pd.DataFrame, whatToShow="TRADES"):
        if df_all is None or df_all.empty:
            logger.warning("Empty dataframe passed to calculate entry prices")
            return None, None

        try:
            logger.info("Calculating entry prices from DataFrame")
            # Use the last row of the dataframe for OHLC values
            row = df_all.iloc[-1]
        
            # Check for column variations (IB sometimes uses 'open_' instead of 'open')
            open_val = row.get("open") if "open" in row else row.get("open_")
            high_val = row.get("high") if "high" in row else row.get("high_")
            low_val = row.get("low") if "low" in row else row.get("low_")
            close_val = row.get("close")
        
            # If any values are still missing, use fallbacks or defaults
            if close_val is None:
                logger.warning("Close price missing, cannot calculate entry prices")
                return None, None
            
            # Use reasonable defaults for missing values based on close price
            if open_val is None:
                open_val = close_val
                logger.warning(f"Open price missing, using close price: {close_val}")
            if high_val is None:
                high_val = max(open_val, close_val)
                logger.warning(f"High price missing, using max of open/close: {high_val}")
            if low_val is None:
                low_val = min(open_val, close_val)
                logger.warning(f"Low price missing, using min of open/close: {low_val}")
        
            # Continue with the rest of the function...
            ohlc4 = (open_val + high_val + low_val + close_val) / 4
            logger.info(f"OHLC4 : {ohlc4}")

            buffer = 0.00015 * ohlc4
            logger.warning(f"Calculated buffer: {buffer}")
            entry_long = close_val + buffer
            entry_short = close_val - buffer

            logger.info(f"Calculated entry prices: close {close_val}, long={entry_long}, short={entry_short} based on ohlc4={ohlc4}, buffer={buffer}")
            return entry_long, entry_short
        except Exception as e:
            logger.error(f"Error in calculate entry prices: {e}")
            return None, None

    async def add_oca_bracket(
        self,
        contract: Contract,
        order: Trade,
        action: str,
        entry_price: float,
        quantity: float,
        stopType: str,
        stopLoss: float,
    ):
        try:
            # Get symbol and timestamp ID
            symbol = contract.symbol
            logger.info(
                f"Adding OCA bracket for {symbol} with action {action} and stopLoss {stopLoss}"
            )

            # Get the timestamp ID directly from the dictionary with the symbol as key
            if symbol not in self.timestamp_id:
                logger.error(f"No timestamp ID found for {symbol}")
                return None

            # Get timestamp data for this symbol
            timestamp_data = self.timestamp_id[symbol]

            priceType = None
            entry_prices = None
            bracket_orders = None

            # Get current price from entry_prices if available
            current_price = None
            if entry_price is not None:
                current_price = entry_price
                priceType = "arg entry_price"
                logger.debug(f"Using provided entry price for {symbol}: {current_price}")
            elif (entry_price is None or entry_price <= 0) and self.close_price.get(symbol) is not None:
                entry_prices = await self.get_entry_prices(symbol)
                if entry_prices is None:
                    logger.warning(f"Entry prices not available for {symbol}")
                    current_price = self.close_price[symbol]
                    
                else:
                    priceType = "ib.postions"
                    latest_positions_oca = await self.ib.reqPositionsAsync()
                    for pos in latest_positions_oca:
                        if pos.contract.symbol == symbol:
                            entry_prices = {
                                "entry_long": pos.avgCost if action == "BUY" else None,
                                "entry_short": pos.avgCost if action == "SELL" else None,
                                "timestamp": datetime.now().isoformat(),
                            }
                            current_price = entry_prices["entry_long"] if action == "BUY" else entry_prices["entry_short"]
                            break
                 
            logger.debug(f"Timestamp ID for {symbol}: {timestamp_data}")

            if current_price is None:
                logger.error(f"Current price is None for {symbol} after checking entry prices")
                return None
               

            if current_price is not None:
               

                priceDiff = ((current_price - entry_price) / entry_price) * 100
                priceAvg = (current_price + entry_price) / 2
                logger.info(
                    f"Entry price for {symbol}: {entry_price} current price: {current_price} priceDiff: {priceDiff} priceAvg: {priceAvg} with stopType {stopType}"
                )
                barSizeSetting= timestamp_data["barSizeSetting"]

                # Calculate position details
                pos = await self.calculate_position(
                    contract,
                    priceAvg,
                    action,
                    stopType,
                    timestamp_data["atrFactor"],
                    timestamp_data["riskPercentage"],
                    timestamp_data["accountBalance"],
                    timestamp_data["rewardRiskRatio"],
                    stopLoss,
                    durationStr = '1 D', 
                    barSizeSetting= barSizeSetting,
                )

                if not pos:
                    logger.error(f"Could not calculate position for {symbol}")
                    return None

                stop_loss_price = pos["stop_loss_price"]
                take_profit_price = pos["take_profit_price"]

                # Place the bracket order
                p_ord_id = order.orderId
                reverseAction = "BUY" if order.action == "SELL" else "SELL"

                # Create the take profit (limit) and stop loss orders
                take_profit = LimitOrder(
                    reverseAction, order.totalQuantity, take_profit_price
                )
                stop_loss = StopOrder(reverseAction, order.totalQuantity, stop_loss_price)

                if take_profit_price is None:
                    logger.error(f"Take profit price is None for {symbol}")
                    raise ValueError("Take profit price is None")
                if stop_loss_price is None:
                    logger.error(f"Stop loss price is None for {symbol}")
                    raise ValueError("Stop loss price is None")

                logger.info(
                    f"Creating bracket orders for {symbol}: entry={entry_price}, stop_loss={stop_loss_price}, take_profit={take_profit_price}, quantity={order.totalQuantity}"
                )

                orders = [take_profit, stop_loss]
                # Group them in an OCA group using oneCancelsAll
                oca_group = f"exit_{symbol}_{p_ord_id}"

                # Create the OCA bracket orders
                try:
                    bracket_orders = self.ib.oneCancelsAll(orders, oca_group, 1)
                    logger.debug(
                        f"Created OCA bracket orders for {symbol} with group {oca_group}"
                    )
                except Exception as e:
                    logger.error(f"Failed to create OCA bracket orders: {e}")
                    return None

            # Place each order
            if bracket_orders is not None:
                for bracket_order in bracket_orders:
                    logger.info(
                        f"Placing order: {symbol} {bracket_order.action} {bracket_order.totalQuantity} @ {bracket_order.lmtPrice if hasattr(bracket_order, 'lmtPrice') else bracket_order.auxPrice}"
                    )
                    trade = self.ib.placeOrder(contract, bracket_order)
                    if trade:
                        
                        # Remove the timestamp ID data after successful order placement
                        if symbol in self.timestamp_id:
                            del self.timestamp_id[symbol]

                        logger.info(f"Order placed for {symbol}: {bracket_order} priceType = {priceType}")
                        trade.fillEvent += self.ib_manager.on_fill_update
                    else:
                        logger.error(
                            f"Order placement failed for {symbol}: {bracket_order}"
                        )

                return bracket_orders
            else:
                logger.error(f"No bracket orders created for {symbol}")
                return None

        except Exception as e:
            logger.error(f"Error in add oca bracket: {e}")
            return None

    async def place_web_bracket_order(
        self,
        contract: Contract,
        order: Trade,
        action: str,
        exit_stop: float,
        exit_limit: float,
        stopType,
    ):
        async with self.semaphore:
            try:
                logger.info(f"jengo bracket order webhook tv_hook {contract.symbol}")
                reverseAction = "BUY" if action == "SELL" else "SELL"

                # Retrieve the qualified contract from the cache using the contract's symbol.
                qualified_contract = self.qualified_contract_cache.get(contract.symbol)
                if not qualified_contract:
                    logger.error(f"Qualified contract not found in cache for {contract.symbol}")
                    return None

                p_ord_id = order.orderId
                logger.debug(f"jengo bracket order webhook p_ord_id: {p_ord_id}")

                # Use the cached qualified_contract for placing the order.
                parent = order
                trade = self.ib.placeOrder(qualified_contract, parent)
                if trade:
                    logger.info(f"Order placed for {qualified_contract.symbol}: {parent}")
                    trade.fillEvent += self.parent_filled
                    order_details = {
                        "ticker": qualified_contract.symbol,
                        "order Action": action,
                        "entry_price": order.lmtPrice,
                        "stop_loss_price": exit_stop,
                        "take_profit_price": exit_limit,
                        "quantity": order.totalQuantity,
                        "stopType": stopType,
                    }
                    logger.info(self.format_order_details_table(order_details))
                else:
                    logger.error(f"Order placement failed for {contract.symbol}: {order}")
                return trade
            except Exception as e:
                logger.error(f"Error processing bracket order for {contract.symbol}: {str(e)}", exc_info=True)
                return False

    async def parent_filled(self, trade: Trade, fill):
        try:
            fill_contract = None
            fill_symbol = None

            # Extract permId early for deduplication check
            permId = trade.order.permId

            # Initialize tracking dictionary if not exists
            if not hasattr(self, "triggered_bracket_orders"):
                self.triggered_bracket_orders = {}

            # Initialize lock if not exists
            if not hasattr(self, "fill_lock"):
                self.fill_lock = asyncio.Lock()

            # Use a lock to prevent race conditions with multiple fill events
            async with self.fill_lock:
                # Check if we've already processed this permId
                if permId in self.triggered_bracket_orders:
                    logger.debug(
                        f"Bracket order already triggered for permId {permId}, skipping duplicate call."
                    )
                    return

                # Mark this permId as being processed BEFORE any async operations
                # This prevents other concurrent calls from proceeding
                self.triggered_bracket_orders[permId] = True

                logger.info(
                    f"parent_filled Fill received for {fill.contract.symbol} with entry price. Sending Bracket for trade.order {trade.order}"
                )

                if fill.contract is not None:
                    logger.info(
                        f"jengo bracket order webhook fill.contract: {fill.contract.symbol}"
                    )
                    fill_contract = fill.contract
                    fill_symbol = fill.contract.symbol
                if fill.contract is None:
                    logger.info(
                        f"jengo bracket order webhook trade.contract: {trade.contract.symbol}"
                    )
                    fill_contract = trade.contract
                    fill_symbol = trade.contract.symbol

                entry_price = float(fill.execution.avgPrice) 
                action = trade.order.action
                order = trade.order
                algoStrategy = trade.order.algoStrategy
                logger.debug(
                    f"jengo bracket order webhook entry_price: {entry_price} for order {order} and permId {permId} and algoStrategy {algoStrategy}"
                )

                quantity = float(trade.order.totalQuantity)

                try:
                    logger.info(
                        f"jengo Get latest positions from TWS (only positions with nonzero quantity): stopLoss {self.timestamp_id.get(fill_symbol, {}).get('stopLoss', 0.0)} for permId {permId}"
                    )
                    latest_positions_oca = await self.ib.reqPositionsAsync()

                    # Optional: Get open orders and trades if needed for verification
                    open_orders_list_id = await self.ib.reqAllOpenOrdersAsync()
                    logger.info(
                        f"jengo Get latest positions from TWS (only positions with nonzero quantity): open_orders_list_id {len(open_orders_list_id)}"
                    )
                    self.orders_dict_oca_symbol = {
                        o.order.permId: o for o in open_orders_list_id
                    }
                    logger.info(f" jengo open_orders_list_id: {self.orders_dict_oca_symbol.keys()}")
                    completed_trades_oca = self.ib.openTrades()

                    stopType = self.timestamp_id.get(fill_symbol, {}).get("stopType")
                    stopLoss = self.timestamp_id.get(fill_symbol, {}).get(
                        "stopLoss", 0.0
                    )
                    logger.info(
                        f" jengo parent_filled stopType {stopType} and stopLoss {stopLoss}"
                    )
                    try:
                        logger.info(f" Checking for active positions for {fill_symbol} with stopType {stopType} and stopLoss {stopLoss}")

                        # Now loop through the positions
                        for position in latest_positions_oca:
                            logger.info(
                                f"Position for: {position.contract.symbol} at shares of {position.position}"
                            )
                            if (
                                position.contract.symbol == fill_symbol
                                and position.position != 0
                            ):
                                logger.info(f" Found active position for {fill_symbol}: {position.position} shares")
                                # For Adaptive algoStrategy, trigger the bracket order
                                if algoStrategy == "Adaptive" or not is_market_hours():
                                    logger.info(
                                        f"Placing oca bracket order for {fill_symbol} with permId {permId} and stopType {stopType} and stopLoss {stopLoss}"
                                    )
                                    oca_order = await self.add_oca_bracket(
                                        fill_contract,
                                        order,
                                        action,
                                        entry_price,
                                        quantity,
                                        stopType,
                                        stopLoss,
                                    )
                                    return oca_order
                                else:
                                    logger.info(
                                        f"Skipping {fill_symbol}: algoStrategy {algoStrategy} does not require oca bracket order and stopType {stopType} and stopLoss {stopLoss}"
                                    )
                    except Exception as e:
                        logger.error(
                            f"Error while checking positions for {fill_symbol}: {e}",
                            exc_info=True,
                        )
                        

                    # If we got here, no matching position was found
                    logger.warning(
                        f"No active position found for {fill_symbol} when placing bracket orders"
                    )

                except Exception as e:
                    logger.error(
                        f"Error in conditions for oca bracket_orders in fill event: {e}"
                    )
                    # If there's an error, we should remove the permId from the triggered dictionary
                    # so that it can be retried
                    if permId in self.triggered_bracket_orders:
                        del self.triggered_bracket_orders[permId]

        except Exception as e:
            logger.error(f"Error updating positions on fill: {e}")
            # Global exception handling should also clean up the triggered dictionary
            if (
                "permId" in locals()
                and hasattr(self, "triggered_bracket_orders")
                and permId in self.triggered_bracket_orders
            ):
                del self.triggered_bracket_orders[permId]
    

    async def new_get_rt_bars(self, contract, symbol, keep_subscription, action, whatToShow):
        try:
            result_container = {"price": None, "source": None}

            whatToShow = "TRADES"

            logger.info(f"Creating new real-time bar subscription for {symbol}")
            rtBars = self.ib.reqRealTimeBars(
                contract=contract,
                barSize=5,
                whatToShow=whatToShow,
                useRTH=False,
                realTimeBarsOptions=[],
            )
            if keep_subscription:
                #rtBars.updateEvent += self.new_on_bar_update
                rtBars.updateEvent += lambda bars, hasNewBar: asyncio.create_task(
                    self.new_on_bar_update(bars, hasNewBar, symbol, action, result_container, whatToShow)
                )
                self.active_rtbars[symbol] = rtBars
                logger.info(
                    f"Created and stored new real-time bar subscription for {symbol}"
                )
            return rtBars
        except Exception as e:
            logger.error(f"Error in get_rt_bars: {e}")
            return None
    
    async def new_on_bar_update(self, bars, hasNewBar, symbol, action, result_container, whatToShow="TRADES"):
        try:
            # Use the qualified contract from cache if available.
            if symbol in self.qualified_contract_cache:
                qualified_contract = self.qualified_contract_cache[symbol]
            else:
                default_contract = {
                    "symbol": symbol,
                    "exchange": "SMART",
                    "secType": "STK",
                    "currency": "USD",
                }
                ib_contract = Contract(**default_contract)
                qualified = await self.ib.qualifyContractsAsync(ib_contract)
                if not qualified:
                    logger.error(f"Contract qualification failed for {symbol}")
                    return None
                qualified_contract = qualified[0]
                self.qualified_contract_cache[symbol] = qualified_contract

            if not hasNewBar:
                logger.debug(f"No new bar update for {symbol}")
                return None

            df = util.df(bars)
            if df is None or df.empty:
                logger.warning(f"Empty dataframe for {symbol}")
                return None
            # Ensure DataFrame is in pandas format.
            df = pd.DataFrame(bars)
            if df.empty:
                logger.warning(f"Empty dataframe received in on_bar_update for {symbol}")
                return

            # Convert time column to tz-naive timestamps.
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_localize(None)
            elif 'date' in df.columns:
                df['time'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
            else:
                df['time'] = pd.Timestamp.now()

            # Add this line to create the minute column for groupby
            df['minute'] = df['time'].dt.floor('min')

            # Determine OHLC column names.
            open_col = 'open' if 'open' in df.columns else ('open_' if 'open_' in df.columns else None)
            high_col = 'high' if 'high' in df.columns else ('high_' if 'high_' in df.columns else None)
            low_col = 'low' if 'low' in df.columns else ('low_' if 'low_' in df.columns else None)
            close_col = 'close' if 'close' in df.columns else None
            volume_col = 'volume' if 'volume' in df.columns else None
            if None in (open_col, high_col, low_col, close_col, volume_col):
                raise KeyError("One or more required OHLC or volume columns are missing in the realtime bar data")

            # ...rest of the function remains the same...
            self.ticker[symbol] = df.iloc[-1]  # Update the ticker with the latest bar data

            row = df.iloc[-1]
            self.open_price[symbol] = row.get("open_") or row.get("open")
            self.high_price[symbol] = row.get("high_") or row.get("high")
            self.low_price[symbol] = row.get("low_") or row.get("low")
            self.close_price[symbol] = row.get("close")
            self.volume[symbol] = row.get("volume")
            logger.debug(f"Received bar update for {symbol}, close: {self.close_price[symbol]}")

            # Aggregate the 5-second bars into 1-minute bars.
            agg_df = df.groupby('minute').agg({
                open_col: 'first',
                high_col: 'max',
                low_col: 'min',
                close_col: 'last',
                volume_col: 'sum'
            }).reset_index().rename(columns={
                open_col: 'open',
                high_col: 'high',
                low_col: 'low',
                close_col: 'close',
                volume_col: 'volume'
            })
            if not hasattr(self, 'minute_bars'):
                self.minute_bars = {}
            if symbol not in self.minute_bars:
                self.minute_bars[symbol] = agg_df
            else:
                self.minute_bars[symbol] = pd.concat([self.minute_bars[symbol], agg_df]).drop_duplicates(subset='minute')
                self.minute_bars[symbol] = self.minute_bars[symbol].sort_values(by='minute')
            minute_df = self.minute_bars[symbol].tail(9)
            logger.debug(f"Updated minute bars for {symbol}: {minute_df}")

            
            # Compute indicators (EMA and ATR) using recent historical data.
            try:
                logger.debug(f"Computing indicators for {symbol} using minute_df")
                ema_bars = await self.ib.reqHistoricalDataAsync(
                    qualified_contract, endDateTime='', durationStr='1 D',
                    barSizeSetting='1 min', whatToShow='TRADES', useRTH=False
                )
                df_ema = util.df(ema_bars)
                close=df_ema["close"]
                # Compute EMA9 using the latest aggregated minute bars
                ema_values = ta.ema(close, 9)
                logger.debug(f"EMA9 for {symbol}:ema_values {ema_values}")
                
                # Properly handle the EMA values regardless of whether it's a Series or array
                if ema_values is not None and len(ema_values) > 0:
                    if isinstance(ema_values, pd.Series):
                        ema_current = ema_values.iloc[-1]
                    else:
                        ema_current = ema_values[-1]
                else:
                    ema_current = None
                    
                self.nine_ema[symbol] = ema_current
                logger.debug(f"EMA9 for {symbol}: {ema_current} self.nine_ema[symbol] {self.nine_ema[symbol]}")

                # Similar fix for EMA20
                ema_values_20 = ta.ema(close, 20)
                if ema_values_20 is not None and len(ema_values_20) > 0:
                    if isinstance(ema_values_20, pd.Series):
                        ema_current_20 = ema_values_20.iloc[-1]
                    else:
                        ema_current_20 = ema_values_20[-1]
                else:
                    ema_current_20 = None
                    
                self.twenty_ema[symbol] = ema_current_20
                
                atr_val = await self.atr.atr_update(minute_df['high'], minute_df['low'], minute_df['close'], symbol)
            except Exception as e:
                logger.error(f"Error computing indicators for {symbol}: {e}")
                ema_current = None
                atr_val = None

            # Compute the entry prices using the latest aggregated minute bar.
            latest_bar = minute_df.iloc[-1].to_dict()
            latest_close = latest_bar['close']
            ohlc4 = (latest_bar['open'] + latest_bar['high'] + latest_bar['low'] + latest_bar['close']) / 4
            buffer = 0.00015 * ohlc4
            entry_long = latest_close + buffer
            entry_short = latest_close - buffer

            # Update the prices dictionary so that every new update refreshes the values.
            self.entry_prices[symbol] = {
                "entry_long": entry_long,
                "entry_short": entry_short,
                "timestamp": datetime.now().isoformat(),
                "ema9": ema_current,
                "ema20": self.twenty_ema[symbol],
                "minute_bars": minute_df.to_dict(orient='records'),
                "atr": atr_val
            }
            price = entry_long if action == "BUY" else entry_short
            result_container["price"] = price
            result_container["source"] = "real_time_aggregated"
            logger.debug(f"Updated entry price for {symbol}: {price}")
            return price

        except Exception as e:
            logger.error(f"Error in new_on_bar_update for {symbol}: {e}")


   
    
    async def kc_bands(self, contract, multiplier):
        try:
            symbol = contract.symbol if hasattr(contract, 'symbol') else 'unknown'
            # Use the cached ATR value for this symbol; if not available, try the alternative cache.
            if symbol in self.atr_cache and isinstance(self.atr_cache[symbol], (int, float, np.number)):
                atr_val = float(self.atr_cache[symbol])
            else:
                # Fall back to the cached value from the PriceCachedATR instance if stored correctly.
                atr_val = float(self.atr.atr_cache.get(symbol, 0))  # 0 if missing
            # Calculate the middle band using the twenty EMA.
            middle_band = float(self.twenty_ema[symbol])
            # Calculate the upper and lower Keltner Channel bands.
            upper_band = middle_band + (multiplier * atr_val)
            lower_band = middle_band - (multiplier * atr_val)

            return {
                'middle_band': middle_band,
                'upper_band': upper_band,
                'lower_band': lower_band,
                'atr': atr_val
            }
        except Exception as e:
            logger.error(f"Error calculating Keltner Channels for {contract.symbol}: {e}")
            return {
                'middle_band': None,
                'upper_band': None,
                'lower_band': None,
                'atr': None
            }
    async def on_order_status_event(self, trade: Trade):
        try:
            
            order_details = {
                "order_id": trade.contract.symbol if hasattr(trade.contract, 'symbol') else 'unknown',
                "order_action": trade.order.action,
                "order_type": trade.order.orderType,
                "algoStrategy": trade.order.algoStrategy,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp()
            }
            
            logger.info(self.format_order_details_table(order_details))
            #await self.test_parent_filled(trade)
        except Exception as e:
            
            logger.error(f"Error processing order status event for {trade.contract.symbol}: {e}", exc_info=True)
            # Handle any specific logic for order status updates if needed
            # For example, you might want to update a database or notify another service

class PriceCachedATR:
    """
    Caches the ATR values to avoid recalculating them for each request.
    This can help improve performance when multiple requests are made in quick succession.
    """
    def __init__(self, length=10):
        self.length = length
        self.atr_cache = {}
        self.contract = Contract()

    async def atr_update(self, high, low, close, symbol):
        try:
            symbol_key = symbol.symbol if hasattr(symbol, 'symbol') else str(symbol)
        
            # Validate inputs
            if high is None or low is None or close is None:
                return None
            
            # Calculate ATR with proper error handling
            atr_array = atr(high, low, close, length=self.length)
            if atr_array is not None and len(atr_array) > 0:
                self.atr_cache[symbol_key] = atr_array
                return atr_array[-1]
            return None
        except Exception as e:
            logger.error(f"Error calculating ATR: {e}")
            return None
