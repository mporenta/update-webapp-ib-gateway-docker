# price.py
from tracemalloc import stop
from ib_async import Contract, Order, Trade, LimitOrder, StopOrder, TagValue, util, IB, MarketOrder
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
import talib as ta


from pydantic import BaseModel

from indicators import  calculate_keltner_channels_ta_lib,  volatility_stop, get_ema, detect_pivot_points, daily_volatility
from timestamps import current_millis, get_timestamp, is_market_hours
from dotenv import load_dotenv
load_dotenv()
# --- Configuration ---
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv('FASTAPI_IB_CLIENT_ID', '1111'))
IB_HOST = os.getenv('IB_GATEWAY_HOST', '127.0.0.1')
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
    rewardRiskRatio: float
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

# For simplicity, our duplicate-check cache is a deque
from dataclasses import dataclass
@dataclass
class WebhookCacheEntry:
    ticker: str
    direction: str
    order_ref: str
    timestamp: int


# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), '.env')
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
        self.entry_prices = {}  # key: symbol, value: {"entry_long": price, "entry_short": price, "timestamp": time}
        self.timestamp_id ={}
        self.client_id = int(os.getenv('FASTAPI_IB_CLIENT_ID', '1111'))
        self.host = os.getenv('WEBHOOK_IB_HOST', '172.23.49.114')
        self.port = int(os.getenv('TBOT_IBKR_PORT', '4002'))
        self.max_attempts = 300
        self.initial_delay = 1
        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        self.open_price= {}
        self.high_price= {}
        self.low_price= {}
        self.close_price= {}
        self.volume= {}
        
        self.account_pnl: AccountPnL = None
        self.semaphore = asyncio.Semaphore(10)
        self.active_rtbars = {}  # Store active real-time bar subscriptions


    
    def format_order_details_table(self, order_details):
        """
        Format order details as a nice pandas DataFrame table for terminal display.
    
        Args:
            order_details: Dictionary containing order details
        
        Returns:
            Formatted string representation of the table
        """
        # Convert dictionary to a DataFrame with 'Parameter' and 'Value' columns
        df = pd.DataFrame({
            'Parameter': order_details.keys(),
            'Value': order_details.values()
        })
    
        # Format numerical values
        for idx, param in enumerate(df['Parameter']):
            value = df.loc[idx, 'Value']
            if isinstance(value, (int, float)):
                if 'price' in param or 'Balance' in param:
                    df.loc[idx, 'Value'] = f"${value:.2f}"
                elif 'percentage' in param or 'Ratio' in param:
                    df.loc[idx, 'Value'] = f"{value:.2f}"
    
        # Return formatted table string
        return f"\n{'='*50}\nORDER DETAILS\n{'='*50}\n{df.to_string(index=False)}\n{'='*50}"


    async def ohlc_vol(self, contract, durationStr, barSizeSetting, whatToShow, useRTH, return_df=False):
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
        logger.info(f"Getting historical data for {contract.symbol} with duration {durationStr}, bar size {barSizeSetting}, and data type {whatToShow}")
        # Get historical data for indicator calculations
        bars = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime='',
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow=whatToShow,
            useRTH=useRTH
        )
        
        if not bars:
            logger.error(f"No historical data available for {contract.symbol}")
            return None
            
        # Convert bars to DataFrame
        df = util.df(bars)
        logger.debug(f"Retrieved {len(df)} bars for {contract.symbol}")
    
        # Return raw DataFrame if requested
        if return_df:
            return df
        
        # Check if we have enough data
        if len(df) < 10:
            logger.warning(f"Not enough data to calculate OHLC averages. Need at least 10 bars, but got {len(df)}")
            return None
        
        open_prices = df['open']
        high = df['high']
        low = df['low']
        close = df['close']
        vol = df['volume']
        hl2 = (high + low) / 2
        hlc3 = (high + low + close) / 3
        ohlc4 = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        logger.info(f"Calculated OHLCV data for {contract.symbol}")
    
        return ohlc4, hlc3, hl2, open_prices, high, low, close, vol

    async def create_order(self, web_request: OrderRequest, contract: Contract, submit_cmd: bool, meanReversion: bool, stopLoss: float):
        try:
            if not self.ib.isConnected():
                logger.info(f"IB not connected in create_order, attempting to reconnect")
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
            logger.info(f"Creating order for {web_request.ticker} with action {web_request.orderAction} and stop type {web_request.stopType}")
            
            symbol = web_request.ticker
            stopType = web_request.stopType
            atrFactor = web_request.atrFactor
            action = web_request.orderAction
            accountBalance=web_request.accountBalance
            riskPercentage = web_request.riskPercentage
            rewardRiskRatio = web_request.rewardRiskRatio
            stopLoss = float(stopLoss)
            self.timestamp_id[symbol] = {
                "unixTimestamp": str(current_millis()),
                "stopType": stopType,
                "action": action,
                "riskPercentage": riskPercentage,
                "atrFactor": atrFactor,
                "rewardRiskRatio": rewardRiskRatio,
                "accountBalance": accountBalance,
                "stopLoss": stopLoss,
            }
            if action not in ["BUY", "SELL"]:
                raise ValueError("Order Action is None")
            # Step 1: Connect to IB and retrieve market data/entry price
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                logger.error(f"Contract qualification failed for {contract.symbol}")
                return None
            # Use the qualified contract instance directly
            qualified_contract = qualified[0]
            logger.info(f"Qualified contract for: {qualified_contract.symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}")
         
                            
            
            # First try to get from cached entry prices
            entry_prices = await self.get_entry_prices(qualified_contract.symbol)
            
            if entry_prices:
                logger.info(f"Using cached entry prices for {qualified_contract.symbol}: {entry_prices}")
                entry_price = entry_prices["entry_long"] if action == "BUY" else entry_prices["entry_short"]
            else:
                # If no cached entry prices, get them fresh
                entry_price = await self.get_price(qualified_contract, action)
            logger.info(f"Entry price for {qualified_contract.symbol}: {entry_price}")
            pos = await self.calculate_position(qualified_contract, entry_price, action, stopType, atrFactor, riskPercentage, accountBalance, rewardRiskRatio, stopLoss)
            ema= pos["ema"]
            stopType = pos["stopType"]
            logger.info(f"Calculated position for {qualified_contract.symbol}: entry={entry_price}, EMA= {ema} stop_loss={pos['stop_loss_price']}, take_profit={pos['take_profit_price']}, quantity={pos['quantity']}")
            if action == "BUY" and entry_price < ema and submit_cmd and not meanReversion:
                raise HTTPException(status_code=500, detail="Entry price is below EMA for a long position")
            if action == "SELL" and entry_price > ema and submit_cmd and not meanReversion:
                raise HTTPException(status_code=500, detail="Entry price is above EMA for a short position")
            stop_loss_price = pos["stop_loss_price"]
            quantity = pos["quantity"]
            take_profit_price = pos["take_profit_price"]
            if take_profit_price is None:
                logger.error(f"Take profit price is None for {symbol}")
                raise ValueError("Take profit price is None")
            if stop_loss_price is None:
                logger.error(f"Stop loss price is None for {symbol}")
                raise ValueError("Stop loss price is None")
            if quantity is None:
                logger.error(f"Quantity is None for {symbol}")
                raise ValueError("Quantity is None")
            logger.info(f"Calculated position for {symbol}: entry={entry_price}, stop_loss={stop_loss_price}, take_profit={take_profit_price}, quantity={quantity}")
        
            # Step 3: Place the bracket order
            reqId = await self.ib_manager.get_req_id()
            
            if not is_market_hours():

                order = LimitOrder(action, totalQuantity=quantity, lmtPrice=round(entry_price,2), tif='GTC', outsideRth=True,  transmit=True, orderId=reqId)
                logger.info(f"Afterhours limit order for {symbol} at price {entry_price}: {order}")
            if is_market_hours():
                logger.info(f"Market Hours Algo Limit order for {symbol} at price {entry_price}")

                order = LimitOrder(
                    action=action,
                    totalQuantity=quantity,
                    lmtPrice=round(entry_price,2),
                    tif='GTC',
                    transmit=True,
                    algoStrategy = 'Adaptive',
                    outsideRth=False,
                    orderRef=f"Adaptive - {symbol}",
                    algoParams = [
                        TagValue('adaptivePriority', 'Urgent')
                    ]
                )

            
            logger.info(f"Placing order with algoStrategy {order.algoStrategy} action: {order.action} order: {order.totalQuantity} @ {entry_price}")
            order_details = {}
            if submit_cmd:
                trade = await self.place_web_bracket_order(qualified_contract, order, action, stop_loss_price or 0.0, take_profit_price or 0.0, stopType)
            
                                                            
            
            # Return order details
                if trade:
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
                    return {"status": trade.orderStatus.status, "order_details": order_details}
            if not submit_cmd:
                  trade_check= await self.ib.whatIfOrderAsync(qualified_contract, order)
                  logger.info(f"Order details only for {symbol} with action {action} and stop type {stopType } trade_check= {trade_check}")
                  
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
    async def create_tv_entry_order(self, web_request: tvOrderRequest, contract: Contract, submit_cmd: bool, meanReversion: bool, stopLoss: float):
        try:
            if not self.ib.isConnected():
                logger.info(f"IB not connected in create_order, attempting to reconnect")
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
            logger.info(f"Creating order for {web_request.ticker} with action {web_request.orderAction} and stop type {web_request.stopType}")
            
            symbol = web_request.ticker
            entry_price = float(web_request.entryPrice)
            stopType = web_request.stopType
            kcAtrFactor = web_request.kcAtrFactor
            vstopAtrFactor = web_request.vstopAtrFactor
            atrFactor = vstopAtrFactor if stopType == "vStop" else kcAtrFactor
            action = web_request.orderAction
            accountBalance=web_request.accountBalance
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
            }
            if action not in ["BUY", "SELL"]:
                raise ValueError("Order Action is None")
            # Step 1: Connect to IB and retrieve market data/entry price
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                logger.error(f"Contract qualification failed for {contract.symbol}")
                return None
            # Use the qualified contract instance directly
            qualified_contract = qualified[0]
            logger.info(f"Qualified contract for: {qualified_contract.symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}")
         
                            
       
            logger.info(f"Entry price for {qualified_contract.symbol}: {entry_price}")
            pos = await self.calculate_position(qualified_contract, entry_price, action, stopType, atrFactor, riskPercentage, accountBalance, rewardRiskRatio, stopLoss)
            ema= pos["ema"]
            stopType = pos["stopType"]
            logger.info(f"Calculated position for {qualified_contract.symbol}: entry={entry_price}, EMA= {ema} stop_loss={pos['stop_loss_price']}, take_profit={pos['take_profit_price']}, quantity={pos['quantity']}")
            
            stop_loss_price = pos["stop_loss_price"]
            quantity = pos["quantity"]
            take_profit_price = pos["take_profit_price"]
            if take_profit_price is None:
                logger.error(f"Take profit price is None for {symbol}")
                raise ValueError("Take profit price is None")
            if stop_loss_price is None:
                logger.error(f"Stop loss price is None for {symbol}")
                raise ValueError("Stop loss price is None")
            if quantity is None:
                logger.error(f"Quantity is None for {symbol}")
                raise ValueError("Quantity is None")
            logger.info(f"Calculated position for {symbol}: entry={entry_price}, stop_loss={stop_loss_price}, take_profit={take_profit_price}, quantity={quantity}")
        
            # Step 3: Place the bracket order
            reqId = await self.ib_manager.get_req_id()
            
            if not is_market_hours():

                order = LimitOrder(action, totalQuantity=quantity, lmtPrice=round(entry_price,2), tif='GTC', outsideRth=True,  transmit=True, orderId=reqId)
                logger.info(f"Afterhours limit order for {symbol} at price {entry_price}: {order}")
            if is_market_hours():
                logger.info(f"Market Hours Algo Limit order for {symbol} at price {entry_price}")

                order = LimitOrder(
                    action=action,
                    totalQuantity=quantity,
                    lmtPrice=round(entry_price,2),
                    tif='GTC',
                    transmit=True,
                    algoStrategy = 'Adaptive',
                    outsideRth=False,
                    orderRef=f"Adaptive - {symbol}",
                    algoParams = [
                        TagValue('adaptivePriority', 'Urgent')
                    ]
                )

            
            logger.info(f"Placing order with algoStrategy {order.algoStrategy} action: {order.action} order: {order.totalQuantity} @ {entry_price}")
            order_details = {}
            if submit_cmd:
                trade = await self.place_web_bracket_order(qualified_contract, order, action, stop_loss_price or 0.0, take_profit_price or 0.0, stopType)
            
                                                            
            
            # Return order details
                if trade:
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
                    return {"status": trade.orderStatus.status, "order_details": order_details}
            if not submit_cmd:
                  trade_check= await self.ib.whatIfOrderAsync(qualified_contract, order)
                  logger.info(f"Order details only for {symbol} with action {action} and stop type {stopType } trade_check= {trade_check}")
                  
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
    async def create_close_order(self, contract: Contract, action: str, qty: int):
        try:
            if not self.ib.isConnected():
                logger.info(f"IB not connected in create_order, attempting to reconnect")
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
            
            order = None
            trade = None
            positions = None
            totalQuantity = None
            qualified_contract = None
            qualified_contract_final = None   
            symbol = contract.symbol    
            logger.info(f"Creating close order for {contract} wtih qty {qty} and action {action}")
            entry_price = None
            whatToShow = 'ASK' if action == 'BUY' else 'BID'
            
            # Use asyncio.gather with ohlc_vol instead of direct reqHistoricalDataAsync
            positions, qualified_contracts, df = await asyncio.gather(
                self.ib.reqPositionsAsync(),
                self.ib.qualifyContractsAsync(contract),
                self.ohlc_vol(
                    contract=contract,
                    durationStr='60 S',
                    barSizeSetting='5 secs',
                    whatToShow='TRADES',
                    useRTH=False,
                    return_df=True
                ) 
            )
            
            logger.info(f"Async data received for {symbol} with action {action} and qty {qty}")
            
            # Check if contract was qualified successfully
            if not qualified_contracts:
                logger.error(f"Could not qualify contract for {symbol}")
                return None
                
            # Initialize qualified_contract BEFORE using it
            qualified_contract = qualified_contracts[0]
            
            for pos in positions:
                
                logger.info(f"Checking position for: {pos.contract.symbol} and qualified_contract.symbol: {qualified_contract.symbol} with action {action} and qty {qty} and pos.position {pos.position}")
                
                if qualified_contract.symbol == pos.contract.symbol and pos.position != 0 and abs(pos.position) == qty:
                    logger.info(f"Position found for {symbol}: {pos.position} @ {pos.avgCost}")
                    totalQuantity = abs(pos.position)
                    
                    if df is not None and not df.empty:
                        entry_price = float(df.close.iloc[-1])
                    
                    qualified_contract_final = qualified_contract
                    
                    if not is_market_hours():
                        logger.info(f"Afterhours limit order for {symbol}")
                        order = LimitOrder(
                            action,
                            totalQuantity,
                            lmtPrice=entry_price,
                            tif='GTC',
                            outsideRth=True,
                            orderRef=f"Close Position - {symbol}",
                        )
                    else:
                        logger.info(f"Market Hours Close order for {symbol} at price {entry_price}")
                        order = MarketOrder(action, totalQuantity)
                    
                    logger.info(f"Placing order with limit order for {symbol} at price {entry_price}")
                    
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
                        logger.error(f"Did shares match for {symbol}? : {pos.position != 0 and pos.contract == qty}")
                        return None

        except Exception as e:
            logger.error(f"Error creating order: {e}")
            return None
    async def get_price(self, contract, action: str, max_wait_seconds=5, keep_subscription=False):
        symbol = contract.symbol
        logger.info(f"Getting price from real-time bars for {symbol}")
        result_container = {"price": None, "source": None}

        # Get or create the real-time bar subscription.
        rtBars = await self._get_rt_bars_subscription(contract, symbol, keep_subscription, action)
    
        # If this is a new subscription (i.e. not stored already), attach the update callback.
        if symbol not in self.active_rtbars:
            logger.info(f"Attaching update event for {symbol}")
            rtBars.updateEvent += lambda bars, hasNewBar: asyncio.create_task(
                self._on_bar_update(bars, symbol, action, result_container)
            )

        # Wait for a price update (via callback or direct check) up to max_wait_seconds.
        await self._wait_for_price(result_container, rtBars, symbol, action, max_wait_seconds)
    
        # Fallback: if no price yet and we're not keeping the subscription persistently.
        if result_container["price"] is None and not keep_subscription:
            logger.warning(f"Failed to get real-time price for {symbol}; using fallback")
            fallback_price, source = await self._fallback_price(contract, symbol, action)
            result_container["price"] = fallback_price
            result_container["source"] = source

        # Cancel the temporary subscription if needed.
        if not keep_subscription and symbol not in self.active_rtbars and rtBars:
            self.ib.cancelRealTimeBars(rtBars)
            logger.info(f"Cancelled temporary real-time bar subscription for {symbol}")

        logger.info(f"Final entry price for {symbol}: {result_container['price']} (source: {result_container['source']})")
        return result_container["price"]

    async def _get_rt_bars_subscription(self, contract, symbol, keep_subscription, action):
        whatToShow = 'MIDPOINT'
        if action == 'BUY' or action == 'SELL':
            whatToShow = 'ASK' if action == 'BUY' else 'BID'
        if symbol in self.active_rtbars:
            logger.info(f"Using existing real-time bar subscription for {symbol}")
            return self.active_rtbars[symbol]
        else:
            logger.info(f"Creating new real-time bar subscription for {symbol}")
            rtBars = self.ib.reqRealTimeBars(
                contract=contract,
                barSize=5,
                whatToShow=whatToShow,
                useRTH=False,
                realTimeBarsOptions=[]
            )
            if keep_subscription:
                self.active_rtbars[symbol] = rtBars
                logger.info(f"Created and stored new real-time bar subscription for {symbol}")
            return rtBars

    async def _on_bar_update(self, bars, symbol, action, result_container):
        try:
            df = util.df(bars)
            if df is None or df.empty:
                logger.warning(f"Empty dataframe received in onBarUpdate for {symbol}")
                return
            if 'close' not in df.columns or 'open_' not in df.columns:
                logger.warning(f"Missing required columns in dataframe for {symbol}")
                return

            # Calculate entry prices using the existing helper.
            entry_long, entry_short = self.calculate_entry_prices(df)
            self.entry_prices[symbol] = {
                "entry_long": entry_long,
                "entry_short": entry_short,
                "timestamp": datetime.now().isoformat()
            }
            price = entry_long if action == 'BUY' else entry_short
            if price is not None:
                
                result_container["price"] = price
                result_container["source"] = "real_time"
                logger.info(f"self.entry_prices is price for {symbol}: {price}")
        except Exception as e:
            logger.error(f"Error in _on_bar_update for {symbol}: {e}")

    async def _wait_for_price(self, result_container, rtBars, symbol, action, max_wait_seconds):
        start_time = time.time()
        while result_container["price"] is None and time.time() - start_time < max_wait_seconds:
            await asyncio.sleep(0.2)
            try:
                df = util.df(rtBars)
                if df is not None and not df.empty and 'open_' in df.columns and 'close' in df.columns:
                    logger.debug(f"Got dataframe with {len(df)} rows for {symbol}")
                    entry_long, entry_short = self.calculate_entry_prices(df)
                    self.entry_prices[symbol] = {
                        "entry_long": entry_long,
                        "entry_short": entry_short,
                        "timestamp": datetime.now().isoformat()
                    }
                    result_container["price"] = entry_long if action == 'BUY' else entry_short
                    result_container["source"] = "direct"
                    logger.info(f"Got self.entry_prices directly for {symbol}: {result_container['price']}")
                    break
            except Exception as e:
                logger.warning(f"Error getting data from rtBars for {symbol}: {e}")

    async def _fallback_price(self, contract, symbol, action):
        """
        Provides a fallback price using cached entry prices (if available) or historical data.
        Returns a tuple: (price, source)
        """
        # Use cached prices if available.
        cached_prices = self.entry_prices.get(symbol)
        if cached_prices:
            logger.debug(f"Using cached entry price for {symbol}: {cached_prices}")
            return (cached_prices["entry_long"] if action == 'BUY' else cached_prices["entry_short"], "cached")
        else:
            try:
                whatToShow = 'ASK' if action == 'BUY' else 'BID'
                df = await self.ohlc_vol(
                    contract=contract,
                    durationStr='60 S',
                    barSizeSetting='5 secs',
                    whatToShow=whatToShow,
                    useRTH=False,
                    return_df=True
                )
                if df is not None and not df.empty:
                    last_price = float(df.close.iloc[-1])
                    entry_long = last_price
                    entry_short = last_price
                    self.entry_prices[symbol] = {
                        "entry_long": entry_long,
                        "entry_short": entry_short,
                        "timestamp": datetime.now().isoformat(),
                        "source": "historical_fallback"
                    }
                    logger.info(f"Using historical fallback price for {symbol}: {last_price}")
                    return (entry_long if action == 'BUY' else entry_short, "historical_fallback")
                else:
                    logger.error(f"No historical data available for {symbol}")
            except Exception as e:
                logger.error(f"Error getting historical fallback for {symbol}: {e}")
        return (None, None)


        
    async def _get_initial_ema(self, contract):
        """
        Get initial 2-D historical data (1-min bars) and compute EMA.
        Returns the DataFrame (df_1) and the latest EMA.
        """
        durationStr = '2 D'
        barSizeSetting = '1 min'
        df_1 = await self.ohlc_vol(
            contract=contract,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow='TRADES',
            useRTH=False,
            return_df=True
        )
        if df_1 is None:
            logger.error(f"No historical data available for {contract.symbol}")
            return None, None
        df_1['ema'] = ta.EMA(df_1['close'].values, timeperiod=20)
        ema = df_1.iloc[-1]['ema']
        return df_1, ema

    async def _compute_stop_loss(self, contract, orderAction, stopType, atrFactor, entry_price, provided_stopLoss, df_1):
        """
        Compute the stop loss price if a valid one was not provided.
        Returns a tuple: (stop_loss_price, lower, upper, updated_stopType)
        """
        # Use provided stop loss if greater than zero.
        if provided_stopLoss > 0:
            return round(provided_stopLoss, 2), None, None, stopType

        # Get additional historical data for indicator calculations.
        durationStr = '35 D'
        barSizeSetting = '1 day'
        ohlc_data = await self.ohlc_vol(
            contract=contract,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow='TRADES',
            useRTH=True,
            return_df=False
        )
        if ohlc_data is None:
            logger.error(f"No extended historical data for {contract.symbol}")
            return None, None, None, stopType

        # Unpack the data
        ohlc4_dv, hlc3_dv, hl2_dv, open_prices_dv, high_dv, low_dv, close_dv, vol_dv = ohlc_data
        ib_connection = self.ib

        # Ensure that contract is a Contract object (not a dict)
        if not hasattr(contract, 'symbol'):
            logger.warning(f"Contract is not a Contract instance; attempting to convert it.")
            try:
                contract = Contract(**contract)
            except Exception as e:
                logger.error(f"Failed to convert contract dict to Contract: {e}")
                return None, None, None, stopType

        # Calculate daily volatility (this may adjust bar size internally)
        try:
            _ = await daily_volatility(ib_connection, contract, high_dv, low_dv, close_dv, stopType)
        except Exception as e:
            logger.error(f"Error in Daily Volatility: {e}")
            return None, None, None, stopType

        # Calculate indicators concurrently.
        keltner, vstop_df, pivot_points = await asyncio.gather(
            calculate_keltner_channels_ta_lib(
                high=df_1['high'], 
                low=df_1['low'], 
                close=df_1['close'], 
                ema_period=20, 
                atr_period=10, 
                multiplier=atrFactor
            ),
            volatility_stop(df_1, length=20, atr_multiplier=atrFactor), 
            detect_pivot_points(df_1, left_bars=5, right_bars=5)
        )
        if isinstance(vstop_df, np.ndarray):
            vstop_df = pd.DataFrame(vstop_df, columns=["vstop", "uptrend"])

        # Add Keltner Channel bands to the data.
        df_1['KC_Middle'] = keltner['middle_band']
        df_1['KC_Upper'] = keltner['upper_band']
        df_1['KC_Lower'] = keltner['lower_band']

        latest = df_1.iloc[-1]
        lower = latest['KC_Lower']
        upper = latest['KC_Upper']

        # Determine stop loss from volatility stop.
        latest_vstop = vstop_df.iloc[-1]
        vstop_val = latest_vstop['vstop']
        uptrend = latest_vstop['uptrend']
        stop_loss_price = None
        if stopType == "vStop":
            if orderAction == "BUY" and uptrend:
                stop_loss_price = vstop_val
            elif orderAction == "SELL" and not uptrend:
                stop_loss_price = vstop_val
            elif (orderAction == "SELL" and uptrend) or (orderAction == "BUY" and not uptrend):
                stopType = "kcStop"
                stop_loss_price = lower if orderAction == "BUY" else upper

        # Adjust using pivot points if pivot stop is selected.
        high_pivots = pivot_points.get("high_pivots", [])
        low_pivots = pivot_points.get("low_pivots", [])
        if orderAction == "BUY" and stopType == "pivot" and stop_loss_price is None:
            if low_pivots:
                stop_loss_price = low_pivots[0]['value'] - 0.02
            else:
                stop_loss_price = entry_price * 0.98
        elif orderAction == "SELL" and stopType == "pivot" and stop_loss_price is None:
            if high_pivots:
                stop_loss_price = high_pivots[0]['value'] + 0.02

        # Fallback if still undefined.
        if stop_loss_price is None:
            if stopType == "kcStop":
                stop_loss_price = lower if orderAction == "BUY" else upper
            else:
                stop_loss_price = entry_price * 0.99 if orderAction == "BUY" else entry_price * 1.01

        stop_loss_price = round(float(stop_loss_price), 2)
        return stop_loss_price, lower, upper, stopType

    def _compute_position_size(self, entry_price, stop_loss_price, accountBalance, riskPercentage, rewardRiskRatio, orderAction):
        """
        Given the entry and stop loss prices, compute the broker commission,
        per-share risk, tolerated risk, quantity and take profit price.
        """
        brokerComish = max(round((accountBalance / entry_price) * 0.005), 1.0)
        perShareRisk = abs(entry_price - stop_loss_price)
        if perShareRisk == 0 or perShareRisk < 0.01:
            logger.warning(f"Per share risk ({perShareRisk}) is too small; defaulting to 1% of entry price")
            perShareRisk = entry_price * 0.01
        toleratedRisk = abs((riskPercentage / 100 * accountBalance) - brokerComish)
        quantity = round(min(toleratedRisk / perShareRisk, math.floor((accountBalance * 0.95) / entry_price)))
        if quantity <= 0:
            logger.warning(f"Calculated quantity ({quantity}) is invalid; using minimum quantity of 1")
            quantity = 1
        if orderAction == "BUY":
            take_profit_price = round(entry_price + (perShareRisk * rewardRiskRatio), 2)
        else:
            take_profit_price = round(entry_price - (perShareRisk * rewardRiskRatio), 2)
        return quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk

    async def calculate_position(self, contract, entry_price: float, orderAction: str, stopType: str,
                                 atrFactor: float, riskPercentage: float, accountBalance: float,
                                 rewardRiskRatio: float, stopLoss: float):
        """
        Main function to calculate a position.
        It gathers initial EMA data, computes a stop loss (if not provided),
        and then calculates the proper position size.
        """
        try:
            logger.info(f"Calculating position for {contract.symbol} with entry {entry_price}, action {orderAction}, "
                        f"stopType {stopType}, atrFactor {atrFactor}, riskPercentage {riskPercentage}, "
                        f"accountBalance {accountBalance}, rewardRiskRatio {rewardRiskRatio}, stopLoss {stopLoss}")
            
            # Step 1: Get initial historical data and EMA.
            df_1, ema = await self._get_initial_ema(contract)
            if df_1 is None:
                return None

            # Step 2: Compute stop loss (if not provided).
            computed_stop_loss, lower, upper, updated_stopType = await self._compute_stop_loss(
                contract, orderAction, stopType, atrFactor, entry_price, stopLoss, df_1
            )
            if computed_stop_loss is None:
                logger.error("Failed to compute stop loss")
                return None

            # Step 3: Compute position size.
            quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = self._compute_position_size(
                entry_price, computed_stop_loss, accountBalance, riskPercentage, rewardRiskRatio, orderAction
            )

            logger.info(f"Position for {contract.symbol}: entry={entry_price}, stop_loss={computed_stop_loss}, "
                        f"take_profit={take_profit_price}, quantity={quantity}, brokerComish={brokerComish}, "
                        f"toleratedRisk={toleratedRisk}, perShareRisk={perShareRisk}, ema={ema}")

            return {
                "stop_loss_price": computed_stop_loss,
                "quantity": quantity,
                "take_profit_price": take_profit_price,
                "ema": ema,
                "upper": upper,
                "lower": lower,
                "stopType": updated_stopType
            }
        except Exception as e:
            logger.error(f"Error calculating position: {e}", exc_info=True)
            return None
   
    async def req_mkt_data(self, contract: Contract):
        """
        Request market data for a contract and set up real-time price monitoring.
        This populates the entry_prices dictionary with long and short entry prices.
        """
        try:
            if not self.ib.isConnected():
                logger.info(f"IB not connected in req_mkt_data, attempting to reconnect")
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
                
            logger.info(f"Getting market data for {contract.symbol}")
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                logger.error(f"Contract qualification failed for {contract.symbol}")
                return None
                
            # Use the qualified contract instance
            qualified_contract = qualified[0]
            symbol = qualified_contract.symbol
            logger.info(f"Qualified contract for: {symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}")
            ticker = self.ib.reqMktData(qualified_contract, '', True, False)
            
            # Start real-time bar subscriptions to populate entry prices if not already active
            if symbol not in self.active_rtbars:
                # This will trigger get_price to populate the entry_prices dictionary with both entry_long and entry_short
                # We don't await the result because we want to set up the subscription without blocking
                asyncio.create_task(self.get_price(qualified_contract, "BUY", keep_subscription=True))

            # Prepare contract details for return
            contract_json = {
                "symbol": symbol,
                "secType": qualified_contract.secType,
                "currency": qualified_contract.currency,
                "exchange": qualified_contract.exchange,
                "localSymbol": qualified_contract.localSymbol,
                "primaryExchange": qualified_contract.primaryExchange,
                "conId": qualified_contract.conId,
                "lastTradeDateOrContractMonth": qualified_contract.lastTradeDateOrContractMonth,
            }
            
            # Check if we already have entry prices for this symbol
            entry_prices = await self.get_entry_prices(symbol)
            if entry_prices:
                # Add entry prices to the return data
                contract_json["entry_long"] = entry_prices["entry_long"]
                contract_json["entry_short"] = entry_prices["entry_short"]
                contract_json["timestamp"] = entry_prices["timestamp"]
                logger.info(f"Returning entry prices for {symbol}: {entry_prices}")
            else:
                # If we don't have prices yet, wait a bit and check again
                for _ in range(10):  # Try for up to 2 seconds
                    await asyncio.sleep(0.2)
                    entry_prices = await self.get_entry_prices(symbol)
                    if entry_prices:
                        contract_json["entry_long"] = entry_prices["entry_long"]
                        contract_json["entry_short"] = entry_prices["entry_short"]
                        contract_json["timestamp"] = entry_prices["timestamp"]
                        logger.info(f"Returning entry prices for {symbol} after wait: {entry_prices}")
                        break
                
                if "entry_long" not in contract_json:
                    # Still no prices, just return what we have
                    logger.warning(f"No entry prices available yet for {symbol}")
                    contract_json["entry_prices_status"] = "pending"
            
            return contract_json
            
        except Exception as e:
            logger.error(f"Error in req_mkt_data: {e}")
            if not self.ib.isConnected():
                logger.info("Reconnecting to IB...")
                await self.ib_manager.connect()
            return {"error": str(e)}

    async def get_entry_prices(self, symbol):
        """
        Get the latest entry prices for a given symbol.
        Returns a dictionary with entry_long and entry_short, or None if not available.
        """
        if symbol in self.entry_prices:
            return self.entry_prices[symbol]
        return None

    

    def calculate_entry_prices(self, df_all: pd.DataFrame):
        """
        Calculate dynamic entry prices for long and short positions.
        Handles various edge cases including missing data.
        """
        if df_all is None or df_all.empty:
            logger.warning("Empty dataframe passed to calculate entry prices")
            return None, None
    
        # Check if required columns exist
        if 'close' not in df_all.columns:
            logger.warning("DataFrame missing 'close' column in calculate entry prices")
            return None, None
    
        if 'whatToShow' not in df_all.columns:
            # For real time bars without whatToShow column
            try:
                last_close = df_all.iloc[-1]['close']
                # Add a small buffer (0.1%) for long and short entries
                entry_long = last_close * 1.001
                entry_short = last_close * 0.999
                return entry_long, entry_short
            except Exception as e:
                logger.error(f"Error processing real-time bars without whatToShow: {e}")
                return None, None

        try:
            bid_df = df_all[df_all['whatToShow'] == 'BID']
            ask_df = df_all[df_all['whatToShow'] == 'ASK']
            
        
            # Handle cases where bid or ask data might be missing
            if bid_df.empty and ask_df.empty:
                # Fall back to using the last close price
                if len(df_all) > 0:
                    last_close = df_all.iloc[-1]['close']
                    # Add a small buffer (0.1%) for long and short entries
                    entry_long = last_close * 1.001  
                    entry_short = last_close * 0.999
                    return entry_long, entry_short
                else:
                    logger.warning("No data available in calculate entry prices")
                    return None, None
        
            # If one of bid or ask is missing, use the available one with an approximated spread
            if bid_df.empty and not ask_df.empty:
                latest_ask = ask_df.iloc[-1]['close']
                estimated_spread = latest_ask * 0.001  # Use 0.1% as an estimated spread
                latest_bid = latest_ask - estimated_spread
            elif not bid_df.empty and ask_df.empty:
                latest_bid = bid_df.iloc[-1]['close']
                estimated_spread = latest_bid * 0.001  # Use 0.1% as an estimated spread
                latest_ask = latest_bid + estimated_spread
            else:
                latest_bid = bid_df.iloc[-1]['close']
                latest_ask = ask_df.iloc[-1]['close']
        
            latest_spread = max(0.01, latest_ask - latest_bid)  # Ensure positive spread
            buffer = 0.1 * latest_spread
            entry_long = latest_ask + buffer
            entry_short = latest_bid - buffer
            logger.info(f"Calculated entry prices: long={entry_long}, short={entry_short} based on bid={latest_bid}, ask={latest_ask}, spread={latest_spread} buffer={buffer}")
            return entry_long, entry_short
        except Exception as e:
            logger.error(f"Error in calculate entry prices: {e}")
            return None, None
            
    
   
    async def add_oca_bracket(self, contract: Contract, order: Trade, action: str, entry_price: float, quantity: float, stopType: str, stopLoss: float):
        try: 
            # Get symbol and timestamp ID
            symbol = contract.symbol
            logger.info(f"Adding OCA bracket for {symbol} with action {action} and stopLoss {stopLoss}")
        
            # Get the timestamp ID directly from the dictionary with the symbol as key
            if symbol not in self.timestamp_id:
                logger.error(f"No timestamp ID found for {symbol}")
                return None
            
            # Get timestamp data for this symbol
            timestamp_data = self.timestamp_id[symbol]
        
            # Get current price from entry_prices if available
            current_price = None
            entry_prices = await self.get_entry_prices(symbol)
        
            logger.debug(f"Timestamp ID for {symbol}: {timestamp_data}")
        
            if entry_prices:
                current_price = entry_prices["entry_long"] if action == "BUY" else entry_prices["entry_short"]
                logger.debug(f"Using cached entry price for {symbol}: {current_price}")
            else:
                # Fall back to getting real-time price
                current_price = await self.get_price(contract, action)
            
            if not current_price:
                logger.error(f"Could not get current price for {symbol}")
                return None
            
            priceDiff = ((current_price - entry_price)/entry_price)*100
            priceAvg = ((current_price + entry_price)/2)
            logger.info(f"Entry price for {symbol}: {entry_price} current price: {current_price} priceDiff: {priceDiff} priceAvg: {priceAvg} with stopType {stopType}")
            
        
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
                stopLoss
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
            take_profit = LimitOrder(reverseAction, order.totalQuantity, take_profit_price)
            stop_loss = StopOrder(reverseAction, order.totalQuantity, stop_loss_price)
        
            if take_profit_price is None:
                logger.error(f"Take profit price is None for {symbol}")
                raise ValueError("Take profit price is None")
            if stop_loss_price is None:
                logger.error(f"Stop loss price is None for {symbol}")
                raise ValueError("Stop loss price is None")
            
            logger.info(f"Creating bracket orders for {symbol}: entry={entry_price}, stop_loss={stop_loss_price}, take_profit={take_profit_price}, quantity={order.totalQuantity}")

            orders = [take_profit, stop_loss]
            # Group them in an OCA group using oneCancelsAll
            oca_group = f"exit_{symbol}_{p_ord_id}"
        
            # Create the OCA bracket orders
            try:
                bracket_orders = self.ib.oneCancelsAll(orders, oca_group, 1)
                logger.debug(f"Created OCA bracket orders for {symbol} with group {oca_group}")
            except Exception as e:
                logger.error(f"Failed to create OCA bracket orders: {e}")
                return None
    
            # Place each order
            if bracket_orders:
                for bracket_order in bracket_orders:
                    logger.info(f"Placing order: {symbol} {bracket_order.action} {bracket_order.totalQuantity} @ {bracket_order.lmtPrice if hasattr(bracket_order, 'lmtPrice') else bracket_order.auxPrice}")    
                    trade = self.ib.placeOrder(contract, bracket_order)
                    if trade:
                        # Remove the timestamp ID data after successful order placement
                        if symbol in self.timestamp_id:
                            del self.timestamp_id[symbol]
                    
                        logger.info(f"Order placed for {symbol}: {bracket_order}")
                        trade.fillEvent += self.ib_manager.on_fill_update
                    else:
                        logger.error(f"Order placement failed for {symbol}: {bracket_order}")
                    
                return bracket_orders
            else:
                logger.error(f"No bracket orders created for {symbol}")
                return None
            
        except Exception as e:
            logger.error(f"Error in add oca bracket: {e}")
            return None
    async def place_web_bracket_order(self, contract: Contract, order: Trade, action: str, exit_stop: float, exit_limit: float, stopType):
        """
        Places bracket order with a MarketOrder entry during market hours and a LimitOrder outside market hours.
        trade = await ib_manager.place_bracket_market_order(ib_contract, order, direction, action, exit_stop or 0.0, exit_limit or 0.0)

        """
       
        async with self.semaphore:
            try:
                logger.info(f"jengo bracket order webhook tv_hook {contract.symbol}")
                
                
                # Decide whether we're going long or short
                #action = "BUY" if direction == "strategy.entrylong" else "SELL"
                reverseAction = "BUY" if action == "SELL" else "SELL"
                qualified_contracts = await self.ib.qualifyContractsAsync(contract)
                if not qualified_contracts:
                    logger.error(f"Could not qualify contract for {contract.symbol}")
                    return None

                qualified_contract = qualified_contracts[0]

                

                p_ord_id = order.orderId
                logger.debug(f"jengo bracket order webhook p_ord_id: {p_ord_id}")

                # Determine the type of parent order based on market hours
                
                logger.debug(f"Market hours - creating MarketOrder for{contract.symbol}")
                parent = order
                logger.debug(f"Parent Order - creating MarketOrder for{parent}")
               

               
                
              

                # Place the orders
               
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
                    logger.debug(f"Bracket order already triggered for permId {permId}, skipping duplicate call.")
                    return
                
                # Mark this permId as being processed BEFORE any async operations
                # This prevents other concurrent calls from proceeding
                self.triggered_bracket_orders[permId] = True
            
                logger.info(f"parent_filled Fill received for {fill.contract.symbol} with entry price {fill.execution.avgPrice}. Sending Bracket for trade.order {trade.order}")
            
                if fill.contract is not None:
                    logger.info(f"jengo bracket order webhook fill.contract: {fill.contract.symbol}")
                    fill_contract = fill.contract
                    fill_symbol = fill.contract.symbol
                if fill.contract is None:
                    logger.info(f"jengo bracket order webhook trade.contract: {trade.contract.symbol}")
                    fill_contract= trade.contract
                    fill_symbol = trade.contract.symbol

                entry_price = float(fill.execution.avgPrice)
                action = trade.order.action
                order = trade.order
                algoStrategy = trade.order.algoStrategy
                logger.debug(f"jengo bracket order webhook entry_price: {entry_price} for order {order} and permId {permId} and algoStrategy {algoStrategy}")
        
                quantity = float(trade.order.totalQuantity)

                try:
                    logger.info(f"jengo Get latest positions from TWS (only positions with nonzero quantity): stopLoss {self.timestamp_id.get(fill_symbol, {}).get('stopLoss', 0.0)} for permId {permId}")
                    latest_positions_oca = await self.ib.reqPositionsAsync()
                
                    # Optional: Get open orders and trades if needed for verification
                    open_orders_list_id = await self.ib.reqAllOpenOrdersAsync()
                    logger.debug(f"jengo Get latest positions from TWS (only positions with nonzero quantity): open_orders_list_id {len(open_orders_list_id)}")
                    self.orders_dict_oca_symbol = {o.order.permId: o for o in open_orders_list_id}
                    completed_trades_oca = self.ib.openTrades()
                
                    stopType = self.timestamp_id.get(fill_symbol, {}).get("stopType")
                    stopLoss = self.timestamp_id.get(fill_symbol, {}).get("stopLoss", 0.0)
                    logger.info(f" jengo parent_filled stopType {stopType} and stopLoss {stopLoss}")
            
                    # Now loop through the positions
                    for position in latest_positions_oca:
                        logger.debug(f"Position for: {position.contract.symbol} at shares of {position.position}")
                        if position.contract.symbol == fill_symbol and position.position != 0:
                            # For Adaptive algoStrategy, trigger the bracket order
                            if algoStrategy == "Adaptive" or not is_market_hours():
                                logger.info(f"Placing oca bracket order for {fill_symbol} with permId {permId} and stopType {stopType} and stopLoss {stopLoss}")
                                oca_order = await self.add_oca_bracket(fill_contract, order, action, entry_price, quantity, stopType, stopLoss)
                                return oca_order
                            else:
                                logger.info(f"Skipping {fill_symbol}: algoStrategy {algoStrategy} does not require oca bracket order and stopType {stopType} and stopLoss {stopLoss}")
                
                    # If we got here, no matching position was found
                    logger.warning(f"No active position found for {fill_symbol} when placing bracket orders")
                
                except Exception as e:
                    logger.error(f"Error in conditions for oca bracket_orders in fill event: {e}")
                    # If there's an error, we should remove the permId from the triggered dictionary
                    # so that it can be retried
                    if permId in self.triggered_bracket_orders:
                        del self.triggered_bracket_orders[permId]
    
        except Exception as e:
            logger.error(f"Error updating positions on fill: {e}")
            # Global exception handling should also clean up the triggered dictionary
            if 'permId' in locals() and hasattr(self, "triggered_bracket_orders") and permId in self.triggered_bracket_orders:
                del self.triggered_bracket_orders[permId]