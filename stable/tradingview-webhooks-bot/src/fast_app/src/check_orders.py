    

# app.py
from asyncio import tasks
from collections import defaultdict, deque


from typing import *
import os, asyncio, time
import re
import threading
import signal
from threading import Lock
from datetime import datetime, timedelta, timezone

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

from models import (
    OrderRequest,
    AccountPnL,
    TickerResponse,
    QueryModel,
    WebhookRequest,
    TickerSub,
    TickerRefresh,
    TickerRequest,
    PriceSnapshot,
    VolatilityStopData,
)
from tv_ticker_price_data import  tv_store_data, price_data_dict, daily_volatility, yesterday_close_bar, yesterday_close, daily_true_range, timeframe_dict, order_placed, subscribed_contracts, ib_open_orders


from pandas_ta.overlap import ema
from pandas_ta.volatility import atr, true_range


from tv_scan import tv_volatility_scan_ib
import numpy as np
from timestamps import current_millis
from log_config import log_config, logger
from ticker_list import ticker_manager

#from indicators import daily_volatility
from p_rvol import PreVol, start_rvol
from trade_dict import trade_to_dict
from my_util import *
from my_util import is_market_hours, inspect_dict_values, shortable_shares, aggregate_5s_to_nsec, format_what_if_details_table
from ib_db import (
    IBDBHandler
  
    
    )

#from app import update_ticker_data, get_historical_data, yesterday_close_bar, compute_volatility_series, bracket_trade_fill
from subscribed_contracts_dict import subscribed_contracts



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
last_daily_bar_dict = defaultdict(dict)  # Store last daily bar for each symbol


# your existing outputs
daily_true_range:    dict[str, float] = {}
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

class CheckBracketIB:
    """Async-safe in-memory cache of qualified IB Contracts."""
    def __init__(self):
        self.ib: IB = None           # ← will be set by app.py
        self._lock = asyncio.Lock()
        self._cache: Dict[str, Contract] = defaultdict(Contract)

    def set_ib(self, ib_instance: IB):
        """Initialize the IB client once at startup."""
        self.ib = ib_instance
        if ib_instance is None:
            raise ValueError("IB instance cannot be None.")
        else:
            logger.info("BracketIB IB instance set successfully.")

ck_bk_ib= CheckBracketIB()  


async def process_web_order_check(web_request: OrderRequest, contract: Contract, barSizeSetting=None, order=None ):
    try:
        
        stopLoss = 0.0
        symbol= None
        trade = None
        stopLoss = web_request.stopLoss
        orderAction = None
        web_order_data = web_request.model_dump()
        bracket = {}
        entryPrice = web_request.entryPrice
        is_valid_timeframe = "secs" in web_request.timeframe or "min" in web_request.timeframe  or "mins" in web_request.timeframe
        

        if barSizeSetting is None:
            if is_valid_timeframe:
                barSizeSetting = web_request.timeframe
            
            else:
                barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(web_request.timeframe)
            
                barSizeSetting = web_request.timeframe
        logger.info(f"Processing webhook request for {web_request} ")
        # ticker= TickerSub(ticker=web_request.ticker)
        symbol = contract.symbol
        
        get_symbol_dict= await get_tv_ticker_dict(symbol)
        if stopLoss is None or stopLoss == 0.0 :
            vstop_values= await price_data_dict.get_snapshot(symbol)
            if vstop_values is not None:
                stopLoss = vstop_values.vStop
                uptrend = vstop_values.uptrend
               
                logger.info(f"Using vstop values for {symbol}: stopLoss={stopLoss}, entryPrice={entryPrice}, uptrend={uptrend}")
            else:
                logger.warning(f"No vstop values found for {symbol}, using defaults.")
                stopLoss = get_symbol_dict.stopLoss
                entryPrice = get_symbol_dict.entryPrice
                uptrend =get_symbol_dict.uptrend
        atrFactor = get_symbol_dict.atrFactor
        vstopAtrFactor =get_symbol_dict.vstopAtrFactor
        if vstopAtrFactor is None or vstopAtrFactor == 0.0:
            vstopAtrFactor = atrFactor
        
        stop_diff = stopLoss - entryPrice if stopLoss and entryPrice else None
        logger.info(f"stopLoss: {stopLoss} and order entryPrice: {entryPrice} and stop_diff: {stop_diff} and uptrend: {uptrend} for {symbol}")

        
        df = historical_data.get(symbol)
        
        
        if (entryPrice is not None and entryPrice != 0) or (
            stopLoss is not None and stopLoss != 0
        ):
            logger.info(f"Using entryPrice from webhook: {entryPrice}")
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(
                web_request.timeframe
            )
            

        upsert_web_order = await order_db.upsert_web_order(web_order_data)
        logger.info(
            f"Upserted web order data: {upsert_web_order} with web_order_data {web_order_data}"
        )

        logger.info(f"New Order request for {symbol} with jengo and uptrend")
        if uptrend == True or uptrend == False:
            orderAction = "BUY" if uptrend else "SELL"
        else:
            orderAction = web_request.orderAction

        # uptrend_orderAction = "BUY" if uptrend  else "SELL"
        logger.info(
            f"Uptrend order Action for {symbol} is {orderAction} and entryPrice {web_request.entryPrice}"
        )
        order = OrderRequest(
            entryPrice=entryPrice,  # Use the latest market price as entry price
            rewardRiskRatio=web_request.rewardRiskRatio,
            quantity=web_request.quantity,  # Use the quantity from the webhook
            riskPercentage=web_request.riskPercentage,
            vstopAtrFactor=vstopAtrFactor,
            timeframe=barSizeSetting,
            kcAtrFactor=vstopAtrFactor,
            atrFactor=atrFactor,
            accountBalance=web_request.accountBalance,
            ticker=symbol,
            orderAction=orderAction,
            stopType=web_request.stopType,
            submit_cmd=web_request.submit_cmd,
            meanReversion=web_request.meanReversion,
            stopLoss=float(stopLoss),
            uptrend=uptrend,
        )
        logger.warning(f"order  is {order}")
        logger.warning(
            f"barSizeSetting  is {barSizeSetting} for  {symbol} and vStop type {web_request.stopType}"
        )
        submit_cmd = web_request.submit_cmd
        meanReversion = web_request.meanReversion
        bk_vol=await daily_volatility.get(symbol)
        if bk_vol is not None:
            vol=bk_vol
        
        action = orderAction
        has_action = "BUY" in action or "SELL" in action
        quantity = web_request.quantity
        if not has_action:
            action = "BUY" if uptrend else "SELL"

        logger.info(
            f"New Order request for  subscribed_contracts {contract} with stopLoss {stopLoss} and web_request.stopLoss {web_request.stopLoss} and stopLoss+5 {stopLoss+5}"
        )
        

        if stopLoss > 0: 
            orderPlace=await order_placed.set(symbol)
        
           
            logger.info(f"Order being placed for {symbol} orderPlace {orderPlace} ")
            trade = await bracket_process_check_web_order(contract, action, quantity, order_details=order, vol=vol)
       
        if trade:
            orderPlace=await order_placed.delete(symbol)
            logger.info(format_order_details_table(trade))
            #web_request_json =jsonable_encoder(trade)
           
          
          
            #logger.info(f"Order Entry for {symbol} successfully placed with order {trade}")
           
            
            return JSONResponse(content={"status": "ok"})
        else:
            logger.info(f"Bracket order not placed for {symbol} ")

            return JSONResponse(content={"status": "Nothing"})
           
       
      
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")
    
    
async def bracket_process_check_web_order(
    contract: Contract,
    action: str,
    quantity: float,
    order_details=None,
    vol=None
):
    try:
      

       
        snapshot = []
        price_source=""
        entryPrice = None
        stopLoss = None
        uptrend = None
        vStop_float = 0.0
        vstopAtrFactor= None
        atrFactorDyn = None
        symbol = contract.symbol
        get_symbol_dict = None
        if order_details.entryPrice > 0.0:
            get_symbol_dict= await get_tv_ticker_dict(symbol)
            entryPrice = get_symbol_dict.entryPrice
            stopLoss = get_symbol_dict.stopLoss
            uptrend =get_symbol_dict.uptrend
        else:
            get_symbol_dict=await price_data_dict.get_snapshot(symbol)
        
        
        stop_diff = stopLoss - entryPrice if stopLoss and entryPrice else None
        
        
        
        if stopLoss == 0.0 or isNan(stopLoss):
            atrFactorDyn = await get_dynamic_atr(symbol, BASE_ATR=None, vol=vol)

            vstopAtrFactor = atrFactorDyn
            if order_details.entryPrice is not None and order_details.entryPrice > 0.0:
                entryPrice = order_details.entryPrice
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(
                order_details.timeframe
            )
            await price_data_dict.add_ticker(ck_bk_ib.ib.ticker(contract), barSizeSetting)
            await asyncio.sleep(0.2)
            snapshot=await price_data_dict.get_snapshot(symbol)
        snapshot = await price_data_dict.get_snapshot(symbol)
        logger.info("\n" + snapshot.to_table())
        logger.debug(f"stopLoss: {stopLoss} and order entryPrice: {entryPrice} and stop_diff: {stop_diff} and uptrend: {uptrend} for {symbol} and get_symbol_dict: {get_symbol_dict} snapshot.uptrend {snapshot.uptrend}")
        if snapshot is None:
            logger.warning(f"Price data is None for {symbol}. Skipping order placement.")
            raise ValueError(f"Price data is None for {symbol}. Skipping order placement.")
        
        action = "BUY" if snapshot.uptrend > 0.0  else "SELL"
        #update_tick = await update_ticker_data(contract, ck_bk_ib=ck_bk_ib)
        if snapshot.hasBidAsk:
            logger.info(f"Bracket OCA order for {symbol}  hasBidAsk:...")
        
            
        shortableShares = snapshot.shortableShares
        shortable= False
        if action == "SELL":
            logger.info(f"Checking if {contract.symbol} is shortable...")
            is_shortable=shortable_shares(contract.symbol, action, quantity, shortableShares)
            logger.info(f"{contract.symbol} is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")

            if shortable_shares:
                logger.info(f"{contract.symbol} is shortable, is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")
                
                shortable = True
            if not shortable:
                logger.warning(f"{contract.symbol} is not shortable, cannot place SELL order, is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")
                
                return None
        logger.info(
            f"Placing LIMIT order for {contract.symbol} with action {action} and quantity {quantity} and entryPrice {order_details.entryPrice} and stopLoss {order_details.stopLoss} "
        )
        correct_order= False
        histData = None
        bars= [] 
        df = pd.DataFrame()
        

        logger.info(f"Placing bracket OCA order for {contract.symbol}...correct_order is {correct_order}")
        
        tasks = []
        
        limit_price = 0.0
        stop_loss_price = 0.0
        bracket_trade = None
        market_price = None
        bracket_orders: List[Order] = []
        order_history = await order_db.get_web_orders_by_ticker(contract.symbol)
        if vol is None or isNan(vol):
            vol=await daily_volatility.get(symbol)
        logger.info(f"Vol is {vol} and Loaded OrderRequest for {symbol}: {order_details.model_dump()}")
        
        

        #snapshot = await price_data_dict.get_snapshot(symbol)
        
        _yesterday_close=yesterday_close[symbol]
        logger.info(f"Yesterday close for {symbol}: {_yesterday_close}")
      
        if vol is None or isNan(vol) or (_yesterday_close is None or isNan(_yesterday_close)):
            logger.warning(f"Volatility is None or NaN for {symbol}, calling compute_volatility_series...")
            df =  await yesterday_close_bar(ck_bk_ib, contract)
            last_daily_bar_dict[symbol] = df.iloc[-1]
            yesterday_close[symbol]=  df.iloc[-2]['close'] if len(df) >= 2 else None
            last_daily_bar =  last_daily_bar_dict[symbol] 
            _yesterday_close =  yesterday_close[symbol]
           
            
            
            vol=await compute_volatility_series(snapshot.high , snapshot.low , _yesterday_close, contract, ck_bk_ib )
            logger.warning(f"Yesterday's close was None or NaN for {symbol}, called yesterday_close_bar: yesterday_close {_yesterday_close} volatility {vol}")
           
            
        
        orderAction = order_details.orderAction
        logger.info(f"OrderAction for {symbol}: {orderAction} calling async gather for orders are historical data...")
        
        
        if stopLoss is not None and stopLoss != 0:
            logger.info(f"Using stopLoss from webhook: {stopLoss}")
            stop_loss_price = stopLoss
        if stop_loss_price <= 0.0 or isNan(stop_loss_price):
            logger.info(f"Using limit_price for {symbol}: {limit_price} where market_price {market_price}")
            vStop_values=await price_data_dict.get_snapshot(symbol)
            vStop_float = vStop_values.vStop if vStop_values.vStop is not None else 0.0
            uptrend = vStop_values.uptrend if vStop_values.uptrend is not None else None
            #await asyncio.sleep(0.2)
            

            
            logger.info(f"Calculating vStop {vStop_float} and uptrend {uptrend} for {symbol}...")
            if vStop_float > 1.0:

                stop_loss_price = (
                    round(vStop_float, 2) + 0.02 if not uptrend else round(vStop_float, 2) - 0.02
                )
        
        
        
        
        

        
        ask_price = 0.0
        bid_price = 0.0
        markPrice = 0.0
        last = 0.0
        take_profit_price = 0.0
        #logger.info(f"Last close for {symbol}: {await price_data_dict.get_snapshot(symbol)}")
        if snapshot.markPrice and (snapshot.markPrice is not None and not isNan(snapshot.markPrice) and snapshot.markPrice > 0):
            markPrice = snapshot.markPrice
        
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
                limit_price = 0.0
                price_source = "None 0.0"
        # Priority: avgPrice > entryPrice > live market data > last close
       
        elif entryPrice and entryPrice > 0.0:
            limit_price = round(entryPrice, 2)
            price_source = "entryPrice"
            logger.info(f"Using entryPrice as limit_price: {limit_price}")
        elif markPrice and markPrice > 0.0:
            limit_price = round(markPrice, 2)
            price_source = "markPrice"
            logger.info(f"Using markPrice as limit_price: {limit_price}")
        elif limit_price is None or limit_price <= 0.0:
            logger.warning(f"Limit price is None or <= 0.0 for {symbol}, using last close price...snapshot is {snapshot} and ck_bk_ib.ib.ticker is {ck_bk_ib.ib.ticker(contract)}")
            await price_data_dict.add_ticker(ck_bk_ib.ib.ticker(contract))
            await asyncio.sleep(0.2)
            snapshot=await price_data_dict.get_snapshot(symbol)

        else:
            price_source = "None 0.0 error"
            raise ValueError(
                f"Invalid limit price for {symbol}:limit_price: {limit_price} and markPrice: {markPrice} andsnapshot.hasBidAsk: {snapshot.hasBidAsk} and snapshot.markPrice: {snapshot.markPrice} and snapshot.bid: {snapshot.bid} and snapshot.ask: {snapshot.ask}"
            )
           
        
        
       
       

        logger.info(f"Calculating position size for {symbol}...")
        if action == "BUY" and round(limit_price, 2) <= round(stop_loss_price, 2):
            logger.warning(
                f"Limit price {limit_price} is less than stop loss price {stop_loss_price} for {symbol}. Adjusting limit price to stop loss price."
            )
            logger.info(f"stop_loss_price before adjustment: {stop_loss_price} for {symbol}  float(order_details.riskPercentage {float(order_details.riskPercentage)} limit_price * 1 - (float(order_details.riskPercentage)/100) { limit_price * 1 -(float(order_details.riskPercentage)/100)}")
            stop_loss_price =  limit_price  * ((1 -((float(order_details.riskPercentage)))/100)) 
        elif action == "SELL" and round(limit_price, 2) >= round(stop_loss_price, 2):   
            logger.warning(
                f"Limit price {limit_price} is greater than stop loss price {stop_loss_price} for {symbol}. Adjusting limit price to stop loss price."
            )
            stop_loss_price =   limit_price * ((1 + ((float(order_details.riskPercentage)))/100)) 

        quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = (
            await compute_position_size(
                limit_price,
                stop_loss_price,
                order_details.accountBalance,
                order_details.riskPercentage,
                order_details.rewardRiskRatio,
                action,
            )
        )
        
        logger.info(
            f"Calculated position size for {symbol}: {quantity} and take_profit_price {take_profit_price}"
        )
        reqId = ck_bk_ib.ib.client.getReqId()
        ocaGroup = f"oca_{symbol}_{quantity}_{round(limit_price,2)}"
        logger.info(
            f"bracket_oca_order: ocaGroup: {ocaGroup} {symbol} limit_price={limit_price} stop_loss={stop_loss_price} entry price source= {price_source}"
        )
        

        reverse_action = "BUY" if action == "SELL" else "SELL"
        stop_loss_price = round(stop_loss_price, 2)
        take_profit_price = round(take_profit_price, 2)
        
        parent_order = LimitOrder(
            action,
            totalQuantity=quantity,
            lmtPrice=round(limit_price, 2),
            tif="GTC",
            outsideRth=True,
            transmit=True,
            
            orderRef=f"Parent Limit - {symbol}",
        )
       
            
        bracket = ck_bk_ib.ib.bracketOrder(
            action,
            quantity,
            round(limit_price, 2),
            take_profit_price,
            stop_loss_price,
            outsideRth=True,
            ocaGroup=ocaGroup,
            ocaType=1,  # OCA type 1 for bracket orders
            orderRef=f"Bracket OCA - {symbol}",
            tif="GTC"
        
        )
        #logger.info(f"Bracket order created: {bracket} for {contract.symbol}")
      
        
        trade= await place_bracket_order(contract, parent_order)
        if trade:
            
            
            order_json = {
                "ticker": contract.symbol,
                "orderAction": action,
                "limit_price": round(limit_price, 2),
                "stop_loss_price": stop_loss_price,
                "take_profit_price": take_profit_price,
                "brokerComish": brokerComish,   
                "perShareRisk": perShareRisk,
                "toleratedRisk": toleratedRisk,
                "entryPrice": entryPrice,
                "stopLoss": stopLoss,
                "uptrend": uptrend,
                "price_source": price_source,
                    
                "quantity": quantity,
                "rewardRiskRatio": order_details.rewardRiskRatio,
                "riskPercentage": order_details.riskPercentage,
                "accountBalance": order_details.accountBalance,
                "stopType": order_details.stopType,
                "atrFactor": order_details.atrFactor,
            }
            
            #web_request_json =jsonable_encoder(order_json)
            #logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss_price {stop_loss_price} and web_request_json: {web_request_json} with order_json: {order_json}")
            return order_json

        
        



    except Exception as e:
        logger.error(f"Error placing bracket OCA order for {symbol}: {e}")
        return False


async def place_bracket_order(contract: Contract, order: Order):
    try:
        whatIfOrderAsync = None
        logger.info(f"Placing bracket order for {contract.symbol}...")
        #vstop_bars = await debug_vstop_bars(contract.symbol)
        
        
 
            
        whatIfOrderAsync = await ck_bk_ib.ib.whatIfOrderAsync(contract, order)
        await asyncio.sleep(1)
        logger.info(f"Bracket order: {order} for {contract.symbol} with whatIfOrderAsync: {whatIfOrderAsync}")
        if whatIfOrderAsync:
            order_state_json = {
                "status":              whatIfOrderAsync.status,
                "initMarginBefore":    whatIfOrderAsync.initMarginBefore,
                "maintMarginBefore":   whatIfOrderAsync.maintMarginBefore,
                "equityWithLoanBefore":whatIfOrderAsync.equityWithLoanBefore,
                "initMarginChange":    whatIfOrderAsync.initMarginChange,
                "maintMarginChange":   whatIfOrderAsync.maintMarginChange,
                "equityWithLoanChange":whatIfOrderAsync.equityWithLoanChange,
                "initMarginAfter":     whatIfOrderAsync.initMarginAfter,
                "maintMarginAfter":    whatIfOrderAsync.maintMarginAfter,
                "equityWithLoanAfter": whatIfOrderAsync.equityWithLoanAfter,
                "commission":          round(whatIfOrderAsync.commission, 2),
                "minCommission":       round(whatIfOrderAsync.minCommission, 2),
                "maxCommission":       round(whatIfOrderAsync.maxCommission, 2),
                "commissionCurrency":  whatIfOrderAsync.commissionCurrency,
                "warningText":         whatIfOrderAsync.warningText,
                "completedTime":       whatIfOrderAsync.completedTime,
                "completedStatus":     whatIfOrderAsync.completedStatus,
            }
            #logger.info(format_what_if_details_table(order_state_json))
            table=(format_what_if_details_table(order_state_json))

            #print(order_state_json)

            print(table)


            
            

            
        
            return whatIfOrderAsync
    except Exception as e:
        logger.error(f"Error placing bracket order for {contract.symbol}: {e}")
        return None
    

async def bracket_trade_update(trade: Trade):
    try:
        avgPrice = 0.0
        await ib_open_orders.set(trade.contract.symbol, trade.order.orderId, trade)
        fills = ck_bk_ib.ib.fills()
        for fill in fills:
            if fill.contract.symbol == trade.contract.symbol:
                avgPrice = fill.execution.avgPrice
                print('fill')
                print(fill)
                logger.info(
                    f"Bracket order filled: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity} avgPrice: {avgPrice} for {trade.contract.symbol} order Status filled: {trade.orderStatus.filled}"
                )
                break
        logger.info(
            f"Bracket order updated: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity}  for {trade.contract.symbol}"
        )
    except Exception as e:
        logger.error(f"Error in bracket_trade_update: {e}")
async def bracket_trade_fill(trade, fill):
    try:
        avgPrice = fill.execution.avgPrice
        await ib_open_orders.set(trade.contract.symbol, trade.order.orderId, trade)
        logger.info(
            f"Bracket order updated: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity} avgPrice: {avgPrice} for {trade.contract.symbol}"
        )
    except Exception as e:
        logger.error(f"Error in bracket_trade_fill: {e}")