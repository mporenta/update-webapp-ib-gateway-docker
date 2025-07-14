
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
from urllib import response

import pytz
import aiosqlite
import math
from math import *
from pnl import get_orders

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

ib = IB()
from models import (
    OrderRequest,
    AccountPnL,
    TickerResponse,
    QueryModel,
    
    TickerSub,
    TickerRefresh,
    TickerRequest,
    PriceSnapshot,
    VolatilityStopData,
    RealTimeBarListModel
)

from tv_ticker_price_data import  tv_store_data, price_data_dict, daily_volatility, yesterday_close_bar, yesterday_close, daily_true_range, timeframe_dict, order_placed, subscribed_contracts, ib_open_orders

# from pnl import IBManager

from tv_scan import tv_volatility_scan_ib
import numpy as np
from timestamps import current_millis
from log_config import log_config, logger
from ticker_list import ticker_manager
from ib_pnl_data import app_pnl_event
from check_orders import  process_web_order_check, ck_bk_ib
from p_rvol import PreVol, start_rvol
from my_util import *
from my_util import is_market_hours, inspect_dict_values, shortable_shares, aggregate_5s_to_nsec, update_ticker_data
from ib_db import (
    IBDBHandler,
   
    
   
    
    
  
    )

from super import supertrend, supertrend_last

from ib_bracket import ib_bracket_order, bk_ib

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
HOST = os.getenv("FAST_API_HOST", "localhost")
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv("FAST_API_CLIENT_ID", "2222"))
ib_host = os.getenv("IB_GATEWAY_HOST", "localhost")
ib_port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
boofMsg = os.getenv("HELLO_MSG")


subscribed_contracts.set_ib(ib)
bk_ib.set_ib(ib)
ck_bk_ib.set_ib(ib)
# default ATR factor



async def bracket_oca_order(
    contract: Contract,
    action: str,
    quantity: float,
    trade: Trade,
    order_details=None,
    parent_quantity=None,
    avgPrice=None,
):
    try:
        snapshot = []
        vStop = None
        vStop_float = None
        completed_order_symbol = None
        current_order_symbol = None
        symbol = contract.symbol
        get_symbol_dict= await get_tv_ticker_dict(symbol)
        stopLoss = get_symbol_dict.stopLoss
        uptrend =get_symbol_dict.uptrend
        entryPrice = get_symbol_dict.entryPrice
        stop_diff = stopLoss - entryPrice if stopLoss and entryPrice else None
        snapshot = await price_data_dict.get_snapshot(symbol)
        
        logger.info(f"stopLoss: {stopLoss} and order entryPrice: {entryPrice} and stop_diff: {stop_diff} and uptrend: {uptrend} for {symbol}")
        price_data = await price_data_dict.get_snapshot(symbol)

        update_tick = await update_ticker_data(contract, ib)
        if snapshot.hasBidAsk:
            logger.info(f"Bracket OCA order for {symbol}  hasBidAsk:...")
        if update_tick:
            logger.info(f"Ticker data updated for {symbol}.")
            
        else:
            logger.warning(f"Failed to update ticker data for {symbol}. Skipping order placement.")
            await asyncio.sleep(1)
            update_tick = await update_ticker_data(contract, ib)
            if update_tick:
                logger.info(f"Ticker data updated for {symbol} on retry.")
                snapshot = await price_data_dict.get_snapshot(symbol)
            else:
                logger.error(f"Failed to update ticker data for {symbol} on retry. Skipping order placement.")
                return None
        correct_order= False
        histData = None
        bars= [] 
        df = pd.DataFrame()
        

        logger.info(f"Placing bracket OCA order for {contract.symbol}...correct_order is {correct_order}")
        parent_order_id = trade.order.orderId
        tasks = []
        
        limit_price = 0.0
        stop_loss_price = 0.0
        bracket_trade = None
        market_price = None
        bracket_orders: List[Order] = []
        order_history = await order_db.get_web_orders_by_ticker(contract.symbol)
        order_details = order_history[0] if order_history else None
        logger.info(f"Loaded OrderRequest for {symbol}: {order_details.model_dump()}")
        
        

        #snapshot = await price_data_dict.get_snapshot(symbol)
        
        _yesterday_close=yesterday_close[symbol]
        last_daily_bar_dict=price_data_dict.last_daily_bar_dict

        vol= await daily_volatility.get(symbol)
        if vol is None or isNan(vol) or (_yesterday_close is None or isNan(_yesterday_close)):
            logger.warning(f"Volatility is None or NaN for {symbol}, calling compute_volatility_series...")
            _yesterday_close= await yesterday_close_bar(ib,contract)
           
            last_daily_bar_dict[symbol] = df.iloc[-1]
            yesterday_close[symbol]=  df.iloc[-2]['close'] if len(df) >= 2 else None
            last_daily_bar =  last_daily_bar_dict[symbol] 
            _yesterday_close =  yesterday_close[symbol]
           
            
            
            vol=await compute_volatility_series(snapshot.high , snapshot.low , _yesterday_close, contract, ib )
            
            logger.warning(f"Yesterday's close was None or NaN for {symbol}, called yesterday_close_bar: yesterday_close {_yesterday_close} volatility {vol}")
           
            
        atrFactorDyn = await get_dynamic_atr(symbol, BASE_ATR=None, vol=vol)

        vstopAtrFactor = atrFactorDyn
        
        entryPrice = order_details.entryPrice
        barSizeSetting = order_details.timeframe
        orderAction = order_details.orderAction
        logger.info(f"OrderAction for {symbol}: {orderAction} calling async gather for orders are historical data...")
        current_orders, completed_orders, = await asyncio.gather(
            ib.reqAllOpenOrdersAsync(),
            ib.reqCompletedOrdersAsync(False),
           
        )
        
        if stopLoss is not None and stopLoss != 0:
            logger.info(f"Using stopLoss from webhook: {stopLoss}")
            stop_loss_price = stopLoss
        else:
            logger.info(f"Using limit_price for {symbol}: {limit_price} where market_price {market_price}")
            await asyncio.sleep(0.2)
            
            vStop_float = vstop_data[symbol].iloc[-1]
            uptrend = uptrend_data[symbol].iloc[-1]
            await price_data_dict.add_vol_stop(symbol, vStop_float, uptrend)

            
            logger.info(f"Calculating vStop {vStop_float} and uptrend {uptrend} for {symbol}...")
            if vStop_float > 1.0:

                stop_loss_price = (
                    round(vStop_float, 2) + 0.02 if not uptrend else round(vStop_float, 2) - 0.02
                )
        
        
        for current_order in current_orders:
            current_order_symbol=current_order.contract.symbol
            if symbol == current_order_symbol:
                correct_order = True
                logger.info(f"Correct current_order found for {current_order}.")
        for completed_order in completed_orders:
            completed_order_symbol=completed_order.contract.symbol
            if symbol == completed_order_symbol:
                correct_order = True
                logger.info(f"Correct completed_order found for {completed_order}.")
                break
        
        if (
            order_details.quantity is not None and order_details.quantity != 0
        ) and order_details.quantity == parent_quantity:
            logger.info(f"Using quantity from webhook: {order_details.quantity}")
            quantity = order_details.quantity
        elif (
            order_details.quantity is not None and order_details.quantity != 0
        ) and order_details.quantity != parent_quantity:
            logger.info(f"Using quantity from webhook: {order_details.quantity}")
            quantity = parent_quantity
        
            barSizeSetting, bar_sec_int = await convert_pine_timeframe_to_barsize(
                order_details.timeframe
            )

        ocaGroup = f"exit_{symbol}_{trade.order.orderId}_{trade.order.orderId}"
        ask_price = 0.0
        bid_price = 0.0
        markPrice = 0.0
        last = 0.0
        take_profit_price = 0.0
        logger.info(f"Last close for {symbol}: {price_data_dict.get_snapshot(symbol)}")
        
        if snapshot.hasBidAsk:
        
            ask_price = snapshot.ask if (snapshot.ask is not None and not isNan(snapshot.ask) and snapshot.ask > 1.1) else 0.0
            bid_price = snapshot.bid if (snapshot.bid is not None and not isNan(snapshot.bid) and snapshot.bid > 1.1) else 0.0
            last = snapshot.last if (snapshot.last is not None and not isNan(snapshot.last) and snapshot.last >1.1) else 0.0
            markPrice = snapshot.markPrice if (snapshot.markPrice is not None and not isNan(snapshot.markPrice) and snapshot.markPrice > 0) else None
        # Priority: avgPrice > entryPrice > live market data > last close
        if avgPrice and not isNan(avgPrice) and avgPrice > 0.0:
            limit_price = round(avgPrice, 2)
            logger.info(f"Using avgPrice as limit_price: {limit_price}")
        elif entryPrice and entryPrice > 0.0:
            limit_price = round(entryPrice, 2)
            logger.info(f"Using entryPrice as limit_price: {limit_price}")
        else:
            ask_price = snapshot.ask if (snapshot.ask is not None and not isNan(snapshot.ask) and snapshot.ask > 1.1) else 0.0
            bid_price = snapshot.bid if (snapshot.bid is not None and not isNan(snapshot.bid) and snapshot.bid > 1.1) else 0.0
            last = snapshot.last if (snapshot.last is not None and not isNan(snapshot.last) and snapshot.last >1.1) else 0.0
            markPrice = snapshot.markPrice if (snapshot.markPrice is not None and not isNan(snapshot.markPrice) and snapshot.markPrice > 0) else None
        
            if action == "BUY" and ask_price > 2.0:
                limit_price = round(ask_price, 2)
            elif action == "SELL" and bid_price > 2.0:
                limit_price = round(bid_price, 2)
            elif not isNan(snapshot.last):
                limit_price = round(snapshot.last, 2)
            elif not isNan(snapshot.markPrice) and snapshot.markPrice > 0.0:
                limit_price = round(snapshot.markPrice, 2)          
            else:
                limit_price = 0.0
            logger.warning(f"Using fallback limit_price: {limit_price}")
        
        
       

        logger.info(
            f"bracket_oca_order: ocaGroup: {ocaGroup} {symbol} limit_price={limit_price} stop_loss={stop_loss_price}"
        )

        logger.info(f"Calculating position size for {symbol}...")

        position_values = await compute_position_size_order(
            limit_price,
            stop_loss_price,
            order_details.accountBalance,
            order_details.riskPercentage,
            order_details.rewardRiskRatio,
            action,
        )
        quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = (
            position_values
        )
        logger.info(
            f"Calculated position size for {symbol}: {quantity} and take_profit_price {take_profit_price}"
        )

        reverse_action = "BUY" if action == "SELL" else "SELL"
        stop_loss_price = round(stop_loss_price, 2)
        take_profit_price = round(take_profit_price, 2)

        # Create limit take profit order
        take_profit_order = LimitOrder(
            reverse_action,
            totalQuantity=1,
            lmtPrice=take_profit_price,
            tif="GTC",
            #parentId=parent_order_id,
            outsideRth=True,
            transmit=True,
            orderRef=f"bk_take_profit_{symbol}_{trade.order.orderId}",
        )


        # Create stop loss order
        stop_loss_order = StopOrder(
            reverse_action,
            totalQuantity=1,
            stopPrice=stop_loss_price,
            tif="GTC",
            #parentId=parent_order_id,
            outsideRth=True,
            transmit=True,
            orderRef=f"bk_stop_loss_{symbol}_{trade.order.orderId}",
        )

        logger.info(
            f"Bracket OCA orders: stop_loss_order: {stop_loss_order} take_profit_order: {take_profit_order} "
        )

        bracket_orders = ib.oneCancelsAll(
            [stop_loss_order, take_profit_order], ocaGroup=ocaGroup, ocaType=1
        )
        for bracket_order in bracket_orders:
            
            tasks.append(place_bracket_order(contract, bracket_order))

       
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Process results to check for errors
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error executing close order: {result}")
            logger.info(
                f"bracket order tasks have been placed for {len(tasks)} orders"
            )
        else:
            logger.info(
                "No bracket order tasks have been placed."
            )

    except Exception as e:
        logger.error(f"Error placing bracket OCA order for {symbol}: {e}")
        return False


async def place_bracket_order(contract: Contract, bracket_order: Order):
    try:
        logger.info(f"Placing bracket order for {contract.symbol}...")
        #vstop_bars = await debug_vstop_bars(contract.symbol)
        vstop_bars = None
        logger.info(f"Debug vStop bars for {contract.symbol}: {vstop_bars}")
        print(vstop_bars)
        trade = ib.placeOrder(contract, bracket_order)
        if trade:
            logger.info(f"Bracket order placed: {trade.contract.symbol}")
        return trade
    except Exception as e:
        logger.error(f"Error placing bracket order for {contract.symbol}: {e}")
        return None





async def place_limit_order(
    contract, action: str, quantity=None, order_details: OrderRequest = None
):
    try:
        vStop_float = None
        symbol = contract.symbol
        orderAction = action
        get_symbol_dict= await get_tv_ticker_dict(symbol)
        if get_symbol_dict is not None:
            
            order_details = get_symbol_dict
            logger.info(f"get_symbol_dict for {symbol}: {order_details}")
            logger.info(f"get_symbol_dict for {symbol}: {order_details.model_dump()}")

        stopLoss = get_symbol_dict.stopLoss
        uptrend =get_symbol_dict.uptrend
        entryPrice = get_symbol_dict.entryPrice
        stop_diff = stopLoss - entryPrice if stopLoss and entryPrice else None
        
        logger.info(f"stopLoss: {stopLoss} and order entryPrice: {entryPrice} and stop_diff: {stop_diff} and uptrend: {uptrend} for {symbol}")
        
        
        entryPrice = order_details.entryPrice
        stopLoss = order_details.stopLoss
        market_price = 0.0
        limit_price = 0.0
        stop_loss_price = 0.0
        ask_price = None
        bid_price = None
        last = None
        trade = None
        order = []
        symbol = contract.symbol
        markPrice= None
        uptrend = None
        vStop = None
        isHalted = None
        snapshot = await price_data_dict.get_snapshot(symbol)
        shortableShares = snapshot.shortableShares
        # Check if vStop and uptrend are already calculated
        inspect_dict_values("vstop_data", vstop_data, symbol)
        inspect_dict_values("uptrend_data", uptrend_data, symbol)

        # If not, fetch historical data
        uptrend_bool = uptrend_data[symbol].iloc[-1]
        vStop_float = vstop_data[symbol].iloc[-1]
        logger.info(f"Calculating vStop and uptrend for {symbol}...vStop_float{vStop_float}")

        # Check the type properly first
        if isinstance(vStop_float, (int, float)) and not isNan(vStop_float):
            logger.info(f"vStop_float: {vStop_float}")
            vStop = vStop_float
            uptrend = uptrend_bool
            stop_loss_price = round(vStop, 2)

        elif stopLoss is not None and stopLoss != 0:
            stop_loss_price = stopLoss
        # Determine action if not explicitly passed
        if uptrend is not None and (entryPrice == 0.0 or action is None or action == ""):
            if uptrend:
                action = "BUY"
            elif not uptrend:
                action = "SELL"
            else:
                action = orderAction
        logger.info(f"The action is {action} for {symbol}")      
        if snapshot.halted == 1.0:
            isHalted = True
            logger.info(f"{contract.symbol} is  halted, cannot place order.")
        if isHalted:
            logger.warning(f"{contract.symbol} is halted, cannot place order.")
            return None  
        logger.info(f"{contract.symbol} is  not halted, can place order. snapshot.halted  {snapshot.halted}")
    
        shortable= False
        if action == "SELL":
            logger.info(f"Checking if {contract.symbol} is shortable...")
            shortable_shares(contract.symbol, action, quantity, shortableShares)

            if shortable_shares:
                logger.info(f"{contract.symbol} is shortable.")
                
                shortable = True
            if not shortable:
                logger.warning(f"{contract.symbol} is not shortable, cannot place SELL order.")
                
                return None
        logger.info(
            f"Placing LIMIT order for {contract.symbol} with action {action} and quantity {quantity} and entryPrice {order_details.entryPrice} and stopLoss {order_details.stopLoss} "
        )
        has_quantity = True
        if order_details.quantity is None  or isNan(quantity) or (quantity == 0 and not isNan(quantity) and quantity is not None):
            has_quantity = False
            logger.info(f"Calculating position size for {contract.symbol}...quantity is {quantity}")
        
        # Default bar size and ATR factor
        barSizeSetting = order_details.timeframe or "15 secs"
        
        

        
        if snapshot.hasBidAsk:

        # Pull latest prices
            ask_price = snapshot.ask if (snapshot.ask is not None and not isNan(snapshot.ask) and snapshot.ask > 1.1) else None
            bid_price = snapshot.bid if (snapshot.bid is not None and not isNan(snapshot.bid) and snapshot.bid > 1.1) else None
            last = snapshot.last if (snapshot.last is not None and not isNan(snapshot.last) and snapshot.last >1.1) else None
            markPrice = snapshot.markPrice if (snapshot.markPrice is not None and not isNan(snapshot.markPrice) and snapshot.markPrice > 0) else None
        
        
        if ask_price is not None and bid_price is not None and entryPrice is None or entryPrice == 0:
            logger.info(f"Using ask_price {ask_price} and bid_price {bid_price} for {symbol}.")
            market_price = ask_price if action == "BUY" else bid_price
            limit_price = market_price  
        if entryPrice is not None and entryPrice != 0:
            logger.info(f"Using entryPrice from order_details: {entryPrice}")
            market_price = entryPrice
            limit_price = market_price
        elif entryPrice is None or entryPrice == 0 and bid_price is not None:

            market_price = markPrice if markPrice is not None and not isNan(markPrice) and markPrice > 0 else last
            limit_price = market_price
            logger.info(f"Using ticker market price for {symbol}: {market_price}")
        
        if market_price == 0.0 or market_price is None or isNan(market_price):
            logger.warning(f"No bid/ask for {symbol}, using historical close.")
            limit_price = snapshot.markPrice

        logger.info(
            f"Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and entryPrice {entryPrice} and stopLoss {stopLoss} and market_price {market_price} and limit_price {limit_price} and vStop {vStop} and uptrend {uptrend} last {last} and snapshot.markPrice {snapshot.markPrice}"
        )
        logger.info(
            f"Order setup for {symbol}: uptrend={uptrend}, vStop={vStop}, last={last}"
        )
        if not is_market_hours():
            
            limit_price=limit_price - 3.0 if action == "SELL" else limit_price + 3.0
            logger.info(f"Market is not open for {symbol}, placing order...limit_price {limit_price}")

        # Compute quantity if not provided
        if not has_quantity:
            logger.info(f"Calculating position size for {symbol}...")
            position_values = await compute_position_size_order(
                round(limit_price, 2),
                stop_loss_price,
                order_details.accountBalance,
                order_details.riskPercentage,
                order_details.rewardRiskRatio,
                action,
            )
            quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = position_values

        if stop_loss_price == 0.0 or limit_price == 0.0 or quantity == 0.0:
            logger.warning(f"Invalid stop_loss_price or limit_price for {symbol} quantity   {quantity} limit_price {limit_price} stop_loss_price {stop_loss_price}")
            raise ValueError("Invalid stop_loss_price or limit_price for {symbol}.")
        reqId = ib.client.getReqId()
        if not is_market_hours():
            order = LimitOrder(
                action,
                totalQuantity=1,
                lmtPrice=round(limit_price, 2),
                tif="GTC",
                outsideRth=True,
                transmit=True,
                orderId=reqId,
                orderRef=f"Parent Limit - {symbol}",
            )
        else:
             
            order = LimitOrder(
                action=action,
                totalQuantity=1,
                lmtPrice=round(limit_price, 2),
                tif="GTC",
                transmit=True,
                algoStrategy="Adaptive",
                outsideRth=False,
                orderRef=f"Parent Adaptive - {symbol}",
                algoParams=[TagValue("adaptivePriority", "Normal")],
            )
            logger.warning(f"algoStrategy order for {symbol} quantity   {quantity} limit_price {limit_price} order {order}")

        # Place the order
        barcket = None
        trade = await place_order(contract, order)
        #trade.filledEvent += on_trade_filled_event
        order_json = {
            "ticker": contract.symbol,
            "orderAction": orderAction,
            "limit_price": round(limit_price, 2),
            "stop_loss_price": stop_loss_price,
            "take_profit_price": take_profit_price,
            "brokerComish": brokerComish,   
            "perShareRisk": perShareRisk,
            "toleratedRisk": toleratedRisk,
            "entryPrice": entryPrice,
            "stopLoss": stopLoss,
            "uptrend": uptrend,
                   
            "quantity": quantity,
            "rewardRiskRatio": order_details.rewardRiskRatio,
            "riskPercentage": order_details.riskPercentage,
            "accountBalance": order_details.accountBalance,
            "stopType": order_details.stopType,
            "atrFactor": order_details.atrFactor,
        }
        logger.info(format_order_details_table(order_json))
        web_request_json =jsonable_encoder(order_json)
        logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss_price {stop_loss_price} and web_request_json: {web_request_json}")

        
        
        

        logger.info(
            f"Limit order placed: {trade.orderStatus.status} for {symbol} {action} {quantity} @ {limit_price} barcket={barcket}"
        )
        return trade

    except Exception as e:
        logger.error(f"Error placing LIMIT order for {symbol}: {e}")
        return None
async def place_order(contract: Contract, order: Order):
    try:
        trade = []
        trade: Trade =  ib.placeOrder(contract, order)
        if trade is not None and trade != []:
            
            
        
            
            return trade
        else:
            logger.error(f"Failed to place order for {contract.symbol}: trade is None or empty.")
            return None
    except Exception as e:
        logger.error(f"Error placing order for {contract.symbol}: {e}")


async def compute_position_size_order(
    entry_price: float,
    stop_loss_price: float,
    account_balance: float,
    risk_percentage: float,
    reward_risk_ratio: float,
    action: str,
):
    try:
        logger.info(
            f"Computing position size for {entry_price}, {stop_loss_price}, {account_balance}, {risk_percentage}, {reward_risk_ratio}, {action}"
        )
        quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = (
            await compute_position_size(
                entry_price,
                stop_loss_price,
                account_balance,
                risk_percentage,
                reward_risk_ratio,
                action,
            )
        )

        return quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk

    except Exception as e:
        logger.error(f"Error computing position size: {e}")
        raise e



    
async def on_trade_filled_event(trade: Trade, fill: Fill = None) -> bool:
    try:
        symbol = trade.contract.symbol
        positions = []
        bracket = []
        await price_data_dict.add_ticker(ib.ticker(trade.contract)) 
        avgPrice = None
        if trade:
            avgPrice = await order_fill(trade)
        
        orderRef = trade.order.orderRef
        logger.info(f"Order status event for trade: {symbol} orderRef: {orderRef} orderId: {trade.order.orderId}")
        
        if orderRef and "Parent" in orderRef:
            logger.info(f"Placing bracket order for orderRef {orderRef}")
            bracket = await bracket_oca_order(
                trade.contract,
                trade.order.action,
                trade.order.totalQuantity,
                trade,
                order_details=None,
                parent_quantity=trade.order.totalQuantity,
                avgPrice=avgPrice
            )
            if bracket:
                logger.info(f"Placed bracket order for parket orderRef {orderRef}")
        else: 
            bracket = []
        
        order_id = trade.order.orderId
        logger.info(f"Order status event for trade: {symbol} trade: {order_id} trade and bracket order {bracket}")
        logger.info(f"Trade filled event: {fill}")

        # Insert trade in DB
        await order_db.insert_trade(trade)
        await asyncio.sleep(1)  # Give some time for the order to be inserted
        await get_orders(trade, symbol)

    except Exception as e:
        logger.error(f"Error in order status event: {e}")
        return False
    


async def order_fill(trade: Trade) -> float:
    try:
        symbol = trade.contract.symbol
        avgPrice = None
        order_id = trade.order.permId
        logger.info(f"Order status event for trade: {symbol} order_id: {order_id}")
        fills = ib.fills()
        for fill in fills:
            if fill.execution.permId == order_id:
                avgPrice = fill.execution.avgPrice
                logger.info(f"Fill found for order {order_id}: {avgPrice}")
                if avgPrice is None or avgPrice == 0:
                    logger.warning(f"No avgPrice found for order {order_id}.")
                    return 0.0
                elif avgPrice > 0:
                    logger.info(f"Avg price for order {order_id} is {avgPrice}")
                    
                    return avgPrice
                else:
                    logger.warning(f"No valid avgPrice found for order {order_id}.")
                    return 0.0

    except Exception as e:
        logger.error(f"Error in order status event: {e}")
        return 0.0