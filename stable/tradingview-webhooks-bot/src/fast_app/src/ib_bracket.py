    

# app.py
from asyncio import tasks
from collections import defaultdict, deque


from typing import *
import os, asyncio, time

from threading import Lock
from datetime import datetime, timedelta, timezone


from math import *


import pandas as pd


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
from tv_ticker_price_data import  price_data_dict, daily_volatility, yesterday_close_bar, yesterday_close, ib_open_orders, parent_ids, logger, volatility_stop_data, barSizeSetting_dict

from tech_a import ema_check
from fill_data import fill_data 
#from indicators import daily_volatility
from my_util import vol_stop, format_order_details_table, shortable_shares, get_timestamp, convert_pine_timeframe_to_barsize, get_tv_ticker_dict, get_dynamic_atr, compute_position_size
from ib_db import (
    IBDBHandler
   
    
    
    
    )




reqId = {}

order_db = IBDBHandler("orders.db")
# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), ".env")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

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

class BracketIB:
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

bk_ib= BracketIB() 

async def ib_bracket_order(
    contract: Contract,
    action: str,
    quantity: float,
    order_details: OrderRequest,
    vol=None,
    stopLoss=None
):
    try:
        symbol = contract.symbol
      

        snapshot = None
        trade = None
        price_source=""
        order_json = None

        entryPrice = 0.0
        stop_loss_price = 0.0

        barSizeSetting = barSizeSetting_dict.get(symbol, "15 secs")  # Default to 15 seconds if not found
        uptrend = None
        vStop_float = 0.0
        vstopAtrFactor= None
        atrFactorDyn = None
        get_symbol_dict = None
        if order_details.stopLoss > 0.0 and stopLoss is not None:
            stop_loss_price =float(stopLoss)
            stop_loss_price = round(stop_loss_price, 2)
            logger.info(f"Using stopLoss from webhook: {stop_loss_price} for {symbol}")
            

        
        if order_details.entryPrice > 0.0:
            get_symbol_dict= await get_tv_ticker_dict(symbol)
            
            stopLoss = get_symbol_dict.stopLoss
            uptrend =get_symbol_dict.uptrend
        else:
            get_symbol_dict=await price_data_dict.get_snapshot(symbol)
        
        
        
        
        tick, y_close_df = await asyncio.gather(price_data_dict.add_ticker(bk_ib.ib.ticker(contract), barSizeSetting), yesterday_close_bar(bk_ib.ib, contract))

        snapshot, vol = await asyncio.gather(price_data_dict.get_snapshot(symbol),  daily_volatility.get(symbol))
        vol= await daily_volatility.get(symbol)
       
        logger.info(f"Vol is {vol} and Loaded OrderRequest for {symbol}: {order_details.model_dump()}")
        
        
        
        
        if stopLoss == 0.0 or isNan(stopLoss):
            atrFactorDyn = await get_dynamic_atr(symbol, BASE_ATR=None, vol=vol)

            vstopAtrFactor = atrFactorDyn
            if order_details.entryPrice is not None and order_details.entryPrice > 0.0:
                entryPrice = order_details.entryPrice
           
            snapshot=await price_data_dict.get_snapshot(symbol)
        snapshot = await price_data_dict.get_snapshot(symbol)
        logger.info("\n" + snapshot.to_table())
        logger.info(f"stopLoss: {stopLoss} and order entryPrice: {entryPrice}  and uptrend: {uptrend} for {symbol} and  snapshot.uptrend {snapshot.uptrend}")
        if snapshot is None:
            logger.warning(f"Price data is None for {symbol}. Skipping order placement.")
            raise ValueError(f"Price data is None for {symbol}. Skipping order placement.")
        
        action = "BUY" if snapshot.uptrend > 0.0  else "SELL"
        if snapshot.hasBidAsk:
            logger.info(f"Bracket OCA order for {symbol}  hasBidAsk:...")
        
        emaCheck=await ema_check(contract, bk_ib.ib, barSizeSetting)
        if emaCheck is not None:
            action = "BUY" if emaCheck =="LONG" else "SELL"
            logger.info(f"emaCheck order Action for {contract.symbol} is {action}  and emaCheck {emaCheck}"
            ) 
        if emaCheck is None:
            raise ValueError(f"emaCheck is None for {symbol}, cannot place order.")
        
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
       
        logger.info(f"Placing bracket OCA order for {contract.symbol}...correct_order is {correct_order}")
  
        limit_price = 0.0
  
        market_price = None
  
        await price_data_dict.add_ticker(bk_ib.ib.ticker(contract), barSizeSetting)
        
        orderAction = order_details.orderAction
        logger.info(f"OrderAction for {symbol}: {orderAction} calling async gather for orders are historical data...")
        
        if entryPrice > 0.0:
            limit_price = round(entryPrice,2)
            uptrend = order_details.uptrend

        if stopLoss is not None and stopLoss != 0:
            logger.info(f"Using stopLoss from webhook: {stopLoss}")
            stop_loss_price = stopLoss
        if stop_loss_price <= 0.0 or isNan(stop_loss_price):
            logger.info(f"Using limit_price for {symbol}: {limit_price} where market_price {market_price}")
            
            vStop_float = snapshot.vStop if snapshot.vStop is not None else 0.0
            uptrend = snapshot.uptrend if snapshot.uptrend is not None else None
            #await asyncio.sleep(0.2)
            

            
            logger.info(f"Calculating vStop {vStop_float} and uptrend {uptrend} for {symbol}...")
            if vStop_float > 1.0:

                stop_loss_price = (
                    round(vStop_float, 2) + 0.02 if not uptrend else round(vStop_float, 2) - 0.02
                )
        
        
        
        
        

        
        ask_price = 0.0
        bid_price = 0.0
        markPrice = 0.0
        take_profit_price = 0.0
        #logger.info(f"Last close for {symbol}: {await price_data_dict.get_snapshot(symbol)}")
        if snapshot.markPrice and (snapshot.markPrice is not None and not isNan(snapshot.markPrice) and snapshot.markPrice > 0):
            markPrice = snapshot.markPrice
        
        if snapshot.hasBidAsk and limit_price == 0.0:
        
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
        # Priority: avgPrice > entryPrice > live market data > last close  24889.35
       
        elif entryPrice > 0.0 and limit_price > 0.0:
            price_source = "entryPrice"
            logger.info(f"Using entryPrice as limit_price: {limit_price}")
        elif markPrice and markPrice > 0.0 and order_details.entryPrice == 0.0 and limit_price == 0.0:
            limit_price = round(markPrice, 2)
            price_source = "markPrice"
            logger.info(f"Using markPrice as limit_price: {limit_price}")
        elif limit_price is None or limit_price <= 0.0 and order_details.entryPrice > 0.0:
            limit_price = round(order_details.entryPrice, 2)
            price_source = "order_details.entryPrice"
            logger.warning(f"Limit price is None or <= 0.0 for {symbol}, using last close price...snapshot is {snapshot}")
            
            #await asyncio.sleep(0.2)
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
        stop_loss_price_adj = await last_bar_and_stop(snapshot, contract, stop_loss_price, barSizeSetting, bk_ib.ib)

        quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = (
            await compute_position_size(
                limit_price,
                stop_loss_price_adj,
                order_details.accountBalance,
                order_details.riskPercentage,
                order_details.rewardRiskRatio,
                action,
                order_details
            )
        )
        
        logger.info(
            f"Calculated position size for {symbol}: {quantity} and take_profit_price {take_profit_price}"
        )
        reqId = bk_ib.ib.client.getReqId()
        parent_ids[contract.symbol] = reqId
        ocaGroup = f"oca_{symbol}_{quantity}_{round(limit_price,2)}"
        logger.info(
            f"bracket_oca_order: ocaGroup: {ocaGroup} {symbol} limit_price={limit_price} stop_loss={stop_loss_price} entry price source= {price_source}"
        )
        

        reverse_action = "BUY" if action == "SELL" else "SELL"
        
        stop_loss_price = round(stop_loss_price_adj, 2)
        
        take_profit_price = round(take_profit_price, 2)
        
       
        #quantity = 1
        logger.warning(f"Actual quantity for {symbol}: {quantity}")
        bracket = bk_ib.ib.bracketOrder(
            action,
            quantity,
            round(limit_price, 2),
            take_profit_price,
            stop_loss_price,
            outsideRth=True,
           
            #ocaGroup=ocaGroup,
            #ocaType=1,  # OCA type 1 for bracket orders limit_price
            orderRef=f"Bracket OCA - {symbol}",
            tif="GTC"
        
        )
        
        #logger.info(f"Bracket order created: {bracket} for {contract.symbol}")
      
        trade= await place_bracket_order(contract, bracket)
                                                                  
                                                                  

        if trade is not None:
            
            
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
            
            #logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss_price {stop_loss_price} and web_request_json: {web_request_json} with order_json: {order_json}")
        logger.info(format_order_details_table(order_json))
        return order_json

        
        



    except Exception as e:
        logger.error(f"Error placing bracket OCA order for {symbol}: {e}")
        return False


async def place_bracket_order(contract: Contract, bracket_order: Order):
    try:
        trade = None
        logger.debug(f"Placing bracket order for {contract.symbol}...")
        #vstop_bars = await debug_vstop_bars(contract.symbol)
        
        for o in bracket_order:
            logger.debug(f"Placing order: {o} for {contract.symbol}")
            trade: Trade = bk_ib.ib.placeOrder(contract, o)
        if trade:
            trade.fillEvent += bracket_trade_fill
            
            
        
        return trade
    except Exception as e:
        logger.error(f"Error placing bracket order for {contract.symbol}: {e}")
        return None
   
async def last_bar_and_stop(
    snapshot,
    contract: Contract,
    stop_loss_price: float,  # your vStop value
    barSizeSetting: str,
    ib: IB = None
) -> float:
    """
    Calculate an adjusted stop‐loss price based on the last 30s bar.

    Parameters
    ----------
    contract : Contract
        The IB contract.
    stop_loss_price : float
        Initial vStop value.
    barSizeSetting : str
        Bar size (e.g. "15 secs").
    ib : IB
        IB client instance.

    Returns
    -------
    float
        Adjusted stop‐loss level:
          • If gap ≥ 0.4%, keep vStop.
          • Otherwise, use close × 0.99 (long) or close × 1.01 (short).
    """
    try:
        stop_loss_price_adj= stop_loss_price
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",
            durationStr="30 S",
            barSizeSetting=barSizeSetting,
            whatToShow="TRADES",
            useRTH=False,
            formatDate=1,
        )
        logger.info(f"stop_loss_price raw {stop_loss_price_adj} and barSizeSetting {barSizeSetting} for {contract.symbol}")
        if not bars:
            logger.warning("No IB history for %s", contract.symbol)
            return stop_loss_price_adj

        df = util.df(bars)
        if df.empty:
            logger.warning("No IB history for %s", contract.symbol)
            return stop_loss_price_adj
        last = df.iloc[-1]
        ohlc = last[["open", "high", "low", "close"]].to_dict()

        vStop_float, uptrend_bool = await volatility_stop_data.get(contract.symbol)

        if uptrend_bool:
            gap_pct = (ohlc["low"] - stop_loss_price) / stop_loss_price * 100
            if gap_pct < 0.4:
                stop_loss_price = ohlc["close"] * 0.99
                stop_loss_price_adj= stop_loss_price
                logger.info(f"ohlc stop_loss_price clean {stop_loss_price_adj} for {contract.symbol}")
        else:
            gap_pct = (stop_loss_price - ohlc["high"]) / ohlc["high"] * 100
            if gap_pct < 0.4:
                logger.info(f"ohlc stop_loss_price clean {stop_loss_price_adj} for {contract.symbol}")
                stop_loss_price = ohlc["close"] * 1.01
                stop_loss_price_adj= stop_loss_price
        logger.info(f"stop_loss_price clean {stop_loss_price_adj}")
        return stop_loss_price_adj
 
    except Exception as e:
        logger.error(f"Error in last_bar_and_stop for {contract.symbol}: {e}")
        return stop_loss_price_adj
    

async def bracket_trade_update(trade: Trade):
    try:
        #await ib_open_orders.set(trade.contract.symbol, trade)
        avgPrice = 0.0
        if "Web close_positions" in trade.order.orderRef: 
            await ib_open_orders.set(trade.contract.symbol, trade)
            logger.info(f"Bracket order for {trade.contract.symbol} is a close position order, skipping update.")
            return
        
        
        logger.info(
            f"Bracket order updated: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity}  for {trade.contract.symbol}"
        )
    except Exception as e:
        logger.error(f"Error in bracket_trade_update: {e}")
        
async def new_trade_update(trade: Trade):
    try:
        logger.info(f"Boof New trade update for {trade.contract.symbol}: {trade.orderStatus.status}")
        await fill_data.set_trade(trade.contract.symbol, trade)
        
        #await ib_open_orders.set(trade.contract.symbol, trade)
        avgPrice = 0.0
        if "Web close_positions" in trade.order.orderRef:
            await ib_open_orders.set(trade.contract.symbol, trade)
            logger.info(f"New trade for {trade.contract.symbol} is a close position order, skipping update.")
            return
        logger.info(f"New trade update for {trade.contract.symbol}: {trade.orderStatus.status}")
        
        
        logger.info(
            f"Bracket order updated: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity}  for {trade.contract.symbol}"
        )
    except Exception as e:
        logger.error(f"Error in bracket_trade_update: {e}")

async def bracket_trade_fill(trade: Trade, fill: Fill):
    sym = trade.contract.symbol
    try:
        # ensure trade is recorded
        await fill_data.set_trade(sym, trade)
        # then record the fill
        #await fill_data.add_fill(sym, fill)

        avg_price = fill.execution.avgPrice
        await ib_open_orders.delete(sym)
        logger.info(
            f"Order {sym} updated: status={trade.orderStatus.status} "
            f"qty={trade.order.totalQuantity} avgPrice={avg_price}"
        )

    except Exception as e:
        logger.error(f"Error in trade_fill for {sym}: {e}")