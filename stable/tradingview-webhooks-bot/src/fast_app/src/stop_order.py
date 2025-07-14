import asyncio
from math import log
from ib_async import IB, Contract, LimitOrder, StopOrder, MarketOrder, util, TagValue, Order, Trade, Fill
from typing import Dict, Tuple, Optional
from tv_ticker_price_data import  volatility_stop_data, tv_store_data, price_data_dict, daily_volatility, yesterday_close_bar, yesterday_close, daily_true_range, timeframe_dict, order_placed, subscribed_contracts, ib_open_orders, barSizeSetting_dict, parent_ids, logger
from my_util import is_float, is_market_hours, vol_stop, get_dynamic_atr, compute_position_size, format_order_details_table, get_tv_ticker_dict,get_timestamp
from models import PriceSnapshot, OrderRequest
from fill_data import fill_data
from collections import defaultdict
from tech_a import ib_data
import numpy as np
ib_instance: Dict[str, IB] = defaultdict(IB)
import time

async def stop_loss_order(contract, order_details: OrderRequest, action,quantity, limit_price,stop_loss_price, snapshot: PriceSnapshot, ib, set_market_order):
    try:
        
        hasBidAsk = snapshot.hasBidAsk if snapshot else None
        if hasBidAsk:
            limit_price = snapshot.last          # IB Tick-by-tick “Last” price

            market_price = (
                snapshot.markPrice
                if snapshot.markPrice             # non-zero / non-NaN?
                else (snapshot.bid + snapshot.ask) * 0.5
            )

            last_bar_close = (
                snapshot.last_bar_close           # set by add_bar() / add_rt_bar()
                or snapshot.close                 # same value – both safe
            )

            yesterday_price = snapshot.yesterday_close_price
        ib_instance["ib"] = ib
        if set_market_order and is_market_hours():
            logger.info(f"stop_loss_order set_market_order is {set_market_order} for {contract.symbol}")
            return await market_stop_loss_order(contract, order_details, action,quantity,stop_loss_price, snapshot, ib, set_market_order)
            

        
                                                                  
        symbol = contract.symbol
        perShareRisk = None
        brokerComish = None
        toleratedRisk = None
        take_profit_price=0.0
        qty =None
        trade=None
        barSizeSetting, bar_sec_int = barSizeSetting_dict.get(symbol, "5 secs")  # default to 5 secs if not set
        stop_loss_price_adj, df, vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float = await last_bar_and_stop(snapshot, contract, stop_loss_price, barSizeSetting, ib)
        logger.debug(f"[{symbol}] Placing Stop-Loss order with stop_loss_price_adj={stop_loss_price_adj}, limitPrice={limit_price}, action={action}, quantity={quantity} and barSizeSetting, bar_sec_int={barSizeSetting}, {bar_sec_int}.")
        
        

        if quantity is None or quantity <= 0:

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
        

        # 3) For each symbol, create and place a parent LimitOrder (entry) + child StopOrder (stop‑loss).

        qty = quantity
        if qty is None or qty <= 0:
            logger.error(f"[{symbol}] Invalid quantity for Stop-Loss order.")
            raise ValueError(f"Invalid quantity for Stop-Loss order for {symbol}.")
        
        tif = "GTC"  # Good‑Til‑Canceled to keep the orders until filled or canceled :contentReference[oaicite:8]{index=8}
        reverse_action = "SELL" if action == "BUY" else "BUY"  # Reverse action for stop order
         # 3a) Parent LimitOrder: orderType = "LMT", transmit=False
        parent_order = None
        parent_trade = None
        parent_order_id = ib.client.getReqId()
        parent_ids[symbol] = parent_order_id
        
        
        if is_market_hours():
            parent_order = LimitOrder(
                action=action,
                totalQuantity=qty,
                lmtPrice=round(limit_price, 2),
                orderId=parent_order_id,
                tif="GTC",
                transmit=False,
                algoStrategy="Adaptive",
                outsideRth=False,
                orderRef=f"Parent Adaptive - {symbol}",
                algoParams=[TagValue("adaptivePriority", "Urgent")],
            )

        
           
        else:
            parent_order = LimitOrder(
                action=action,
                totalQuantity=qty,
                orderId=parent_order_id,
                lmtPrice=round(limit_price, 2),
                tif=tif,
                outsideRth=True,
                transmit=False,
                orderRef=f"Parent Limit - {symbol}"
            )
        
        parent_trade=ib.placeOrder(contract, parent_order)
        #parent_trade.fillEvent += stop_loss_trade_fill
        if not parent_trade or parent_trade is None:
            logger.error(f"[{symbol}] Failed to place parent LimitOrder.")
            return
        
        logger.debug(f"[{symbol}] Parent LimitOrder placed with parent ID {parent_ids[symbol]}.")

        # 3b) Child StopOrder: orderType = "STP", auxPrice = stop_price, parentId=parent_order_id, transmit=True
        child_order_id = ib.client.getReqId()
        
        child_stop = StopOrder(
            action= reverse_action,
            totalQuantity=qty,
            stopPrice=round(stop_loss_price_adj,2),
            tif=tif,
            outsideRth=True,
            orderId=child_order_id,
            parentId= parent_ids[symbol],  # Attach to parent :contentReference[oaicite:10]{index=10}
            transmit=True  # This will send both parent + child in one atomic call :contentReference[oaicite:11]{index=11}
        )
        child_order=ib.placeOrder(contract, child_stop)
        if child_order:
            trade= parent_trade
        
            logger.info(
                f"[{symbol}] placed Limit(@{limit_price}, LMT) and Stop‑Loss(@{stop_loss_price_adj}, STP) "
                f"— parentId={parent_order_id}, child_order={child_order}."
            )
            order_json = {
                "ticker": contract.symbol,
                "orderAction": action,
                "limit_price": round(limit_price, 2),
                "stop_loss_price": round(stop_loss_price_adj,2),
                "take_profit_price": take_profit_price,
                "brokerComish": brokerComish,   
                "perShareRisk": perShareRisk,
                "toleratedRisk": toleratedRisk,
                "order_details.entryPrice": order_details.entryPrice,
                "order_details.stopLoss": order_details.stopLoss,
                "uptrend": snapshot.uptrend,
                
                   
                "quantity": quantity,
                "rewardRiskRatio": order_details.rewardRiskRatio,
                "riskPercentage": order_details.riskPercentage,
                "accountBalance": order_details.accountBalance,
                "stopType": order_details.stopType,
                "atrFactor": order_details.atrFactor,
            }
            
            #logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss_price {stop_loss_price} and web_request_json: {web_request_json} with order_json: {order_json}")
        logger.info(format_order_details_table(order_json))
            
        return trade
    except Exception as e:
        logger.error(f"Error placing Stop-Loss order for {contract.symbol}: {e}")
        return None
   

async def market_stop_loss_order(contract, order_details: OrderRequest, action,quantity,stop_loss_price, snapshot: PriceSnapshot, ib: IB=None, set_market_order=None):
    try:
        ib_instance["ib"] = ib
        snapshot = await price_data_dict.get_snapshot(contract.symbol)
                                                                  
        symbol = contract.symbol
        perShareRisk = None
        brokerComish = None
        toleratedRisk = None
        take_profit_price=0.0
        qty =None
        trade=None
        barSizeSetting, bar_sec_int  = barSizeSetting_dict.get(symbol, "5 secs")  # default to 5 secs if not set
        stop_loss_price_adj, df, vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float = await last_bar_and_stop(snapshot, contract, stop_loss_price, barSizeSetting, ib)
        limit_price=snapshot.markPrice
        logger.info(f"[{symbol}] Placing Stop-Loss order with stop_loss_price_adj={stop_loss_price_adj}, , action={action}, quantity={quantity} and market price={limit_price}, barSizeSetting, bar_sec_int = {barSizeSetting}, {bar_sec_int}.")
        
        
        

        if quantity is None or quantity <= 0:

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
        

        # 3) For each symbol, create and place a parent LimitOrder (entry) + child StopOrder (stop‑loss).

        qty = quantity
        if qty is None or qty <= 0:
            logger.error(f"[{symbol}] Invalid quantity for Stop-Loss order.")
            raise ValueError(f"Invalid quantity for Stop-Loss order for {symbol}.")
        
        tif = "GTC"  # Good‑Til‑Canceled to keep the orders until filled or canceled :contentReference[oaicite:8]{index=8}
        reverse_action = "SELL" if action == "BUY" else "BUY"  # Reverse action for stop order
         # 3a) Parent LimitOrder: orderType = "LMT", transmit=False
        parent_order = None
        parent_trade = None
        parent_order_id = ib.client.getReqId()
        parent_ids[symbol] = parent_order_id
        
        
        
        parent_order = MarketOrder(
                action=action,
                totalQuantity=qty,
             
                orderId=parent_order_id,
                tif="GTC",
                transmit=False,
                
                outsideRth=False,
                orderRef=f"Parent MarketOrder - {symbol}"
                
            )

        
           
        
        
        parent_trade=ib.placeOrder(contract, parent_order)
        #parent_trade.fillEvent += stop_loss_trade_fill
        if not parent_trade or parent_trade is None:
            logger.error(f"[{symbol}] Failed to place parent LimitOrder.")
            return
        
        logger.info(f"[{symbol}] Parent LimitOrder placed with parent ID {parent_ids[symbol]}.")

        # 3b) Child StopOrder: orderType = "STP", auxPrice = stop_price, parentId=parent_order_id, transmit=True
        child_order_id = ib.client.getReqId()
        
        child_stop = StopOrder(
            action= reverse_action,
            totalQuantity=qty,
            stopPrice=round(stop_loss_price_adj,2),
            tif=tif,
            outsideRth=True,
            orderId=child_order_id,
            parentId= parent_ids[symbol],  # Attach to parent :contentReference[oaicite:10]{index=10}
            transmit=True  # This will send both parent + child in one atomic call :contentReference[oaicite:11]{index=11}
        )
        child_order=ib.placeOrder(contract, child_stop)
        if child_order:
            trade= parent_trade
        
            logger.info(
                f"[{symbol}] placed MarketOrder(@{limit_price}, LMT) and Stop‑Loss(@{stop_loss_price_adj}, STP) "
                f"— parentId={parent_order_id}, child_order={child_order}."
            )
            order_json = {
                "ticker": contract.symbol,
                "orderAction": action,
                "limit_price": round(limit_price, 2),
                "stop_loss_price": stop_loss_price_adj,
                "take_profit_price": take_profit_price,
                "brokerComish": brokerComish,   
                "perShareRisk": perShareRisk,
                "toleratedRisk": toleratedRisk,
                "order_details.entryPrice": order_details.entryPrice,
                "order_details.stopLoss": order_details.stopLoss,
                "uptrend": snapshot.uptrend,
                
                   
                "quantity": quantity,
                "rewardRiskRatio": order_details.rewardRiskRatio,
                "riskPercentage": order_details.riskPercentage,
                "accountBalance": order_details.accountBalance,
                "stopType": order_details.stopType,
                "atrFactor": order_details.atrFactor,
            }
            
            #logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss_price {stop_loss_price} and web_request_json: {web_request_json} with order_json: {order_json}")
        logger.info(f"ib_ticker for {contract.symbol} ")
        logger.info(format_order_details_table(order_json))
            
        return trade
    except Exception as e:
        logger.error(f"Error placing Stop-Loss order for {contract.symbol}: {e}")
        return None
async def market_bracket_order(contract, order_details: OrderRequest, action,quantity,stop_loss_price, snapshot: PriceSnapshot, ib: IB=None):
    try:
        ib_instance["ib"] = ib
        logger.info(ib_instance["ib"])
        snapshot = await price_data_dict.get_snapshot(contract.symbol)
                                                                  
        symbol = contract.symbol
        perShareRisk = None
        brokerComish = None
        toleratedRisk = None
        take_profit_price=0.0
        qty =None
        trade=None
        barSizeSetting, bar_sec_int  = barSizeSetting_dict.get(symbol, "5 secs")  # default to 5 secs if not set
        stop_loss_price_adj, df, vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float = await last_bar_and_stop(snapshot, contract, stop_loss_price, barSizeSetting, ib)
        limit_price=snapshot.markPrice
        logger.info(f"[{symbol}] Placing Stop-Loss order with stop_loss_price_adj={stop_loss_price_adj}, , action={action}, quantity={quantity} and market price={limit_price}, barSizeSetting, bar_sec_int = {barSizeSetting}, {bar_sec_int}.")
        
        
        


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

        

        # 3) For each symbol, create and place a parent LimitOrder (entry) + child StopOrder (stop‑loss).

        qty = quantity
        if qty is None or qty <= 0:
            logger.error(f"[{symbol}] Invalid quantity for Stop-Loss order.")
            raise ValueError(f"Invalid quantity for Stop-Loss order for {symbol}.")
        
        tif = "GTC"  # Good‑Til‑Canceled to keep the orders until filled or canceled :contentReference[oaicite:8]{index=8}
        parent_order = None
        parent_trade = None
        parent_order_id = ib.client.getReqId()
        parent_ids[symbol] = parent_order_id
        
        
        
        parent_order = MarketOrder(
                action=action,
                totalQuantity=qty,
             
                orderId=parent_order_id,
                tif="GTC",
                transmit=True,
                
                outsideRth=True,
                orderRef=f"Parent MarketOrder Bracket - {symbol}"
                
            )

        
           
        if parent_order:
        
            parent_trade=ib.placeOrder(contract, parent_order)
            parent_trade.fillEvent += stop_loss_trade_fill
            order_json = {
                "ticker": contract.symbol,
                "orderAction": action,
                "stop_loss_price": stop_loss_price_adj,
                "take_profit_price": take_profit_price,
                "brokerComish": brokerComish,   
                "perShareRisk": perShareRisk,
                "toleratedRisk": toleratedRisk,
                "order_details.entryPrice": order_details.entryPrice,
                "order_details.stopLoss": order_details.stopLoss,
                "uptrend": snapshot.uptrend,
                "parent_trade.orderStatus.orderId": parent_trade.orderStatus.orderId,
                "parent_trade.order.totalQuantity": parent_trade.order.totalQuantity,
                "quantity": quantity,
                "rewardRiskRatio": order_details.rewardRiskRatio,
                "riskPercentage": order_details.riskPercentage,
                "accountBalance": order_details.accountBalance,
                "stopType": order_details.stopType,
                "atrFactor": order_details.atrFactor,
            }
            if not parent_trade or parent_trade is None:
                logger.error(f"[{symbol}] Failed to place parent MarketOrder.")
                return
            get_parent_id = await fill_data.get_parent_id(symbol)
            logger.info(f"Bracket order fills_dict get_parent_id for {symbol} {get_parent_id} parent_ids[symbol] {parent_ids[symbol]}")
            return order_json
    except Exception as e:
        logger.error(f"Error placing Stop-Loss order for {contract.symbol}: {e}")
        return None

        

 
async def last_bar_and_stop(
    snapshot,
    contract: Contract,
    stop_loss_price: float,  # your vStop value
    barSizeSetting: str,
    ib=ib_instance["ib"]
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
        vol_stop_data = await volatility_stop_data.get(contract.symbol)
        vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float=vol_stop_data
        if stop_loss_price is None or not is_float(stop_loss_price):
            stop_loss_price=vStop_float
            
        stop_loss_price_raw = stop_loss_price
        stop_loss_price_adj= stop_loss_price
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",
            durationStr="3600 S",
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
        


        if uptrend_bool:
            gap_pct = (ohlc["low"] - stop_loss_price) / stop_loss_price * 100
            if gap_pct < 0.4:
                stop_loss_price = ohlc["close"] - (atr_float*(2*vstopAtrFactor_float))
                stop_loss_price_adj= stop_loss_price
                logger.info(f"stop_loss_price_raw {stop_loss_price_raw} ohlc stop_loss_price clean {stop_loss_price_adj} for {contract.symbol}, and atr_float*(2*vstopAtrFactor_float) = {atr_float} * {(2*vstopAtrFactor_float)} = {atr_float*(2*vstopAtrFactor_float)}")
        else:
            gap_pct = (stop_loss_price - ohlc["high"]) / ohlc["high"] * 100
            if gap_pct < 0.4:
                logger.info(f"stop_loss_price_raw {stop_loss_price_raw} ohlc stop_loss_price clean {stop_loss_price_adj} for {contract.symbol}, and atr_float*(2*vstopAtrFactor_float) = {atr_float} * {(2*vstopAtrFactor_float)} = {atr_float*(2*vstopAtrFactor_float)}")
                stop_loss_price = ohlc["close"] + (atr_float*(2*vstopAtrFactor_float))
                stop_loss_price_adj= stop_loss_price
        logger.info(f"stop_loss_price clean {stop_loss_price_adj}")
        return stop_loss_price_adj, df, vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float
 
    except Exception as e:
        logger.error(f"Error in last_bar_and_stop for {contract.symbol}: {e}")
        return stop_loss_price_adj
    

async def stop_loss_trade_fill(trade: Trade, fill: Fill=None):
    try:
        ib=ib_instance["ib"]
        get_trade = None
        sym = trade.contract.symbol
        get_trade= await fill_data.get_trade(sym)
        if get_trade is None:
            logger.info(f"stop_loss_trade_fill for {trade.contract.symbol} get_trade is None")
            return
        symbol=sym
        logger.info(f"stop_loss_trade_fill for {trade.contract.symbol}")
        stop_loss_price = None
        avgFillPrice= 0.0
        barSizeSetting, order_details, snapshot, get_parent_id, vol = await asyncio.gather(price_data_dict.get_snapshot(sym), price_data_dict.get_snapshot(sym), price_data_dict.get_snapshot(sym), fill_data.get_parent_id(sym), daily_volatility.get(sym))
        await fill_data.add_fill(sym, fill)
        logger.info(f"Parent order  fills_dict get_parent_id for {sym} {get_parent_id} parent_ids[symbol] {parent_ids[sym]} and trade.orderStatus.orderId {trade.orderStatus.orderId}")
        fills_dict = await fill_data.get_fills(sym)
        
        logger.info(f"Parent order  fills_dict get_trade for {sym} {get_trade}")
        logger.info(f"Parent order  fills_dict for {sym} {fills_dict}")
        if fills_dict:
            quantity = get_trade.order.totalQuantity
            avgFillPrice=get_trade.orderStatus.avgFillPrice
        logger.info(f"Parent order  fills_dict avgFillPrice for {sym} {avgFillPrice}")
        if get_parent_id == parent_ids[sym]:
            logger.info(f"Parent order  fills_dict get_parent_id for {sym} {get_parent_id} parent_ids[symbol] {parent_ids[sym]} and trade.orderStatus.orderId {trade.orderStatus.orderId}")
     
        if trade.order.orderId == get_parent_id and avgFillPrice > 0.0:
            logger.info(f"1 Parent order  matches {sym} {avgFillPrice}")
            await fill_data.delete(sym)

            
            
            logger.info(f"order_details Parent order  matches {sym} {order_details}")
            quantity = trade.order.totalQuantity
            p_action =trade.order.action
            limit_price= round(avgFillPrice,2)
            last_close = snapshot.markPrice
            logger.info(f"limit_price Parent order  matches {sym} {limit_price} p_action {p_action} and quantity {quantity} and snapshot.markPrice {last_close}")

            
            
            logger.info(f"limit_price Parent order  matches {sym} {limit_price} p_action {p_action} and quantity {quantity}")
            logger.info(f"limit_price Parent order  matches {sym} {limit_price} p_action {p_action} and quantity {quantity}")
            action = "SELL" if p_action == "BUY" else "BUY"  # Reverse action for stop order
            
            stop_loss_price_adj, df, vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float = await last_bar_and_stop(snapshot, trade.contract, snapshot.vStop, barSizeSetting, ib)
            logger.info(f"action Parent order  matches {sym} {action} and stop_loss_price {stop_loss_price_adj}")

            bracket=await add_bracket_order(trade, trade.contract, order_details, action,quantity, last_close,round(stop_loss_price_adj,2), snapshot, ib=ib_instance["ib"])
            #await ib_open_orders.delete(trade.contract.symbol, trade)
            if bracket:
                logger.info(
                f"Parent order filled: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity} avgPrice: {avgFillPrice} for {trade.contract.symbol} order Status filled: {trade.orderStatus.filled}"
            )
                return bracket
            
        logger.info(
            f"Child backet skipped: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity}  for {trade.contract.symbol}"
        )
        
   
    
        #await ib_open_orders.delete(trade.contract.symbol, trade)
        logger.info(
            f"Bracket order updated: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity} avgPrice: {avgFillPrice} for {trade.contract.symbol}"
        )

    except Exception as e:
        logger.error(f"Error in stop_loss_trade_fill: {e}")


async def limit_bracket_order(contract, order_details: OrderRequest, action,quantity,limit_price, stop_loss_price, snapshot: PriceSnapshot, ib: IB=None):
    try:
        order_json={}
        ib_instance["ib"] = ib
        logger.info(ib)
        logger.info(f"ib_ticker for {contract.symbol} ")
        snapshot = await price_data_dict.get_snapshot(contract.symbol)
        logger.info(f"snapshot for {contract.symbol} ")
        logger.info(snapshot)
                                                                  
        symbol = contract.symbol
        perShareRisk = None
        brokerComish = None
        toleratedRisk = None
        take_profit_price=0.0
        qty =None
        trade=None
        barSizeSetting, bar_sec_int  = barSizeSetting_dict.get(symbol, "5 secs")  # default to 5 secs if not set ib=ib_instance["ib"])
        stop_loss_price_adj, df, vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float = await last_bar_and_stop(snapshot, contract, stop_loss_price, barSizeSetting, ib=ib_instance["ib"])
        if limit_price is None or not is_float(limit_price):
            limit_price=round(snapshot.close,2)
            if limit_price is None or not is_float(limit_price):
                raise ValueError(f"Invalid limit price for {symbol}. Please provide a valid limit price or ensure the snapshot has a valid markPrice.")
            
        logger.info(f"[{symbol}] Placing Stop-Loss order with stop_loss_price_adj={stop_loss_price_adj}, , action={action}, quantity={quantity} and market price={limit_price}, barSizeSetting, bar_sec_int = {barSizeSetting}, {bar_sec_int}.")
        
        
        


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
        

        # 3) For each symbol, create and place a parent LimitOrder (entry) + child StopOrder (stop‑loss).

        qty = quantity
        if qty is None or qty <= 0:
            logger.error(f"[{symbol}] Invalid quantity for Stop-Loss order.")
            raise ValueError(f"Invalid quantity for Stop-Loss order for {symbol}.")
        
        tif = "GTC"  # Good‑Til‑Canceled to keep the orders until filled or canceled :contentReference[oaicite:8]{index=8}
        reverse_action = "SELL" if action == "BUY" else "BUY"  # Reverse action for stop order
         # 3a) Parent LimitOrder: orderType = "LMT", transmit=False
        parent_order = None
        parent_trade = None
        parent_order_id = ib.client.getReqId()
        parent_ids[symbol] = parent_order_id
        get_parent_id = await fill_data.get_parent_id(symbol)
        if get_parent_id == parent_ids[symbol]:
            logger.info(f"Bracket order fills_dict get_parent_id for {symbol} {get_parent_id} parent_ids[symbol] {parent_ids[symbol]}")
        
        
        
     
        if is_market_hours():
            parent_order = LimitOrder(
                action=action,
                totalQuantity=qty,
                lmtPrice=round(limit_price, 2),
                orderId=parent_order_id,
                tif="GTC",
                transmit=True,
                algoStrategy="Adaptive",
                outsideRth=False,
                orderRef=f"Parent Adaptive Bracket - {symbol}",
                algoParams=[TagValue("adaptivePriority", "Urgent")],
            )

        
           
        else:
            parent_order = LimitOrder(
                action=action,
                totalQuantity=qty,
                orderId=parent_order_id,
                lmtPrice=round(limit_price, 2),
                tif=tif,
                outsideRth=True,
                transmit=True,
                orderRef=f"Parent Limit Bracket - {symbol}"
            )

     
           
        if parent_order:
            logger.info(f"Placing parent LimitOrder for {symbol} with action {action}, quantity {qty}, limit_price {round(limit_price,2)} and stop_loss_price {stop_loss_price_adj}.")
            
        
            parent_trade=ib.placeOrder(contract, parent_order)
            parent_trade.fillEvent += stop_loss_trade_fill
            order_json = {
                "ticker": contract.symbol,
                "orderAction": action,
                "limit_price": round(limit_price, 2),
                "stop_loss_price": stop_loss_price_adj,
                "take_profit_price": take_profit_price,
                "brokerComish": brokerComish,   
                "perShareRisk": perShareRisk,
                "toleratedRisk": toleratedRisk,
                "order_details.entryPrice": order_details.entryPrice,
                "order_details.stopLoss": order_details.stopLoss,
                "uptrend": snapshot.uptrend,
                
                   
                "quantity": quantity,
                "rewardRiskRatio": order_details.rewardRiskRatio,
                "riskPercentage": order_details.riskPercentage,
                "accountBalance": order_details.accountBalance,
                "stopType": order_details.stopType,
                "atrFactor": order_details.atrFactor,
            }
        if not parent_trade or parent_trade is None:
            logger.error(f"[{symbol}] Failed to place parent LimitOrder.")
            return
        get_parent_id = await fill_data.get_parent_id(symbol)
        logger.info(f"Bracket order fills_dict get_parent_id for {symbol} {get_parent_id} parent_ids[symbol] {parent_ids[symbol]}")
        return order_json
    except Exception as e:
        logger.error(f"Error placing Stop-Loss order for {contract.symbol}: {e}")
        return None

    
async def add_bracket_order(parent_trade,contract, order_details: OrderRequest, action,quantity, limit_price,stop_loss_price, snapshot: PriceSnapshot=None, ib=ib_instance["ib"]):
    try:
        fills_dict = None
        order_json={}
        parent_trade_action =parent_trade.order.action
        
        
                                                                  
        symbol = contract.symbol
        perShareRisk = None
        brokerComish = None
        toleratedRisk = None
        take_profit_price=0.0
        qty =None
        bracket_o_list=None
        trade=None
        bracket_orders=[]
        child_order = None
        barSizeSetting, bar_sec_int = barSizeSetting_dict.get(symbol, "5 secs")  # default to 5 secs if not set
        stop_loss_price_adj, df, vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float = await last_bar_and_stop(snapshot, contract, stop_loss_price, barSizeSetting, ib=ib_instance["ib"])
        logger.info(f"[{symbol}] Placing Stop-Loss order with stop_loss_price_adj={stop_loss_price_adj}, limitPrice={limit_price}, action={action}, quantity={quantity} and barSizeSetting, bar_sec_int={barSizeSetting}, {bar_sec_int}.")
        
        


        quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = (
            await compute_position_size(
                limit_price,
                stop_loss_price_adj,
                order_details.accountBalance,
                order_details.riskPercentage,
                order_details.rewardRiskRatio,
                parent_trade_action,
                order_details
            )
        )
        

        # 3) For each symbol, create and place a parent LimitOrder (entry) + child StopOrder (stop‑loss).

        qty = quantity
        if qty is None or qty <= 0:
            logger.error(f"[{symbol}] Invalid quantity for Stop-Loss order.")
            raise ValueError(f"Invalid quantity for Stop-Loss order for {symbol}.")
        
        tif = "GTC"  # Good‑Til‑Canceled to keep the orders until filled or canceled :contentReference[oaicite:8]{index=8}
        reverse_action = "SELL" if parent_trade_action == "BUY" else "BUY"  # Reverse action for stop order
         # 3a) Parent LimitOrder: orderType = "LMT", transmit=False
        
        
        
        

        # 3b) Child StopOrder: orderType = "STP", auxPrice = stop_price, parentId=parent_order_id, transmit=True
        get_parent_id = await fill_data.get_parent_id(symbol)
        if get_parent_id == parent_ids[symbol]:
            logger.info(f"Bracket order fills_dict get_parent_id for {symbol} {get_parent_id} parent_ids[symbol] {parent_ids[symbol]}")
        child_order_id = ib.client.getReqId()
        ocaGroup = f"oca_{symbol}_{qty}_{parent_trade.orderStatus.orderId}"
        logger.info(
            f"bracket_oca_order: ocaGroup: {ocaGroup} {symbol} limit_price={limit_price} stop_loss={stop_loss_price} "
        )
        

        
        stop_loss_price = round(stop_loss_price_adj, 2)
        
        take_profit_price = round(take_profit_price, 2)
        outsideRth=parent_trade.order.outsideRth
        
       
        #quantity = 1
        logger.warning(f"Actual quantity for {symbol}: {quantity}")
     
        

        child_limit_order_id = ib.client.getReqId()
        child_limit = LimitOrder(
                action= reverse_action,
                totalQuantity=qty,
                lmtPrice=round(take_profit_price,2),
                tif=tif,
                outsideRth=outsideRth,
                #ocaGroup=ocaGroup,
                #orderId=child_limit_order_id,
                 orderRef=f"Bracket OCA TP - {symbol}",
                
                transmit=True  # This will send both parent + child in one atomic call :contentReference[oaicite:11]{index=11}
            )
        child_stop = StopOrder(
            action= reverse_action,
            totalQuantity=qty,
            stopPrice=round(stop_loss_price_adj,2),
            tif=tif,
            outsideRth=outsideRth,
            #orderId=child_order_id,
              # Attach to parent :contentReference[oaicite:10]{index=10}
            transmit=True,  # This will send both parent + child in one atomic call :contentReference[oaicite:11]{index=11}
            #ocaGroup=ocaGroup,
            #ocaType=1,  # OCA type 1 for bracket orders limit_price
            orderRef=f"Bracket OCA Stop - {symbol}",
          
        )
        bracket_orders = [child_limit, child_stop]
        fills_dict = await fill_data.get_trade(symbol)
        logger.info(f"fills_dict for {symbol} {fills_dict}")
        if fills_dict is None:
            logger.info(f"fills_dict for {symbol} is None or empty, creating new fills_dict.")
            #child_order=ib.placeOrder(contract, child_stop)
            #child_limit_order=ib.placeOrder(contract, child_limit)
            bracket_o_list=ib.oneCancelsAll(bracket_orders, ocaGroup, 1)
            if bracket_o_list is not None:
                for bracket_o in bracket_o_list:
                    logger.info(
                        f"Placing order: {contract.symbol} {bracket_o.action} {bracket_o.totalQuantity} @ {bracket_o.lmtPrice if hasattr(bracket_o, 'lmtPrice') else bracket_o.auxPrice}"
                    )
                    
                        
                    trade = ib.placeOrder(contract, bracket_o)
                    if trade:
                
            
                        logger.info(
                            f"[{symbol}] placed Limit(@{limit_price}, LMT) and Stop‑Loss(@{stop_loss_price_adj}, STP) "
                            f"— child_order_id={child_order_id}, child_order={child_order} bracket_o_list={bracket_o_list} with bracket_orders={bracket_orders}."
                        )
                        
                
                        
                    
                        logger.info(
                            f"[{symbol}] placed Limit(@{limit_price}, LMT) and take_profit_price: (@{round(take_profit_price,2)}, STP) "
                            f"— child_order_id={child_order_id}, child_order={child_order}."
                        )
                        order_json = {
                            "ticker": contract.symbol,
                            "orderAction": action,
                            "limit_price": round(limit_price, 2),
                            "stop_loss_price": round(stop_loss_price_adj,2),
                            "take_profit_price": take_profit_price,
                            "brokerComish": brokerComish,   
                            "perShareRisk": perShareRisk,
                            "toleratedRisk": toleratedRisk,
                            "order_details.entryPrice": order_details.entryPrice,
                            "order_details.stopLoss": order_details.stopLoss,
                            "uptrend": snapshot.uptrend,
                            
                            
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
        logger.error(f"Error placing Stop-Loss order for {contract.symbol}: {e}")
        return None




async def new_trade_update_asyncio(trade: Trade):
    try:
        logger.info(f"Boof New trade update for {trade.contract.symbol}: {trade.orderStatus.status}")
        await fill_data.set_trade(trade.contract.symbol, trade)
        
        if "Web close_positions" in trade.order.orderRef:
            await ib_open_orders.set(trade.contract.symbol, trade)
            logger.debug(f"New trade for {trade.contract.symbol} is a close position order, skipping update.")
            return
        logger.debug(f"New trade update for {trade.contract.symbol}: {trade.orderStatus.status}")
        
        
        logger.debug(
            f"Bracket order updated: status {trade.orderStatus.status} action: {trade.order.action} qty: {trade.order.totalQuantity}  for {trade.contract.symbol}"
        )
       
    except Exception as e:
        logger.error(f"Error in bracket_trade_update: {e}")