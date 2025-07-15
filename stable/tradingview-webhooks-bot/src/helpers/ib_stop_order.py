import asyncio
from math import log
from ib_async import IB, Contract, LimitOrder, StopOrder, MarketOrder, util, TagValue, Order, Trade, Fill
from ib_async.util import isNan

from typing import Dict, Tuple, Optional
from tv_ticker_price_data import  volatility_stop_data, tv_store_data, price_data_dict, daily_volatility, yesterday_close_bar, yesterday_close, daily_true_range, timeframe_dict, order_placed, subscribed_contracts, ib_open_orders, barSizeSetting_dict, parent_ids, logger
from my_util import is_float, is_market_hours, vol_stop, get_dynamic_atr, compute_position_size, format_order_details_table, get_tv_ticker_dict,get_timestamp
from models import PriceSnapshot, OrderRequest
from fill_data import fill_data
from collections import defaultdict
from tech_a import ib_data
import numpy as np
ib_instance: Dict[str, IB] = defaultdict(IB)
from ib_db import order_db

#contract,  req, action, quantity, round(limit_price, 2),round(stop_loss, 2), snapshot, ib, set_market_order, barSizeSetting
async def the_order(contract:Contract, req: OrderRequest, action:str,quantity:float, limit_price:float,stop_loss: float, snapshot: PriceSnapshot, ib:IB=None, set_market_order:bool=False, barSizeSetting: str ="1 min", takeProfitBool:bool=False):
    try:
        logger.info(f"stop_loss_order for {contract.symbol} with action: {action}, quantity: {quantity}, limit_price: {limit_price}, stop_loss: {stop_loss}")
        barSizeSetting = barSizeSetting_dict.get(contract.symbol)
        
        hasBidAsk = snapshot.hasBidAsk if snapshot else None
        if hasBidAsk:
            logger.debug(f"stop_loss_order for {contract.symbol} with action: {action}, quantity: {quantity}, limit_price: {limit_price}, stop_loss: {stop_loss}")
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
        logger.debug(f"stop_loss_order for {contract.symbol} with action: {action}, quantity: {quantity}, limit_price: {limit_price}, stop_loss: {stop_loss}")
        

        
                                                                  
        symbol = contract.symbol
        order_json={}
        child_stop = None
        perShareRisk = None
        brokerComish = None
        toleratedRisk = None
        take_profit_price=0.0
        quantity =None
        trade=None
        logger.debug(f"stop_loss_order for {contract.symbol} with action: {action}, quantity: {quantity}, limit_price: {limit_price}, stop_loss: {stop_loss}")
 
        logger.debug(f"[{symbol}] Placing Stop-Loss order with stop_loss={stop_loss}, limitPrice={limit_price}, action={action}, quantity={quantity} and barSizeSetting, bar_sec_int={barSizeSetting}.")
        
        

        if quantity is None or quantity <= 0:

            quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = (
                await compute_position_size(
                    limit_price,
                    stop_loss,
                    req.accountBalance,
                    req.riskPercentage,
                    req.rewardRiskRatio,
                    action,
                    req
                )
            )
        

        # 3) For each symbol, create and place a parent LimitOrder (entry) + child StopOrder (stop‑loss).

        
        if quantity is None or quantity <= 0:
            logger.error(f"[{symbol}] Invalid quantity for Stop-Loss order.")
            raise ValueError(f"Invalid quantity for Stop-Loss order for {symbol}.")
        
        tif = "GTC"  # Good‑Til‑Canceled to keep the orders until filled or canceled :contentReference[oaicite:8]{index=8}
        reverse_action = "SELL" if action == "BUY" else "BUY"  # Reverse action for stop order
         # 3a) Parent LimitOrder: orderType = "LMT", transmit=False
        parent_order = None
        parent_trade = None
        parent_order_id = ib.client.getReqId()
        parent_ids[symbol] = parent_order_id
        
        

        if is_market_hours() and not set_market_order and not takeProfitBool:
            parent_order = LimitOrder(
                action=action,
                totalQuantity=quantity,
                lmtPrice=round(limit_price, 2),
                orderId=parent_order_id,
                tif="GTC",
                transmit=False,
                algoStrategy="Adaptive",
                outsideRth=False,
                orderRef=f"Parent Adaptive - {symbol}",
                algoParams=[TagValue("adaptivePriority", "Urgent")],
            )
        elif is_market_hours() and not set_market_order and  takeProfitBool:
            parent_order = LimitOrder(
                action=action,
                totalQuantity=quantity,
                lmtPrice=round(limit_price, 2),
                orderId=parent_order_id,
                tif="GTC",
                transmit=False,
                algoStrategy="Adaptive",
                outsideRth=False,
                orderRef=f"Parent Adaptive Bracket_{symbol}",
                algoParams=[TagValue("adaptivePriority", "Urgent")],
            )

        
           
        elif not is_market_hours() and not set_market_order and not takeProfitBool:
            parent_order = LimitOrder(
                action=action,
                totalQuantity=quantity,
                orderId=parent_order_id,
                lmtPrice=round(limit_price, 2),
                tif=tif,
                outsideRth=True,
                transmit=False,
                orderRef=f"Parent Limit - {symbol}"
            )
        elif not is_market_hours() and not set_market_order and  takeProfitBool:
            parent_order = LimitOrder(
                action=action,
                totalQuantity=quantity,
                orderId=parent_order_id,
                lmtPrice=round(limit_price, 2),
                tif=tif,
                outsideRth=True,
                transmit=False,
                orderRef=f"Parent Limit Bracket_{symbol}"
            )
        elif  is_market_hours() and  set_market_order and not takeProfitBool:
            parent_order = MarketOrder(
                action=action,
                totalQuantity=quantity,
             
                orderId=parent_order_id,
                tif="GTC",
                transmit=True,
                
                outsideRth=False,
                orderRef=f"Parent MarketOrder  - {symbol}"
                
            )
        elif  is_market_hours() and  set_market_order and  takeProfitBool:
            parent_order = MarketOrder(
                action=action,
                totalQuantity=quantity,
             
                orderId=parent_order_id,
                tif="GTC",
                transmit=True,
                
                outsideRth=False,
                orderRef=f"Parent MarketOrder Bracket_{symbol}"
                
            )
        parent_trade=ib.placeOrder(contract, parent_order)
        parent_trade.fillEvent += stop_loss_trade_fill
        if not parent_trade or parent_trade is None:
            logger.error(f"[{symbol}] Failed to place parent LimitOrder.")
            return
        
        logger.debug(f"[{symbol}] Parent LimitOrder placed with parent ID {parent_ids[symbol]}.")
        child_order = None

        # 3b) Child StopOrder: orderType = "STP", auxPrice = stop_price, parentId=parent_order_id, transmit=True
        if parent_trade and not takeProfitBool:
            child_order_id = ib.client.getReqId()
            
            child_stop = StopOrder(
                action= reverse_action,
                totalQuantity=quantity,
                stopPrice=round(stop_loss,2),
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
                    f"[{symbol}] placed Limit(@{limit_price}, LMT) and Stop‑Loss(@{stop_loss}, STP) "
                    f"— parentId={parent_order_id}, child_order={child_order}."
                )
        order_json = {
            "ticker": contract.symbol,
            "parent_trade":parent_trade.order,
            "orderAction": action,
            "limit_price": round(limit_price, 2),
            "stop_loss": round(stop_loss,2),
            "take_profit_price": take_profit_price,
            "brokerComish": brokerComish,   
            "perShareRisk": perShareRisk,
            "toleratedRisk": toleratedRisk,
            "req.limit_price": req.limit_price,
            "req.stop_loss": req.stop_loss,
            "uptrend": snapshot.uptrend,
            
                
            "quantity": quantity,
            "rewardRiskRatio": req.rewardRiskRatio,
            "riskPercentage": req.riskPercentage,
            "accountBalance": req.accountBalance,
            "stopType": req.stopType,
            "atrFactor": req.atrFactor,
        }
            
            #logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss {stop_loss} and web_request_json: {web_request_json} with order_json: {order_json}")
            
        if child_order is None:
            if not parent_trade or parent_trade is None:
                logger.error(f"[{symbol}] Failed to place parent LimitOrder.")
                return
        get_parent_id = await fill_data.get_parent_id(symbol)
        logger.info(f"Bracket order fills_dict get_parent_id for {symbol} {get_parent_id} parent_ids[symbol] {parent_ids[symbol]}")
        logger.info(format_order_details_table(order_json))  
        return parent_trade
    except Exception as e:
        logger.error(f"Error placing Stop-Loss order for {contract.symbol}: {e}")
        return None
            

        


    

async def stop_loss_trade_fill(trade: Trade, fill: Fill=None):
    try:
        if "Bracket_" not in trade.order.orderRef:
            
            
            logger.info(f"Not Bracket trade for {trade.contract.symbol} with orderRef: {trade.order.orderRef}")
            return
        ib=ib_instance["ib"]
        get_trade = None
        sym = trade.contract.symbol
        barSizeSetting = barSizeSetting_dict.get(trade.contract.symbol)
        get_trade= await fill_data.get_trade(sym)
        if get_trade is None:
            logger.info(f"stop_loss_trade_fill for {trade.contract.symbol} get_trade is None")
            return
        symbol=sym
        logger.info(f"stop_loss_trade_fill for {trade.contract.symbol}")
        stop_loss = None
        avgFillPrice= 0.0
        tvStoreData, req, snapshot, get_parent_id, vol = await asyncio.gather(tv_store_data.get(sym), tv_store_data.get(sym), price_data_dict.get_snapshot(sym), fill_data.get_parent_id(sym), daily_volatility.get(sym))

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

            
            
            logger.info(f"req Parent order  matches {sym} {req}")
            quantity = trade.order.totalQuantity
            p_action =trade.order.action
            limit_price= round(avgFillPrice,2)
            last_close = snapshot.markPrice
            logger.info(f"limit_price Parent order  matches {sym} {limit_price} p_action {p_action} and quantity {quantity} and snapshot.markPrice {last_close}")

            
            
            logger.info(f"limit_price Parent order  matches {sym} {limit_price} p_action {p_action} and quantity {quantity}")
            logger.info(f"limit_price Parent order  matches {sym} {limit_price} p_action {p_action} and quantity {quantity}")
            action = "SELL" if p_action == "BUY" else "BUY"  # Reverse action for stop order
            
            logger.info(f"action Parent order  matches {sym} {action} and stop_loss {stop_loss}")

            bracket=await add_bracket_order(trade, trade.contract, req, action,quantity, last_close,round(stop_loss,2), snapshot, ib=ib_instance["ib"])
            #await ib_open_orders.delete(trade.contract.symbol, trade)
            if bracket:
                logger.info(
                f"Parent order filled: status {trade.orderStatus.status} action: {trade.order.action} quantity: {trade.order.totalQuantity} avgPrice: {avgFillPrice} for {trade.contract.symbol} order Status filled: {trade.orderStatus.filled}"
            )
                return bracket
            
        logger.info(
            f"Child backet skipped: status {trade.orderStatus.status} action: {trade.order.action} quantity: {trade.order.totalQuantity}  for {trade.contract.symbol}"
        )
        
   
    
        #await ib_open_orders.delete(trade.contract.symbol, trade)
        logger.info(
            f"Bracket order updated: status {trade.orderStatus.status} action: {trade.order.action} quantity: {trade.order.totalQuantity} avgPrice: {avgFillPrice} for {trade.contract.symbol}"
        )

    except Exception as e:
        logger.error(f"Error in stop_loss_trade_fill: {e}")

async def ib_stop_order(contract, req: OrderRequest, action,quantity,limit_price, stop_loss, snapshot: PriceSnapshot, barSizeSetting: str, takeProfitBool:bool, ib: IB=None):
    try:
        snapshot=await price_data_dict.get_snapshot(contract.symbol)
        ib_instance["ib"] = ib
        symbol= contract.symbol
        barSizeSetting=barSizeSetting_dict[symbol]
        if stop_loss < 1.0 or snapshot is None:
            raise ValueError(f"Invalid stop_loss value {stop_loss} for {contract.symbol}. Must be greater than 1.0")
        
        set_market_order = req.set_market_order
        
        logger.debug(f"snapshot for {contract.symbol} ")
        logger.debug(snapshot)
        logger.info(f"takeProfitBool {takeProfitBool} Placing stop loss order for {contract.symbol} with stop_loss: {stop_loss} and limit_price: {limit_price}")
        orderPlace=await order_placed.set(symbol)
        trade= await the_order(contract,  req, action, quantity, round(limit_price, 2),round(stop_loss, 2), snapshot, ib, set_market_order, barSizeSetting, takeProfitBool)

            
          
        
        
        if trade:
            logger.info(f"Order placed for {contract.symbol} ")
            return trade
            
    except Exception as e:
        logger.error(f"Error placing Stop-Loss order for {contract.symbol}: {e}")
        return None


    
async def add_bracket_order(parent_trade,contract, req: OrderRequest, action,quantity, limit_price,stop_loss, snapshot: PriceSnapshot=None, ib=ib_instance["ib"]):
    try:
        
        fills_dict = None
        order_json={}
        parent_trade_action =parent_trade.order.action
        
        
                                                                  
        symbol = contract.symbol
        barSizeSetting=barSizeSetting_dict[symbol]
        perShareRisk = None
        brokerComish = None
        toleratedRisk = None
        take_profit_price=0.0
        quantity =None
        bracket_o_list=None
        trade=None
        bracket_orders=[]
        child_order = None
        logger.info(f"[{symbol}] Placing Stop-Loss order with stop_loss={stop_loss}, limitPrice={limit_price}, action={action}, quantity={quantity} and barSizeSetting, bar_sec_int={barSizeSetting}, .")
        
        


        quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = (
            await compute_position_size(
                limit_price,
                stop_loss,
                req.accountBalance,
                req.riskPercentage,
                req.rewardRiskRatio,
                parent_trade_action,
                req
            )
        )
        

        # 3) For each symbol, create and place a parent LimitOrder (entry) + child StopOrder (stop‑loss).

        
        if quantity is None or quantity <= 0:
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
        ocaGroup = f"oca_{symbol}_{quantity}_{parent_trade.orderStatus.orderId}"
        logger.info(
            f"bracket_oca_order: ocaGroup: {ocaGroup} {symbol} limit_price={limit_price} stop_loss={stop_loss} "
        )
        

        
        stop_loss = round(stop_loss, 2)
        
        take_profit_price = round(take_profit_price, 2)
        outsideRth=parent_trade.order.outsideRth
        
       
        #quantity = 1
        logger.warning(f"Actual quantity for {symbol}: {quantity}")
     
        

        child_limit_order_id = ib.client.getReqId()
        child_limit = LimitOrder(
                action= reverse_action,
                totalQuantity=quantity,
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
            totalQuantity=quantity,
            stopPrice=round(stop_loss,2),
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
                            f"[{symbol}] placed Limit(@{limit_price}, LMT) and Stop‑Loss(@{stop_loss}, STP) "
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
                            "stop_loss": round(stop_loss,2),
                            "take_profit_price": take_profit_price,
                            "brokerComish": brokerComish,   
                            "perShareRisk": perShareRisk,
                            "toleratedRisk": toleratedRisk,
                            "req.limit_price": req.limit_price,
                            "req.stop_loss": req.stop_loss,
                            "uptrend": snapshot.uptrend,
                            
                            
                            "quantity": quantity,
                            "rewardRiskRatio": req.rewardRiskRatio,
                            "riskPercentage": req.riskPercentage,
                            "accountBalance": req.accountBalance,
                            "stopType": req.stopType,
                            "atrFactor": req.atrFactor,
                        }
                            
                            #logger.info(f"json for: Placing LIMIT order for {symbol} with action {action} and quantity {quantity} and limit_price {limit_price} and stop_loss {stop_loss} and web_request_json: {web_request_json} with order_json: {order_json}")
                        logger.info(format_order_details_table(order_json))
            
        return order_json
    except Exception as e:
        logger.error(f"Error placing Stop-Loss order for {contract.symbol}: {e}")
        return None




async def new_trade_update_asyncio(trade: Trade):
    try:
        logger.info(f"Boof New trade update for {trade.contract.symbol}: {trade.orderStatus.status}") 
        await fill_data.set_trade(trade.contract.symbol, trade)
        await order_db.insert_trade(trade)
        
        
        if "None" in trade.order.orderRef:
            
            
            logger.debug(f"New Bracket trade for {trade.contract.symbol}")
            return
        logger.debug(f"New trade update for {trade.contract.symbol}: {trade.orderStatus.status}")
        
        
        logger.debug(
            f"Bracket order updated: status {trade.orderStatus.status} action: {trade.order.action} quantity: {trade.order.totalQuantity}  for {trade.contract.symbol}"
        )
       
    except Exception as e:
        logger.error(f"Error in bracket_trade_update: {e}")