#pnl_close.py
from collections import defaultdict, deque
import stat
from typing import *
import os, asyncio

from math import *

import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Query, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse, Response
from ib_async.util import isNan
from ib_async import *
from account_values import account_metrics_store

from dotenv import load_dotenv
    
    
from add_contract import add_new_contract


from models import (
   
    AccountPnL
   
)
from log_config import log_config, logger
from tv_ticker_price_data import  tv_store_data, price_data_dict, daily_volatility, yesterday_close, timeframe_dict, order_placed, subscribed_contracts, ib_open_orders, barSizeSetting_dict, parent_ids,reconcile_open_orders, pnl_started, orders_in_flight

from my_util import is_market_hours, format_order_details_table

log_config.setup()

async def update_pnl(pnl: AccountPnL, ib: IB=None):
        try:
            account_pnl= None

            if pnl:
                logger.debug(f"2 PnL updated: {pnl}")
                account_pnl = AccountPnL(
                    unrealizedPnL=float(pnl.unrealizedPnL or 0.0),
                    realizedPnL=float(pnl.realizedPnL or 0.0),
                    dailyPnL=float(pnl.dailyPnL or 0.0),
                    
                )
                pnl_calc = account_pnl.dailyPnL - account_pnl.unrealizedPnL
            logger.debug(
                f"3 PnL updated: account_pnl.dailyPnL {account_pnl.dailyPnL} + unrealizedPnL {account_pnl.unrealizedPnL} = {pnl_calc}"
            )
            if account_pnl is None:
                account_pnl = account_metrics_store.pnl_data
            # Check risk threshold and trigger closing positions if needed.
            
            await check_pnl_threshold(account_pnl, ib)
            return account_pnl
        except Exception as e:
            logger.error(f"Error updating PnL: {e}")

async def check_pnl_threshold(account_pnl: AccountPnL, ib: IB):
    try:
        pnlInit = None
        markPrice=0.0
        limit_price=0.0
        order=None
        

        from dotenv import load_dotenv
        load_dotenv()
        # 1) Threshold from env (e.g. "-300" means close if <= -300)
        RISK_THRESHOLD = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))

        # 2) Only act when at-or-below threshold
        if account_pnl is None or account_pnl.dailyPnL > RISK_THRESHOLD:
            logger.debug(f"PnL {account_pnl and account_pnl.dailyPnL} above threshold {RISK_THRESHOLD}. No action.")
            return

        logger.warning(f"PnL {account_pnl.dailyPnL} ≤ {RISK_THRESHOLD}! Starting position‐closure pass…")
        if not await pnl_started.start():
            logger.info("pnl reqGlobalCancel Canceling all orders...")
            ib.reqGlobalCancel()

        # 3) Purge any orders_in_flight that IB no longer reports
        active_ids = await ib_open_orders.all_flat()
        logger.info(f"Active order IDs: {active_ids}")
        for sym, oid in list(orders_in_flight.items()):
            if oid not in active_ids:
                logger.info(f"Order {oid} for {sym} no longer open → removing from in‐flight.")
                orders_in_flight.pop(sym, None)
                await ib_open_orders.delete(sym)
            else:
                logger.info(f"Order {oid} for {sym} still open → keeping in‐flight.")
                pnlInit=not await pnl_started.end()
                logger.info(f"PnL threshold check is {pnlInit}")

        # 4) Fetch live positions & try to close one
        for pos in ib.portfolio() or []:
            symbol = pos.contract.symbol
            size   = pos.position
            qty    = abs(size)
            logger.info(f"Checking position for {symbol}: qty={qty}")

            # skip flat
            if qty == 0:
                continue

            # skip if already waiting on a close
            if symbol in orders_in_flight:
                logger.info(f"Skipping {symbol}: order {orders_in_flight[symbol]} still pending.")
                continue

            # ensure we have a Contract object & up‐to‐date tick
            barSize = barSizeSetting_dict.get(symbol)
            contract = await add_new_contract(symbol, barSize, ib)
            ticker   = await price_data_dict.get_snapshot(symbol)
            if ticker:
                markPrice = ticker.markPrice if ticker else 0.0

            # determine action
            action = "SELL" if size > 0 else "BUY"
            logger.info(f"Checking position for {symbol}: action={action}, qty={qty}, markPrice={ticker.markPrice}")

            # build the order
            if is_market_hours():
                logger.info(f"Market is open, closing position for {symbol} with action {action} and quantity {qty}")
                order = MarketOrder(action, qty)
            else:
                if markPrice > 0.0:
                    limit_price = markPrice

                elif limit_price == 0.0 or limit_price is None:
                    limit_price = ticker.ask if action == "BUY" else ticker.bid
                if limit_price <= 2.0 or limit_price is None:
                    logger.warning(f"Limit price for {contract.symbol} is None or 0, using default value of 1.0")
                    limit_price = ticker.markPrice

                logger.info(f"Market is closed, using limit order for {symbol} with action {action} and quantity {qty} with order {orders_in_flight[symbol]} still pending")
                ReqId=ib.client.getReqId()
                logger.info(f"Generated order ID: {ReqId} for {symbol}")
               
                order = LimitOrder(
                    action,
                    totalQuantity=qty,
                    lmtPrice=round(limit_price, 2),
                    tif="GTC",
                    orderId=ReqId,
                    outsideRth=True,
                    transmit=True,
                    orderRef=f"Web close_positions – {symbol}",
                )

            logger.info(f"Placing close order for {contract.symbol} with action {action}, qty={qty}, limit price={limit_price}")
            trade: Trade = ib.placeOrder(contract, order)
            if trade:
                await ib_open_orders.sync_symbol(contract.symbol,ib)
                logger.info(f"Placed close‐order for {symbol}")
            else:
                logger.error(f"Failed to place close order for {symbol}")

            # 5) only one symbol per pass
            return

    except Exception as e:
        logger.error(f"Error in check_pnl_threshold: {e}")
        raise HTTPException(status_code=500, detail="Error checking PnL threshold")





