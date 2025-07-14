from dotenv import load_dotenv
import os
import datetime
import asyncio
from collections import defaultdict, deque
import time
import jsonpickle
from dataclasses import  field
from datetime import timezone
from models import OrderRequest, PriceSnapshot, VolatilityStopData
from typing import Dict, Optional, Any
from ib_async import Contract, Ticker, IB, RealTimeBarList, BarData, RealTimeBar, util
load_dotenv()
boof = os.getenv("HELLO_MSG")
from app_ib_conn import ib_connection
ib=ib_connection.ib
from bar_size import convert_pine_timeframe_to_barsize
from helpers.price_data_dict import barSizeSetting_dict, price_data_dict 

from ib_async.util import isNan
from ib_async import Contract, Ticker, IB, RealTimeBarList

from models import PriceSnapshot, OrderRequest
    
from tv_ticker_price_data import  tv_store_data, daily_volatility, yesterday_close_bar, yesterday_close, subscribed_contracts, logger, agg_bars


async def resolve_contract(req: OrderRequest, barSizeSetting, ib: IB, tv_hook: bool=False) -> tuple[Contract, PriceSnapshot, str]:    
    """
    Resolve the contract for the given symbol and bar size setting.
    
    :param symbol: The ticker symbol to resolve.
    :param barSizeSetting: The bar size setting (e.g., "5 secs", "15 secs").
    barSizeSetting_tv: The bar size setting for TradingView.
    :param tv_hook: Whether this is for a TradingView hook (default False).
    :param ib: The IB instance (optional).
    :return: A tuple containing the Contract, PriceSnapshot, and bar size setting.
    """
    
    
    logger.debug(f"Resolved barSizeSetting: {barSizeSetting} for symbol: {req.symbol}")
    contract,snapshot = await add_new_contract(req.symbol, barSizeSetting, ib)
    logger.debug(f"Resolved contract for {req.symbol}: {contract}")
    logger.debug(f"Resolved PriceSnapshot for {req.symbol}: {snapshot}")
    return contract, snapshot


async def add_new_contract(symbol, barSizeSetting: str, ib: IB) -> PriceSnapshot:
    try:
        
        vol = await daily_volatility.get(symbol)
        logger.info(f"add_new_contract: {symbol} barSizeSetting: {barSizeSetting} vol: {vol} : ")

        contract: Contract | None = None
       

        already_subscribed = await subscribed_contracts.has(symbol)
        subscribe_needed = not already_subscribed
        logger.debug(
            f"add_new_contract: {symbol} barSizeSetting: {barSizeSetting} already_subscribed: {already_subscribed} : "
        )

        if subscribe_needed:
            logger.debug(f"add_new_contract: {symbol} barSizeSetting: {barSizeSetting} subscribe_needed: {subscribe_needed} : ")
            # --- qualify + subscribe live market data -------------------------
            contract = await subscribed_contracts.get(symbol, barSizeSetting)
            ib.reqMktData(contract, genericTickList="221,236", snapshot=False)
            logger.debug(f"Subscribed {symbol} for live market data")
            first_ticker=await price_data_dict.add_ticker(ib.ticker(contract))
            logger.info(f"Adding bar for {contract.symbol} to add_first_rt_bar")
            bars: RealTimeBarList = ib.reqRealTimeBars(contract, 5, "TRADES", False)
            logger.debug(f"Subscribed {symbol} for real-time bars with bar size {bars.barSize}")
            bars.updateEvent += on_bar_update
            first_rt_bar=await price_data_dict.add_first_rt_bar(symbol)
            

         
            logger.debug(
                f"Subscribed {symbol}; first markPrice {first_ticker.markPrice:.4f} – front loading RT bars…"
            )

            # ---------- NEW --------------
            # pull ~8 minutes of 5‑sec history so the BarAggregator & indicators
            # have something to chew on before the first live bar arrives.
            logger.info(f"Front loading {symbol} with  5‑sec bars…")
            
            # ---------- /NEW -------------

        else:
            logger.debug(f"add_new_contract: {symbol} barSizeSetting: {barSizeSetting} subscribe_needed: {subscribe_needed} : ")
            contract = await subscribed_contracts.get(symbol, barSizeSetting)
        snapshot = await price_data_dict.get_snapshot(symbol)
        # ensure daily volatility is in cache
        if vol is None or isNan(vol):
            logger.debug(f"Computing daily volatility for {symbol}…")
            df = await yesterday_close_bar(ib, contract)
            if df.empty:
                raise RuntimeError("No historical data for " + contract.symbol)
            logger.debug(f"Daily volatility for {symbol} is {df['close'].iloc[-1]}")

        if contract is None:
            raise RuntimeError(f"Contract for {symbol} not found or could not be created.")
        snapshot = await price_data_dict.get_snapshot(symbol)
        return contract, snapshot

    except Exception as e:
        logger.error(f"Error in add_new_contract for {symbol}: {e}")
        raise


def floor_time_to_5s(dt: datetime):
    return dt.replace(second=(dt.second // 5) * 5, microsecond=0) 

async def on_bar_update(bars, has_new_bar):
    try:
        if not bars or len(bars) == 0:
            return
        if not has_new_bar:
            return

        symbol = bars.contract.symbol
        contract= bars.contract
        
        # Floor the bar time to a 5-second interval.
        last = bars[-1]
        time_ = floor_time_to_5s(last.time)
        bar_size = getattr(bars, "_bar_size", 5)
        # Build the bar_dict dictionary.
        # Only update if the incoming value is not NaN; otherwise preserve (or leave as None).
        rt_bar_dict = {
            "time": getattr(last, "date", getattr(last, "time", None)),
            "open_": last.open_,
            "high": last.high,
            "low": last.low,
            "close": last.close,
            "volume": getattr(last, "volume", None),
        }
        
        logger.debug(rt_bar_dict)
        price_data=await price_data_dict.add_rt_bar(symbol, rt_bar_dict)
        barSnap = await price_data_dict.get_snapshot(symbol)
        logger.debug(f"Realtime bar for {symbol} added")
        logger.debug(barSnap.rt_bar)
        await agg_bars.update(symbol)
        
     

        
    except Exception as e:
        logger.error(f"Failed to write realtime bar: {e}")

  