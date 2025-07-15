import asyncio
from collections import defaultdict, deque
from xml.etree.ElementTree import tostring
import jsonpickle
from dataclasses import  field
from datetime import timezone
from bar_size import convert_pine_timeframe_to_barsize
from models import OrderRequest, PriceSnapshot, VolatilityStopData
from typing import Any, Dict, Optional, ClassVar, FrozenSet, List, Tuple
from log_config import log_config, logger
from ib_async import *
from ib_async.objects import  BarDataList, RealTimeBarList, RealTimeBar
from ib_async.order import Trade, OrderStatus
from ib_async.util import isNan
import pandas as pd
import talib as ta  
import numpy as np
from tv_ticker_price_data import volatility_stop_data, price_data_dict, daily_volatility,  subscribed_contracts, yesterday_close_bar, agg_bars
from tv_ticker_price_data import price_data_dict, bar_sec_int_dict
from models import PriceSnapshot
import math

class VolatilityCalc:
    """An async-safe in-memory store for volatility stop data."""
    def __init__(self):
        self._data: Dict[str, VolatilityStopData] = defaultdict(VolatilityStopData)
        # full-series storage still kept for internal use, but not exposed
        self._series_vstop: Dict[str, pd.Series] = {}
        self._series_atr: Dict[str, pd.Series] = {}
        self._series_uptrend: Dict[str, pd.Series] = {}
        self._vstopAtrFactor: Dict[str, float] = {}
        self._cache: Dict[str, VolatilityStopData] = {}
        self._lock = asyncio.Lock()

    async def set(self, symbol: str, vStop: pd.Series, uptrend: pd.Series, atr: pd.Series, vstopAtrFactor: float) -> None:
        try:
            async with self._lock:
                if symbol not in self._data:
                    self._data[symbol] = VolatilityStopData()

                # keep the full Series around if you need it internally
                self._series_vstop[symbol] = vStop
                self._series_atr[symbol] = atr
                self._series_uptrend[symbol] = uptrend
                self._vstopAtrFactor[symbol] = vstopAtrFactor

                # pull off the last value and cast to native types
                last_vstop_np   = vStop.iloc[-1]
                last_atr_np   = atr.iloc[-1]
                last_uptrend_np = uptrend.iloc[-1]

                volstop_data = self._data[symbol]
                volstop_data.vStop   = float(last_vstop_np)
                volstop_data.atr   = float(last_atr_np)
                volstop_data.vstopAtrFactor   = float(vstopAtrFactor)
                volstop_data.uptrend = bool(last_uptrend_np)

                # cache it so get() will see it
                self._cache[symbol] = volstop_data

                logger.debug(
                    f"Volatility stop set for {symbol}: "
                    f"vStop={volstop_data.vStop}, uptrend={volstop_data.uptrend}"
                )
        except Exception as e:
            logger.error(f"Error setting volatility stop for {symbol}: {e}")
            raise

    async def get(self, symbol: str) -> Optional[Tuple[float, bool]]:
        async with self._lock:
            if symbol not in self._cache:
                logger.debug(f"No volatility stop data for {symbol}")
                return None
            data = self._cache[symbol]
            # these are already native float/bool
            return data.vStop, data.uptrend, data.atr, data.vstopAtrFactor

    async def all(self) -> Dict[str, Tuple[float, bool]]:
        async with self._lock:
            # return only native values
            return {
                symbol: (d.vStop, d.uptrend)
                for symbol, d in self._data.items()
            }

# instantiate
volatility_stop_calc = VolatilityCalc()


async def get_dynamic_atr(symbol: str, BASE_ATR=None, vol=None) -> float:
    atrFactor = None
    if BASE_ATR is None:
        atrFactor = 1.5
    else:
        atrFactor = BASE_ATR
    vol = await daily_volatility.get(symbol)
    if vol is None or math.isnan(vol):
        logger.warning(f"No volatility data found for {symbol}, using default atrFactor: {atrFactor}")
        return atrFactor
    if vol < 10.0:
        return atrFactor
    elif vol < 20.0:
        return 2.0
    elif vol < 50.0:
        return 2.5
    else:
        return 3.0
async def derive_stop_loss(req: OrderRequest, contract: Contract, barSizeSetting: str, snapshot: PriceSnapshot, ib: IB = None) -> Tuple[Optional[float], Optional[bool]]:
 
    try:
        symbol = contract.symbol
        already_subscribed = await subscribed_contracts.has(symbol)
        logger.debug(f"Already subscribed to {contract.symbol}: {already_subscribed} with barSizeSetting: {barSizeSetting}")
        
        

        # subscribe to live market data only once
        
        logger.debug(f"Fetching historical bars for subscribed contracts")
        df = pd.DataFrame()
        bars = None
        last_bar = None
        
        logger.info(f"2 Retrieved vStop and uptrend for {symbol}")
        hasBidAsk = snapshot.hasBidAsk if snapshot else None
        

            
            
        vol= await daily_volatility.get(symbol)
        if vol is None:
            logger.error(f"No volatility data found for {symbol}")
          
            await yesterday_close_bar(ib, contract)
            vol= await daily_volatility.get(symbol)
        
        
        logger.debug(f"Processing TV post data for {contract.symbol}")
       
        
        atrFactor_order_request = req.atrFactor if req.atrFactor else 1.5
        atrlen= 20
        # determine bar size string
        logger.debug(f"1 Retrieved vStop and uptrend for {symbol}")
        
        logger.debug(f"3 Retrieved vStop and uptrend for {symbol}")
        atrFactorDyn = await get_dynamic_atr(symbol, BASE_ATR=atrFactor_order_request, vol=vol)
        if atrFactorDyn is not None:
                atrFactor_order_request = atrFactorDyn
        if atrFactor_order_request > 2.0:
            atrlen=3
        durationStr = str((bar_sec_int_dict[symbol]*atrlen)*2)
        df = await agg_bars.get_df(symbol, barSizeSetting)
        logger.info(f"2 Retrieved vStop and uptrend for {symbol}  agg_bars length is {len(df)} and durationStr: {durationStr} and barSizeSetting: {barSizeSetting}")
        if len(df) < 25:
            logger.info(f"Fetching reqHistoricalDataAsync data for {contract.symbol} with barSizeSetting: {barSizeSetting} because agg_bars length is {len(df)}")

            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1
            )
            if not bars:
                logger.warning(f"No IB history for {contract.symbol}")
                return None
            
            df = util.df(bars)
            if df.empty:
                logger.warning(f"No  data found for {contract.symbol} with barSizeSetting: {barSizeSetting}" )
            Last_bar_json = handle_bar(contract, bars)
            logger.debug(f"Last bar JSON for {contract.symbol}: {Last_bar_json}")
            await price_data_dict.add_bar(contract.symbol, barSizeSetting, Last_bar_json)
        logger.debug(f"4 Retrieved vStop and uptrend for {symbol} and len(df): {len(df)}")
        
        

        vStop, uptrend, atr = vol_stop_calc(df, atrlen, atrFactor_order_request)
        vStop_three, uptrend_three, atr_three = vol_stop_calc(df, 3, atrFactor_order_request)
        await volatility_stop_calc.set(symbol, vStop_three, uptrend_three, atr_three, atrFactor_order_request)
        vol_stop_data_three = await volatility_stop_calc.get(symbol)
        vStop_float_3,uptrend_bool_3, atr_float_3, vstopAtrFactor_float_3=vol_stop_data_three
        logger.debug(f"vStop float for {contract.symbol}: {vStop_float_3}, uptrend: {uptrend_bool_3} with atrFactor_order_request: {vstopAtrFactor_float_3} and atr_float: {atr_float_3} and atr length {atrlen} for {symbol}")


        logger.debug(f"5 Retrieved vStop and uptrend for {symbol}")
        df["vStop"], df["uptrend"],  df["uptrend"] = vStop, uptrend, atr
        if vStop is not None:
            logger.debug(f"vStop float for {contract.symbol}: {vStop}, uptrend: {uptrend}")
            
            await volatility_stop_data.set(symbol, vStop, uptrend, atr, atrFactor_order_request)
            vol_stop_data = await volatility_stop_data.get(symbol)
            vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float=vol_stop_data
        
            
            await price_data_dict.add_vol_stop(symbol, vStop_float, uptrend_bool, atrFactor_order_request, atr_float)
            vStop_float_dict, uptrend_dict=await price_data_dict.get_vol_stop(symbol)
            
            logger.info(f"Retrieved vStop and uptrend for {symbol}: vStop: {vStop_float_dict}, uptrend: {uptrend_dict} with atrFactor_order_request: {vstopAtrFactor_float} and atr_float: {atr_float} and atr length {atrlen} for {symbol}")
            

        
        
        response = {
            "symbol": contract.symbol
            
            
            
        }
        snapshot=await price_data_dict.get_snapshot(symbol)
        
        
        logger.debug(snapshot.bar.close)
        return vStop_float_dict, uptrend_dict

    except Exception as e:
        logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
        
def handle_bar(contract: Contract, bars: BarDataList) -> dict | None:
    """
    Return the most-recent bar in plain-dict form.
    Works the same on trading days and weekends.
    """
    try:
        if not bars:
            logger.warning(f"No IB history for {contract.symbol}")
            return None

        # use the last BarData object – IB always delivers in chronological order
        last = bars[-1]                        # BarData instance

        bar_dict = {
            "open":   last.open,
            "close":  last.close,
            "high":   last.high,
            "low":    last.low,
            "volume": last.volume,
            "time":   last.date
        }
        logger.debug(f"Last_bar_json for {contract.symbol}: {bar_dict}")
        return bar_dict

    except Exception as e:
        logger.error(f"Error handling bar for {contract.symbol}: {e}")
        raise

def vol_stop_calc(data: pd.DataFrame, atrlen: int, atrfactor: float):
    """
    
       atr_series = atr(
            high       = data["high"],
            low        = data["low"],
            close      = data["close"],
            length     = atrlen
        
        )
    """
    
    try:
        # 1) average of high+low
        logger.debug(f"Calculating volatility stop for bars with atrlen={atrlen} and atrfactor={atrfactor} length {len(data)}")
        
        atrM = None
        last_atr = None
        
        avg_hl = (data["high"] + data["low"]) / 2
        close_d = data["close"].values
        high_d, low_d = data["high"].values, data["low"].values
        atr_np = ta.ATR(high_d, low_d, close_d, timeperiod=atrlen)
        logger.debug(f"Calculating volatility stop for bars with atrlen={atrlen} and atrfactor={atrfactor}")
        
        atr_series= pd.Series(atr_np, index=data.index) if atr_np is not None else None
        
        
    
        

        # 2) compute ATR * factor, ask for full series
     
        last_atr = atr_series.iloc[-1] if not None else 0.0
        logger.debug(f"ATR series for {atrlen} bars: {last_atr}")
        

        # 3) guard against None or too little data
        if atr_series is None or not isinstance(atr_series, (pd.Series, pd.DataFrame)):
            logger.warning(
                f"ATR returned None or invalid for period {atrlen} "
                f"with only {len(data)} bars; defaulting ATR to zero."
            )
            atr_series = pd.Series(0.0, index=data.index)
        atrM = atr_series * atrfactor
        logger.debug(f"ATR: atrM= {atrM} and atr_series={atr_series} with atrfactor={atrfactor}")
        atrM = atrM.fillna(0)
        logger.debug(f"ATR: atrM= {atrM} and atr_series={atr_series} with atrfactor={atrfactor}")
            
        
        

        # 4) now use avg_hl as your 'close' for the volatility stop logic
        close = avg_hl
        max_price = close.copy()
        min_price = close.copy()
        vStop = pd.Series(np.nan, index=close.index)
        uptrend = pd.Series(True, index=close.index)

        for i in range(1, len(close)):
            max_price.iloc[i] = max(max_price.iloc[i - 1], close.iloc[i])
            min_price.iloc[i] = min(min_price.iloc[i - 1], close.iloc[i])
            # initialize vStop
            if np.isnan(vStop.iloc[i - 1]):
                vStop.iloc[i] = close.iloc[i]
            else:
                vStop.iloc[i] = vStop.iloc[i - 1]

            if uptrend.iloc[i - 1]:
                vStop.iloc[i] = max(vStop.iloc[i], max_price.iloc[i] - atrM.iloc[i])
            else:
                vStop.iloc[i] = min(vStop.iloc[i], min_price.iloc[i] + atrM.iloc[i])

            uptrend.iloc[i] = (close.iloc[i] - vStop.iloc[i]) >= 0

            # reset pivot on trend flip
            if uptrend.iloc[i] != uptrend.iloc[i - 1]:
                max_price.iloc[i] = close.iloc[i]
                min_price.iloc[i] = close.iloc[i]
                vStop.iloc[i] = (
                    max_price.iloc[i] - atrM.iloc[i]
                    if uptrend.iloc[i]
                    else min_price.iloc[i] + atrM.iloc[i]
                )
        return vStop, uptrend, atr_series
    except Exception as e:
        logger.error(f"Error calculating volatility stop: {e}")
        return None


