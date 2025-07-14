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
from log_config import log_config, logger
load_dotenv()
bar_sec_int_dict: Dict[str, Dict[str, Any]] =  defaultdict(dict)

barSizeSetting_dict: Dict[str, Dict[str, Any]] =  defaultdict(dict)
class TickerPriceData:
    def __init__(self):
        
        self._data     = defaultdict(lambda: deque(maxlen=100))               # raw dict bars
        self.rt_bars   = defaultdict(lambda: deque(maxlen=100))               # RealTimeBar objects
        self.bars      = defaultdict(lambda: deque(maxlen=100))               # BarData objects
        self.price_data: Dict[str, PriceSnapshot] = {}
        self.last_daily_bar_dict = defaultdict(dict)
        self.bar_sub:  Dict[str, bool] = defaultdict(dict)
        self._ticker: Dict[str, Ticker] = defaultdict(Ticker)
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.global_lock = asyncio.Lock()  # for operations touching multiple symbols

    def _get_lock(self, symbol: str) -> asyncio.Lock:
        # ensures a single lock per symbol
        return self._locks[symbol]
    
    
    async def add_ticker(self, ticker: Ticker, barSizeSetting=None, post_log= True) -> PriceSnapshot:
        """
        Upsert a PriceSnapshot from the live Ticker event.
        Creates it if missing, then copies over all fields,
        coercing any None/NaN floats to 0.0.
        """
        sym = ticker.contract.symbol
        #logger.debug(f"Adding ticker: {ticker}")
        lock = self._get_lock(sym)
        async with lock:
            
            self._ticker[sym] = ticker
            if barSizeSetting_dict.get(sym):
                barSizeSetting = barSizeSetting_dict.get(sym)
            if barSizeSetting is None :
                # infer barSizeSetting from the symbol's barSizeSetting_web
                barSizeSetting = "15 secs"
                #logger.info(f"Inferred barSizeSetting for {symbol}: {barSizeSetting}") 

            # instantiate if missing
            snapshot = self.price_data.get(sym)
            if snapshot is None:
                snapshot = PriceSnapshot()
                self.price_data[sym] = snapshot
                #logger.debug(f"Created new PriceSnapshot for {symbol}")

            # always store the raw Ticker & barSize
            snapshot.ticker         = ticker
            snapshot.last           = ticker.last
            snapshot.barSizeSetting = barSizeSetting
            snapshot.hasBidAsk      = ticker.hasBidAsk()
            snapshot.shortableShares = ticker.shortableShares
            snapshot.halted         = ticker.halted
            snapshot.time           = ticker.time
            snapshot.markPrice     = ticker.markPrice
            snapshot.yesterday_close_price = ticker.close

            # for each float‐like field: force None/NaN → 0.0
            ATTRS = ("last", "open", "high", "low",
            "bid", "ask", "volume", "markPrice",
            "shortableShares", "halted")        

            for attr in ATTRS:
                val = getattr(ticker, attr)
                setattr(snapshot, attr, 0.0 if (val is None or util.isNan(val)) else val)

            # leave snapshot.uptrend alone (could be None or a prior bool)
            if post_log:

                logger.debug(f"Updated snapshot for {sym}: {snapshot}")
            return snapshot
  
    async def add_first_rt_bar(self, symbol: str) -> bool:
        """
        Append a volume stop to the snapshot for the given symbol.
        This is used to track volume-based trading stops.
        """
        lock = self._get_lock(symbol)
        async with lock:
            if symbol in self.bar_sub:
                return True
            else:
                self.bar_sub[symbol] = True
            
                return False
    async def add_vol_stop(self, symbol: str, vol_stop: float, trend_bol: bool, vstopAtrFactor=None, atr_float=None) -> None:
        """
        Append a volume stop to the snapshot for the given symbol.
        This is used to track volume-based trading stops.
        """
        lock = self._get_lock(symbol)
        async with lock:
            if symbol not in self.price_data:
                self.price_data[symbol] = PriceSnapshot()
            snapshot = self.price_data.get(symbol)
            snapshot.vStop = vol_stop
            snapshot.vstopAtrFactor = vstopAtrFactor
            
            snapshot.atr = atr_float
            snapshot.uptrend = trend_bol
            return snapshot
    
    async def add_bar(self, symbol: str, barSizeSetting: str, bar_dict: dict):
        lock = self._get_lock(symbol)
        async with lock:
            try:
                logger.debug(f"Adding bar for {symbol} with barSizeSetting {barSizeSetting}: {bar_dict}")
                # ── 1. sanitise numeric fields ────────────────────────────────
                for k in ("open", "high", "low", "close", "volume"):
                    v = bar_dict.get(k)
                    if v is None or util.isNan(v):
                        bar_dict[k] = 0.0

                # keep raw dict history
                self._data[symbol].append(bar_dict)

                bd = BarData(
                    date     = bar_dict["time"],
                    open     = bar_dict["open"],
                    high     = bar_dict["high"],
                    low      = bar_dict["low"],
                    close    = bar_dict["close"],
                    volume   = int(bar_dict["volume"]),
                    average  = 0.0,
                    barCount = 0,
                )

                # ── 2. append ONLY if it’s newer than the last cached bar ─────
                if self.bars[symbol] and bd.date <= self.bars[symbol][-1].date:
                    # older/duplicate bar → ignore
                    return

                self.bars[symbol].append(bd)
                logger.debug(f"Added bar for {symbol}: {bd}")

                # ── 3. update snapshot with **latest** bar info ───────────────
                snap = self.price_data.setdefault(symbol, PriceSnapshot())
                snap.barSizeSetting = barSizeSetting
                snap.bar            = bd
                snap.close          = bd.close           # current close
                snap.last_bar_close = bd.close           # newest bar close
                snap.bars           = self._data[symbol] # raw history deque
                logger.debug(f"Updated snapshot for {symbol} with new bar: {snap}")

            except Exception as e:
                logger.error(f"Error adding bar for {symbol}: {e}")
                raise

    async def add_rt_bar(self, symbol: str, rt_bar_dict: dict):
        try:
            lock = self._get_lock(symbol)
            async with lock:
                # sanitize incoming RT bar
                for k in ("open_","high","low","close","volume"):
                    v = rt_bar_dict.get(k)
                    if v is None or util.isNan(v):
                        rt_bar_dict[k] = 0.0

                rt = RealTimeBar(**rt_bar_dict)
                self.rt_bars[symbol].append(rt)

                bd = BarData(
                    date     = rt.time,
                    open     = rt.open_,
                    high     = rt.high,
                    low      = rt.low,
                    close    = rt.close,
                    volume   = int(rt.volume),
                    average  = 0.0,
                    barCount = rt.count
                )
                self.bars[symbol].append(bd)

                snap = self.price_data.setdefault(symbol, PriceSnapshot())
                snap.last_bar_close = rt.close
                snap.close          = rt.close
                snap.rt_bar         = rt
                snap.bar            = bd
                snap.bars           = self._data[symbol]
        except Exception as e:
            logger.error(f"Error adding RT bar for {symbol}: {e}")
            raise e
    async def get_snapshot(self, symbol: str) -> PriceSnapshot | None:
        lock = self._get_lock(symbol)
        async with lock:
            snapshot = self.price_data.get(symbol)
            if symbol not in self.price_data:
                logger.debug(f"No snapshot found for {symbol}")
                return None
            logger.debug(f"Getting snapshot for {symbol}")
            return snapshot
    async def get_ticker(self, symbol: str, ib: IB = None) -> Ticker | None:
        lock = self._get_lock(symbol)
        async with lock:
            
            if symbol not in self._ticker:
                logger.debug(f"No _ticker found for {symbol}")
                return None
            
            logger.info(f"Getting _ticker for {symbol}:")
            return self._ticker[symbol]

    async def has_bar(self, symbol: str) -> bool:
        """Check if there are any confirmed bars for the symbol."""
        lock = self._get_lock(symbol)
        async with lock:
            if self.rt_bars[symbol]:
                return True
           
            else:
                return False
        
    async def get_vol_stop(self, symbol: str) -> Optional[tuple]:
        """Return the volume stop and trend for a symbol, if available."""
        logger.debug(f"1 Getting volume stop for {symbol}")
        lock = self._get_lock(symbol)
        async with lock:
            logger.debug(f"2 Getting volume stop for {symbol} inside lock")
            snapshot = self.price_data.get(symbol)
            if snapshot:
                vol_stop = snapshot.vStop
                trend_bol = snapshot.uptrend
                if vol_stop is not None and trend_bol is not None:
                    logger.debug(f"3 Getting volume stop for {symbol} inside lock vol_stop {vol_stop}, trend_bol {trend_bol}")
                    return vol_stop, trend_bol
              
             
                return snapshot
            return None

    async def get_bars(self, symbol: str) -> deque:
        """Return the deque of confirmed bars for a symbol."""
        lock = self.global_lock 
        async with lock:
            if symbol not in self.bars:
                logger.debug(f"No bars found for {symbol}")
                return None
            return self.bars[symbol]

    async def all_snapshots(self) -> Dict[str, PriceSnapshot]:
        lock = self.global_lock 
        async with lock:
            return dict(self.price_data)

    async def all_bars(self) -> Dict[str, deque]:
        lock = self.global_lock 
        async with lock:
            return dict(self._data)
price_data_dict = TickerPriceData() 