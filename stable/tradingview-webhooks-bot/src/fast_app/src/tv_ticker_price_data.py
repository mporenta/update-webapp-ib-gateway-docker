
import asyncio
from collections import defaultdict, deque

from datetime import timezone
from models import OrderRequest, VolatilityStopData
from typing import Any, Dict, Optional, Tuple
from log_config import log_config, logger
from ib_async import *
from ib_async.order import Trade, OrderStatus
from ib_async.util import isNan
import pandas as pd
import re
log_config.setup()
timeframe_dict: Dict[str, str] = {}
yesterday_close: Dict[str, float] = {}
daily_true_range:    dict[str, float] = {}
parent_ids: Dict[str, Dict[str, Any]] =  defaultdict(dict)

from helpers.price_data_dict import barSizeSetting_dict, price_data_dict, bar_sec_int_dict

# --------------------------------------------------------------------------
# high-time-frame aggregation of the live 5-second bars already in memory
# --------------------------------------------------------------------------


_SEC_SIZES  = (1, 5, 10, 15, 30)
_MIN_SIZES  = (1, 2, 3, 5, 10, 15, 20, 30)
_HOUR_SIZES = (1, 2, 3, 4, 8)
_FREQS: dict[str, str] = {
        **{f"{n} secs":  f"{n}s"    for n in _SEC_SIZES},
        **{f"{n} sec":   f"{n}s"    for n in _SEC_SIZES},             # alias
        **{f"{n} min":   f"{n}min"  for n in _MIN_SIZES},
        **{f"{n} mins":  f"{n}min"  for n in _MIN_SIZES},             # alias
        **{f"{n} hour":  f"{n}h"    for n in _HOUR_SIZES},
        **{f"{n} hours": f"{n}h"    for n in _HOUR_SIZES},            # alias
        "1 day":   "1D",
        "1 week":  "1W",
        "1 month": "1ME",      
    }

class BarAggregator:
    """
    Keeps rolling n-second bars (15 s, 1 min, …) derived from the native
    5-second RealTimeBar stream we already store in price_data_dict.rt_bars.

    • Call `update(symbol)` once after every on_bar_update() tick.
    • Get the latest DataFrame with `get_df(symbol, '1 min')`, etc.
    """
  
    def __init__(self, maxlen: int = 300):
        #  { "NVDA": { "1 min": deque([BarData,…]) , "15 secs": deque([...]) } }
        self.bars: dict[str, dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=maxlen)))
        self._FREQS = _FREQS
    def _barSize_to_rule(self, barSizeSetting: str) -> str:
        """Fallback for exotic bar sizes (“17 secs”, “7 mins”, …)."""
        if barSizeSetting in self._FREQS:
            return self._FREQS[barSizeSetting]

        m = re.match(r"(\d+)\s*(sec|secs|min|mins|hour|hours)", barSizeSetting, re.I)
        if not m:
            raise ValueError(f"Unsupported TF: {barSizeSetting}")

        n, unit = int(m.group(1)), m.group(2).lower()
        if unit.startswith("sec"):   return f"{n}s"
        if unit.startswith("min"):   return f"{n}min"
        if unit.startswith("hour"):  return f"{n}h"
        raise ValueError(f"Unsupported TF: {barSizeSetting}")


    

    async def update(self, symbol: str):
        """Build/append higher-TF bar(s) after each new 5-sec bar."""
        try:
            if not price_data_dict.rt_bars[symbol]:
                return

            # ---- 1) put the raw 5-second deque into a DataFrame -------------
            df5 = pd.DataFrame(
                [b.__dict__ for b in price_data_dict.rt_bars[symbol]],
                columns=["time", "open_", "high", "low", "close", "volume"]
            )
            if df5.empty:
                return
            # guarantee TZ awareness & set index
            df5["time"] = pd.to_datetime(df5["time"]).dt.tz_localize(
                None).dt.tz_localize(timezone.utc)
            df5 = df5.set_index("time")

            # ---- 2) resample into each desired frequency --------------------
            for name, rule in self._FREQS.items():
                resamp = df5.resample(rule, label="right", closed="right")
                dfN = pd.DataFrame({
                    "open":   resamp["open_"].first(),
                    "high":   resamp["high"].max(),
                    "low":    resamp["low"].min(),
                    "close":  resamp["close"].last(),
                    "volume": resamp["volume"].sum()
                }).dropna(how="any")

                # append only *new* bars
                last_time_cached = (self.bars[symbol][name][-1].date
                                    if self.bars[symbol][name] else None)
                new_rows = dfN.loc[dfN.index > last_time_cached] if last_time_cached else dfN
                for ts, row in new_rows.iterrows():
                    self.bars[symbol][name].append(
                        BarData(date=ts, **row.to_dict(), average=0.0, barCount=0))
        except Exception as e:
            logger.error(f"Error in BarAggregator.update({symbol}): {e}")
            raise e
    async def get_df(self, symbol: str, barSizeSetting: str) -> pd.DataFrame:
        """
        Return an OHLCV DataFrame for any barSizeSetting Pine/TWS can produce
        (e.g. '15 secs', '30 mins', '2 hours', '1 day', ...).

        If no bars have been cached yet → an empty, indexed DataFrame is returned.
        """
        try:
            # find / build the pandas resample rule for this barSizeSetting
            rule = self._FREQS.get(barSizeSetting) or self._barSize_to_rule(barSizeSetting)

            # make sure we keep a deque for this TF even if it’s still empty
            bars = list(self.bars[symbol][barSizeSetting])

            if not bars:                           # nothing cached yet
                return (pd.DataFrame(
                            columns=["date", "open", "high", "low", "close", "volume"]
                        ).set_index("date")
                        .astype({"open": float, "high": float,
                                 "low": float,  "close": float, "volume": float}))

            return (pd.DataFrame([b.__dict__ for b in bars])
                    .set_index("date")
                    .sort_index())

        except Exception as e:
            logger.error(f"Error in BarAggregator.get_df({symbol}, {barSizeSetting}): {e}")
            raise

        
  

# singleton
agg_bars = BarAggregator()



class tradingViewPostData:
    """An async-safe in-memory store, accessible app-wide."""
    def __init__(self):
        self.new_price_data: Dict[str, OrderRequest] = {}
        self._data: Dict[str, Dict[str, Any]]        = defaultdict(dict)
        self._lock                                   = asyncio.Lock()
        

    async def set(self, symbol: str, req: OrderRequest, tv_hook: bool = False) -> OrderRequest:
        async with self._lock:
            try:
                logger.debug(f"Setting data for {symbol} with tv_hook={tv_hook}")
                # ── use the OrderRequest directly ───────────────────────────
                value   = req                       # already a pydantic model
                symbol  = value.symbol              # attribute access, not []

                # keep both object and JSON copy
                self._data[symbol]         = value.model_dump()
                self.new_price_data[symbol] = value
                logger.debug(f"Set data for {symbol}: {value}")

                # ── work out the timeframe / barSizeSetting ────────────────
                timeframe = value.barSizeSetting_tv if tv_hook else value.barSizeSetting_web
                if timeframe:
                    logger.debug(f"Setting barSizeSetting for {symbol}: {timeframe}")
                    bar_size, bar_sec_int = await convert_pine_timeframe_to_barsize_dict(
                        timeframe=timeframe,
                        tv_hook=tv_hook,
                        value=value,
                        symbol=symbol           # pass the model
                    )
                    logger.debug(f"Set barSizeSetting for {symbol}: {bar_size}")
                    barSizeSetting_dict[symbol] = bar_size
                    bar_sec_int = int(bar_sec_int)
                    bar_sec_int_dict[symbol] = bar_sec_int

                    logger.debug(f"Updated barSizeSetting_dict for {symbol}: {barSizeSetting_dict[symbol]} and bar_sec_int_dict: {bar_sec_int_dict[symbol]}")
                else:
                  
                    logger.warning(f"{symbol}: no barSizeSetting provided at all")

                return value

            except Exception as e:
                logger.error(f"Error setting data for {symbol}: {e}")
                raise
    async def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            try:
                return self._data.get(symbol)
            except Exception as e:
                logger.error(f"Error getting data for {symbol}: {e}")
                return None
    async def all(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return dict(self._data)  # shallow copy

# singleton instance
tv_store_data = tradingViewPostData()


class VolatilityStop:
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
volatility_stop_data = VolatilityStop()

class dailyVolatility:
    """An async-safe in-memory store for daily volatility data."""
    def __init__(self):
        self._data: Dict[str, float] = defaultdict(float)
        self._lock = asyncio.Lock()

    async def set(self, symbol: str, value: float) -> None:
        async with self._lock:
            
            self._data[symbol] = value

    async def get(self, symbol: str) -> Optional[float]:
        async with self._lock:
            if symbol not in self._data:
                logger.debug(f"No daily volatility data for {symbol}")
                return None
            return self._data.get(symbol)

    async def all(self) -> Dict[str, float]:
        async with self._lock:
            return dict(self._data)  # shallow copy
daily_volatility = dailyVolatility()



async def yesterday_close_bar(
    ib: IB,
    contract: Contract
) -> Optional[pd.DataFrame]:
    """
    Fetch the last 2 daily bars for `contract`, compute and store:
      - yesterday_close[symbol]
      - price_data_dict.last_daily_bar_dict[symbol]
      - daily_volatility[symbol]
    Returns the DataFrame of daily bars (2 or more rows), or None on failure.
    """
   
    try:
        logger.debug(f"Fetching yesterday close bar for {contract.symbol}")
        yc=None
        df= pd.DataFrame()
        bars= None
        symbol = contract.symbol
        ib_ticker = await price_data_dict.get_snapshot(contract.symbol)
        hasBidAsk = ib_ticker.hasBidAsk if ib_ticker else None
        high=ib_ticker.high if ib_ticker else 0.0
        low=ib_ticker.low if ib_ticker else 0.0
        yesterday_price = ib_ticker.yesterday_close_price

        logger.info(f"Fetching yesterday close bar for {symbol}, hasBidAsk={hasBidAsk}, yesterday_price={yesterday_price}")
        if yesterday_price is None or isNan(yesterday_price) or not hasBidAsk:
            logger.warning(f"No yesterday price found for {symbol}, fetching historical data.")
        
            # pull 2 days of 1-day bars
            bars = await ib.reqHistoricalDataAsync(
                contract=contract,
                endDateTime="",          # now
                durationStr="2 D",       # last two calendar days
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
            #await asyncio.sleep(1)  # give the IB client a moment

            df = util.df(bars)
            logger.debug(f"Fetched {len(df)} daily bars for {symbol}")
            if len(df) < 2:
                logger.error(f"Not enough bars for {symbol}: got {len(df)}")
                return None

            # yesterday’s close is the *penultimate* row
            logger.debug(f"Last 2 daily bars for {symbol}:\n{df.tail(2)}")
            
            yc = df.iloc[-2]["close"]
            yesterday_price= float(yc)
            yesterday_close[symbol] = float(yc)
            logger.debug(f"Yesterday's close for {symbol}: {yesterday_price}")
        yesterday_close[symbol] = yesterday_price
        

        # last daily bar is the final row
        last_bar = df.iloc[-1]
        price_data_dict.last_daily_bar_dict[symbol] = last_bar

        # compute true range and daily volatility
        if not hasBidAsk:
            high = last_bar["high"]
            low  = last_bar["low"]
        tr = max(
            high - low,
            abs(high - yesterday_price),
            abs(low  - yesterday_price)
        )
        daily_true_range[symbol] = tr
        vol = tr * 100.0 / abs(low) if low != 0 else None
        if vol is not None:
            await daily_volatility.set(symbol, vol)

        logger.debug(
            f"[{symbol}] yesterday_close={yesterday_price:.2f}, "
            f"TR={tr:.2f}, volatility={vol:.2f}"
        )
        return df

    except Exception as e:
        logger.error(f"Error in yesterday_close_bar for {symbol}: {e}")
        return None


class pnlStarted:
    """An async-safe in-memory store, accessible app-wide."""
    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] =  defaultdict(dict)
        self._start: bool = False
        
        self._lock = asyncio.Lock()
    async def start(self) -> bool:
        async with self._lock:
            try:
                if not self._start:
                    logger.info(f"Starting pnl data in pnlStarted? {self._start}")
                    self._start = True
                    return False
                    
                self._start = True
                logger.info(f"start pnl in pnlStarted? {self._start}")
                return self._start
            except Exception as e:
                logger.error(f"Error starting pnl data: {e}")
    async def end(self) -> bool:
        async with self._lock:
            try:
                if not self._start:
                    logger.info(f"end pnl in pnlStarted ? {self._start}")
                    self._start= False
                    return False
                    
                self._start=False
                logger.info(f"end pnl in pnlStarted ? {self._start}")
                return self._start
            except Exception as e:
                logger.error(f"Error Ending pnl data: {e}")
    async def set(self, symbol: str) -> None:
        async with self._lock:
            try:
                self._data[symbol] 
                logger.debug(f"Setting data for {symbol} in pnlStarted  self._data[symbol] {self._data[symbol]}")
                return None
            except Exception as e:
                logger.error(f"Error setting data for {symbol}: {e}")
    async def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            try:
                if symbol in self._data:
                    logger.debug(f"Getting data for {symbol} from pnlStarted")
                    return True
                return False
            except Exception as e:
                logger.error(f"Error getting data for {symbol}: {e}")
                return None
    async def delete(self, symbol: str) -> None:
        async with self._lock:
            try:
                if symbol in self._data:
                    del self._data[symbol]
            except Exception as e:
                logger.error(f"Error deleting data for {symbol}: {e}")
pnl_started = pnlStarted()

class orderPlaced:
    """An async-safe in-memory store, accessible app-wide."""
    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] =  defaultdict(dict)
        
        self._lock = asyncio.Lock()

    async def set(self, symbol: str) -> None:
        async with self._lock:
            try:
                self._data[symbol] 
                logger.debug(f"Setting data for {symbol} in orderPlaced  self._data[symbol] {self._data[symbol]}")
                return None
            except Exception as e:
                logger.error(f"Error setting data for {symbol}: {e}")
    async def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            try:
                if symbol in self._data:
                    logger.debug(f"Getting data for {symbol} from orderPlaced")
                    return True
                return False
            except Exception as e:
                logger.error(f"Error getting data for {symbol}: {e}")
                return None
    async def delete(self, symbol: str) -> None:
        async with self._lock:
            try:
                if symbol in self._data:
                    del self._data[symbol]
            except Exception as e:
                logger.error(f"Error deleting data for {symbol}: {e}")

# singleton instance
order_placed = orderPlaced()

class SubscribedContracts:
    """Async-safe in-memory cache of qualified IB Contracts."""
    def __init__(self):
        self.ib: IB = None           # ← will be set by app.py 
        self._lock = asyncio.Lock()
        self._cache: Dict[str, Contract] = defaultdict(Contract)

    def set_ib(self, ib_instance: IB):
        """Initialize the IB client once at startup."""
        self.ib = ib_instance
    async def get_w_ib(self, symbol: str, barSizeSetting=None, ib: IB= None) -> Optional[Contract]:
        """Get (or qualify & cache) a Contract for `symbol`."""
        async with self._lock:
            if symbol in self._cache:
                return self._cache[symbol]

            if not self.ib:
                logger.error("IB client not initialized on SubscribedContracts!")
                return None

            # build a bare‐bones Contract
            c = Contract(symbol=symbol, exchange="SMART", secType="STK", currency="USD")
            logger.debug(f"Qualifying contract for {symbol}…")
            qualified = await self.ib.qualifyContractsAsync(c)
            if not qualified:
                logger.error(f"Failed to qualify {symbol}")
                return None
            symbol: Ticker = ib.reqMktData(qualified[0], genericTickList = "221,236", snapshot=False)
            await price_data_dict.add_ticker(symbol, barSizeSetting)
            self._cache[symbol] = qualified[0]
            return qualified[0]

    async def get(self, symbol: str, barSizeSetting=None) -> Optional[Contract]:
        """Get (or qualify & cache) a Contract for `symbol`."""
        async with self._lock:
            if symbol in self._cache:
                return self._cache[symbol]

            if not self.ib:
                logger.error("IB client not initialized on SubscribedContracts!")
                return None

            # build a bare‐bones Contract
            c = Contract(symbol=symbol, exchange="SMART", secType="STK", currency="USD")
            logger.debug(f"Qualifying contract for {symbol}…")
            qualified = await self.ib.qualifyContractsAsync(c)
            if not qualified:
                logger.error(f"Failed to qualify {symbol}")
                return None
            self._cache[symbol] = qualified[0]
            return qualified[0]
    async def has(self, symbol: str) -> bool:
        if symbol in self._cache:
            return True
        else:
            
            return False
    async def delete(self, symbol: str) -> None:
        async with self._lock:
            try:
                if symbol in self._cache:
                    del self._cache[symbol]
            except Exception as e:
                logger.error(f"Error deleting data for {symbol}: {e}")

subscribed_contracts = SubscribedContracts()




orders_in_flight: Dict[str,int] = {}

class IbOpenOrders:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._data: Dict[str, Dict[int, Trade]] = defaultdict(dict)

    async def set(self, symbol: str, trade: Trade) -> None:
        async with self._lock:
            self._data[symbol][trade.order.orderId] = trade

    async def get(self, symbol: str) -> dict[int, Trade]:
        async with self._lock:
            if symbol not in self._data:
                logger.debug(f"No open orders for {symbol}")
                return None
            return dict(self._data.get(symbol, {}))

    async def all_flat(self) -> Dict[str, Trade]:
        async with self._lock:
            return { f"{symbol}_{oid}": trade
                     for symbol, orders in self._data.items()
                     for oid, trade in orders.items() }

    async def sync_symbol(self, symbol: str, ib: IB) -> None:
        live = { t.order.orderId: t for t in (ib.openTrades() or []) }
        async with self._lock:
            if live.orderStatus.status in ("Filled","Cancelled"):
                status = live.orderStatus.status if live.orderStatus else "Unknown"
                oid = live.orderStatus.orderId
            
            await pnl_started.set(symbol)
            await self.delete(live.contract.symbol, live)
            logger.info(f"Order {oid} for {symbol} is {status}, removed from tracking")
            await self.set(symbol, live)  # overwrite existing data
            self._data[symbol][live.order.orderId]  = live

    async def delete(self, symbol: str, trade: Trade = None) -> None:
        async with self._lock:
            if trade is None:
                self._data.pop(symbol, None)
            else:
                self._data[symbol].pop(trade.order.orderId, None)
                if not self._data[symbol]:
                    self._data.pop(symbol, None)

    async def on_order_status_event(self, trade: Trade):
        status = trade.orderStatus.status if trade.orderStatus else "Unknown"
        symbol = trade.contract.symbol
        oid = trade.orderStatus.orderId
        logger.info(f"Order {oid} for {symbol} is {status}")
        await pnl_started.set(symbol)
        await self.set(symbol, trade)
        # once IB reports Filled or Cancelled, forget that order
        if trade.orderStatus.status in ("Filled","Cancelled"):
            
            await pnl_started.set(symbol)
            await self.delete(trade.contract.symbol, trade)
            logger.info(f"Order {oid} for {symbol} is {status}, removed from tracking")
            return None
        
        
ib_open_orders = IbOpenOrders()  


    


# the four “open” states you care about:
async def reconcile_open_orders(symbol: str, ib=None) -> bool:
    """
    Return True if:
      - there is at least one stored Trade in ib_open_orders for `symbol`
        whose status is in _OPEN_STATES.
    Otherwise return False.
    """
    _OPEN_STATES = {
        OrderStatus.PendingSubmit,
        OrderStatus.ApiPending,
        OrderStatus.PreSubmitted,
        OrderStatus.Submitted,
    }

    # Fetch all stored open orders as a flat dict: key => Trade
    try:
        logger.info(f"Reconciling open orders for symbol: {symbol}")
        trade_dict = await ib_open_orders.all_flat()  # { "<symbol>_<orderId>": Trade, ... }
        logger.info(f"Fetched {len(trade_dict)} open orders for symbol: {symbol}")
    except Exception as e:
        logger.error(f"Error fetching stored open orders: {e}")
        return False

    # Scan through each stored Trade and check for matching symbol and open status
    for trade in trade_dict.values():
        if trade.contract.symbol != symbol:
            continue
        live_state = trade.orderStatus.status
        if live_state in _OPEN_STATES:
            return True

    return False
def parse_timeframe_to_seconds_dict(tf: str) -> int:
    tf = tf.strip().lower()
    if tf.endswith("secs") or tf.endswith("sec"):
        return int(tf.split()[0])
    if tf.endswith("mins") or tf.endswith("min"):
        return int(tf.split()[0]) * 60
    if tf.endswith("s"):
        return int(tf[:-1])
    if tf.endswith("m"):
        return int(tf[:-1]) * 60
    raise ValueError(f"Unrecognized timeframe: {tf}")

async def convert_pine_timeframe_to_barsize_dict(
        timeframe: str | None,
        post_log=True,
        tv_hook: bool = False,
        value: OrderRequest=None,
        symbol: str = None
) -> tuple[str, int]:
    """
    Convert Pine Script timeframe string to TWS API barSizeSetting

    Args:
        timeframe (str): Pine Script timeframe string (e.g., "10S", "30", "1D")

    Returns:
        str: Corresponding TWS API barSizeSetting
    """
    # Direct mapping of Pine Script timeframes to TWS API barSizeSetting
    try:
        if value is None and timeframe is None:
            raise ValueError("timeframe cannot be None when req is None")

        if value is not None:
            symbol     = value.symbol
            timeframe  = value.barSizeSetting_tv if tv_hook else value.barSizeSetting_web

        
        barSizeSetting = None
        if tv_hook:
            logger.debug(f"Resolving contract for TradingView hook with symbol: {symbol} and barSizeSetting_tv: {value.barSizeSetting_tv}")
            timeframe = value.barSizeSetting_tv
           
        else:
            logger.debug(f"Resolving contract for symbol: {symbol} and barSizeSetting_web: {value.barSizeSetting_web}")
            timeframe = value.barSizeSetting_web
           

        is_valid_timeframe = "secs" in timeframe or "min" in timeframe  or "mins" in timeframe
        bar_sec_int = None
        
        if not is_valid_timeframe and post_log:
            logger.debug(f"Converting Pine timeframe '{timeframe}' to barSizeSetting...")
        if is_valid_timeframe:
            barSizeSetting = timeframe
            
            bar_sec_int=parse_timeframe_to_seconds_dict(barSizeSetting)
            if post_log:
                logger.debug(f"Converted timeframe '{timeframe}' to barSizeSetting: {barSizeSetting} and bar_sec_int: {bar_sec_int}")

            return barSizeSetting, bar_sec_int

        # Seconds
        if timeframe == "1S":
            barSizeSetting = "1 secs"
        elif timeframe == "5S":
            barSizeSetting = "5 secs"
        elif timeframe == "10S":
            barSizeSetting = "10 secs"
        elif timeframe == "15S":
            barSizeSetting = "15 secs"
        elif timeframe == "30S":
            barSizeSetting = "30 secs"

        # Minutes (Pine uses numbers without units)
        elif timeframe == "1":
            barSizeSetting = "1 min"
        elif timeframe == "2":
            barSizeSetting = "2 mins"
        elif timeframe == "3":
            barSizeSetting = "3 mins"
        elif timeframe == "5":
            barSizeSetting = "5 mins"
        elif timeframe == "10":
            barSizeSetting = "10 mins"
        elif timeframe == "15":
            barSizeSetting = "15 mins"
        elif timeframe == "20":
            barSizeSetting = "20 mins"
        elif timeframe == "30":
            barSizeSetting = "30 mins"

        # Hours (Pine expresses hours in minutes)
        elif timeframe == "60":
            barSizeSetting = "1 hour"
        elif timeframe == "120":
            barSizeSetting = "2 hours"
        elif timeframe == "180":
            barSizeSetting = "3 hours"
        elif timeframe == "240":
            barSizeSetting = "4 hours"
        elif timeframe == "480":
            barSizeSetting = "8 hours"

        # Days, Weeks, Months
        elif timeframe == "1D":
            barSizeSetting = "1 day"
        elif timeframe == "1W":
            barSizeSetting = "1 week"
        elif timeframe == "1M":
            barSizeSetting = "1 month"

        # Default to 1 minute if timeframe not recognized
        else:
            
            barSizeSetting = timeframe
            barSizeSetting_dict[symbol]= barSizeSetting
            if post_log:
                logger.debug(f"Unrecognized timeframe '{timeframe}', defaulting to '1 min'")
        bar_sec_int=parse_timeframe_to_seconds_dict(barSizeSetting)
        if post_log:
            logger.debug(f"Converted timeframe '{timeframe}' to barSizeSetting: {barSizeSetting} and bar_sec_int: {bar_sec_int}")
        return barSizeSetting, bar_sec_int

    except Exception as e:
        logger.error(f"Error converting Pine timeframe '{timeframe}' to barSizeSetting: {e}")
        raise ValueError(f"Unrecognized timeframe: {timeframe}") from e