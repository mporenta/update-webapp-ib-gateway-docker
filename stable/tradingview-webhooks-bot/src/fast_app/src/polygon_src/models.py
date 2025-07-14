from hmac import new
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional, ClassVar, FrozenSet, List
from log_config import log_config, logger
from math import isnan as isNan, log
import pandas as pd
from ib_async import *
from dataclasses import dataclass, field
from collections import deque
import pandas as pd
import datetime
from dataclasses import dataclass, field
import arrow
from typing import Optional, Dict, Any
from polygon.rest.models import Agg, TickerDetails
from datetime import date, datetime
from typing import Optional, List, Any, Dict
import pytz

from pydantic import BaseModel, Field, field_validator
from sqlmodel import SQLModel, Field as SField, create_engine, Session, select, Column, JSON, Date, DateTime

# 1. Pydantic schema for the 'data' block
class WshData(BaseModel):
    earnings_date: Optional[date]
    announce_datetime: Optional[datetime]
    time_of_day: Optional[str]
    wshe_earnings_date_status: Optional[str]
    # capture anything unexpected here:
    raw: Dict[str, Any]

    

# 2. SQLModel table
class WshEvent(SQLModel, table=True):
    id: Optional[int] = SField(default=None, primary_key=True)
    event_type: str
    index_date_type: str
    index_date: str
    earnings_date: Optional[date] = SField(sa_column=Column(Date))
    announce_datetime: Optional[datetime] = SField(sa_column=Column(DateTime(timezone=True)))
    data: Dict[str, Any] = SField(sa_column=Column(JSON), default={})
    raw_event: Dict[str, Any] = SField(sa_column=Column(JSON), default={})



@dataclass
class PriceSnapshot:
    yesterday_close_price: Optional[float] = None
    open:                Optional[float] = None
    high:                Optional[float] = None
    low:                 Optional[float] = None
    last:                Optional[float] = None
    bid:                 Optional[float] = None
    ask:                 Optional[float] = None
    volume:              Optional[float] = None
    last_bar_close:      Optional[float] = None
    close:               Optional[float] = None
    confirmed_5s_bar:    Optional[Dict[str,Any]] = None
    ticker:              Any   = None    # skip in table
    bars:                Any   = None    # skip in table
    halted:              Optional[float] = None
    hasBidAsk:           Optional[bool]  = None
    shortableShares:     Optional[float] = None
    vStop:               Optional[float] = None
    uptrend:             Optional[bool]  = None
    markPrice:           Optional[float] = None
    barSizeSetting:      Optional[str] = None
    bar:              Optional[Agg] = None
    ticker_details: Optional[TickerDetails] = None
  
    def to_table(self) -> str:
        rows = [
            ("Yest. Close",    self.yesterday_close_price),
            ("Open",           self.open),
            ("High",           self.high),
            ("Low",            self.low),
            ("Last",           self.last),
            ("Bid",            self.bid),
            ("Ask",            self.ask),
            ("Volume",         self.volume),
            ("Last Bar Close", self.last_bar_close),
            ("Close",          self.close),
            ("Halted",         self.halted),
            ("Has Bid/Ask",    self.hasBidAsk),
            ("Shortable",      self.shortableShares),
            ("vStop",          self.vStop),
            ("Uptrend",        self.uptrend),
            ("Market Price",     self.markPrice),
        ]

        # Helper to stringify values
        def fmt(v):
            if v is None:
                return "-"
            if isinstance(v, bool):
                return str(v)
            try:
                return f"{v:,.2f}"
            except Exception:
                return str(v)

        names = [n for n, _ in rows]
        vals  = [fmt(v) for _, v in rows]

        name_w = max(len(n) for n in names)
        val_w  = max(len(v) for v in vals)

        sep = f"+{'-'*(name_w+2)}+{'-'*(val_w+2)}+"
        lines = [sep,
                 f"| {'Field'.ljust(name_w)} | {'Value'.rjust(val_w)} |",
                 sep]
        for n, v in zip(names, vals):
            lines.append(f"| {n.ljust(name_w)} | {v.rjust(val_w)} |")
        lines.append(sep)
        return "\n".join(lines)

@dataclass
class ConfirmedBar:
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vStop: Optional[float] = None
    ema9: Optional[float] = None
    uptrend: Optional[bool] = None


@dataclass
class VolatilityStopData:
    vStop: Optional[float] = None
    uptrend: Optional[bool] = None
    _series: Optional[pd.Series] = field(default=None, repr=False)
    
    @property
    def iloc(self):
        """
        Return a SeriesAccessor that provides iloc-like access.
        If _series is None, returns a special accessor that returns single values.
        """
        return SeriesAccessor(self)
    
    def update_series(self, values: List):
        """
        Update internal series with provided values
        """
        self._series = pd.Series(values)
    
    def get_values(self):
        """
        Get the current values as a tuple (vStop, uptrend)
        """
        return (self.vStop, self.uptrend)


class SeriesAccessor:
    def __init__(self, parent):
        self.parent = parent
    
    def __getitem__(self, idx):
        # If we have a series, use pandas iloc
        if self.parent._series is not None:
            return self.parent._series.iloc[idx]
        
        # Otherwise, for backwards compatibility, return the single values
        # when accessing with negative index (typical for last element)
        if idx < 0:
            return self.parent
        
        # For other indices, raise IndexError
        raise IndexError(f"Index {idx} out of bounds for single value VolatilityStopData")

class OrderRequest(BaseModel):
    ticker: Optional[str] = None
    entryPrice: Optional[float] = None
    rewardRiskRatio: Optional[float] = None
    quantity: Optional[float] = None
    riskPercentage: Optional[float] = None
    vstopAtrFactor: Optional[float] = None
    timeframe: Optional[str] = None
    kcAtrFactor: Optional[float] = None
    atrFactor: Optional[float] = None
    accountBalance: Optional[float] = None
    orderAction: Optional[str] = None
    stopType: Optional[str] = None
    submit_cmd: Optional[bool] = None
    meanReversion: Optional[bool] = None
    stopLoss: Optional[float] = None
    uptrend: Optional[bool] = None
    unixtime: Optional[int] = None
   





class TickerResponse(BaseModel):
    symbol: str
    bid: Optional[float]
    ask: Optional[float]
    last: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]
    timestamp: Optional[str]
class QueryModel(BaseModel):
    query: str


# Request cache to help filter duplicates if desired
class WebhookMetric(BaseModel):
    name: str
    value: float


class WebhookRequest(BaseModel):
    timestamp: int
    ticker: str
    currency: str
    timeframe_old: str
    clientId: int
    key: str
    contract: str
    orderRef: str
    direction: str
    metrics: list[WebhookMetric]


class TickerSub(BaseModel):
    ticker: str
    barSizeSetting: str

class TickerRefresh(BaseModel):
    ticker: str
    barSizeSetting: str
    vstopAtrFactor: float
   
# Request model for our endpoint





class TickerRequest(BaseModel):
    ticker: str



class RealTimeBarModel(BaseModel):
    time: int
    endTime: int
    open_: float = Field(..., alias="open")
    high: float
    low: float
    close: float
    volume: float
    wap: float
    count: int

    model_config = {
        "populate_by_name": True,      # allow using .open_ when validating
    }


class RealTimeBarListModel(BaseModel):
    reqId: int
    contract: Any                    # replace with your ContractModel
    barSize: int
    whatToShow: str
    useRTH: bool
    realTimeBarsOptions: List[Any]   # replace with TagValueModel
    bars: List[RealTimeBarModel]

    model_config = {
        "populate_by_name": True,
        "from_attributes": True,      # if you’re passing objects directly
    }



@dataclass
class AccountPnL:
    unrealized_pnl: float
    realized_pnl: float
    total_pnl: float

    def to_table(self) -> str:
        # build rows
        rows = [
            ("Unrealized PnL", self.unrealized_pnl),
            ("Realized   PnL", self.realized_pnl),
            ("Total      PnL", self.total_pnl),
        ]
        # format values with commas and two decimals
        vals = [f"{v:,.2f}" for _, v in rows]
        # compute column widths
        name_w = max(len(name) for name, _ in rows)
        val_w  = max(len(v)    for v in vals)
        # horizontal separator
        sep = f"+{'-'*(name_w+2)}+{'-'*(val_w+2)}+"
        # header
        lines = [
            sep,
            f"| {'Metric'.ljust(name_w)} | {'PnL'.rjust(val_w)} |",
            sep,
        ]
        # data rows
        for (name, _), v in zip(rows, vals):
            lines.append(f"| {name.ljust(name_w)} | {v.rjust(val_w)} |")
        lines.append(sep)
        return "\n".join(lines)

import asyncio
from collections import defaultdict, deque





log_config.setup()

timeframe_dict: Dict[str, str] = {}
yesterday_close: Dict[str, float] = {}
daily_true_range:    dict[str, float] = {}


wsh_dict: Dict[str, WshEventData] = defaultdict(WshEventData)

class tradingViewPostData:
    """An async-safe in-memory store, accessible app-wide."""
    def __init__(self):
        self._data: Dict[str, Dict[str, Any]] =  defaultdict(dict)
        
        self._lock = asyncio.Lock()
        self.new_price_data   = defaultdict(OrderRequest)

    async def set(self, ticker: str, value: Dict[str, Any]) -> None:
        async with self._lock:
            try:
                
                self._data[ticker] = value
                self.new_price_data[ticker] = OrderRequest(**value)  # Update new_price_data with the new value 
                timeframe_dict[ticker] = self.new_price_data[ticker].timeframe
                logger.debug(f"Setting timeframe_dict for {ticker}: {timeframe_dict[ticker]}")
                return self.new_price_data[ticker]
            except Exception as e:
                logger.error(f"Error setting data for {ticker}: {e}")
    async def get(self, ticker: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            try:
                return self._data.get(ticker)
            except Exception as e:
                logger.error(f"Error getting data for {ticker}: {e}")
                return None
    async def all(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return dict(self._data)  # shallow copy

# singleton instance
tv_store_data = tradingViewPostData()

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
            return self._data.get(symbol)

    async def all(self) -> Dict[str, float]:
        async with self._lock:
            return dict(self._data)  # shallow copy
daily_volatility = dailyVolatility()


class TickerPriceData:
    def __init__(self):
        
        self._data     = defaultdict(lambda: deque(maxlen=100))               # raw dict bars
        self.rt_bars   = defaultdict(lambda: deque(maxlen=100))               # RealTimeBar objects
        self.bars      = defaultdict(lambda: deque(maxlen=100))               # BarData objects
        self._lock     = asyncio.Lock()
        self.price_data: Dict[str, PriceSnapshot] = {}
        self.last_daily_bar_dict = defaultdict(dict)

    async def add_ticker(self, ticker: TickerDetails) -> PriceSnapshot:
        """
        Upsert a PriceSnapshot from the live Ticker event.
        Creates it if missing, then copies over all fields,
        coercing any None/NaN floats to 0.0.
        """
        logger.debug(f"Adding ticker: {ticker}")
        async with self._lock:
            sym = ticker.ticker

            # instantiate if missing
            snapshot = self.price_data.get(sym)
            if snapshot is None:
                snapshot = PriceSnapshot()
                self.price_data[sym] = snapshot
                logger.debug(f"Created new PriceSnapshot for {sym}")

            # always store the raw Ticker & barSize
            snapshot.symbol         = ticker.ticker
            snapshot.primary_exchange      = ticker.primary_exchange
            snapshot.market_cap = ticker.market_cap
            snapshot.market= ticker.market
            

            # for each float‐like field: force None/NaN → 0.0
            for attr in (
                "open","high","low","close",
                "volume"
            ):
                val = getattr(ticker, attr)
                clean = 0.0 if (val is None or isNan(val)) else val
                setattr(snapshot, attr, clean)

            # leave snapshot.uptrend alone (could be None or a prior bool)

            logger.debug(f"Updated snapshot for {snapshot}")
            return snapshot
 
   
    async def get_snapshot(self, symbol: str) -> PriceSnapshot | None:
        async with self._lock:
            return self.price_data.get(symbol)

   

    async def all_snapshots(self) -> Dict[str, PriceSnapshot]:
        async with self._lock:
            return dict(self.price_data)

    async def all_bars(self) -> Dict[str, deque]:
        async with self._lock:
            return dict(self._data)
price_data_dict = TickerPriceData()
class SubscribedContracts:
    """Async-safe in-memory cache of qualified IB Contracts."""
    def __init__(self):
        self.ib: IB = None           # ← will be set by app.py
        self._lock = asyncio.Lock()
        self._cache: Dict[str, Contract] = defaultdict(Contract)
        self._get_contract = {}

    def set_ib(self, ib_instance: IB):
        """Initialize the IB client once at startup."""
        self.ib = ib_instance

    async def get(self, symbol) -> Optional[Contract]:
        """Get (or qualify & cache) a Contract for `symbol`."""
        #logger.info(f"Getting contract for {symbol}")
        c= None
        contract=None
        conId=None
        new_symbol=None
        if isinstance(symbol, str):
            new_symbol = symbol
            logger.debug(f"Getting contract for new_symbol {new_symbol}")
            if new_symbol in self._cache:
                #print(f"2 Getting contract for {new_symbol}")
                contract = self._cache[new_symbol]
                conId = contract.conId
                logger.info(f"Getting contract for {self._cache[new_symbol]} {contract}")
                
                return contract
        else:
            
           
            for sym in symbol:
                new_symbol = sym
                
                 
            
        
        
                 
        async with self._lock:
            #print(f"1 Getting contract for {new_symbol}")
            

            if self.ib is None:
                logger.error("IB client not initialized on SubscribedContracts!")
                return None
            
            
            # build a bare‐bones Contract
            logger.debug(f"sym Found cached contract for {new_symbol}: {contract}")
            c = Contract(symbol=new_symbol, exchange="SMART", secType="STK", currency="USD")
            
            
            qualified = await self.ib.qualifyContractsAsync(c)
            if not qualified:
                logger.warning(f"Failed to qualify {new_symbol}")
                
            conId = qualified[0].conId
            await asyncio.sleep(0.2)
            logger.debug(f"Qualified contract for {qualified}: conId {conId}")
            self._get_contract[symbol] = symbol
            
            
            self._get_contract[new_symbol] = qualified[0]
            self._cache[new_symbol] = qualified[0]
           
            contract = self._cache[new_symbol]
            logger.debug(f"Qualified contract for {contract.symbol}: conId {conId}")
            return contract
        
     
        
        
    async def has(self, symbol: str) -> bool:
        if symbol in self._cache:
            return True
        else:
            
            return False
    async def delete(self, ticker: str) -> None:
        async with self._lock:
            try:
                if ticker in self._cache:
                    del self._cache[ticker]
            except Exception as e:
                logger.error(f"Error deleting data for {ticker}: {e}")

subscribed_contracts = SubscribedContracts()




def is_coming_soon(date_obj=None, check_days=0, check_next_week=True):
    """
    Check if a date is within the next X days or within next week.
    
    Parameters:
    -----------
    date_obj : datetime, arrow, str, optional
        The date to check. Can be a datetime object, Arrow object, or string.
        If None, the function will check if the provided conditions are true for any date.
    
    check_days : int, optional
        Number of days to check into the future. Default is 5.
    
    check_next_week : bool, optional
        Whether to check if the date falls in next week. Default is True.
        
    Returns:
    --------
    bool
        True if the date is within the next X days or falls within next week.
        False otherwise.
    """
    # Get current date/time
    now = arrow.now()
    
    # Convert input to Arrow object if it's not None
    if date_obj is not None:
        if isinstance(date_obj, str):
            try:
                date = arrow.get(date_obj)
            except Exception as e:
                raise ValueError(f"Could not parse date string: {e}")
        elif hasattr(date_obj, 'year'):  # Check if it's a datetime-like object
            date = arrow.get(date_obj)
        else:
            raise TypeError("date_obj must be a string, datetime, or Arrow object")
        #print(f"Checking date: {date}")
        # Ensure the date is in the future
        if date < now:
            return False
        
        # Check if date is within the next X days
        next_x_days = now.shift(days=check_days)
        if date <= next_x_days:
            return True
            
        # Check if date is within next week
        if check_next_week:
            # Calculate start and end of next week
            # Next week starts after the current week ends
            current_week_end = now.ceil('week')  # End of current week
            next_week_start = current_week_end.shift(seconds=1)  # Start of next week
            next_week_end = next_week_start.shift(weeks=1).shift(seconds=-1)  # End of next week
            
            # Check if date falls within next week
            if next_week_start <= date <= next_week_end:
                return True
        
        return False
    else:
        # If no date provided, just return current configuration
        return {"checking_next_days": check_days, "checking_next_week": check_next_week}
    


def convert_dates_for_wsh_events(events):
    """
    Process all WSH events and convert dates:
    - earnings_date: Convert from YYYYMMDD to YYYY-MM-DD
    - announce_datetime: Convert to NY timezone with date and time
    """
    tz_ny = pytz.timezone("America/New_York")
    processed_events = []
    
    for event in events:
        # Create a deep copy of the event to avoid modifying the original
        processed_event = event.copy()
        data = event.get("data", {})
        
        # Handle earnings_date - simple format conversion
        if "earnings_date" in data:
            earnings_date = data["earnings_date"]
            # Format from YYYYMMDD to YYYY-MM-DD
            if len(earnings_date) == 8:
                formatted_date = f"{earnings_date[:4]}-{earnings_date[4:6]}-{earnings_date[6:]}"
                # Store both original and formatted values
                processed_event["data"]["earnings_date_original"] = earnings_date
                processed_event["data"]["earnings_date"] = formatted_date
        
        # Handle announce_datetime - convert to NY timezone
        if "announce_datetime" in data:
            announce_dt = data["announce_datetime"]
            
            # Format: "20250513 21:12:00+0000"
            if " " in announce_dt and "+" in announce_dt:
                dt = datetime.strptime(announce_dt, "%Y%m%d %H:%M:%S%z")
            
            # Format: "20241218T050000+0000"
            elif "T" in announce_dt and "+" in announce_dt:
                dt = datetime.fromisoformat(announce_dt.replace("T", " ").replace("+0000", "+00:00"))
            else:
                # If format is unrecognized, keep original
                processed_events.append(processed_event)
                continue
                
            # Convert to NY timezone
            ny_dt = dt.astimezone(tz_ny)
            announce_real_time = ny_dt.time()
            formatted_dt = ny_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
            announce_time= ny_dt.strftime("%H:%M:%S")
            announce_date= ny_dt.strftime("%Y-%m-%d")

            
            # Store both original and formatted values
            processed_event["data"]["announce_datetime_original"] = announce_dt
            processed_event["data"]["announce_datetime"] = formatted_dt
            processed_event["data"]["announce_date"] = announce_date
            processed_event["data"]["announce_time"] = announce_time
            processed_event["data"]["announce_real_time"] = announce_real_time
        
        processed_events.append(processed_event)
    
    return processed_events