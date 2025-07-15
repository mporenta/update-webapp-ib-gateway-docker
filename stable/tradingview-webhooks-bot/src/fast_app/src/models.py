from pydantic import BaseModel, Field
from datetime import datetime
import numpy as np
from dataclasses import dataclass, field, asdict, is_dataclass
from collections import deque
import pandas as pd
from typing import Optional, Deque, List, Any, Dict
from ib_async import Ticker, RealTimeBar, BarData


 

@dataclass
class PriceSnapshot:
    yesterday_close_price: Optional[float] = None
    open:      Optional[float] = None
    high:      Optional[float] = None
    low:       Optional[float] = None
    last:      Optional[float] = None
    bid:       Optional[float] = None
    ask:       Optional[float] = None
    volume:    Optional[float] = None
    last_bar_close: Optional[float] = None
    time:      Optional[datetime] = None
    close:     Optional[float] = None

    confirmed_5s_bar: Optional[Dict[str, Any]] = None
    symbol:     Optional[Ticker] = None
    bars:       Any = None          # skip in table

    halted:     Optional[float] = None
    hasBidAsk:  Optional[bool] = None
    shortableShares: Optional[float] = None

    vStop:      Optional[float] = None
    atr:        Optional[float] = None
    vstopAtrFactor: Optional[float] = None
    uptrend:    Optional[bool] = None

    markPrice:  Optional[float] = None
    barSizeSetting: Optional[str] = None

    bar:    Optional[BarData] = None
    rt_bar: Optional[RealTimeBar] = None

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        from my_util import to_clean_dict          # late import to dodge cycles
        return to_clean_dict(self) or {}


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
class VolatilityStopData:
    vStop: Optional[float] = None
    atr: Optional[float] = None
    vstopAtrFactor: Optional[float] = None
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
        return (self.vStop, self.uptrend, self.atr, self.vstopAtrFactor)
    def get_atr(self):
        """
        Get the current values as a tuple (vStop, uptrend)
        """
        return (self.atr, self.vstopAtrFactor)


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
    symbol: Optional[str] = None
    limit_price: Optional[float] = None
    rewardRiskRatio: Optional[float] = None
    quantity: Optional[float] = None
    riskPercentage: Optional[float] = None
    vstopAtrFactor: Optional[float] = None
    barSizeSetting_tv: Optional[str] = None
    barSizeSetting_web: Optional[str] = None
    kcAtrFactor: Optional[float] = None
    atrFactor: Optional[float] = None
    accountBalance: Optional[float] = None
    orderAction: Optional[str] = None
    stopType: Optional[str] = None
    set_market_order: Optional[bool] = None
    takeProfitBool: Optional[bool] = None
    stop_loss: Optional[float] = None
    uptrend: Optional[bool] = None
    unixtime: Optional[int] = None
    notes: Optional[str] = None
   





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
    symbol: str
    currency: str
    timeframe_old: str
    clientId: int
    key: str
    contract: str
    orderRef: str
    direction: str
    metrics: list[WebhookMetric]


class TickerSub(BaseModel):
    symbol: str
    barSizeSetting: str

class TickerRefresh(BaseModel):
    symbol: str
    barSizeSetting: str
    vstopAtrFactor: float
   
# Request model for our endpoint





class TickerRequest(BaseModel):
    symbol: str




@dataclass
class AccountPnL:
    unrealizedPnL: float
    realizedPnL: float
    dailyPnL: float

    def to_table(self) -> str:
        # build rows
        rows = [
            ("Unrealized PnL", self.unrealizedPnL),
            ("Realized   PnL", self.realizedPnL),
            ("Total      PnL", self.dailyPnL),
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
