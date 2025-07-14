from typing import Optional, List, Dict
import asyncio
from collections import defaultdict
from log_config import log_config, logger
from ib_async import Trade, Fill, util
import pandas as pd
from tv_ticker_price_data import parent_ids


class TradeFillStore:
    """
    Async‐safe store of Trade objects (with their fills embedded),
    keyed by contract symbol (UUID).  Uses per-symbol locks for fine-grained concurrency.
    """

    def __init__(self) -> None:
        log_config.setup()
        self._store: Dict[str, Trade] = {}
        # per-symbol locks to avoid global contention
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.global_lock = asyncio.Lock()  # for operations touching multiple symbols
        self.logger = logger
        self.filled_parent_id: Dict[str, int] = defaultdict(int)

    def _get_lock(self, symbol: str) -> asyncio.Lock:
        # ensures a single lock per symbol
        return self._locks[symbol]

    async def set_trade(self, symbol: str, trade: Trade) -> None:
        """
        Register a Trade under `symbol`.  If any fills were
        added *before* the trade arrived, they’ll get re-attached here.
        """
        self.logger.debug(f"[Store] scheduling set_trade for {symbol}")
        lock = self._get_lock(symbol)
        async with lock:
            self.logger.debug(f"[Store] set_trade: {symbol} → {trade!r}")
            # attach any existing fills to new trade
            existing_trade = self._store.get(symbol)
            if existing_trade and existing_trade.fills:
                trade.fills.extend(existing_trade.fills)
            # track order parent id if provided
            parent_id = parent_ids.get(symbol)
            if parent_id == trade.order.orderId:
                self.filled_parent_id[symbol] = parent_id
                self.logger.debug(f"Updated filled_parent_id[{symbol}] = {parent_id}")
            # store the trade
            self._store[symbol] = trade
            self.logger.debug(f"[Store] stored trade for {symbol}")

    async def add_fill(self, symbol: str, fill: Fill) -> None:
        """
        Append a Fill to the Trade at `symbol`.  If the Trade
        isn't yet present, we create an empty placeholder Trade()
        so nothing is lost.
        """
        lock = self._get_lock(symbol)
        async with lock:
            trade = self._store.get(symbol)
            if trade is None:
                trade = Trade()
                self._store[symbol] = trade
                self.logger.debug(f"[Store] auto-created placeholder Trade for {symbol}")
            trade.fills.append(fill)
            self.logger.debug(f"[Store] add_fill: {symbol} → {fill}")
            # capture parent id from fill if matches
            pid = getattr(fill, 'parentId', None)
            if pid is not None:
                self.filled_parent_id[symbol] = pid
                self.logger.debug(f"Filled parent id for {symbol} set to {pid}")

    async def get_trade(self, symbol: str) -> Optional[Trade]:
        """Retrieve the Trade (with its .fills list) or None if missing."""
        lock = self._get_lock(symbol)
        async with lock:
            if symbol in self._store:
                return self._store[symbol]
            return None

    async def get_fills(self, symbol: str) -> List[Fill]:
        """Retrieve `trade.fills` for symbol, or empty list if no trade."""
        lock = self._get_lock(symbol)
        async with lock:
            trade = self._store.get(symbol)
            fills = list(trade.fills) if trade else []
            self.logger.debug(f"[Store] get_fills({symbol}) → {fills}")
            return fills

    async def delete(self, symbol: str) -> None:
        """Drop the Trade (and all fills) for `symbol`."""
        lock = self._get_lock(symbol)
        async with lock:
            self.logger.debug(f"[Store] delete({symbol})")
            self._store.pop(symbol, None)
            self.filled_parent_id.pop(symbol, None)
        # cleanup lock
        self._locks.pop(symbol, None)

    async def list_symbols(self) -> List[str]:
        """All symbols currently tracked."""
        # global lock ensures consistency
        async with self.global_lock:
            syms = list(self._store.keys())
            self.logger.debug(f"[Store] list_symbols → {syms}")
            return syms

    async def to_dataframe(self, symbol: str) -> pd.DataFrame:
        """
        Dump fills for `symbol` into a pandas.DataFrame.
        Uses ib_async.util.df under the hood.
        """
        fills = await self.get_fills(symbol)
        df = util.df(fills)
        self.logger.debug(f"[Store] to_dataframe({symbol}) → {len(df)} rows")
        return df

    async def clear(self) -> None:
        """Wipe the entire store."""
        async with self.global_lock:
            self.logger.debug("[Store] clear()")
            self._store.clear()
            self._locks.clear()
            self.filled_parent_id.clear()

    async def get_parent_id(self, symbol: str) -> Optional[int]:
        """Get the recorded parent order id for a symbol, if any."""
        lock = self._get_lock(symbol)
        async with lock:
            pid = self.filled_parent_id.get(symbol)
            self.logger.debug(f"[Store] get_parent_id({symbol}) → {pid}")
            return pid


# singleton instance
fill_data = TradeFillStore()
