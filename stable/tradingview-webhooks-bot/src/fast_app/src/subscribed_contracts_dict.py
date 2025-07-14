# subscribed_contracts_dict.py
import asyncio
from collections import defaultdict
from typing import Any, Dict, Optional
from log_config import logger
from ib_async import Contract, IB

class SubscribedContracts:
    """Async-safe in-memory cache of qualified IB Contracts."""
    def __init__(self):
        self.ib: IB = None           # ← will be set by app.py
        self._lock = asyncio.Lock()
        self._cache: Dict[str, Contract] = defaultdict(Contract)

    def set_ib(self, ib_instance: IB):
        """Initialize the IB client once at startup."""
        self.ib = ib_instance

    async def get(self, symbol: str) -> Optional[Contract]:
        """Get (or qualify & cache) a Contract for `symbol`."""
        async with self._lock:
            if symbol in self._cache:
                return self._cache[symbol]

            if not self.ib:
                logger.error("IB client not initialized on SubscribedContracts!")
                return None

            # build a bare‐bones Contract
            c = Contract(symbol=symbol, exchange="SMART", secType="STK", currency="USD")
            logger.info(f"Qualifying contract for {symbol}…")
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

subscribed_contracts = SubscribedContracts()
