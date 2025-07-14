import asyncio
import datetime
import aiosqlite
from zoneinfo import ZoneInfo
from ib_async import IB, Stock
from ib_async.order import LimitOrder
from log_config import log_config, logger



###############################################################################
# Database handler class with composite primary key (ticker, timestamp)
# Timestamps are stored as Unix epoch in ms.
###############################################################################

class DBHandler:
    def __init__(self, db_path="market_data.db"):
        self.db_path = db_path
        self.conn = None
        self.shutting_down = False

    async def connect(self):
        logger.info(f"Connecting to database at {self.db_path}...")
        self.conn = await aiosqlite.connect(self.db_path)
        # Create the ticks table with composite primary key.
        await self.conn.execute(
            "CREATE TABLE IF NOT EXISTS ticks ("
            "ticker TEXT, "
            "timestamp INTEGER, "  # Unix epoch in ms
            "bid REAL, "
            "bidSize REAL, "
            "ask REAL, "
            "askSize REAL, "
            "high REAL, "
            "low REAL, "
            "close REAL, "
            "PRIMARY KEY (ticker, timestamp)"
            ")"
        )
        logger.info("Ticks table created or already exists.")
        # Create the bars table with composite primary key.
        await self.conn.execute(
            "CREATE TABLE IF NOT EXISTS bars ("
            "ticker TEXT, "
            "timestamp INTEGER, "  # Unix epoch in ms
            "open REAL, "          # from RealTimeBar.open_
            "high REAL, "
            "low REAL, "
            "close REAL, "
            "volume REAL, "
            "PRIMARY KEY (ticker, timestamp)"
            ")"
        )
        logger.info("Bars table created or already exists.")
        await self.conn.commit()
        logger.info("Database connection established and tables are ready.")    

    async def write_ticker(self, ticker):
        ticker_symbol = getattr(ticker.contract, "symbol", str(ticker.contract))
        now = int(datetime.datetime.utcnow().timestamp() * 1000)
        await self.conn.execute(
            "INSERT INTO ticks (ticker, timestamp, bid, bidSize, ask, askSize, high, low, close) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ticker_symbol,
                now,
                ticker.bid,
                ticker.bidSize,
                ticker.ask,
                ticker.askSize,
                ticker.high,
                ticker.low,
                ticker.close,
            ),
        )
        await self.conn.commit()

    async def write_bar(self, bar, contract):
        logger.info(f"Writing bar for contract: {contract.symbol if hasattr(contract, 'symbol') else str(contract)}")
        # Here we expect 'bar' to be an instance of RealTimeBar.
        ticker_symbol = getattr(contract, "symbol", str(contract))
        # Use the bar's own timestamp if available; otherwise fallback to current UTC time.
        if bar.time:
            ts = int(bar.time.timestamp() * 1000)
        else:
            ts = int(datetime.datetime.utcnow().timestamp() * 1000)
        # Note: the dataclass defines open_ (with underscore) for the open price.
        open_val = getattr(bar, "open_", None)
        high_val = bar.high
        low_val = bar.low
        close_val = bar.close
        volume = bar.volume
        await self.conn.execute(
            "INSERT INTO bars (ticker, timestamp, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (ticker_symbol, ts, open_val, high_val, low_val, close_val, volume),
        )
        await self.conn.commit()

    async def write_historical_bar(self, bar, contract):
        """
        Write a historical bar to the database.
        This includes automatic reconnection if the connection is closed.
        """
        try:
            # Check if connection exists before attempting to use it
            if self.conn is None or self.shutting_down:
                logger.warning(f"Cannot write historical bar: DB connection is closed or shutting down")
                try:
                    # Fix: Store the new connection
                    self.conn = await aiosqlite.connect(self.db_path)
                    logger.info(f"Reconnected to the database at {self.db_path}")
                except Exception as conn_err:
                    logger.error(f"Failed to reconnect to database: {conn_err}")
                    return  # Exit early if reconnection failed
            
            logger.info(f"Writing bar for contract.symbol: {contract.symbol}")
            logger.info(f"Writing bar for contract: {contract}")
        
            # Extract data safely with proper error handling
            ticker_symbol = getattr(contract, "symbol", str(contract))
        
            # Handle different date formats
            if hasattr(bar, 'date') and bar.date:
                ts = int(bar.date.timestamp() * 1000)
            else:
                ts = int(datetime.datetime.utcnow().timestamp() * 1000)
            
            # Get values safely with fallbacks
            open_val = getattr(bar, "open", None) or getattr(bar, "open_", None) or 0.0
            high_val = getattr(bar, "high", None) or 0.0
            low_val = getattr(bar, "low", None) or 0.0
            close_val = getattr(bar, "close", None) or 0.0
            volume = getattr(bar, "volume", 0) or 0
        
            # Use INSERT OR REPLACE to handle duplicates
            await self.conn.execute(
                "INSERT OR REPLACE INTO bars (ticker, timestamp, open, high, low, close, volume) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ticker_symbol, ts, open_val, high_val, low_val, close_val, volume),
            )
            await self.conn.commit()
        
        except Exception as e:
            logger.error(f"Error in write_historical_bar: {e}")
            # If the error is related to the database connection, mark it as None
            # so we'll attempt to reconnect on the next operation
            if "database is closed" in str(e) or "NoneType" in str(e):
                logger.warning("Database connection issue detected, will reconnect on next operation")
                self.conn = None

    async def close(self):
        if self.conn:
            try:
                self.shutting_down = True  # Set flag before closing
                await self.conn.close()
                self.conn = None
                logger.info("Database connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")