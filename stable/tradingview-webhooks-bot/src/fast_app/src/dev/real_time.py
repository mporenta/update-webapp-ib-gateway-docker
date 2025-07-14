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

    async def connect(self):
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
        await self.conn.commit()

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

    async def close(self):
        if self.conn:
            await self.conn.close()

###############################################################################
# IB API streamer class using async connection methods.
# It subscribes to market data and real-time bars.
###############################################################################

class IBStreamer:
    def __init__(self, client_id=1111, host="127.0.0.1", port=4002):
        self.ib = IB()
        self.client_id = client_id
        self.host = host
        self.port = port

    def register_callbacks(self, db_handler: DBHandler):
        self.ib.pendingTickersEvent += lambda tickers: self.on_pending_tickers(tickers, db_handler)
        self.ib.barUpdateEvent += lambda bars, hasNewBar: self.on_bar_update(bars, hasNewBar, db_handler)

    def on_pending_tickers(self, tickers, db_handler: DBHandler):
        for ticker in tickers:
            asyncio.create_task(db_handler.write_ticker(ticker))

    def on_bar_update(self, bars, hasNewBar, db_handler: DBHandler):
        if hasNewBar and bars and len(bars) > 0:
            # bars is a RealTimeBarList (a list of RealTimeBar)
            last_bar = bars[-1]
            asyncio.create_task(db_handler.write_bar(last_bar, bars.contract))

    async def connect_and_subscribe(self, contract):
        logger.info("Connecting to IB asynchronously...")
        await self.ib.connectAsync(self.host, self.port, self.client_id)
        qualified = await self.ib.qualifyContractsAsync(contract)
        if not qualified:
            logger.info("Failed to qualify contract!")
            return None, None
        logger.info("Subscribing to real-time market data for", contract.symbol)
        ticker = self.ib.reqMktData(contract, "", False, False)
        logger.info("Subscribing to real-time bars for", contract.symbol)
        bars = self.ib.reqRealTimeBars(contract, 5, "MIDPOINT", False)
        return ticker, bars

    
    async def graceful_disconnect(self, contract, bars):
        try:
            if self.ib.isConnected():

                self.ib.cancelMktData(contract)
                self.ib.cancelRealTimeBars(bars)
                logger.info("Unsubscribing events and disconnecting from IB...")
                self.ib.pnlEvent.clear()
                self.ib.orderStatusEvent.clear()
                self.ib.updatePortfolioEvent.clear()
                self.ib.disconnect()
                logger.info("Disconnected successfully.")
                return True
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
            return False

###############################################################################
# Async function to create a LimitOrder using the most recent tick price from DB.
###############################################################################

async def create_limit_order_from_db(db_handler: DBHandler, ticker_symbol: str, action="BUY", quantity=100):
    async with db_handler.conn.execute(
        "SELECT close FROM ticks WHERE ticker = ? ORDER BY timestamp DESC LIMIT 1",
        (ticker_symbol,),
    ) as cursor:
        row = await cursor.fetchone()
        if row is None:
            raise ValueError(f"No tick data available for ticker {ticker_symbol}.")
        limit_price = row[0]
    order = LimitOrder(action, quantity, limit_price)
    return order

###############################################################################
# Async function to aggregate raw bars into OHLC bars over a specified interval,
# aligning to the regular trading session start (09:30 America/New_York)
###############################################################################

async def aggregate_bars(db_handler: DBHandler, ticker: str, interval_sec: int) -> list:
    """
    Aggregate raw bar data from the 'bars' table into OHLC bars with the specified
    interval (in seconds), aligned to the regular trading session.
    
    The trading session is assumed to start at 09:30 Eastern Time. For each raw
    bar, we convert the timestamp to Eastern time, determine the session start (09:30),
    and compute the offset to bucket the bar.
    
    Returns:
        A list of dictionaries with keys:
          'timestamp': Start of the aggregated bar (Unix epoch in ms)
          'open': Open price (from first bar)
          'high': Highest price within the interval
          'low': Lowest price within the interval
          'close': Close price (from last bar)
          'volume': Sum of volumes within the interval
    """
    interval_ms = interval_sec * 1000
    query = "SELECT timestamp, open, high, low, close, volume FROM bars WHERE ticker = ? ORDER BY timestamp ASC"
    async with db_handler.conn.execute(query, (ticker,)) as cursor:
        rows = await cursor.fetchall()
    
    aggregated = []
    current_group = None
    current_group_key = None
    
    for row in rows:
        raw_ts, o, h, l, c, vol = row
        # Convert raw timestamp to Eastern time.
        dt = datetime.datetime.fromtimestamp(raw_ts / 1000, tz=ZoneInfo("America/New_York"))
        # Determine the trading session start (09:30 Eastern).
        session_start = dt.replace(hour=9, minute=30, second=0, microsecond=0)
        if dt < session_start:
            session_start -= datetime.timedelta(days=1)
        session_start_ms = int(session_start.timestamp() * 1000)
        offset = raw_ts - session_start_ms
        group_key = session_start_ms + (offset // interval_ms) * interval_ms

        if current_group is None or group_key != current_group_key:
            if current_group is not None:
                aggregated.append(current_group)
            current_group_key = group_key
            current_group = {
                'timestamp': group_key,
                'open': o,
                'high': h,
                'low': l,
                'close': c,
                'volume': vol if vol is not None else 0
            }
        else:
            current_group['high'] = max(current_group['high'], h)
            current_group['low'] = min(current_group['low'], l)
            current_group['close'] = c
            current_group['volume'] += vol if vol is not None else 0
    if current_group is not None:
        aggregated.append(current_group)
    return aggregated

###############################################################################
# Main async function
###############################################################################

async def main():
    db_handler = DBHandler("market_data.db")
    await db_handler.connect()

    streamer = IBStreamer(client_id=123)
    contract = Stock("AAPL", "SMART", "USD")
    streamer.register_callbacks(db_handler)

    ticker, bars = await streamer.connect_and_subscribe(contract)
    if ticker is None or bars is None:
        await db_handler.close()
        return

    logger.info("Streaming market data... Press Ctrl+C to exit.")
    

    try:
        order = await create_limit_order_from_db(db_handler, ticker_symbol=contract.symbol, action="BUY", quantity=100)
        logger.info("Created LimitOrder with limit price:", order)
    except Exception as e:
        logger.info("Error creating limit order:", e)

    agg_15s = await aggregate_bars(db_handler, ticker=contract.symbol, interval_sec=15)
    agg_1m = await aggregate_bars(db_handler, ticker=contract.symbol, interval_sec=60)

    logger.info("\nAggregated 15-second bars:")
    for bar in agg_15s:
        logger.info(bar)
    logger.info("\nAggregated 1-minute bars:")
    for bar in agg_1m:
        logger.info(bar)

    await db_handler.close()

if __name__ == "__main__":
    log_config.setup()
    asyncio.run(main())
