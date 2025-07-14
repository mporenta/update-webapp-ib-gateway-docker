import asyncio
from datetime import datetime, time
import math
import os

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from log_config import log_config, logger

# Import IB API components and the util module for dataframe conversion.
from ib_async import IB, Contract, util

# Define constants for IB connection.
IB_HOST = "127.0.0.1"
IB_PORT = 4002        # IB Gateway port.
CLIENT_ID = 333       # Unique client ID.

class MarketDataStreamer:
    """
    Connects to IB and streams market data for a given contract.
    Retrieves 24h historical data (converted to a pandas DataFrame),
    computes 9-period and 21-period EMAs on the close prices,
    and if the market is open subscribes to realtime tick updates.
    When realtime ticks arrive, new rows are appended to the DataFrame
    and the EMA values are updated recursively.
    If the market is closed, only historical data is used.
    """
    def __init__(self, contract):
        self.contract = contract
        self.ib = IB()
        self.ticker = None
        # DataFrame to hold both historical and realtime data.
        self.df = pd.DataFrame()  # Expected columns: date, open, high, low, close, volume, average, barCount, EMA9, EMA21
        # Last computed EMA values (for realtime updates).
        self.ema9 = None
        self.ema21 = None
        # Smoothing factors for EMA.
        self.alpha9 = 2 / (9 + 1)
        self.alpha21 = 2 / (21 + 1)
        # Connection parameters.
        self.client_id = CLIENT_ID
        self.host = IB_HOST
        self.port = IB_PORT
        self.max_attempts = 300
        self.initial_delay = 1
        self.shutting_down = False

    def is_market_open(self) -> bool:
        """
        Simple check for US market hours: Monday-Friday, 9:30am to 4:00pm Eastern.
        Adjust this as needed for your timezone.
        """
        now = datetime.now()
        if now.weekday() >= 5:  # Saturday/Sunday
            return False
        market_open = time(9, 30)
        market_close = time(16, 0)
        return market_open <= now.time() <= market_close

    async def connect(self):
        """Connect to IB with retry logic and register disconnect handling."""
        while True:
            try:
                logger.info(f"Attempting to connect to IB at {self.host}:{self.port} with clientId {self.client_id}...")
                await self._ib_connect_async()
                logger.info(f"Connected to IB at {self.host}:{self.port} with clientId {self.client_id}")
                break
            except Exception as e:
                logger.error(f"Connection failed: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
        async def on_disconnected():
            if self.shutting_down:
                return
            logger.warning("IB connection lost. Attempting to reconnect...")
            asyncio.create_task(self._reconnect())
        self.ib.disconnectedEvent += on_disconnected

    async def _ib_connect_async(self) -> bool:
        attempt = 0
        while attempt < self.max_attempts:
            try:
                logger.info(f"Connecting to IB at {self.host}:{self.port} : client_id:{self.client_id}...")
                await self.ib.connectAsync(host=self.host, port=self.port, clientId=self.client_id, timeout=40)
                if self.ib.isConnected():
                    logger.info("Connected to IB Gateway")
                    self.ib.errorEvent += self.error_code_handler
                    return True
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {e}")
            attempt += 1
            await asyncio.sleep(self.initial_delay)
        logger.error("Max reconnection attempts reached")
        return False

    async def error_code_handler(self, reqId: int, errorCode: int, errorString: str, contract: Contract):
        logger.debug(f"Error for reqId {reqId}: {errorCode} - {errorString}")

    async def fetch_historical_data(self):
        """
        Retrieve the past 24 hours of historical data using reqHistoricalDataAsync,
        convert the list of bars to a pandas DataFrame, and compute initial EMA values.
        """
        logger.info("Requesting 24h historical data for %s...", self.contract.symbol)
        try:
            bars = await self.ib.reqHistoricalDataAsync(
                self.contract,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=False,
                formatDate=2
            )
        except Exception as e:
            logger.error(f"Historical data request failed: {e}")
            raise

        if not bars:
            logger.warning("No historical data returned for the past 24h.")
            return

        # Convert bars to a DataFrame using the ib_async util.
        self.df = util.df(bars)
        # Ensure the 'date' column is parsed as datetime.
        self.df["date"] = pd.to_datetime(self.df["date"])
        logger.info("Historical data received: %d bars", len(self.df))
        self._initialize_ema_from_history()

    def _initialize_ema_from_history(self):
        """
        Compute the 9-period and 21-period EMAs on the 'close' prices using pandas.
        Store the last EMA values for realtime updates.
        """
        if self.df.empty:
            return

        self.df["EMA9"] = self.df["close"].ewm(span=9, adjust=False).mean()
        self.df["EMA21"] = self.df["close"].ewm(span=21, adjust=False).mean()

        self.ema9 = self.df["EMA9"].iloc[-1]
        self.ema21 = self.df["EMA21"].iloc[-1]

        logger.info("Initialized EMA9=%.3f, EMA21=%.3f from historical data", self.ema9, self.ema21)

    def subscribe_realtime(self):
        """
        Subscribe to realtime market data.
        Each tick updates the last EMA values and appends a new row to the DataFrame.
        """
        logger.info("Subscribing to realtime market data for %s...", self.contract.symbol)
        self.ticker = self.ib.reqMktData(self.contract, "", False, False)

        def on_tick(ticker):
            try:
                last_price = ticker.last
                if last_price is None or math.isnan(last_price):
                    return
                ts = datetime.now()
                # Avoid duplicate ticks.
                if not self.df.empty and last_price == self.df["close"].iloc[-1]:
                    return

                # Compute new EMA values using recursive EMA formula.
                new_ema9 = last_price * self.alpha9 + (self.ema9 if self.ema9 is not None else last_price) * (1 - self.alpha9)
                new_ema21 = last_price * self.alpha21 + (self.ema21 if self.ema21 is not None else last_price) * (1 - self.alpha21)
                self.ema9, self.ema21 = new_ema9, new_ema21

                # Create a new row for the tick.
                new_row = pd.DataFrame({
                    "date": [ts],
                    "open": [last_price],
                    "high": [last_price],
                    "low": [last_price],
                    "close": [last_price],
                    "volume": [None],
                    "average": [None],
                    "barCount": [None],
                    "EMA9": [new_ema9],
                    "EMA21": [new_ema21]
                })
                self.df = pd.concat([self.df, new_row], ignore_index=True)
                logger.info(f"Tick {ts.strftime('%H:%M:%S')} - Price: {last_price:.2f}, EMA9: {new_ema9:.2f}, EMA21: {new_ema21:.2f}")
                self._update_chart()
            except Exception as e:
                logger.error(f"Error in on_tick handler: {e}", exc_info=True)

        self.ticker.updateEvent += on_tick

    def setup_chart(self):
        """
        Initialize the Matplotlib chart for live updates.
        """
        plt.ion()
        self.fig, self.ax = plt.subplots(figsize=(10, 6))
        self.ax.set_title(f"Real-Time Price and EMA (9, 21) for {self.contract.symbol}")
        self.ax.set_xlabel("Time")
        self.ax.set_ylabel("Price")
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        self._update_chart(initial=True)
        plt.show()

    def _update_chart(self, initial: bool = False):
        """
        Replot the chart using data from the DataFrame.
        """
        if self.df.empty:
            return

        self.ax.clear()
        self.ax.set_title(f"Real-Time Price and EMA (9, 21) for {self.contract.symbol}")
        self.ax.set_xlabel("Time")
        self.ax.set_ylabel("Price")
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        self.ax.plot(self.df["date"], self.df["close"], label="Price", color="black")
        if "EMA9" in self.df.columns:
            self.ax.plot(self.df["date"], self.df["EMA9"], label="EMA 9", color="blue")
        if "EMA21" in self.df.columns:
            self.ax.plot(self.df["date"], self.df["EMA21"], label="EMA 21", color="red")
        self.ax.legend()
        self.ax.relim()
        self.ax.autoscale_view()
        self.fig.canvas.draw()
        plt.pause(0.001)

    async def _reconnect(self):
        """
        Attempt to reconnect to IB and re-subscribe to market data.
        """
        if self.ib.isConnected():
            return
        try:
            self.ib.disconnect()
        except Exception:
            pass
        while not self.shutting_down:
            try:
                logger.info("Reconnecting to IB...")
                await self._ib_connect_async()
                logger.info("Reconnected to IB server.")
                self.subscribe_realtime()
                break
            except Exception as e:
                logger.error(f"Reconnect attempt failed: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    async def start(self):
        """
        Start the market data streamer:
          - Connect to IB.
          - Fetch historical data and convert it to a DataFrame.
          - Compute initial EMAs.
          - Set up the chart.
          - If the market is open, subscribe to realtime data.
          - Otherwise, use only historical data.
        """
        logger.info("Starting MarketDataStreamer...")
        await self.connect()
        await self.fetch_historical_data()
        self.setup_chart()
        if self.is_market_open():
            logger.info("Market is open. Subscribing to realtime data.")
            self.subscribe_realtime()
        else:
            logger.info("Market is closed. Using only historical data.")
        await asyncio.Future()  # Run until externally cancelled.

    def stop(self):
        """
        Stop the streamer: cancel subscriptions and disconnect.
        """
        self.shutting_down = True
        try:
            if self.ticker:
                self.ib.cancelMktData(self.contract)
        except Exception as e:
            logger.warning(f"Failed to cancel market data: {e}")
        try:
            self.ib.disconnect()
        except Exception as e:
            logger.warning(f"Error during IB disconnect: {e}")
        logger.info("Disconnected from IB. Streamer stopped.")

# Example usage:
if __name__ == "__main__":
    log_config.setup()
    contract_data = {
        "symbol": "TSLA",      # Example ticker.
        "exchange": "SMART",
        "secType": "STK",
        "currency": "USD"
    }
    ib_contract = Contract(**contract_data)
    streamer = MarketDataStreamer(ib_contract)
    try:
        asyncio.run(streamer.start())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received: shutting down streamer...")
        streamer.stop()
