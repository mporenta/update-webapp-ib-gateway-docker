# pnl.py


import asyncio, os, time, pytz
from datetime import datetime
from dataclasses import dataclass
from ib_async import (
    IB,
    Contract,
    MarketOrder,
    LimitOrder,
    PortfolioItem,
    StopOrder,
    TagValue,
    Trade,
    PnL,
    Client,
    Wrapper,
    Ticker,
    Order,
    Position,
    util,
    Fill,
)
from dotenv import load_dotenv


from typing import Dict, List

import json
import aiosqlite
from timestamps import current_millis, get_timestamp, is_market_hours

from ib_db import (
    IBDBHandler,
    order_status_handler,
    write_positions_to_db,
    clear_positions_table,
    delete_zero_positions,
    get_all_position_symbols,
)

boofMsg = os.getenv("HELLO_MSG")

order_db = IBDBHandler("orders.db")
from vol_stop_price import PriceDataNew

# Setup logging and environment
from log_config import log_config, logger

log_config.setup()


@dataclass
class AccountPnL:
    unrealized_pnl: float
    realized_pnl: float
    total_pnl: float
    timestamp: str


web_order = {
    "ticker": "MSTR",
    "entryPrice": 0,
    "rewardRiskRatio": 2,
    "quantity": 0,
    "atrFactor": 1.5,
    "vstopAtrFactor": 1.5,
    "kcAtrFactor": 2,
    "riskPercentage": 1,
    "accountBalance": 7500,
    "orderAction": "BUY",
    "stopType": "vStop",
    "meanReversion": False,
    "timeframe": "15 secs",
    "stopLoss": 0,
    "submit_cmd": True,
}
load_dotenv()


class IBManager:
    def __init__(self, ib: IB):

        # self.db_handler = db_handler  # Use the global db_handler instance
        self.ib = ib
        # self.ib_vol_data = PriceDataNew()
        self.client_id = int(os.getenv("IB_CLIENT_ID", "1"))
        self.host = os.getenv("IB_HOST", "localhost")
        self.port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
        self.risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300.00"))
        self.max_attempts = 300
        self.initial_delay = 1
        self.account = None
        self.shutting_down = False

        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        self.all_trades = {}
        self.orders_dict: Dict[str, Trade] = {}
        self.orders_dict_oca_id: Dict[str, Trade] = {}
        self.orders_dict_oca_symbol: Dict[str, Trade] = {}
        self.account_pnl: AccountPnL = None
        self.semaphore = asyncio.Semaphore(10)
        self.wrapper = Wrapper(self)
        self.client = Client(self.wrapper)
        self.active_ticker_to_dict = {}
        self.active_tickers: Dict[str, Ticker] = {}
        self.net_liquidation = {}
        self.settled_cash = {}
        self.buying_power = {}
        self.order_db = order_db

    async def connect(self) -> bool:
        attempt = 0
        while attempt < self.max_attempts:
            try:
                logger.info(
                    f"Connecting to IB boofMsg {boofMsg} at {self.host}:{self.port} : client_id:{self.client_id}...with self.risk_amount {self.risk_amount}"
                )
                await self.ib.connectAsync(
                    host=self.host, port=self.port, clientId=self.client_id, timeout=40
                )
                if self.ib.isConnected():
                    logger.info("Connected to IB Gateway")

                    logger.info("Connecting to Database.")

                    await self.order_db.connect()
                    logger.info(
                        "Order database connection established, subscribing to events."
                    )
                    await self.subscribe_events()
                    self.ib.disconnectedEvent += self.on_disconnected
                    self.ib.errorEvent += self.error_code_handler
                    # await self.ib_vol_data.process_web_order(web_order)
                    return True
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {e}")
            attempt += 1
            await asyncio.sleep(self.initial_delay)
        logger.error("Max reconnection attempts reached")
        return False

    async def subscribe_events(self):
        try:
            logger.info("Jengo getting to IB data...")
            self.account = (
                self.ib.managedAccounts()[0] if self.ib.managedAccounts() else None
            )
            if not self.account:
                return
            self.ib.reqPnL(self.account)
            logger.info(f"Jengo getting to reqPnL data... {self.account}")
            await asyncio.sleep(3)  # Allow time for PnL data to be fetched
            logger.info(f"Jengo getting to pnl data...")
            pnl = self.ib.pnl(self.account)
            logger.info(f"Jengo received to pnl data... {pnl}")
            init_values = await self.init_get_pnl_event(pnl)
            logger.info(f"Jengo received to init_values data... {init_values}")
            # Subscribe to key IB events. (Assumes event attributes exist.)
            self.ib.pnlEvent += self.on_pnl_event
            # self.ib.newOrderEvent += self.on_order_status_event
            self.ib.orderStatusEvent += self.on_order_status_event
            self.ib.updatePortfolioEvent += self.on_position_update
            self.ib.disconnectedEvent += self.on_disconnected
            # self.ib.cancelOrderEvent += self.on_canceled_order_status_event
            await self.fast_get_ib_data()
            await self.get_position_update()
            logger.info("Subscribed to IB events")
        except Exception as e:
            logger.error(f"Error subscribing to events: {e}")

    async def graceful_disconnect(self):
        try:
            self.shutting_down = True  # signal shutdown

            # Remove all event handlers
            if self.ib.isConnected():
                logger.info("Unsubscribing events and disconnecting from IB...")

                # Get all event attributes
                event_attrs = [attr for attr in dir(self.ib) if attr.endswith("Event")]

                # Clear all event handlers
                for event_attr in event_attrs:
                    try:
                        event = getattr(self.ib, event_attr)
                        if hasattr(event, "clear"):
                            event.clear()
                    except Exception as e:
                        logger.warning(f"Error clearing event {event_attr}: {e}")

                # Disconnect from IB
                self.ib.disconnect()
                logger.info("Disconnected successfully.")

            return True

        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
            return False

    async def on_disconnected(self):
        try:
            logger.warning("Disconnected from IB.")
            # Check the shutdown flag and do not attempt reconnect if shutting down.
            if self.shutting_down:
                logger.info("Shutting down; not attempting to reconnect.")
                return
            await asyncio.sleep(1)
            if not self.ib.isConnected():
                await self.connect()
        except Exception as e:

            logger.error(f"Error in on_disconnected: {e}")
            # Attempt to reconnect if not shutting down

    async def ohlc_vol(
        self, contract, durationStr, barSizeSetting, whatToShow, useRTH, return_df=False
    ):
        """
        Get historical OHLCV data from Interactive Brokers.

        Args:
            contract: The IB contract
            durationStr: Duration string for historical data (e.g., '1 D', '60 S')
            barSizeSetting: Bar size for historical data (e.g., '1 min', '5 secs')
            whatToShow: Type of data (e.g., 'TRADES', 'BID', 'ASK')
            useRTH: Whether to use regular trading hours data only
            return_df: If True, returns the raw DataFrame instead of processed values

        Returns:
            If return_df is True: DataFrame with OHLCV data
            Otherwise: Tuple of (ohlc4, hlc3, hl2, open, high, low, close, volume)
            None if no data is available
        """
        logger.info(
            f"Getting historical data for {contract.symbol} with duration {durationStr}, bar size {barSizeSetting}, and data type {whatToShow}"
        )
        # Get historical data for indicator calculations
        bars = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime="",
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow=whatToShow,
            useRTH=useRTH,
        )

        if not bars:
            logger.error(f"No historical data available for {contract.symbol}")
            return None

        # Convert bars to DataFrame
        df = util.df(bars)
        logger.info(f"Received {len(df)} bars for {contract.symbol}")
        logger.info(df)
        logger.debug(f"Retrieved {len(df)} bars for {contract.symbol}")
        logger.info(
            f"Calling write_bar for {contract.symbol}"
        )  # Log the first few rows for debugging
        last_bar = bars[-1]

        # Return raw DataFrame if requested
        if return_df:
            return df

        # Check if we have enough data
        if len(df) < 10:
            logger.warning(
                f"Not enough data to calculate OHLC averages. Need at least 10 bars, but got {len(df)}"
            )
            return None

        open_prices = df["open"]
        high = df["high"]
        low = df["low"]
        close = df["close"]
        vol = df["volume"]
        hl2 = (high + low) / 2
        hlc3 = (high + low + close) / 3
        ohlc4 = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
        logger.info(f"Calculated OHLCV data for {contract.symbol}")

        return ohlc4, hlc3, hl2, open_prices, high, low, close, vol

    async def init_get_pnl_event(self, pnl: PnL):
        """Handle aggregate PnL updates"""
        if not self.ib.isConnected():
            return
        try:
            self.account = (
                self.ib.managedAccounts()[0] if self.ib.managedAccounts() else None
            )
            if not self.account:
                return

            positions: Position = []
            open_orders_list, positions, account_values = await asyncio.gather(
                self.ib.reqAllOpenOrdersAsync(),
                self.ib.reqPositionsAsync(),
                self.ib.accountSummaryAsync(),
            )
            for trade in open_orders_list:

                self.orders_dict = {
                    order.contract.symbol: order for order in open_orders_list
                }
                symbol = trade.contract.symbol
                self.open_orders[symbol] = {
                    "symbol": symbol,
                    "order_id": trade.order.orderId,
                    "status": trade.orderStatus.status,
                    "timestamp": await get_timestamp(),
                }
            logger.info("Getting initial account values")

            if account_values:
                for account_value in account_values:
                    if account_value.tag == "NetLiquidation":
                        self.net_liquidation = float(account_value.value)
                        logger.info(
                            f"NetLiquidation updated: ${self.net_liquidation:,.2f}"
                        )
                    elif account_value.tag == "SettledCash":
                        self.settled_cash = float(account_value.value)
                        logger.info(f"SettledCash updated: ${self.settled_cash:,.2f}")
                    elif account_value.tag == "BuyingPower":
                        self.buying_power = float(account_value.value)
                        logger.info(f"BuyingPower updated: ${self.buying_power:,.2f}")

            # Store PnL data
            logger.info(f"PnL data updated pnl arg: {pnl}")
            for pnl_val in pnl:
                if pnl_val.account == self.account:
                    logger.info(
                        f"PnL data updated for account {pnl_val.account}: {pnl_val}"
                    )
                self.account_pnl = AccountPnL(
                    unrealized_pnl=float(pnl_val.unrealizedPnL or 0.0),
                    realized_pnl=float(pnl_val.realizedPnL or 0.0),
                    total_pnl=float(pnl_val.dailyPnL or 0.0),
                    timestamp=await get_timestamp(),
                )

            logger.debug(f"update pnl to self.account_pnl db: {self.account_pnl}")

            new_positions = {}
            for pos in positions:
                if pos.position != 0:
                    symbol = pos.contract.symbol
                    new_positions[symbol] = {
                        "symbol": symbol,
                        "position": pos.position,
                        "average_cost": pos.avgCost,
                        "timestamp": await get_timestamp(),
                    }

            self.current_positions = new_positions
            logger.debug(f"Positions updated: {list(self.current_positions.keys())}")
            return (
                self.account_pnl,
                self.current_positions,
                self.net_liquidation,
                self.settled_cash,
                self.buying_power,
            )

        except Exception as e:
            logger.error(f"Error updating initial account values: {str(e)}")

    async def on_position_update(self, positions):
        try:
            # self.ib.orderStatusEvent += self.on_order_status_event

            new_positions = {}
            symbol = None
            # Handle list of positions
            if isinstance(positions, list):
                for pos in positions:
                    if pos.position != 0:
                        symbol = pos.contract.symbol
                        new_positions[symbol] = {
                            "symbol": symbol,
                            "position": pos.position,
                            "market_price": getattr(pos, "marketPrice", 0.0),
                            "market_value": getattr(pos, "marketValue", 0.0),
                            "average_cost": pos.averageCost,
                            "unrealized_pnl": pos.unrealizedPNL,
                            "realized_pnl": pos.realizedPNL,
                            "account": pos.account,
                            "timestamp": await get_timestamp(),
                        }
            else:
                # Handle single position object
                if positions.position != 0:
                    symbol = positions.contract.symbol
                    new_positions[symbol] = {
                        "symbol": symbol,
                        "position": positions.position,
                        "market_price": getattr(positions, "marketPrice", 0.0),
                        "market_value": getattr(positions, "marketValue", 0.0),
                        "average_cost": positions.averageCost,
                        "unrealized_pnl": positions.unrealizedPNL,
                        "realized_pnl": positions.realizedPNL,
                        "account": positions.account,
                        "timestamp": await get_timestamp(),
                    }
                # Replace the entire current_positions with the new non-zero positions
                self.current_positions = new_positions
                # trade= await self.on_fill_update(trade=None, fill=None)
                logger.debug(
                    f"self.current_positions updated: {self.current_positions}"
                )
                return self.current_positions
        except Exception as e:
            logger.error(f"Error updating positions: {e}")
            return (
                self.current_positions
            )  # Return the last known positions in case of error

    async def get_position_update(self):
        try:
            logger.debug("Getting positions...")
            current_positions = {}

            # Get current positions from IB
            ib_positions = await self.ib.reqPositionsAsync()

            # If no positions returned from IB, clear the positions table and exit.
            if not ib_positions:
                logger.debug("No positions found in IB. Clearing positions table.")
                await clear_positions_table()
                return {}

            # Process IB positions: build dictionary of nonzero positions.
            for pos in ib_positions:
                symbol = pos.contract.symbol
                if pos.position != 0:
                    current_positions[symbol] = {
                        "symbol": symbol,
                        "position": pos.position,
                        "average_cost": pos.avgCost,
                        "account": pos.account,
                        "timestamp": await get_timestamp(),
                    }
                else:
                    logger.debug(
                        f"IB returned zero position for {symbol}; it will be removed from DB."
                    )

            # Retrieve all symbols currently stored in the positions table.
            db_symbols = await get_all_position_symbols()
            logger.debug(f"Symbols in DB: {db_symbols}")

            # For each symbol in the DB, if it’s not in current_positions (i.e. IB shows zero or missing), delete it.
            for symbol in db_symbols:
                if symbol not in current_positions:
                    await delete_zero_positions({symbol: {}})
                    logger.debug(
                        f"Deleted {symbol} from DB because IB returned a zero or no position."
                    )

            # Write/update the current nonzero positions to the database.
            await write_positions_to_db(current_positions)
            return current_positions

        except Exception as e:
            logger.error(f"Error updating positions: {e}")
            return (
                self.current_positions
            )  # Return the last known positions if error occurs.

    async def get_positions_from_db(self, db_path="orders.db"):
        """
        Read all positions from the positions table and return as a structured JSON object.

        Args:
            db_path: Path to the SQLite database file

        Returns:
            JSON string containing all positions data
        """
        try:
            # Connect to database
            conn = await aiosqlite.connect(db_path)

            # Get all positions from the table
            positions_dict = {}

            async with conn.execute("SELECT * FROM positions") as cursor:
                async for row in cursor:
                    # Assuming columns are: symbol, position, average_cost, account, timestamp
                    symbol, position, average_cost, account, timestamp = row

                    positions_dict[symbol] = {
                        "symbol": symbol,
                        "position": position,
                        "average_cost": average_cost,
                        "account": account,
                        "timestamp": timestamp,
                    }

            # Close connection
            await conn.close()

            # Structure the data
            result = {
                "positions": positions_dict,
                "count": len(positions_dict),
                "timestamp": await get_timestamp(),  # Assuming get_timestamp is available
            }

            # Convert to JSON string

            logger.debug(f"Retrieved {len(positions_dict)} positions from database")

            return result

        except Exception as e:
            logger.error(f"Error retrieving positions from database: {e}")
            # Return empty result on error
            return json.dumps(
                {"positions": {}, "count": 0, "timestamp": await get_timestamp()}
            )

    async def on_canceled_order_status_event(self, trade: Trade):
        try:
            logger.info(
                f"Order canceled status event received for {trade.contract.symbol}"
            )
            await order_status_handler(self, self.order_db, trade)

            symbol = trade.contract.symbol
            self.open_orders[symbol] = {
                "symbol": symbol,
                "order_id": trade.order.orderId,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp(),
            }
            self.orders_dict[symbol] = (
                trade  # Update the orders_dict with the latest trade
            )
            portfolio_item = []

            # portfolio_item = self.ib.portfolio()

            # Synchronous call, no await needed fill.execution.avgPrice
            if portfolio_item:
                for pos in portfolio_item:
                    logger.debug(
                        f"Position for: {pos.contract.symbol} at shares of {pos.position}"
                    )
                    if pos.contract.symbol == symbol and pos.position == 0:
                        logger.debug(
                            f"Updating position for  == 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if pos.contract.symbol == symbol and pos.position != 0:
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if pos.contract.symbol == symbol:
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
            logger.debug(
                f"Order status updated for {symbol}: {trade.orderStatus.status}"
            )
            return self.open_orders
        except Exception as e:
            logger.error(f"Error in order status event: {e}")

    async def on_order_status_event(self, trade: Trade):
        try:
            logger.debug(f"Order status event received for {trade.contract.symbol}")
            await order_status_handler(self, self.order_db, trade)

            symbol = trade.contract.symbol
            self.open_orders[symbol] = {
                "symbol": symbol,
                "order_id": trade.order.orderId,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp(),
            }
            self.orders_dict[symbol] = (
                trade  # Update the orders_dict with the latest trade
            )
            portfolio_item = []

            # portfolio_item = self.ib.portfolio()

            # Synchronous call, no await needed fill.execution.avgPrice
            if portfolio_item:
                for pos in portfolio_item:
                    logger.debug(
                        f"Position for: {pos.contract.symbol} at shares of {pos.position}"
                    )
                    if pos.contract.symbol == symbol and pos.position == 0:
                        logger.debug(
                            f"Updating position for  == 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if pos.contract.symbol == symbol and pos.position != 0:
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if pos.contract.symbol == symbol:
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
            logger.debug(
                f"Order status updated for {symbol}: {trade.orderStatus.status}"
            )
        except Exception as e:
            logger.error(f"Error in order status event: {e}")

    async def on_pnl_event(self, pnl):
        if not self.ib.isConnected():
            return
        portfolio_item = []
        trades = []
        portfolio_item = self.ib.portfolio()
        trades = await self.ib.reqAllOpenOrdersAsync()
        for trade in trades:
            self.orders_dict = {order.contract.symbol: order for order in trades}
            symbol = trade.contract.symbol
            self.open_orders[symbol] = {
                "symbol": symbol,
                "order_id": trade.order.orderId,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp(),
            }
            # Synchronous call, no await needed fill.execution.avgPrice
            if portfolio_item:
                for pos in portfolio_item:
                    logger.debug(
                        f"Position for: {pos.contract.symbol} at shares of {pos.position}"
                    )
                    if (
                        pos.contract.symbol == trade.contract.symbol
                        and pos.position == 0
                    ):
                        logger.debug(
                            f"Updating position for  == 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if (
                        pos.contract.symbol == trade.contract.symbol
                        and pos.position != 0
                    ):
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
                    if pos.contract.symbol == trade.contract.symbol:
                        logger.debug(
                            f"Updating position for  != 0 {pos.contract.symbol} with share position: {pos.position} for orderId {trade.order.orderId}"
                        )
        logger.debug(f"PnL event received: {pnl}")
        logger.debug(f"self.account_pnl event received: {self.account_pnl}")

        if pnl:
            logger.debug(f" on pnl event for jengo Received PnL data: {pnl}")
            self.account_pnl = AccountPnL(
                unrealized_pnl=float(
                    pnl.unrealizedPnL or 0.0
                ),  # Changed attribute name
                realized_pnl=float(pnl.realizedPnL or 0.0),  # Changed attribute name
                total_pnl=float(
                    pnl.dailyPnL or 0.0
                ),  # Changed attribute name if needed
                timestamp=await get_timestamp(),
            )

        positions, trades, update_pnl = await asyncio.gather(
            self.get_position_update(),
            self.ib.reqAllOpenOrdersAsync(),
            self.update_pnl(self.account_pnl),
        )

        for trade in trades:

            self.orders_dict = {order.contract.symbol: order for order in trades}

            symbol = trade.contract.symbol
            self.open_orders[symbol] = {
                "symbol": symbol,
                "order_id": trade.order.orderId,
                "status": trade.orderStatus.status,
                "timestamp": await get_timestamp(),
            }
            logger.debug(
                f"Order status updated for {symbol}: {trade.orderStatus.status}"
            )

    async def update_pnl(self, pnl: AccountPnL):
        try:

            if pnl:
                logger.debug(f"2 PnL updated: {pnl}")
                self.account_pnl = AccountPnL(
                    unrealized_pnl=float(pnl.unrealized_pnl or 0.0),
                    realized_pnl=float(pnl.realized_pnl or 0.0),
                    total_pnl=float(pnl.total_pnl or 0.0),
                    timestamp=await get_timestamp(),
                )
                pnl_calc = self.account_pnl.total_pnl - self.account_pnl.unrealized_pnl
            logger.debug(
                f"3 PnL updated: self.account_pnl.total_pnl {self.account_pnl.total_pnl} + self.unrealized_pnl {self.account_pnl.unrealized_pnl} = {pnl_calc}"
            )
            # Check risk threshold and trigger closing positions if needed.
            await self.check_pnl_threshold()
            return self.account_pnl
        except Exception as e:
            logger.error(f"Error updating PnL: {e}")

    async def check_pnl_threshold(self):

        if self.account_pnl and self.account_pnl.total_pnl <= self.risk_amount:
            logger.warning(
                f"PnL {self.account_pnl.total_pnl} below threshold {self.risk_amount}. Initiating position closure."
            )
            try:
                # Get all data in parallel for efficiency
                latest_positions, open_orders_list = await asyncio.gather(
                    self.ib.reqPositionsAsync(), self.ib.reqAllOpenOrdersAsync()
                )

                # Early exit if no positions to close
                if not latest_positions:
                    logger.info("No positions found to close.")
                    return None

                # Create dictionaries for open positions and orders
                positions_dict = {
                    pos.contract.symbol: pos
                    for pos in latest_positions
                    if pos.position != 0
                }

                # If no non-zero positions, nothing to do
                if not positions_dict:
                    logger.info("No non-zero positions to close.")
                    return None

                # Get orders dictionary (symbol -> order)
                self.orders_dict = {
                    order.contract.symbol: order for order in open_orders_list
                }

                # Get open trades
                completed_trades = self.ib.openTrades()
                trades_dict = {
                    trade.contract.symbol: trade for trade in completed_trades
                }

                # Log data for debugging
                logger.debug(
                    f"Found {len(positions_dict)} positions with non-zero quantity"
                )
                logger.debug(f"Open orders count: {len(self.orders_dict)}")
                logger.debug(f"Open trades count: {len(trades_dict)}")

                # Combine keys to get symbols that already have pending orders or trades
                pending_symbols = set(self.orders_dict.keys()) | set(trades_dict.keys())
                logger.debug(f"Symbols with pending orders/trades: {pending_symbols}")

                # Create tasks for positions that don't have pending orders
                tasks = []
                for symbol, position in positions_dict.items():
                    if symbol not in pending_symbols:
                        contract_data = {
                            "symbol": symbol,
                            "exchange": "SMART",
                            "secType": "STK",
                            "currency": "USD",
                        }
                        ib_contract = Contract(**contract_data)
                        action = "SELL" if position.position > 0 else "BUY"
                        qty = abs(position.position)
                        logger.debug(
                            f"Getting market data for {symbol} position: {position.position}"
                        )

                        # Get market data first, then place close order
                        # await self.req_mkt_data(ib_contract)
                        tasks.append(self.create_close_order(ib_contract, action, qty))

                    else:
                        logger.info(
                            f"Skipping {symbol}: already has a pending order/trade."
                        )

                # Execute all close orders simultaneously
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    # Process results to check for errors
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            logger.error(f"Error executing close order: {result}")
                    logger.info(
                        f"Close orders have been placed for {len(tasks)} positions"
                    )
                else:
                    logger.info(
                        "No positions eligible for closure; pending orders/trades exist for all positions."
                    )

            except Exception as e:
                logger.error(f"Error in check_pnl_threshold: {e}", exc_info=True)

    async def create_close_order(self, contract: Contract, action: str, qty: int):
        try:
            if not self.ib.isConnected():
                logger.info(
                    f"IB not connected in create_order, attempting to reconnect"
                )
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")

            order = None
            trade = None
            positions = None
            totalQuantity = None
            qualified_contract = None
            qualified_contract_final = None
            symbol = contract.symbol
            logger.info(
                f"Creating close order for {contract} wtih qty {qty} and action {action}"
            )
            entry_price = None
            whatToShow = "ASK" if action == "BUY" else "BID"

            # Use asyncio.gather with ohlc_vol() instead of direct reqHistoricalDataAsync

            positions, df = await asyncio.gather(
                self.ib.reqPositionsAsync(),
                self.ohlc_vol(
                    contract=contract,
                    durationStr="60 S",
                    barSizeSetting="5 secs",
                    whatToShow=whatToShow,
                    useRTH=False,
                    return_df=True,
                ),
            )

            logger.info(
                f"Async data received for {symbol} with action {action} and qty {qty}"
            )

            # Check if contract was qualified successfully
            if not contract:
                logger.error(f"Could not qualify contract for {symbol}")
                return None

            # Initialize qualified_contract BEFORE using it
            qualified_contract = contract

            for pos in positions:

                logger.info(
                    f"Checking position for: {pos.contract.symbol} and qualified_contract.symbol: {qualified_contract.symbol} with action {action} and qty {qty} and pos.position {pos.position}"
                )

                if (
                    qualified_contract.symbol == pos.contract.symbol
                    and pos.position != 0
                    and abs(pos.position) == qty
                ):
                    logger.info(
                        f"Position found for {symbol}: {pos.position} @ {pos.avgCost}"
                    )
                    totalQuantity = abs(pos.position)

                    if df is not None and not df.empty:
                        entry_price = float(df.close.iloc[-1])

                    qualified_contract_final = qualified_contract

                    if not is_market_hours():
                        logger.info(f"Afterhours limit order for {symbol}")
                        order = LimitOrder(
                            action,
                            totalQuantity,
                            lmtPrice=entry_price,
                            tif="GTC",
                            outsideRth=True,
                            orderRef=f"Close Position - {symbol}",
                        )
                    else:
                        logger.info(
                            f"Market Hours Close order for {symbol} at price {entry_price}"
                        )
                        order = MarketOrder(action, totalQuantity)

                    logger.info(
                        f"Placing order with limit order for {symbol} at price {entry_price}"
                    )

                    trade = self.ib.placeOrder(qualified_contract_final, order)
                    if trade:
                        logger.info(f"Placed order for {symbol} with action {action}")
                        trade.fillEvent += self.ib_manager.on_fill_update
                        logger.info(f"Placed close order for {symbol}")
                        order_details = {
                            "ticker": symbol,
                            "action": action,
                            "entry_price": entry_price,
                            "quantity": pos.position,
                        }
                        logger.info(self.format_order_details_table(order_details))
                        return {"order_details": order_details}
                    else:
                        logger.error(f"Failed to place order for {symbol}")
                        logger.error(
                            f"Did shares match for {symbol}? : {pos.position != 0 and pos.contract == qty}"
                        )
                        return None

        except Exception as e:
            logger.error(f"Error creating order: {e}")
            return None

    async def place_close_order(self, pos):
        try:
            logger.info(
                f"place_close_order Placing close order for {pos.contract.symbol} with position {pos.position}"
            )
            symbol = pos.contract.symbol
            # Qualify the contract first
            qualified_contracts = await self.ib.qualifyContractsAsync(pos.contract)
            if not qualified_contracts:
                logger.error(f"Could not qualify contract for {pos.contract.symbol}")
                return None

            qualified_contract = qualified_contracts[0]
            logger.info(f"place_close_order qualified_contract: {qualified_contract}")
            action = "SELL" if pos.position > 0 else "BUY"
            quantity = abs(pos.position)
            orderRef = f"close_{symbol}_{await get_timestamp()}"
            # Choose order type based on market hours.
            if is_market_hours():
                order = MarketOrder(
                    action, totalQuantity=quantity, tif="GTC", orderRef=orderRef
                )
            else:
                # For limit orders, get last price and add a small offset
                last_price = await self.ib.reqHistoricalDataAsync(
                    qualified_contract,
                    endDateTime="",
                    durationStr="300 S",
                    barSizeSetting="1 min",
                    whatToShow="BID" if action == "BUY" else "ASK",
                    useRTH=False,
                )
                if not last_price:
                    logger.error(f"Price data unavailable for {symbol}")
                    return
                tick_offset = -0.01 if action == "SELL" else 0.01
                limit_price = round(last_price[-1].close + tick_offset, 2)
                order = LimitOrder(
                    action,
                    totalQuantity=quantity,
                    lmtPrice=limit_price,
                    tif="GTC",
                    outsideRth=True,
                    orderRef=orderRef,
                )
            trade = self.ib.placeOrder(qualified_contract, order)
            if trade:
                logger.info(f"Placed close order for {symbol}")
                trade.fillEvent += self.on_fill_update
                return trade
        except Exception as e:
            logger.error(f"Error placing close order for {symbol}: {e}")

    async def place_order(
        self, contract: Contract, order, direction: str, price: float = 0.0
    ) -> Trade:
        async with self.semaphore:
            try:

                qualified = await self.ib.qualifyContractsAsync(contract)
                if not qualified:
                    logger.error(f"Contract qualification failed for {contract.symbol}")
                    return None
                qualified_contract = qualified[0]
                if direction == "strategy.close_all":
                    trade = await self.place_close_order_from_contract(
                        qualified_contract, order
                    )

                if direction != "strategy.close_all":
                    trade = self.ib.placeOrder(qualified_contract, order)
                if trade:
                    logger.info(
                        f"Order placed for place_order {contract.symbol} at price {price} and order type {order}"
                    )
                    trade.fillEvent += self.on_fill_update

                    return trade
            except Exception as e:
                logger.error(f"Error placing order for {contract.symbol}: {e}")
                return None

    async def place_web_bracket_market_order(
        self,
        contract: Contract,
        order: Trade,
        action: str,
        exit_stop: float,
        exit_limit: float,
    ):
        """
        Places bracket order with a MarketOrder entry during market hours and a LimitOrder outside market hours.
        trade = await ib_manager.place_bracket_market_order(ib_contract, order, direction, action, exit_stop or 0.0, exit_limit or 0.0)

        """
        async with self.semaphore:
            try:
                logger.info(f"jengo bracket order webhook tv_hook {contract.symbol}")

                # Decide whether we're going long or short
                # action = "BUY" if direction == "strategy.entrylong" else "SELL"
                reverseAction = "BUY" if action == "SELL" else "SELL"
                qualified_contracts = await self.ib.qualifyContractsAsync(contract)
                if not qualified_contracts:
                    logger.error(f"Could not qualify contract for {contract.symbol}")
                    return None

                qualified_contract = qualified_contracts[0]

                p_ord_id = order.orderId
                logger.info(f"jengo bracket order webhook p_ord_id: {p_ord_id}")

                # Determine the type of parent order based on market hours

                logger.info(f"Market hours - creating MarketOrder for{contract.symbol}")
                parent = order
                logger.info(f"Parent Order - creating MarketOrder for{parent}")

                # Place the orders

                trade = self.ib.placeOrder(qualified_contract, parent)
                if trade:
                    logger.info(
                        f"Order placed for {qualified_contract.symbol}: {parent}"
                    )

                else:
                    logger.error(
                        f"Order placement failed for {contract.symbol}: {order}"
                    )

                return trade
            except Exception as e:
                logger.error(
                    f"Error processing bracket order for {contract.symbol}: {str(e)}",
                    exc_info=True,
                )
                return False

    async def place_bracket_market_order(
        self,
        contract: Contract,
        order: Trade,
        action: str,
        exit_stop: float,
        exit_limit: float,
    ):
        """
        Places bracket order with a MarketOrder entry during market hours and a LimitOrder outside market hours.
        trade = await ib_manager.place_bracket_market_order(ib_contract, order, direction, action, exit_stop or 0.0, exit_limit or 0.0)

        """
        async with self.semaphore:
            try:
                logger.info(f"jengo bracket order webhook tv_hook {contract.symbol}")

                # Decide whether we're going long or short
                # action = "BUY" if direction == "strategy.entrylong" else "SELL"
                reverseAction = "BUY" if action == "SELL" else "SELL"

                logger.info(f"jengo bracket order webhook tv_hook {contract.symbol}")

                # Qualify the contract first
                qualified_contracts = await self.ib.qualifyContractsAsync(contract)
                if not qualified_contracts:
                    logger.error(f"Could not qualify contract for {contract.symbol}")
                    return None

                qualified_contract = qualified_contracts[0]
                logger.info(
                    f"jengo bracket order webhook qualified_contract: {qualified_contract.symbol}"
                )

                p_ord_id = order.orderId
                logger.info(f"jengo bracket order webhook p_ord_id: {p_ord_id}")

                # Determine the type of parent order based on market hours

                logger.info(f"Market hours - creating MarketOrder for{contract.symbol}")
                parent = order
                logger.info(f"Parent Order - creating MarketOrder for{parent}")

                # Create the take profit order
                takeProfit = LimitOrder(
                    reverseAction,
                    order.totalQuantity,
                    exit_limit,
                    parentId=p_ord_id,
                    tif=order.tif,
                    orderRef=parent.orderRef,
                    transmit=False,
                )
                logger.info(f"jengo bracket order webhook takeProfit: {takeProfit}")

                # Create the stop loss order
                stopLoss = StopOrder(
                    action=reverseAction,
                    totalQuantity=order.totalQuantity,
                    stopPrice=exit_stop,
                    parentId=p_ord_id,
                    tif=order.tif,
                    orderRef=parent.orderRef,
                    transmit=True,
                )
                logger.info(f"jengo bracket order webhook stopLoss: {stopLoss}")

                # Place the orders
                for o in [parent, takeProfit, stopLoss]:
                    trade = self.ib.placeOrder(qualified_contract, o)
                    if trade:
                        logger.info(f"Order placed for {contract.symbol}: {o}")
                        trade.fillEvent += self.on_fill_update

                    else:
                        logger.error(
                            f"Order placement failed for {contract.symbol}: {o}"
                        )

                return trade
            except Exception as e:
                logger.error(
                    f"Error processing bracket order for {contract.symbol}: {str(e)}",
                    exc_info=True,
                )
                return False

    async def place_close_order_from_contract(self, contract: Contract, order):
        # Similar to place_close_order but when only a contract is given.
        positions = await self.ib.reqPositionsAsync()
        logger.info(
            f"jengo close_all webhook tv_hook {contract.symbol} and checking current positions {positions}"
        )
        for pos in positions:
            quantity = abs(pos.position)
            if pos.contract.symbol == contract.symbol and pos.position != 0:
                logger.info(f"Placed close_all order for {contract.symbol}")
                return await self.place_close_order(pos)

        logger.info(f"No active position found for {contract.symbol}")
        return None

    async def get_req_id(self):
        try:
            return self.ib.client.getReqId()
        except Exception as e:
            logger.error(f"Error getting request ID: {e}")
            return None

    async def fast_get_ib_data(self) -> dict:
        """Return current positions, pnl, orders and trades for the webapp."""
        try:
            if not self.ib.isConnected():
                return {
                    "positions": [],
                    "pnl": [
                        {
                            "unrealized_pnl": 0.0,
                            "realized_pnl": 0.0,
                            "total_pnl": 0.0,
                            "net_liquidation": 0.0,
                        }
                    ],
                }

            orders, trades, account_values = await asyncio.gather(
                self.ib.reqAllOpenOrdersAsync(),
                self.ib.reqCompletedOrdersAsync(False),
                self.ib.accountSummaryAsync(),
            )

            # Convert Order object to dict
            def order_to_dict(order) -> dict:
                return {
                    "contract": {
                        "symbol": order.contract.symbol,
                        "conId": order.contract.conId,
                        "secType": order.contract.secType,
                        "exchange": order.contract.exchange,
                        "primaryExchange": getattr(
                            order.contract, "primaryExchange", None
                        ),
                        "currency": order.contract.currency,
                    },
                    "order": {
                        "orderId": order.order.orderId,
                        "clientId": order.order.clientId,
                        "permId": order.order.permId,
                        "action": order.order.action,
                        "totalQuantity": order.order.totalQuantity,
                        "lmtPrice": order.order.lmtPrice,
                        "auxPrice": order.order.auxPrice,
                        "tif": order.order.tif,
                        "outsideRth": order.order.outsideRth,
                    },
                    "orderStatus": {
                        "status": order.orderStatus.status,
                        "filled": order.orderStatus.filled,
                        "remaining": order.orderStatus.remaining,
                        "avgFillPrice": order.orderStatus.avgFillPrice,
                        "permId": order.orderStatus.permId,
                        "parentId": order.orderStatus.parentId,
                        "lastFillPrice": order.orderStatus.lastFillPrice,
                        "clientId": order.orderStatus.clientId,
                        "whyHeld": order.orderStatus.whyHeld,
                        "mktCapPrice": order.orderStatus.mktCapPrice,
                    },
                }

            # Convert Trade object to dict
            def trade_to_dict(trade) -> dict:
                return {
                    "contract": {
                        "symbol": trade.contract.symbol,
                        "conId": trade.contract.conId,
                        "secType": trade.contract.secType,
                        "exchange": trade.contract.exchange,
                        "primaryExchange": getattr(
                            trade.contract, "primaryExchange", None
                        ),
                        "currency": trade.contract.currency,
                    },
                    "order": {
                        "orderId": trade.order.orderId,
                        "clientId": trade.order.clientId,
                        "permId": trade.order.permId,
                        "action": trade.order.action,
                        "totalQuantity": trade.order.totalQuantity,
                        "lmtPrice": trade.order.lmtPrice,
                        "auxPrice": trade.order.auxPrice,
                        "tif": trade.order.tif,
                        "outsideRth": trade.order.outsideRth,
                    },
                    "orderStatus": {
                        "status": trade.orderStatus.status,
                        "filled": trade.orderStatus.filled,
                        "remaining": trade.orderStatus.remaining,
                        "avgFillPrice": trade.orderStatus.avgFillPrice,
                        "permId": trade.orderStatus.permId,
                        "parentId": trade.orderStatus.parentId,
                        "lastFillPrice": trade.orderStatus.lastFillPrice,
                        "clientId": trade.orderStatus.clientId,
                        "whyHeld": trade.orderStatus.whyHeld,
                        "mktCapPrice": trade.orderStatus.mktCapPrice,
                    },
                    "fills": [str(fill) for fill in trade.fills],
                    "log": [
                        {
                            "time": (
                                log_entry.time.isoformat() if log_entry.time else None
                            ),
                            "status": log_entry.status,
                            "message": log_entry.message,
                            "errorCode": log_entry.errorCode,
                        }
                        for log_entry in trade.log
                    ],
                    "advancedError": trade.advancedError,
                }

            # Build dictionaries for orders and trades.
            self.orders_dict = {order.contract.symbol: order for order in orders}
            self.all_trades = {trade.contract.symbol: trade for trade in trades}
            orders_list = [order_to_dict(o) for o in self.orders_dict.values()]
            trades_list = [trade_to_dict(t) for t in self.all_trades.values()]

            # Convert positions to simple dicts
            positions = self.ib.portfolio()
            current_ts = await get_timestamp()
            pos_list = [
                {
                    "symbol": pos.contract.symbol,
                    "position": float(pos.position),
                    "market_price": float(getattr(pos, "marketPrice", 0.0)),
                    "market_value": float(getattr(pos, "marketValue", 0.0)),
                    "average_cost": float(pos.averageCost or 0),
                    "unrealized_pnl": float(getattr(pos, "unrealizedPNL", 0.0)),
                    "realized_pnl": float(getattr(pos, "realizedPNL", 0.0)),
                    "account": pos.account,
                    "timestamp": current_ts,
                }
                for pos in positions
                if pos.position != 0
            ]

            pnl_info = {
                "unrealized_pnl": (
                    self.account_pnl.unrealized_pnl if self.account_pnl else 0.0
                ),
                "realized_pnl": (
                    self.account_pnl.realized_pnl if self.account_pnl else 0.0
                ),
                "total_pnl": self.account_pnl.total_pnl if self.account_pnl else 0.0,
                "net_liquidation": next(
                    (
                        float(val.value)
                        for val in account_values
                        if val.tag == "NetLiquidation" and val.currency == "USD"
                    ),
                    0.0,
                ),
            }
            return {
                "positions": pos_list,
                "pnl": [pnl_info],
                "orders": orders_list,
                "trades": trades_list,
            }
        except Exception as e:
            logger.error(f"Error in fast_get_ib_data: {e}")
            return {"positions": [], "pnl": [], "orders": [], "trades": []}

    async def on_fill_update(self, trade: Trade, fill):
        try:
            if trade is None:
                logger.debug("Trade is None in on_fill_update, skipping update.")
                return None
            if fill is None:
                logger.debug(f"Fill is None for trade {trade}")
                orders = await self.ib.reqAllOpenOrdersAsync()
                if orders:
                    self.orders_dict = {
                        order.contract.symbol: order for order in orders
                    }
                    symbol = trade.contract.symbol  # Safe since trade is not None
                    self.open_orders[symbol] = {
                        "symbol": symbol,
                        "order_id": trade.order.orderId,
                        "status": trade.orderStatus.status,
                        "timestamp": await get_timestamp(),
                    }
                return None
            if fill:
                logger.info(
                    f"Fill received for fill {fill.contract.symbol}. jengo Updating positions..."
                )

        except Exception as e:
            logger.error(f"Error updating positions on fill: {e}")

    async def unsub_mkt_data(self, contract: Contract):
        try:
            logger.info(f"Canceling market data ... {contract.symbol}")
            # Use the original contract instance if available, otherwise qualify again (not recommended)
            target_contract = (await self.ib.qualifyContractsAsync(contract))[0]
            self.ib.cancelMktData(target_contract)
            logger.info(
                f"Market data subscription for {target_contract.symbol} canceled"
            )
            del self.active_tickers[target_contract.symbol]

            return {"symbol": target_contract.symbol}
        except Exception as e:
            logger.error(f"Error canceling market data: {e}")

    async def get_active_tickers(self) -> list[dict]:
        try:
            # Use the ib.tickers() method to get all active ticker objects
            tickers_list = self.ib.tickers()
            logger.debug(f"tickers_list tickers: {tickers_list}")
            self.active_tickers = {
                ticker.contract.symbol: ticker for ticker in tickers_list
            }
            logger.debug(f"Active tickers: {self.active_tickers}")
            tckDict = await self.ticker_to_dict(self.active_tickers)
            for t in tckDict:
                logger.debug(f"Ticker data: {t}")
            # Convert each ticker to a dictionary for JSON serialization
            return [await self.ticker_to_dict(t) for t in self.active_tickers.values()]
        except Exception as e:
            logger.error(f"Error getting active tickers: {e}")
            return []

    async def ticker_to_dict(self, ticker) -> dict:
        try:
            logger.debug(f"Converting ticker to dict: {ticker}")

            def format_datetime(dt):
                if dt is None:
                    return None
                if isinstance(dt, datetime):
                    return dt.isoformat()
                return str(dt)

            # If ticker is already a dict, simply return it (or reformat if needed)
            if isinstance(ticker, dict):
                logger.warning("Ticker is already a dict; returning formatted version.")
                # Optionally reformat its time field if present.
                if "time" in ticker:
                    ticker["time"] = format_datetime(ticker["time"])
                return ticker

            # Ensure ticker has a contract attribute
            if not hasattr(ticker, "contract"):
                logger.warning("Ticker does not have a contract attribute.")
                return {}

            # Format the time attribute
            timestamp = format_datetime(getattr(ticker, "time", None))

            # Convert ticks if available
            tick_list = []
            if hasattr(ticker, "ticks") and ticker.ticks:
                logger.info(
                    f"Converting {len(ticker.ticks)} ticks for {ticker.contract.symbol}"
                )
                for tick in ticker.ticks:
                    tick_list.append(
                        {
                            "time": format_datetime(getattr(tick, "time", None)),
                            "tickType": getattr(tick, "tickType", None),
                            "price": getattr(tick, "price", None),
                            "size": getattr(tick, "size", None),
                        }
                    )
                    logger.info(f"Tick data: {tick_list[-1]}")

            result = {
                "conId": (
                    ticker.contract.conId if hasattr(ticker.contract, "conId") else None
                ),
                "symbol": (
                    ticker.contract.symbol
                    if hasattr(ticker.contract, "symbol")
                    else None
                ),
                "secType": (
                    ticker.contract.secType
                    if hasattr(ticker.contract, "secType")
                    else None
                ),
                "exchange": (
                    ticker.contract.exchange
                    if hasattr(ticker.contract, "exchange")
                    else None
                ),
                "primaryExchange": getattr(ticker.contract, "primaryExchange", None),
                "currency": (
                    ticker.contract.currency
                    if hasattr(ticker.contract, "currency")
                    else None
                ),
                "localSymbol": (
                    ticker.contract.localSymbol
                    if hasattr(ticker.contract, "localSymbol")
                    else None
                ),
                "tradingClass": (
                    ticker.contract.tradingClass
                    if hasattr(ticker.contract, "tradingClass")
                    else None
                ),
                "time": timestamp,
                "minTick": getattr(ticker, "minTick", None),
                "bid": getattr(ticker, "bid", None),
                "bidSize": getattr(ticker, "bidSize", None),
                "bidExchange": getattr(ticker, "bidExchange", None),
                "ask": getattr(ticker, "ask", None),
                "askSize": getattr(ticker, "askSize", None),
                "askExchange": getattr(ticker, "askExchange", None),
                "last": getattr(ticker, "last", None),
                "lastSize": getattr(ticker, "lastSize", None),
                "lastExchange": getattr(ticker, "lastExchange", None),
                "prevBidSize": getattr(ticker, "prevBidSize", None),
                "prevAsk": getattr(ticker, "prevAsk", None),
                "prevLastSize": getattr(ticker, "prevLastSize", None),
                "volume": getattr(ticker, "volume", None),
                "close": getattr(ticker, "close", None),
                "ticks": tick_list,
                "bboExchange": getattr(ticker, "bboExchange", None),
                "snapshotPermissions": getattr(ticker, "snapshotPermissions", None),
            }
            logger.debug(f"Converted ticker to dict: {result}")
            return result
        except Exception as e:
            logger.error(f"Error converting ticker to dict: {e}")
            return {}

    async def app_pnl_event(self):
        portfolio = self.ib.portfolio()

        # Convert each PortfolioItem to a dict
        portfolio_data = [
            {
                "symbol": item.contract.symbol,
                "secType": item.contract.secType,
                "exchange": item.contract.exchange,
                "currency": item.contract.currency,
                "position": item.position,
                "marketPrice": item.marketPrice,
                "marketValue": item.marketValue,
                "averageCost": item.averageCost,
                "unrealizedPNL": item.unrealizedPNL,
                "realizedPNL": item.realizedPNL,
                "account": item.account,
            }
            for item in portfolio
        ]

        logger.info(f"Current portfolio_data: {portfolio_data}")

        orders, close_trades = await asyncio.gather(
            self.ib.reqAllOpenOrdersAsync(),
            self.ib.reqCompletedOrdersAsync(False),
        )
        open_orders_event = await self.open_orders_event(orders)
        logger.info(f"Open orders updated: {open_orders_event}")

        pnl = self.ib.pnl(self.account)
        logger.info(f"PnL data: {pnl}")

        for pnl_val in pnl:
            if pnl_val.account == self.account:
                logger.debug(
                    f"PnL data updated for account {pnl_val.account}: {pnl_val}"
                )
            self.account_pnl = AccountPnL(
                unrealized_pnl=float(pnl_val.unrealizedPnL or 0.0),
                realized_pnl=float(pnl_val.realizedPnL or 0.0),
                total_pnl=float(pnl_val.dailyPnL or 0.0),
                timestamp=await get_timestamp(),
            )

        return orders, portfolio_data, self.account_pnl, close_trades

    async def open_orders_event(self, orders: Trade):
        try:
            logger.debug("Updating open orders...")
            # In the app_pnl_event method
            if self.client_id == 1111:
                for order in orders:
                    symbol = order.contract.symbol
                    if (
                        symbol in self.open_orders
                        and self.open_orders[symbol]["order_id"] != order.order.orderId
                    ):
                        logger.debug(f"Stale order detected for {symbol}, updating...")
                        del self.open_orders[symbol]  # Remove stale order entry
                    elif symbol not in self.open_orders:
                        # Initialize the entry for new symbols
                        logger.debug(
                            f"New order detected for {symbol}, adding to tracking..."
                        )
                        self.open_orders[symbol] = {
                            "symbol": symbol,
                            "order_id": order.order.orderId,
                            "status": order.orderStatus.status,
                            "timestamp": await get_timestamp(),
                        }

            return self.open_orders  # Return the updated open orders dictionary
        except Exception as e:

            logger.error(f"Error in open_orders_event: {e}")
            return self.open_orders

    async def qualify_contract(self, contract: Contract):
        qualified = await self.ib.qualifyContractsAsync(contract)
        if not qualified:
            logger.error(f"Contract qualification failed for {contract.symbol}")
            return None
        # Use the qualified contract instance directly
        qualified_contract = qualified[0]
        logger.info(
            f"Qualified contract for: {qualified_contract.symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}"
        )
        return qualified_contract

    async def error_code_handler(
        self, reqId: int, errorCode: int, errorString: str, contract: Contract
    ):
        logger.warning(f"Error for reqId {reqId}: {errorCode} - {errorString}")
