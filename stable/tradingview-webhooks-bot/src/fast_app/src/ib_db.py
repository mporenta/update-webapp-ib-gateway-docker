# ib_db.py
import aiosqlite
import asyncio
from pydantic import ValidationError, BaseModel
import os
from collections import defaultdict
import pytz
import math
from datetime import datetime, date, time, timezone
from typing import Dict, List, Optional, Union, Any, Tuple
import json
from dataclasses import asdict
from ib_async import Trade, Order, OrderStatus, Contract, IB
from ib_async.objects import RealTimeBar, Position, PortfolioItem
from log_config import logger, log_config
from timestamps import current_millis, get_timestamp, is_market_hours
from models import OrderRequest, PriceSnapshot
from aiosqlite import Connection
import pandas as pd
from pandas_ta.volatility import true_range

log_config.setup()
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "orders.db")

daily_open:     dict[str, float] = {}
daily_high:     dict[str, float] = {}
daily_low:      dict[str, float] = {}
daily_true_range:    dict[str, float] = {}
daily_volatility:    dict[str, float] = {}

class IBDBHandler:
    """
    Database handler for storing and retrieving orders, ib_positions, and PnL from Interactive Brokers.
    Uses aiosqlite for async database operations.
    """

    def __init__(self, db_path: str = DB_PATH):
        """
        Initialize the IBDBHandler.

        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = DB_PATH
        self.conn: Optional[aiosqlite.Connection] = None
        self.lock = asyncio.Lock()
        self.shutting_down = False
        self.ib: Optional[IB] = None

    def set_ib(self, ib_instance: IB):
        """Initialize the IB client once at startup."""
        self.ib = ib_instance

    async def connect(self) -> None:
        """
        Connect to the SQLite database.
        """
        try:
            logger.info(f"Connecting to orders database at {self.db_path}")
            self.conn = await aiosqlite.connect(self.db_path)
           
            # Enable foreign keys constraint
            await self.conn.execute("PRAGMA foreign_keys = ON")
            await self.conn.commit()
            await self.create_tables()
            logger.debug("Successfully connected to orders database")
        except Exception as e:
            logger.error(f"Error connecting to orders database: {e}")
            raise


    async def create_tables(self) -> None:
        """
        Create necessary tables in the database if they don't exist.
        """
        try:
            async with self.lock:
                # Create orders table
                await self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orders (
                        order_id INTEGER PRIMARY KEY,
                        client_id INTEGER,
                        perm_id INTEGER,
                        symbol TEXT,
                        sec_type TEXT,
                        exchange TEXT,
                        action TEXT,
                        order_type TEXT,
                        total_quantity REAL,
                        lmt_price REAL,
                        aux_price REAL,
                        tif TEXT,
                        outside_rth INTEGER,
                        order_ref TEXT,
                        parent_id INTEGER,
                        transmit INTEGER,
                        created_at TEXT,
                        updated_at TEXT,
                        status TEXT,
                        filled REAL,
                        remaining REAL,
                        avg_fill_price REAL,
                        last_fill_price REAL,
                        why_held TEXT,
                        account TEXT,
                        contract_details TEXT,
                        order_details TEXT,
                        order_status_details TEXT
                    )
                    """
                )

              
                await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS ib_positions (
                    symbol TEXT PRIMARY KEY,
                    secType TEXT,
                    conId INTEGER,
                    exchange TEXT,
                    currency TEXT,
                    localSymbol TEXT,
                    primaryExchange TEXT,
                    tradingClass TEXT,
                    position REAL NOT NULL,
                    averageCost REAL,
                    account TEXT,
                    timestamp TEXT NOT NULL
                )""")

              

                # Create PnL history table (persistent)
                await self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pnl_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        req_id INTEGER,
                        daily_pnl REAL,
                        unrealized_pnl REAL,
                        realized_pnl REAL,
                        timestamp TEXT
                    )
                    """
                )

                # Create fills table
                await self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fills (
                        fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id INTEGER,
                        execution_id TEXT,
                        symbol TEXT,
                        sec_type TEXT,
                        side TEXT,
                        shares REAL,
                        price REAL,
                        time TEXT,
                        exchange TEXT,
                        commission REAL,
                        commission_currency TEXT,
                        realized_pnl REAL,
                        FOREIGN KEY (order_id) REFERENCES orders(order_id)
                    )
                    """
                )

                # Create trades table
                await self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trades (
                        conId INTEGER,
                        symbol TEXT,
                        orderId INTEGER,
                        exchange TEXT,
                        currency TEXT,
                        localSymbol TEXT,
                        tradingClass TEXT,
                        action TEXT,
                        totalQuantity REAL,
                        orderType TEXT,
                        lmtPrice REAL,
                        auxPrice REAL,
                        tif TEXT,
                        ocaType INTEGER,
                        ocaGroup TEXT,
                        orderRef TEXT,
                        displaySize INTEGER,
                        outsideRth BOOLEAN,
                        trailStopPrice REAL,
                        volatilityType INTEGER,
                        deltaNeutralOrderType TEXT,
                        referencePriceType INTEGER,
                        account TEXT,
                        clearingIntent TEXT,
                        adjustedOrderType TEXT,
                        cashQty REAL,
                        dontUseAutoPriceForHedge BOOLEAN,
                        usePriceMgmtAlgo BOOLEAN,
                        minCompeteSize INTEGER,
                        competeAgainstBestOffset REAL,
                        status TEXT,
                        filled REAL,
                        remaining REAL,
                        avgFillPrice REAL,
                        permId INTEGER,
                        parentId INTEGER,
                        lastFillPrice REAL,
                        clientId INTEGER,
                        whyHeld TEXT,
                        mktCapPrice REAL,
                        log_time TEXT,
                        log_status TEXT,
                        log_message TEXT,
                        log_errorCode INTEGER,
                        advancedError TEXT,
                        PRIMARY KEY (orderId, symbol)
                    )
                    """
                )

                # Create order_logs table
                await self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS order_logs (
                        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id INTEGER,
                        time TEXT,
                        status TEXT,
                        message TEXT,
                        error_code INTEGER,
                        FOREIGN KEY (order_id) REFERENCES orders(order_id)
                    )
                    """
                )

                # Create web_orders table
                await self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS web_orders (
                        ticker TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        entry_price REAL,
                        reward_risk_ratio REAL,
                        quantity REAL,
                        risk_percentage REAL,
                        vstop_atr_factor REAL,
                        timeframe TEXT,
                        kc_atr_factor REAL,
                        atr_factor REAL,
                        account_balance REAL,
                        order_action TEXT,
                        stop_type TEXT,
                        set_market_order BOOLEAN,
                        takeProfitBool INTEGER,
                        stop_loss REAL,
                        PRIMARY KEY (ticker, timestamp)
                    )
                    """
                )

                # Create realtime_bars table
                await self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS realtime_bars (
                        symbol    TEXT,
                        bar_size  INTEGER,
                        ts        INTEGER,
                        open      REAL,
                        high      REAL,
                        low       REAL,
                        close     REAL,
                        volume    REAL,
                        PRIMARY KEY(symbol, bar_size, ts)
                    )
                    """
                )
                
                # portfolio‐item snapshots
                await self.conn.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_items_table (
                    symbol TEXT PRIMARY KEY,
                    secType TEXT,
                    conId INTEGER,
                    exchange TEXT,
                    currency TEXT,
                    localSymbol TEXT,
                    primaryExchange TEXT,
                    tradingClass TEXT,
                    position REAL NOT NULL,
                    averageCost REAL,
                    marketPrice REAL,
                    marketValue REAL,
                    unrealizedPNL REAL,
                    realizedPNL REAL,
                    account TEXT,
                    timestamp TEXT NOT NULL
                )""")
                

               
        
            
               

                await self.conn.commit()
                logger.debug("Database tables created or confirmed")
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
            raise

   
     
    

    async def insert_realtime_bar(self, symbol: str, bar: dict, bar_size: int):
        ts = int(bar["time"].timestamp())
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                INSERT OR REPLACE INTO realtime_bars
                  (symbol, bar_size, ts, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol, bar_size, ts,
                    bar["open"], bar["high"], bar["low"],
                    bar["close"], bar["volume"]
                )
            )
            await conn.commit()

    async def insert_realtime_bars_batch(self, batch_params):
        """Insert multiple realtime bars in a single transaction."""
        async with self.lock:
            await self.conn.executemany(
                """
                INSERT OR REPLACE INTO realtime_bars
                  (symbol, bar_size, ts, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch_params
            )
            await self.conn.commit()

    async def fetch_realtime_bars(self, symbol: str, bar_size: int, limit: int):
        async with aiosqlite.connect(self.db_path) as conn:
            cursor = await conn.execute(
                """
                SELECT ts, open, high, low, close, volume
                  FROM realtime_bars
                 WHERE symbol = ? AND bar_size = ?
              ORDER BY ts DESC
                 LIMIT ?
                """,
                (symbol, bar_size, limit)
            )
            rows = await cursor.fetchall()

        bars = [
            RealTimeBar(
                time=datetime.fromtimestamp(ts),
                open_=o, high=h, low=l, close=c, volume=v
            )
            for ts, o, h, l, c, v in rows
        ]
        return list(reversed(bars))

    async def upsert_web_order(self, data: dict) -> None:
        """
        Insert or replace a web-submitted order request into the web_orders table.
        """
        try:
            if not self.conn:
                await self.connect()

            async with self.lock:
                await self.conn.execute(
                    """
                    INSERT OR REPLACE INTO web_orders (
                        ticker, timestamp, entry_price, reward_risk_ratio, quantity, risk_percentage,
                        vstop_atr_factor, timeframe, kc_atr_factor, atr_factor,
                        account_balance, order_action, stop_type, set_market_order,
                        takeProfitBool, stop_loss
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data["ticker"],
                        await get_timestamp(),
                        data.get("entryPrice"),
                        data.get("rewardRiskRatio"),
                        data.get("quantity"),
                        data.get("riskPercentage"),
                        data.get("vstopAtrFactor"),
                        data.get("timeframe"),
                        data.get("kcAtrFactor"),
                        data.get("atrFactor"),
                        data.get("accountBalance"),
                        data.get("orderAction"),
                        data.get("stopType"),
                        data.get("set_market_order"),
                        data.get("takeProfitBool"),
                        data.get("stopLoss"),
                    )
                )
                await self.conn.commit()
                logger.debug(f"Inserted web order for {data['ticker']} with data: {data}")
        except Exception as e:
            logger.error(f"Error inserting web order: {e}")

    async def get_web_orders_by_ticker(self, ticker: str) -> List[OrderRequest]:
        orders: List[OrderRequest] = []
        try:
            if not self.conn:
                await self.connect()

            async with self.conn.execute(
                "SELECT * FROM web_orders WHERE ticker = ? ORDER BY timestamp DESC",
                (ticker,)
            ) as cursor:
                columns = [desc[0] for desc in cursor.description]
                async for row in cursor:
                    row_dict = dict(zip(columns, row))

                    # 🔁 Map DB keys (snake_case) to model keys (camelCase)
                    mapped = {
                        "entryPrice": row_dict.get("entry_price"),
                        "rewardRiskRatio": row_dict.get("reward_risk_ratio"),
                        "quantity": row_dict.get("quantity"),
                        "riskPercentage": row_dict.get("risk_percentage"),
                        "vstopAtrFactor": row_dict.get("vstop_atr_factor"),
                        "timeframe": row_dict.get("timeframe"),
                        "kcAtrFactor": row_dict.get("kc_atr_factor"),
                        "atrFactor": row_dict.get("atr_factor"),
                        "accountBalance": row_dict.get("account_balance"),
                        "ticker": row_dict.get("ticker"),
                        "orderAction": row_dict.get("order_action"),
                        "stopType": row_dict.get("stop_type"),
                        "set_market_order": bool(row_dict.get("set_market_order", 0)),
                        "takeProfitBool": bool(row_dict.get("takeProfitBool", 0)),
                        "stopLoss": row_dict.get("stop_loss"),
                    }

                    try:
                        order = OrderRequest(**mapped)
                        orders.append(order)
                    except ValidationError as ve:
                        logger.warning(f"Validation error converting row to OrderRequest: {ve}")

            return orders

        except Exception as e:
            logger.error(f"Error retrieving web orders for {ticker}: {e}")
            return []

    async def close(self) -> None:
        """
        Close the database connection.
        """
        try:
            self.shutting_down = True
            if self.conn:
                await self.conn.close()
                self.conn = None
                logger.debug("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

    def _serialize_contract(self, contract: Contract) -> str:
        """
        Serialize a Contract object to JSON string.
        """
        if not contract:
            return "{}"

        contract_dict = {
            "symbol": contract.symbol,
            "secType": contract.secType,
            "exchange": contract.exchange,
            "currency": contract.currency,
            "localSymbol": getattr(contract, "localSymbol", None),
            "conId": getattr(contract, "conId", 0),
            "primaryExchange": getattr(contract, "primaryExchange", None),
            # Add other relevant contract fields if needed
        }
        return json.dumps(contract_dict)

    def _serialize_order(self, order: Order) -> str:
        """
        Serialize an Order object to JSON string.
        """
        if not order:
            return "{}"

        order_dict = {
            "orderId": order.orderId,
            "clientId": order.clientId,
            "permId": order.permId,
            "action": order.action,
            "totalQuantity": order.totalQuantity,
            "orderType": order.orderType,
            "lmtPrice": order.lmtPrice,
            "auxPrice": order.auxPrice,
            "tif": order.tif,
            "transmit": order.transmit,
            "parentId": order.parentId,
            "outsideRth": order.outsideRth,
            "orderRef": order.orderRef,
            "account": order.account,
            # Include other relevant fields if needed
        }
        return json.dumps(order_dict)

    def _serialize_order_status(self, status: OrderStatus) -> str:
        """
        Serialize an OrderStatus object to JSON string.
        """
        if not status:
            return "{}"

        status_dict = {
            "status": status.status,
            "filled": status.filled,
            "remaining": status.remaining,
            "avgFillPrice": status.avgFillPrice,
            "permId": status.permId,
            "parentId": status.parentId,
            "lastFillPrice": status.lastFillPrice,
            "clientId": status.clientId,
            "whyHeld": status.whyHeld,
            "mktCapPrice": status.mktCapPrice,
        }
        return json.dumps(status_dict)

    async def insert_order(self, trade: Trade) -> int:
        """
        Insert a new order into the database.

        Args:
            trade: Trade object from IB API

        Returns:
            order_id: The order ID
        """
        try:
            if not self.conn:
                await self.connect()

            async with self.lock:
                logger.debug("Inserting order into database")
                # Check if order already exists
                async with self.conn.execute(
                    "SELECT order_id FROM orders WHERE order_id = ?",
                    (trade.order.orderId,)
                ) as cursor:
                    if await cursor.fetchone():
                        # Order exists, update it instead
                        return await self.update_order(trade)

                # Serialize complex objects
                contract_json = self._serialize_contract(trade.contract)
                order_json = self._serialize_order(trade.order)
                status_json = self._serialize_order_status(trade.orderStatus)
                now = datetime.now().isoformat()

                await self.conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, client_id, perm_id, symbol, sec_type, exchange,
                        action, order_type, total_quantity, lmt_price, aux_price, 
                        tif, outside_rth, order_ref, parent_id, transmit,
                        created_at, updated_at, status, filled, remaining, 
                        avg_fill_price, last_fill_price, why_held, account,
                        contract_details, order_details, order_status_details
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade.order.orderId,
                        trade.order.clientId,
                        trade.order.permId,
                        trade.contract.symbol,
                        trade.contract.secType,
                        trade.contract.exchange,
                        trade.order.action,
                        trade.order.orderType,
                        trade.order.totalQuantity,
                        trade.order.lmtPrice,
                        trade.order.auxPrice,
                        trade.order.tif,
                        1 if trade.order.outsideRth else 0,
                        trade.order.orderRef,
                        trade.order.parentId,
                        1 if trade.order.transmit else 0,
                        now,
                        now,
                        trade.orderStatus.status,
                        trade.orderStatus.filled,
                        trade.orderStatus.remaining,
                        trade.orderStatus.avgFillPrice,
                        trade.orderStatus.lastFillPrice,
                        trade.orderStatus.whyHeld,
                        trade.order.account,
                        contract_json,
                        order_json,
                        status_json,
                    )
                )

                # Insert log entries
                for log_entry in trade.log:
                    await self.conn.execute(
                        """
                        INSERT INTO order_logs (
                            order_id, time, status, message, error_code
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            trade.order.orderId,
                            log_entry.time.isoformat() if log_entry.time else None,
                            log_entry.status,
                            log_entry.message,
                            log_entry.errorCode,
                        )
                    )

                # Insert fills
                for fill in trade.fills:
                    await self.insert_fill(trade.order.orderId, fill)

                await self.conn.commit()
                logger.debug(f"Inserted order {trade.order.orderId} for {trade.contract.symbol}")
                return trade.order.orderId

        except Exception as e:
            logger.error(f"Error inserting order: {e}")
            if self.conn:
                await self.conn.rollback()
            return 0

    async def update_order(self, trade: Trade) -> int:
        """
        Update an existing order in the database.

        Args:
            trade: Trade object from IB API

        Returns:
            order_id: The order ID
        """
        try:
            if not self.conn:
                await self.connect()

            async with self.lock:
                # Serialize complex objects
                contract_json = self._serialize_contract(trade.contract)
                order_json = self._serialize_order(trade.order)
                status_json = self._serialize_order_status(trade.orderStatus)
                now = datetime.now().isoformat()

                await self.conn.execute(
                    """
                    UPDATE orders SET
                        client_id = ?,
                        perm_id = ?,
                        symbol = ?,
                        sec_type = ?,
                        exchange = ?,
                        action = ?,
                        order_type = ?,
                        total_quantity = ?,
                        lmt_price = ?,
                        aux_price = ?,
                        tif = ?,
                        outside_rth = ?,
                        order_ref = ?,
                        parent_id = ?,
                        transmit = ?,
                        updated_at = ?,
                        status = ?,
                        filled = ?,
                        remaining = ?,
                        avg_fill_price = ?,
                        last_fill_price = ?,
                        why_held = ?,
                        account = ?,
                        contract_details = ?,
                        order_details = ?,
                        order_status_details = ?
                    WHERE order_id = ?
                    """,
                    (
                        trade.order.clientId,
                        trade.order.permId,
                        trade.contract.symbol,
                        trade.contract.secType,
                        trade.contract.exchange,
                        trade.order.action,
                        trade.order.orderType,
                        trade.order.totalQuantity,
                        trade.order.lmtPrice,
                        trade.order.auxPrice,
                        trade.order.tif,
                        1 if trade.order.outsideRth else 0,
                        trade.order.orderRef,
                        trade.order.parentId,
                        1 if trade.order.transmit else 0,
                        now,
                        trade.orderStatus.status,
                        trade.orderStatus.filled,
                        trade.orderStatus.remaining,
                        trade.orderStatus.avgFillPrice,
                        trade.orderStatus.lastFillPrice,
                        trade.orderStatus.whyHeld,
                        trade.order.account,
                        contract_json,
                        order_json,
                        status_json,
                        trade.order.orderId,
                    )
                )

                # Update or insert new log entries
                for log_entry in trade.log:
                    async with self.conn.execute(
                        """
                        SELECT log_id FROM order_logs 
                         WHERE order_id = ? AND status = ? AND message = ? AND 
                               (time = ? OR (time IS NULL AND ? IS NULL))
                        """,
                        (
                            trade.order.orderId,
                            log_entry.status,
                            log_entry.message,
                            log_entry.time.isoformat() if log_entry.time else None,
                            log_entry.time.isoformat() if log_entry.time else None,
                        )
                    ) as cursor:
                        if not await cursor.fetchone():
                            await self.conn.execute(
                                """
                                INSERT INTO order_logs (
                                    order_id, time, status, message, error_code
                                ) VALUES (?, ?, ?, ?, ?)
                                """,
                                (
                                    trade.order.orderId,
                                    log_entry.time.isoformat() if log_entry.time else None,
                                    log_entry.status,
                                    log_entry.message,
                                    log_entry.errorCode,
                                )
                            )

                # Insert any new fills
                existing_execution_ids = set()
                async with self.conn.execute(
                    "SELECT execution_id FROM fills WHERE order_id = ?",
                    (trade.order.orderId,)
                ) as cursor:
                    async for row in cursor:
                        existing_execution_ids.add(row[0])

                for fill in trade.fills:
                    if hasattr(fill, "execution") and hasattr(fill.execution, "execId"):
                        if fill.execution.execId not in existing_execution_ids:
                            await self.insert_fill(trade.order.orderId, fill)

                await self.conn.commit()
                logger.debug(f"Updated order {trade.order.orderId} for {trade.contract.symbol}")
                return trade.order.orderId

        except Exception as e:
            logger.error(f"Error updating order: {e}")
            if self.conn:
                await self.conn.rollback()
            return 0

    async def insert_fill(self, order_id: int, fill) -> None:
        """
        Insert a fill record into the database.

        Args:
            order_id: Order ID
            fill: Fill object from IB API
        """
        try:
            if not hasattr(fill, "execution"):
                logger.warning(f"Fill object does not have execution attribute: {fill}")
                return

            exec_id = getattr(
                fill.execution, "execId", f"unknown_{datetime.now().timestamp()}"
            )

            await self.conn.execute(
                """
                INSERT INTO fills (
                    order_id, execution_id, symbol, sec_type, side, 
                    shares, price, time, exchange, commission, 
                    commission_currency, realized_pnl
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    exec_id,
                    fill.contract.symbol,
                    fill.contract.secType,
                    fill.execution.side,
                    fill.execution.shares,
                    fill.execution.price,
                    (
                        fill.execution.time.isoformat()
                        if hasattr(fill.execution, "time") and fill.execution.time
                        else None
                    ),
                    fill.execution.exchange,
                    (
                        fill.commissionReport.commission
                        if hasattr(fill, "commissionReport")
                        else None
                    ),
                    (
                        fill.commissionReport.currency
                        if hasattr(fill, "commissionReport")
                        else None
                    ),
                    (
                        fill.commissionReport.realizedPNL
                        if hasattr(fill, "commissionReport")
                        else None
                    ),
                )
            )

            logger.debug(f"Inserted fill record for order {order_id}, execution {exec_id}")

        except Exception as e:
            logger.error(f"Error inserting fill: {e}")
            raise

    async def get_order(self, order_id: int) -> Dict:
        """
        Retrieve an order by its ID.

        Args:
            order_id: Order ID

        Returns:
            Dict with order details
        """
        try:
            if not self.conn:
                await self.connect()

            async with self.conn.execute(
                "SELECT * FROM orders WHERE order_id = ?", (order_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    logger.warning(f"Order {order_id} not found in database")
                    return {}

                columns = [desc[0] for desc in cursor.description]
                order_data = dict(zip(columns, row))

                # Get fills
                fills = []
                async with self.conn.execute(
                    "SELECT * FROM fills WHERE order_id = ?", (order_id,)
                ) as fill_cursor:
                    fill_columns = [desc[0] for desc in fill_cursor.description]
                    async for fill_row in fill_cursor:
                        fills.append(dict(zip(fill_columns, fill_row)))

                order_data["fills"] = fills

                # Get logs
                logs = []
                async with self.conn.execute(
                    "SELECT * FROM order_logs WHERE order_id = ? ORDER BY time ASC",
                    (order_id,)
                ) as log_cursor:
                    log_columns = [desc[0] for desc in log_cursor.description]
                    async for log_row in log_cursor:
                        logs.append(dict(zip(log_columns, log_row)))

                order_data["logs"] = logs
                return order_data

        except Exception as e:
            logger.error(f"Error retrieving order {order_id}: {e}")
            return {}

    async def get_orders_by_symbol(self, symbol: str) -> List[Dict]:
        """
        Retrieve all orders for a specific symbol.

        Args:
            symbol: Contract symbol

        Returns:
            List of order dictionaries
        """
        try:
            if not self.conn:
                await self.connect()

            orders = []
            async with self.conn.execute(
                "SELECT order_id FROM orders WHERE symbol = ? ORDER BY created_at DESC",
                (symbol,)
            ) as cursor:
                async for row in cursor:
                    order_id = row[0]
                    order_data = await self.get_order(order_id)
                    if order_data:
                        orders.append(order_data)

            return orders

        except Exception as e:
            logger.error(f"Error retrieving orders for symbol {symbol}: {e}")
            return []

    async def get_recent_orders(self, limit: int = 100) -> List[Dict]:
        """
        Retrieve recent orders, limited by the specified count.

        Args:
            limit: Maximum number of orders to retrieve

        Returns:
            List of order dictionaries
        """
        try:
            if not self.conn:
                await self.connect()

            orders = []
            async with self.conn.execute(
                "SELECT order_id FROM orders ORDER BY created_at DESC LIMIT ?", (limit,)
            ) as cursor:
                async for row in cursor:
                    order_id = row[0]
                    order_data = await self.get_order(order_id)
                    if order_data:
                        orders.append(order_data)

            return orders

        except Exception as e:
            logger.error(f"Error retrieving recent orders: {e}")
            return []

    async def get_active_orders(self) -> List[Dict]:
        """
        Retrieve all active orders (not filled or cancelled).

        Returns:
            List of order dictionaries
        """
        try:
            if not self.conn:
                await self.connect()

            orders = []
            async with self.conn.execute(
                """
                SELECT order_id FROM orders 
                 WHERE status NOT IN ('Filled', 'Cancelled', 'ApiCancelled') 
                 ORDER BY created_at DESC
                """
            ) as cursor:
                async for row in cursor:
                    order_id = row[0]
                    order_data = await self.get_order(order_id)
                    if order_data:
                        orders.append(order_data)

            return orders

        except Exception as e:
            logger.error(f"Error retrieving active orders: {e}")
            return []

    async def get_bracket_children(self, parent_order_id: int) -> List[Dict[str, Any]]:
        """
        Fetch all bracket child orders tied to a parent using parentId and ocaGroup.
        """
        if not self.conn:
            await self.connect()

        query = """
            SELECT * FROM orders
             WHERE parent_id = ? OR ocaGroup IN (
                SELECT ocaGroup FROM orders WHERE order_id = ?
             )
             ORDER BY created_at ASC
        """
        async with self.conn.execute(query, (parent_order_id, parent_order_id)) as cursor:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) async for row in cursor]

    async def is_bracket_parent(self, order_id: int) -> bool:
        children = await self.get_bracket_children(order_id)
        return len(children) > 0

    # -------------------
    # Positions & History
    # -------------------
    async def insert_positions(self, positions: List[Position]) -> None:
        """Upsert a batch of Position snapshots (one row per symbol)."""
        try:
            symbol= None
            conId= None
            exchange= None
            currency= None
            secType= None
            contract: Contract = None

            if not positions:
                return
            await self.connect()
            ts = datetime.now(timezone.utc).isoformat()
            rows = []
            
            for item in positions:
                contract= item.contract
                logger.debug(f"Inserting {item} positions into database")
                symbol = item.contract.symbol
                conId = item.contract.conId
                exchange = item.contract.exchange
                currency = item.contract.currency
                secType = item.contract.secType

                
                rows.append((
                    symbol,
                    secType, conId, exchange, currency,
                    getattr(contract, "localSymbol", None),
                    getattr(contract, "primaryExchange", None),
                    getattr(contract, "tradingClass", None),
                    item.position, item.avgCost,
                    item.account,
                    ts
                ))
            async with self.lock:
                await self.conn.executemany(
                    """
                    INSERT OR REPLACE INTO ib_positions
                    (symbol, secType, conId, exchange, currency,
                    localSymbol, primaryExchange, tradingClass,
                    position, averageCost, account, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows
                )
                await self.conn.commit()
            logger.debug(f"Upserted {len(rows)} positions")
        except Exception as e:
            logger.error(f"Error inserting positions: {e}")
            if self.conn:
                await self.conn.rollback()
    async def insert_portfolio_items(self, portfolio_items: List[PortfolioItem]) -> None:
        """Upsert a batch of PortfolioItem snapshots (one row per symbol)."""
        if not portfolio_items:
            return
        await self.connect()
        ts = datetime.now(timezone.utc).isoformat()
        rows = []
        for item in portfolio_items:
            item.contract
            rows.append((
                item.contract.symbol,
                item.contract.secType, item.contract.conId, item.contract.exchange, item.contract.currency,
                getattr(item.contract, "localSymbol", None),
                getattr(item.contract, "primaryExchange", None),
                getattr(item.contract, "tradingClass", None),
                item.position, item.averageCost,
                item.marketPrice, item.marketValue,
                item.unrealizedPNL, item.realizedPNL,
                item.account,
                ts
            ))
        async with self.lock:
            await self.conn.executemany(
                """
                INSERT OR REPLACE INTO portfolio_items_table
                  (symbol, secType, conId, exchange, currency,
                   localSymbol, primaryExchange, tradingClass,
                   position, averageCost,
                   marketPrice, marketValue,
                   unrealizedPNL, realizedPNL,
                   account, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows
            )
            await self.conn.commit()
        logger.debug(f"Upserted {len(rows)} portfolio portfolio_items")

    async def fetch_positions_list(self) -> List[Dict[str, Any]]:
        """Return the entire ib_positions table (one dict per symbol)."""
        await self.connect()
        async with self.conn.execute(
            "SELECT * FROM ib_positions ORDER BY symbol"
        ) as cur:
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) async for row in cur]

    async def fetch_portfolio_list(self) -> List[Dict[str, Any]]:
        """Return the entire portfolio_items table (one dict per symbol)."""
        await self.connect()
        async with self.conn.execute(
            "SELECT * FROM portfolio_items_table ORDER BY symbol"
        ) as cur:
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) async for row in cur]

 

    async def insert_pnl_history(self, req_id: int, daily_pnl: float, unrealized_pnl: float, realized_pnl: float) -> None:
        """
        Insert a PnL event into pnl_history.

        Args:
            req_id: Request ID for the PnL subscription
            daily_pnl: Daily PnL
            unrealized_pnl: Unrealized PnL
            realized_pnl: Realized PnL
        """
        try:
            if not self.conn:
                await self.connect()

            ts = datetime.now().isoformat()
            async with self.lock:
                await self.conn.execute(
                    """
                    INSERT INTO pnl_history (
                        req_id, daily_pnl, unrealized_pnl, realized_pnl, timestamp
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (req_id, daily_pnl, unrealized_pnl, realized_pnl, ts)
                )
                await self.conn.commit()
        except Exception as e:
            logger.error(f"Error inserting PnL history for req_id {req_id}: {e}")
    

    

    

    async def delete_zero_positions(self, zero_positions: Dict[str, Any], db_path: str = DB_PATH) -> None:
        """
        Delete ib_positions with zero quantity from the ib_positions table.

        Args:
            zero_positions: Dictionary of symbol keys for which position is zero
        """
        if not zero_positions:
            return

        try:
            conn = await aiosqlite.connect(db_path)
            await conn.execute(
                """
                  CREATE TABLE IF NOT EXISTS ib_positions (
                   symbol TEXT PRIMARY KEY,
                   position REAL,
                   averageCost REAL,
                   account TEXT,
                   timestamp TEXT
               )
                """
            )

            for symbol in zero_positions.keys():
                await conn.execute("DELETE FROM ib_positions WHERE symbol = ?", (symbol,))
                logger.debug(f"Deleted zero position for {symbol} from database")

            await conn.commit()
            await conn.close()
            logger.debug(f"Deleted {len(zero_positions)} zero ib_positions from database")
        except Exception as e:
            logger.error(f"Error deleting zero ib_positions from database: {e}")

    

    # -----------------------------
    # Trades and related utilities
    # -----------------------------

    async def insert_trade(self, trade: Trade) -> None:
        """
        Insert a trade into the trades table.
        """
        contract = trade.contract
        symbol = contract.symbol
        order = trade.order
        status = trade.orderStatus
        log_entry = trade.log[-1] if trade.log else None

        # Fallback logic for orderId
        raw_order_id = status.orderId or order.orderId
        fallback_id = order.permId or status.permId
        order_id = raw_order_id if raw_order_id and raw_order_id > 0 else fallback_id

        async with aiosqlite.connect(DB_PATH) as db:
            # Check if ocaGroup column exists
            cursor = await db.execute("PRAGMA table_info(trades)")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            if "ocaGroup" not in column_names:
                try:
                    logger.debug("Adding ocaGroup column to trades table")
                    await db.execute("ALTER TABLE trades ADD COLUMN ocaGroup TEXT")
                    await db.commit()
                    logger.debug("Successfully added ocaGroup column")
                except Exception as e:
                    logger.error(f"Error adding ocaGroup column: {e}")

            oca_group = getattr(order, "ocaGroup", None)

            await db.execute(
                """
                INSERT OR REPLACE INTO trades (
                    symbol, conId, orderId, exchange, currency, localSymbol, tradingClass,
                    action, totalQuantity, orderType, lmtPrice, auxPrice, tif, ocaType,
                    ocaGroup, orderRef, displaySize, outsideRth, trailStopPrice, volatilityType,
                    deltaNeutralOrderType, referencePriceType, account, clearingIntent,
                    adjustedOrderType, cashQty, dontUseAutoPriceForHedge, usePriceMgmtAlgo,
                    minCompeteSize, competeAgainstBestOffset, status, filled,
                    remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice, log_time, log_status, log_message, log_errorCode, advancedError
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    contract.symbol,
                    contract.conId,
                    order_id,
                    contract.exchange,
                    contract.currency,
                    contract.localSymbol,
                    contract.tradingClass,
                    order.action,
                    order.totalQuantity,
                    order.orderType,
                    order.lmtPrice,
                    order.auxPrice,
                    order.tif,
                    order.ocaType,
                    oca_group,
                    order.orderRef,
                    order.displaySize,
                    order.outsideRth,
                    order.trailStopPrice,
                    order.volatilityType,
                    order.deltaNeutralOrderType,
                    order.referencePriceType,
                    order.account,
                    order.clearingIntent,
                    order.adjustedOrderType,
                    order.cashQty,
                    order.dontUseAutoPriceForHedge,
                    order.usePriceMgmtAlgo,
                    order.minCompeteSize,
                    order.competeAgainstBestOffset,
                    status.status,
                    status.filled,
                    status.remaining,
                    status.avgFillPrice,
                    order.permId,
                    status.parentId,
                    status.lastFillPrice,
                    status.clientId,
                    status.whyHeld,
                    status.mktCapPrice,
                    log_entry.time.isoformat() if log_entry else None,
                    log_entry.status if log_entry else None,
                    log_entry.message if log_entry else None,
                    log_entry.errorCode if log_entry else None,
                    trade.advancedError,
                )
            )
            logger.debug(f"Inserted trade for {contract.symbol} with orderId {order_id}")
            await db.commit()

    async def get_all_trades(self):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT * FROM trades")
            rows = await cursor.fetchall()
            await cursor.close()
            return rows

    async def delete_trade(self, order_id: int, symbol: str):
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM trades WHERE orderId = ? AND symbol = ?", (order_id, symbol)
            )
            await db.commit()

order_db = IBDBHandler("orders.db")
# Example function to integrate with IBManager.on_order_status_event
async def order_status_handler(ib_manager, order_db: IBDBHandler, trade: Trade):
    """
    Handle order status updates, storing them in the database.

    Args:
        ib_manager: The IBManager instance
        order_db: IBDBHandler instance
        trade: Trade object from IB API
    """
    try:
        # Log the received order status
        logger.debug(f"Order status event received for {trade.contract.symbol}")

        # Insert or update the order in the database
        await order_db.insert_order(trade)

        # Update ib_manager's internal state
        symbol = trade.contract.symbol
        ib_manager.open_orders[symbol] = {
            "order_id": trade.order.orderId,
            "status": trade.orderStatus.status,
            "timestamp": await get_timestamp(),
        }
        ib_manager.orders_dict[symbol] = trade

        # Handle position updates if needed
        portfolio_item = ib_manager.ib.portfolio()
        if portfolio_item:
            for pos in portfolio_item:
                if pos.contract.symbol == symbol:
                    logger.debug(f"Position update for {pos.contract.symbol}: {pos.position} shares")

        # Update PnL data (example usage)
        await ib_manager.on_pnl_event(pnl={})

        logger.debug(f"Order status updated for {symbol}: {trade.orderStatus.status}")

    except Exception as e:
        logger.error(f"Error in order status handler: {e}")

async def order_status_handler_fast_api(order_db: IBDBHandler, trade: Trade):
    """
    Handle order status updates, storing them in the database.
    """
    try:
        # Log the received order status
        logger.debug(f"Order status event received for {trade.contract.symbol}")

        # Insert or update the order in the database
        await order_db.insert_order(trade)

        logger.debug(
            f"Order status updated for {trade.contract.symbol}: {trade.orderStatus.status}"
        )

    except Exception as e:
        logger.error(f"Error in order status handler: {e}")
