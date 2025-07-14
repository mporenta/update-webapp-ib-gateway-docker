# ploy_db.py
import aiosqlite
import asyncio
from pydantic import ValidationError, BaseModel
import os
from sqlalchemy import create_engine, Column, String, Integer, Float, BigInteger
from datetime import datetime, date, time, timezone, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple
import json
from dataclasses import asdict
from ib_async import Trade, Order, OrderStatus, Contract
from ib_async.objects import RealTimeBar
from log_config import logger, log_config
from models import OrderRequest, PriceSnapshot
from models import *
import pandas as pd


DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "csv_data.db")

class PolyDBHandler:
    """
    Database handler for storing and retrieving orders from Interactive Brokers.
    Uses aiosqlite for async database operations.
    """

    def __init__(self, db_path: str = DB_PATH):
        """
        Initialize the PolyDBHandler.

        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        
        self.lock = asyncio.Lock()
        self.shutting_down = False

    async def db_connect(self) -> None:
        """
        Connect to the SQLite database.
        """
        try:
            logger.info(f" db_connect Connecting to orders database at {self.db_path}")
            self.conn = await aiosqlite.connect(self.db_path)
            # Enable foreign keys constraint
            await self.conn.execute("PRAGMA foreign_keys = ON")
            await self.conn.commit()
            await self.create_tables()
            logger.info("Successfully connected to orders database")
        except Exception as e:
            logger.error(f"Error connecting to orders database: {e}")
            raise

    async def create_tables(self) -> None:
        async with self.lock:
            await self.conn.executescript(r"""
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS market_data (
                    Symbol TEXT PRIMARY KEY,
                    YesterdayClose REAL,
                    TodayVolume REAL,
                    PreMarketVolume REAL,
                    PostMarketVolume REAL,
                    AllVolume REAL,
                    Avg21DayVolume REAL,
                    AfterHoursVolume REAL,
                    AfterHoursVolumePct REAL,
                    LastUpdated TEXT
                );

                CREATE TABLE IF NOT EXISTS earnings (
                    Symbol TEXT PRIMARY KEY,
                    Exchange TEXT,
                    "Average Volume 10 days" REAL,
                    "Upcoming earnings date" TEXT,
                    "Recent earnings date" TEXT,
                    "Pre-market Volume" REAL
                );

                -- WSHE events: one row per symbol
                CREATE TABLE IF NOT EXISTS wsh_events (
                    Symbol TEXT PRIMARY KEY,
                    earnings_date     DATE,
                    announce_datetime DATETIME,
                    event_type        TEXT,
                    data              JSON,
                    raw_event         JSON
                );
            """)
            logger.info("Created tables if they did not exist.")
            await self.conn.commit()

    async def upsert_wsh_events(self, events: List[Dict[str, Any]]) -> None:
        """Insert or update WSHE events, keyed by Symbol."""
        if self.conn is None:
            await self.db_connect()
        logger.debug(f"Upserting {len(events)} WSHE events.")
        async with self.lock:
            logger.debug(f" 2 Upserting {len(events)} WSHE events.")
            cur = await self.conn.cursor()
            for ev in events:
                logger.debug(f"Processing event: {ev}")
                d = ev.get("data", {})
                symbol = d.get("company", {}).get("contract") or d.get("symbol")
                # parse earnings_date YYYYMMDD → ISO
                earnings = None
                if d.get("earnings_date"):
                    ed = d["earnings_date"]
                    earnings = f"{ed[:4]}-{ed[4:6]}-{ed[6:8]}"
                # parse announce_datetime
                announce = None
                if d.get("announce_datetime"):
                    announce = (
                        d["announce_datetime"]
                         .replace("T", " ")
                         .replace("+0000", "+00:00")
                    )
                await cur.execute(
                    r"""
                    INSERT INTO wsh_events (
                        Symbol,
                        earnings_date,
                        announce_datetime,
                        event_type,
                        data,
                        raw_event
                    ) VALUES (?, ?, ?, ?, json(?), json(?))
                    ON CONFLICT(Symbol) DO UPDATE SET
                        earnings_date     = excluded.earnings_date,
                        announce_datetime = excluded.announce_datetime,
                        event_type        = excluded.event_type,
                        data              = excluded.data,
                        raw_event         = excluded.raw_event
                    """,
                    (
                        symbol,
                        earnings,
                        announce,
                        ev.get("event_type"),
                        json.dumps(d),
                        json.dumps(ev)
                    )
                )
                logger.debug(f"Upserted WSHE event for {symbol}: {ev.get('event_type')}")
            await self.conn.commit()

    async def get_symbols_by_earnings_date(self, on_date: date) -> List[str]:
        """Return all symbols whose earnings_date equals `on_date`."""
        await self.db_connect()
        async with self.lock:
            cur = await self.conn.execute(
                "SELECT Symbol FROM wsh_events WHERE earnings_date = ?",
                (on_date.isoformat(),)
            )
            rows = await cur.fetchall()
            return [row[0] for row in rows]

    async def get_symbols_recent_announces(self, since_hours: int = 48) -> List[str]:
        """Return all symbols with announce_datetime in the last `since_hours` hours."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        await self.db_connect()
        async with self.lock:
            cur = await self.conn.execute(
                "SELECT Symbol FROM wsh_events WHERE announce_datetime >= ?",
                (cutoff,)
            )
            rows = await cur.fetchall()
            return [row[0] for row in rows]

    async def get_events_by_type(self, event_type: str) -> List[Dict[str, Any]]:
        """Return all rows matching a given event_type."""
        if self.conn is None:
            await self.db_connect()

        async with self.lock:
            logger.debug(f"Fetching events for event type: {event_type}")
            cur = await self.conn.execute(
                "SELECT * FROM wsh_events WHERE event_type = ? ORDER BY announce_datetime",
                (event_type,)
            )
            cols = [c[0] for c in cur.description]
            rows = await cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]

    async def get_events_by_earnings_date(self, on_date: date) -> List[Dict[str, Any]]:
        """Return all events whose earnings_date equals the given `on_date`."""
        if self.conn is None:
            await self.db_connect()

        async with self.lock:
            logger.debug(f"Fetching events for earnings date: {on_date}")
            cur = await self.conn.execute(
                "SELECT * FROM wsh_events WHERE earnings_date = ?",
                (on_date.isoformat(),)
            )
            cols = [c[0] for c in cur.description]
            rows = await cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]

    async def get_recent_announcements(self, since_hours: int = 48) -> List[Dict[str, Any]]:
        """Return all events announced in the last `since_hours` hours."""
        cutoff = datetime.now() - timedelta(hours=since_hours)
        if self.conn is None:
            await self.db_connect()

        async with self.lock:
            logger.debug(f"Fetching recent announcements since: {cutoff}")
            cur = await self.conn.execute(
                "SELECT * FROM wsh_events "
                "WHERE announce_datetime IS NOT NULL "
                "  AND announce_datetime >= ?",
                (cutoff.isoformat(),)
            )
            cols = [c[0] for c in cur.description]
            rows = await cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]


                
    async def update_symbol_data_in_db(self, results):
        """Update the database with all the fetched market data."""
        if not results:
            return
        
        try:
           
            if self.conn is None:
                await self.db_connect()
            
            async with self.lock:
                cursor = await self.conn.cursor()
            
                
                # Insert or update data for each symbol
                now = datetime.now().isoformat()
                
                for symbol, data in results.items():
                    await cursor.execute("""
                        INSERT INTO market_data (
                            Symbol, 
                            YesterdayClose, 
                            TodayVolume,
                            PreMarketVolume, 
                            PostMarketVolume, 
                            AllVolume,
                            Avg21DayVolume,
                            AfterHoursVolume,
                            AfterHoursVolumePct,
                            LastUpdated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(Symbol) DO UPDATE SET
                            YesterdayClose = excluded.YesterdayClose,
                            TodayVolume = excluded.TodayVolume,
                            PreMarketVolume = excluded.PreMarketVolume,
                            PostMarketVolume = excluded.PostMarketVolume,
                            AllVolume = excluded.AllVolume,
                            Avg21DayVolume = excluded.Avg21DayVolume,
                            AfterHoursVolume = excluded.AfterHoursVolume,
                            AfterHoursVolumePct = excluded.AfterHoursVolumePct,
                            LastUpdated = excluded.LastUpdated
                    """, (
                        symbol,
                        data['yesterday_close'],
                        data['today_volume'],
                        data['pre_market_volume'],
                        data['post_market_volume'],
                        data['all_volume'],
                        data['avg_21_day_volume'],
                        data['after_hours_rVol'],
                        data['after_hours_rVol_percent'],
                        now
                    ))
                
                await self.conn.commit()
                logger.debug(f"Updated database with market data for {len(results)} symbols")
                
                # Optional: Create a view or index for common queries
                await cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_market_data_volume 
                    ON market_data(AfterHoursVolumePct DESC, Avg21DayVolume DESC)
                """)
                
                await cursor.execute("""
                    CREATE VIEW IF NOT EXISTS high_volume_symbols AS
                    SELECT Symbol, YesterdayClose, TodayVolume, Avg21DayVolume, 
                        AfterHoursVolume, AfterHoursVolumePct
                    FROM market_data
                    WHERE AfterHoursVolumePct >= 0.2
                    ORDER BY AfterHoursVolumePct DESC
                """)
                
                await self.conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating database with market data: {e}")
            # Make sure we don't leave the transaction open
            if self.conn:
                await self.conn.rollback()
                
        finally:
            # Optionally close the connection here if needed
            # If you want to keep it open for future operations, remove this part
            if self.conn:
                await self.conn.close()
                self.conn = None            
    async def _upsert_to_db(self, df: pd.DataFrame):
        """Create table if needed and insert/update each row by Symbol."""
        if self.conn is None:
            await self.db_connect()
           
        async with self.lock:
            cursor = await self.conn.cursor()
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS earnings (
                    Symbol TEXT PRIMARY KEY,
                    Exchange TEXT,
                    "Average Volume 10 days" REAL,
                    "Upcoming earnings date" TEXT,
                    "Recent earnings date" TEXT,
                    "Pre-market Volume" REAL
                )
            """)
            for _, row in df.iterrows():
                await cursor.execute("""
                    INSERT INTO earnings (
                        "Symbol", "Exchange", "Average Volume 10 days",
                        "Upcoming earnings date", "Recent earnings date",
                        "Pre-market Volume"
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(Symbol) DO UPDATE SET
                        Exchange = excluded.Exchange,
                        "Average Volume 10 days" = excluded."Average Volume 10 days",
                        "Upcoming earnings date" = excluded."Upcoming earnings date",
                        "Recent earnings date" = excluded."Recent earnings date",
                        "Pre-market Volume" = excluded."Pre-market Volume"
                """, (
                    row["Symbol"],
                    row["Exchange"],
                    row["Average Volume 10 days"],
                    row["Upcoming earnings date"],
                    row["Recent earnings date"],
                    row["Pre-market Volume"]
                ))
            await self.conn.commit()
            await self.conn.close()
            logger.debug(f"Upserted {len(df)} rows into {self.db_path}")
            
    async def _fetch_wsh_times(self, symbol: str) -> Tuple[Optional[date], Optional[datetime]]:
        """Get (earnings_date, announce_datetime) from the wsh_events table."""
        await self._connect_db()
        async with self.poly_db.conn.execute(
            "SELECT earnings_date, announce_datetime FROM wsh_events WHERE Symbol = ?",
            (symbol,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None, None

        ed_str, ann_str = row
        ed = date.fromisoformat(ed_str) if ed_str else None
        ann = (
            datetime.fromisoformat(ann_str)
                    .astimezone(pytz.timezone("America/New_York"))
        ) if ann_str else None
        return ed, ann
