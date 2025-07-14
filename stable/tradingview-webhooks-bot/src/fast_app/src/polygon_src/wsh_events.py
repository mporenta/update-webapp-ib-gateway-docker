# wsh_events.py
import asyncio
import logging
from datetime import date, datetime
from math import log
from typing import Any, Dict, Optional
import json
import aiosqlite
import pytz

from dateutil import parser
from pydantic import BaseModel, Field, model_validator
from ib_async import *
import arrow
from log_config import log_config, logger

log_config.setup()
# ── CONFIGURE LOGGER ────────────────────────────────────────────────────────────



# ── Pydantic MODEL ─────────────────────────────────────────────────────────────

class WshEventRecord(BaseModel):
    ticker: str
    event_key: str
    event_type: str

    # ← THIS MUST BE HERE
    data: Dict[str, Any]

    earnings_date: Optional[date] = None
    announce_datetime: Optional[datetime] = None
    raw: Dict[str, Any]

    @model_validator(mode='before')
    def extract_dates(cls, values):
        data = values.get("data", {})

        # 1) earnings_date
        ed = None
        if data.get("earnings_date"):
            try:
                ed = datetime.strptime(data["earnings_date"], "%Y%m%d").date()
            except Exception:
                logger.warning(f"Bad earnings_date: {data['earnings_date']!r}")
        elif isinstance(data.get("date"), dict):
            sd = data["date"].get("start_date")
            if sd:
                try:
                    ed = datetime.strptime(sd, "%Y%m%d").date()
                except Exception:
                    logger.warning(f"Bad nested start_date: {sd!r}")
        values["earnings_date"] = ed

        # 2) announce_datetime
        ad = None
        raw_ad = data.get("announce_datetime")
        if raw_ad:
            try:
                ad = parser.parse(raw_ad) \
                          .astimezone(pytz.timezone("America/New_York"))
            except Exception:
                logger.warning(f"Bad announce_datetime: {raw_ad!r}")
        values["announce_datetime"] = ad

        # no ValueError here anymore
        return values

    class Config:
        arbitrary_types_allowed = True

# ── DATABASE OPERATIONS ────────────────────────────────────────────────────────

DB_FILE = "wsh_events.db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS events (
    ticker TEXT NOT NULL,
    event_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    earnings_date DATE,
    announce_datetime DATETIME,
    raw_json TEXT NOT NULL,
    PRIMARY KEY (ticker, event_key)
)
"""

INSERT_EVENT_SQL = """
INSERT OR IGNORE INTO events
    (ticker, event_key, event_type, earnings_date, announce_datetime, raw_json)
VALUES (?, ?, ?, ?, ?, ?)
"""

SELECT_BY_TICKER_SQL = """
SELECT ticker, event_key, event_type, earnings_date, announce_datetime, raw_json
FROM events
WHERE ticker = ?
ORDER BY announce_datetime, earnings_date
"""

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(CREATE_TABLE_SQL)
        await db.commit()

async def store_events(events_json: Any):
    # … your JSON‑string → list logic …

    await init_db()
    async with aiosqlite.connect(DB_FILE) as db:
        
        for ev in events_json:
            
            if not isinstance(ev, dict):
                logger.debug(f"Invalid event format: {ev}")
                continue
            
            rec = WshEventRecord(
                ticker     = ev["data"]["company"]["contract"],
                event_key  = ev["event_key"],
                event_type = ev["event_type"],
                data       = ev["data"],     # ← now used by validator
                raw        = ev,
            )
           

            # only drop those with truly no dates
            if rec.earnings_date is None and rec.announce_datetime is None:
                logger.debug(f"Dropping {rec.event_key} —no dates for ticker {rec.ticker}")
                continue
            logger.debug(f"Wrote events to DB: {rec.ticker} {rec.event_key} {rec.event_type}")
            await db.execute(
                INSERT_EVENT_SQL,
                (
                    rec.ticker,
                    rec.event_key,
                    rec.event_type,
                    rec.earnings_date.isoformat() 
                      if rec.earnings_date else None,
                    rec.announce_datetime.strftime("%Y-%m-%d %H:%M:%S")
                      if rec.announce_datetime else None,
                    json.dumps(rec.raw),
                )
            )
            
        await db.commit()
    


async def get_events_by_ticker(ticker: str) -> list[WshEventRecord]:
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute(SELECT_BY_TICKER_SQL, (ticker,))
        rows = await cursor.fetchall()
        await cursor.close()

    events: list[WshEventRecord] = []
    for _t, event_key, event_type, _, _, raw_json in rows:
        try:
            raw = json.loads(raw_json)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON in raw_json for {event_key}, returning empty data")
            continue

        # Now build the model from exactly the same pieces you used when storing
        ev = WshEventRecord(
            ticker      = ticker,
            event_key   = event_key,
            event_type  = event_type,
            data        = raw["data"],
            raw         = raw,
        )
        events.append(ev)

    return events
