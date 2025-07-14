import json
from math import log
import signal
import threading
from tkinter import N
import arrow
from numpy import poly
from regex import W
from sqlmodel import SQLModel, create_engine, Session, select
from ib_async import (
    Contract,
    util,
    IB,
    WshEventData
)

from log_config import log_config, logger
from typing import *
import pandas as pd
import os, asyncio, pytz, time
from pathlib import Path
from datetime import *
from dotenv import load_dotenv
from models import subscribed_contracts, wsh_dict, is_coming_soon, WshEvent, WshData, convert_dates_for_wsh_events
from rvol import get_rvol, get_one_min_data, one_min_bars_dict
from poly_client import client, poly_bad_response
from poly_db import PolyDBHandler
BadResponse = poly_bad_response

poly_db= PolyDBHandler()
env_file = os.path.join(os.path.dirname(__file__), ".env")

log_config.setup()
# --- Configuration ---
client_id = int(os.getenv("IB_CSV_GATGEWAY_CLIENT_ID", "50"))
ib_host = os.getenv("IB_CSV_GATGEWAY_HOST", "localhost")
ib_port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
boofMsg = os.getenv("HELLO_MSG")

# assume ib, poly_db, subscribed_contracts already set up on ImportCSV
import pytz
from datetime import datetime, date, time, timedelta

async def fetch_and_filter_earnings(symbols: list[str], events) -> list[str]:
    """
    Use your existing get_events_by_type to pull all wshe_eps rows,
    then keep only those whose announce_datetime:
      – fell on yesterday ≥ 4 PM NYT, or
      – fell on today   ≤ 9:30 AM NYT
    and whose Symbol is in our `symbols` list.
    """
    # 1) grab every EPS announcement
    

    # set up our NY‐time cutoffs
    tz_ny    = pytz.timezone("America/New_York")
    now_ny   = datetime.now(tz_ny)
    today    = now_ny.date()
    yesterday= today - timedelta(days=1)
    cutoff_pm= tz_ny.localize(datetime.combine(yesterday, time(16, 0)))
    cutoff_am= tz_ny.localize(datetime.combine(today,     time(9, 30)))

    filtered = []
    for sym in symbols:
        # ev["announce_datetime"] comes back as an ISO string from SQLite
        ann_raw = sym["announce_datetime"]
        try:
            ann = datetime.fromisoformat(ann_raw)
        except Exception:
            continue
        # ensure UTC tzinfo if missing, then convert to NY
        if ann.tzinfo is None:
            ann = ann.replace(tzinfo=pytz.UTC)
        ann = ann.astimezone(tz_ny)

        # keep only if window matches
        if (ann.date() == yesterday and ann >= cutoff_pm) \
        or (ann.date() == today     and ann <= cutoff_am):
            filtered.append(sym)

    return filtered

async def get_wsh_details(Symbols, ib=None, limit: int = 100) -> bool:
    conId = None
    events = []

    for sym in Symbols:

        contract = await subscribed_contracts.get(sym)
        startDate = arrow.now().shift(days=-2).format("YYYYMMDD")
        endDate = arrow.now().shift(days=+1).format("YYYYMMDD")
        symbol = contract.symbol
        conId = contract.conId
        logger.info(f"Getting WSH details for {symbol}...for dates {startDate} to {endDate}")
        """""
        data = WshEventData(
            filter = f'''{{
                "country": "All",
                "watchlist": ["{conId}"],
                "limit": {limit},
                "wshe_ed": "true"
            }}''',
            startDate="",
            endDate=""
        )
        """
        data = WshEventData(
            filter = f'''{{
                "country": "All",
                "watchlist": ["{conId}"],
                "limit": {limit},
                "wshe_ed": "true",
                "wshe_eps": "true",
                "wshe_fq": "true",
            }}''',
            startDate=startDate,
            endDate=endDate
        )
        events_json=None
        event_type=None
        earnings_date=None
        earnings_date= None
        earn_date_obj= None
        ann_date_obj= None
        tz_ny          = pytz.timezone("America/New_York")
        now_ny         = datetime.now(tz_ny)
        today_ny       = now_ny.date()                        # date object
        yesterday_ny   = today_ny - timedelta(days=1) 
        is_earnings= False        # date object

                

            
        events_json = await ib.getWshEventDataAsync(data)
        events = json.loads(events_json)
        await poly_db.db_connect()
        await poly_db.upsert_wsh_events(events)
        filtered=await fetch_and_filter_earnings(Symbols, events)
        return filtered


        
        processed_events = convert_dates_for_wsh_events(events)
        for event in processed_events:
            event_data = event.get("data", {})
            event_type = event.get("event_type", "")
            logger.info(f"Processing WSH event_type {event_type} for {symbol}...")
            if event_type == "wshe_eps":
                
                return True
                
                    
                
            elif "earnings_date" in event_data:
                    earnings_date = event_data["earnings_date"]
                    earn_date_obj = datetime.strptime(earnings_date, "%Y-%m-%d").date()
                    time_check=earn_date_obj - timedelta(days=1)
                    logger.info(f"Symbol: {symbol}, Earnings Date: {event_data['earnings_date']} earnings_date: {earnings_date} time_check: {time_check} ")
            if earn_date_obj == today_ny or earn_date_obj == yesterday_ny:
                
                return True
                
            
            else:
                logger.info(f"Symbol: {symbol}, Event Type: {event_type} with earnings_date: {earnings_date} and time_check: {time_check} ")
                return False
async def batch_market_data(symbols: list[str], threshold: float, ib=None):
    """Batch‑qualify contracts and fetch bars with a concurrency limit."""
    sem = asyncio.Semaphore(10)
    async def worker(sym):
        logger.info(f"Fetching {sym} data...")
        async with sem:
            # cache this per run
            c = await subscribed_contracts.get(sym)
            daily, one_min = await asyncio.gather(
                ib.reqHistoricalDataAsync(c, "", "22 D",  "1 day",  whatToShow="TRADES", useRTH=False),
                ib.reqHistoricalDataAsync(c, "", "2  D",  "1 min",  whatToShow="TRADES", useRTH=False),
            )
            # convert both to DataFrames
            df_day    = util.df(daily)
            df_min    = util.df(one_min)
            return sym, df_day, df_min

    results = await asyncio.gather(*[worker(s) for s in symbols])
    # assemble into dicts
    data = {}
    for sym, df_day, df_min in results:
        if df_day.empty: continue
        # compute yesterday’s close and 21-day avg
        yesterday_close = df_day.iloc[-2].close
        avg21 = df_day.volume[:-1].mean()
        # find announce dt for this sym
        ed, ann = await poly_db._fetch_wsh_times(sym)
        tz_ny = pytz.timezone("America/New_York")
        ann = ann.astimezone(tz_ny)
        # compute AH volumes by slicing the minute df
        yesterday = ann.date() - timedelta(days=0) if ann.date() == date.today() else ann.date()
        post = df_min[
            (df_min.date >= ann) &
            (df_min.date <= datetime.combine(yesterday, time(20,0), tzinfo=tz_ny))
        ].volume.sum()
        pre = df_min[
            (df_min.date >= datetime.combine(date.today(),    time(0,0), tzinfo=tz_ny)) &
            (df_min.date <= ann.replace(hour=9, minute=29, second=59))
        ].volume.sum()
        total_ah = pre + post
        data[sym] = {
            "yesterday_close": yesterday_close,
            "avg21": avg21,
            "pre_vol": pre,
            "post_vol": post,
            "total_ah": total_ah,
            "RVOL": (total_ah/avg21 if avg21 else 0),
        }
        logger.info(f"RVOL for {sym}: {data[sym]['RVOL']}")
    return data


async def run_rvol_pipeline(
    df: pd.DataFrame,
    threshold: float,
    ib: IB,
    poly_db: PolyDBHandler
) -> List[str]:
    # 1) validate & prep
    required = ["Symbol","Exchange","Average Volume 10 days",
                "Upcoming earnings date","Recent earnings date","Pre-market Volume"]
    for c in required:
        if c not in df.columns:
            raise KeyError(f"Missing column {c} in CSV")

    exch_map = df.set_index("Symbol")["Exchange"].to_dict()
    all_syms = df["Symbol"].tolist()
    syms=await get_wsh_details(all_syms, ib)

    # 2) filter by earnings window
    if not syms:
        logger.info("No earnings in window; nothing to do.")
        return []

    # 3) batch-fetch volumes & compute RVOL
    data = await batch_market_data(syms, threshold, ib)

    # 4) build DataFrame and filter safely
    out = pd.DataFrame.from_dict(data, orient="index")
    if "RVOL" in out.columns and not out.empty:
        out = out[out["RVOL"] >= threshold].sort_values("RVOL", ascending=False)
        filtered = out.index.tolist()
    else:
        filtered = []

    # 5) write tickers file
    tickers = ",".join(f"{exch_map[sym]}:{sym}" for sym in filtered)
    with open("pre_market_filtered_tickers.txt", "w") as f:
        f.write(tickers)

    return filtered