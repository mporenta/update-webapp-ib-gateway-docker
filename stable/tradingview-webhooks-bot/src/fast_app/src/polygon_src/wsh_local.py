# main_wsh_rvol.py
import glob
from math import log
from tracemalloc import stop
from ib_async import (
    Contract,
    Order,
    Trade,
    LimitOrder,
    StopOrder,
    TagValue,
    util,
    IB,
    MarketOrder,
    Ticker,
    WshEventData
)

from log_config import log_config, logger
from typing import *
import pandas as pd
import os, asyncio
import pandas as pd
import numpy as np
from pathlib import Path
import pytz
import arrow
import json
from wsh_events import store_events, get_events_by_ticker
from datetime import date, datetime, timedelta, time
from dotenv import load_dotenv
from poly_client import client
env_file = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=env_file)
# --- Configuration ---
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
boof_msg= os.getenv("HELLO_MSG", "BOOF! Risk exceeded, exiting.")
# Instantiate IB manager
wsh_events_dir = os.getenv("WSH_EVENTS_DIR")
if wsh_events_dir and "C:\\Users" in wsh_events_dir:
    directory = wsh_events_dir
else:
    directory = os.path.dirname(os.path.abspath(__file__))

logger.info(f"Using directory: {directory}")
rvolFactor_env = float(os.getenv("REG_RVOL_FACTOR", "0.3"))
wsh_rvolFactor = float(os.getenv("WSH_RVOL_FACTOR", "0.2"))

#self.directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"


log_config.setup()
async def start_rvol(rvolFactor: float, back_test_route=False) -> bool:
    try:
        ib_manager = IB()
        pre_rvol = PreVolWsH(ib_manager)
        logger.info("Starting pre-market volume analysis using pre_rvol")
        
        await pre_rvol.start(rvolFactor, wshEvents=True, back_test_route=False)
        if pre_rvol.ib.isConnected():
            logger.info("IB connection established successfully.")
            return True
        else:
            logger.warning("Failed to establish IB connection.")
            return False
    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")

class PreVolWsH:
    def __init__(self, ib_manager=None):
        
        self.ib = ib_manager if ib_manager else IB()
        self.entry_prices = (
            {}
        )  # key: symbol, value: {"entry_long": price, "entry_short": price, "timestamp": time} async def start(self, rvolFactor, wshEvents=False):
        self.timestamp_id = {}
        self.client_id = int(os.getenv("WSH_CLIENT_ID", "114"))
        self.ib_host = os.getenv("IB_GATEWAY_HOST", "localhost")
        self.ib_port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
        self.max_attempts = 300
        self.initial_delay = 1
        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        self.open_price = {}
        self.high_price = {}
        self.low_price = {}
        self.close_price = {}
        self.volume = {}
        self.vol_since = {}
        self.polygon_client = client
        self.non_earnings_movers: list[str] = []
        self.non_earnings_rvol = rvolFactor_env
        self.directory = directory
        self.wsh_rvolFactor = wsh_rvolFactor

        self.semaphore = asyncio.Semaphore(10)
        self.active_rtbars = {}  # Store active real-time bar subscriptions


    async def process_non_earnings_rvol(
        self,
        contract: Contract,
        avg_vol: float
    ) -> bool:
        """
        For symbols with no recent EPS, check pre‑market RVOL using a lower threshold,
        record them in self.non_earnings_movers, and return True if they pass.
        """
        # define announce_dt = yesterday at 16:00 ET (same as your earnings cutoff)
        ny = pytz.timezone("America/New_York")
        now_et = datetime.now(ny)
        if now_et.weekday() == 0:  # Monday → go back to last Friday
            ref_date = (now_et - timedelta(days=3)).date()
        else:
            ref_date = (now_et - timedelta(days=1)).date()
        announce_dt = ny.localize(datetime.combine(ref_date, time(16, 0, 0)))


        # run RVOL check with the non‑earnings threshold
        is_heavy = await self.is_high_rel_vol(
            contract,
            announce_dt,
            avg_vol,
            self.non_earnings_rvol
        )

        if is_heavy:
            logger.info(f"Non-earnings RVOL mover: {contract.symbol} with avg_vol: {avg_vol} and announce_dt: {announce_dt}")
            self.non_earnings_movers.append(contract.symbol)
        return is_heavy
    
    async def screen_after_hours_movers(
        self,    
        file_path: str,
        contract: Contract,
        rvolFactor
        
    
    ) -> list[str]:
        """
        1) Load CSV with a 'Recent earnings date' column (YYYY‑MM‑DD, America/New_York TZ)
        2) For each symbol, find matching wshe_eps events on that date
        3) Run is_high_rel_vol with rvol_factor for pre‑market volume spike
        4) For those that pass, fetch minute bars 09:30–11:00 ET and pick symbols with ≥3% move
        """
        startDate = arrow.now().shift(days=-30).format("YYYYMMDD")
        endDate = arrow.now().shift(days=+1).format("YYYYMMDD")
        symbol = contract.symbol
        conId = contract.conId
        logger.info(f"Getting WSH details for {symbol}...")
        
        limit=100
        data = WshEventData(
            filter = f'''{{
                "country": "All",
                "watchlist": ["{conId}"],
                "limit": {limit},
                "wshe_ed": "true",
                "wshe_eps": "true",
                "wshe_fq": "true",
            }}'''
        )
        events_json = await self.ib.getWshEventDataAsync(data)
        events = json.loads(events_json)
        
        await store_events(events)
        logger.info(f"Screening after-hours movers with rvolFactor: {rvolFactor} from {file_path}")
        ny = pytz.timezone("America/New_York")
        # 1) load CSV, localize to ET midnight
        df = pd.read_csv(file_path, parse_dates=["Recent earnings date"])
        df["earnings_ts"] = df["Recent earnings date"].dt.tz_localize(ny)

        movers: list[str] = []

        for _, row in df.iterrows():
            symbol      = row["Symbol"]
            earnings_ts = row["earnings_ts"]          # 00:00 ET on earnings date
            earnings_dt = earnings_ts.date()

            # 2) pull **all** wshe_eps events for that ticker
            events = await get_events_by_ticker(symbol)

            # 3) keep only EPS events on *that* date
            eps_events = [
                ev for ev in events
                if ev.event_type == "wshe_eps"
                and ev.announce_datetime
                and ev.announce_datetime.astimezone(ny).date() == earnings_dt
            ]

            if not eps_events:
                continue

            # qualify contract once
            contract = await self.get_contract(symbol)
            avg_vol  = await self.get_avg_volume(symbol)

            for ev in eps_events:
                logger.info(f"Processing EPS event for {ev.ticker} on {ev.announce_datetime}")
                # 4) check pre‑market relative volume
                if not await self.is_high_rel_vol(
                    contract=contract,
                    announce_dt=ev.announce_datetime,
                    avg_vol=avg_vol,
                    rvolFactor=wsh_rvolFactor
                ):
                    logger.info(f"Symbol: {symbol} Event Type: {ev.event_type} with earnings_date: {ev.earnings_date} and time_check: {ev.announce_datetime} - is not a mover with pre-market rVol < {rvolFactor}")
                    continue

                # 5) fetch 09:30 – 11:00 bars
                start_dt = ny.localize(datetime.combine(earnings_dt, time(9,30)))
                end_dt   = ny.localize(datetime.combine(earnings_dt, time(11,0)))

                aggs = list(self.polygon_client.list_aggs(
                    ticker=symbol,
                    multiplier=1,
                    timespan="minute",
                    from_=start_dt,
                    to=end_dt,
                    adjusted=True,
                    sort="asc",
                    limit=5000
                ))
                if not aggs:
                    logger.warning(f"No minute bars found for {symbol} from {start_dt} to {end_dt}")
                    continue
                logger.info(f"Found {len(aggs)} minute bars for {symbol} from {start_dt} to {end_dt}")
                open_p  = aggs[0].open
                close_p = aggs[-1].close
                if (close_p - open_p) / open_p >= 0.03:
                    movers.append(symbol)
                    logger.info(f"Symbol: {symbol}, movers: {movers} Event Type: {ev.event_type} with earnings_date: {ev.earnings_date} and time_check: {ev.announce_datetime} - is a mover with pre-market rVol >= {rvolFactor} and price change >= 3%")
                    break   # no need to test other events for this symbol

        
        
        if len(movers) != 0:          
            return True

        else:
            logger.info(f"No after-hours movers found with rvolFactor {rvolFactor}")
            return False
        
    async def screen_after_hours_movers_all_dates(
        self,
        contract: Contract,
        rvolFactor: float
    ) -> list[tuple[str, date]]:
        """
        For the given contract.symbol:
        1) Pull all wshe_eps events via get_events_by_ticker()
        2) For each announce_datetime:
           a) check pre‑market relative volume ≥ rvolFactor
           b) if yes, fetch 9:30–11:00 minute bars and check price move ≥ 3%
        3) Return list of (symbol, announce_date) for all such movers
        """
        symbol = contract.symbol
        ny = pytz.timezone("America/New_York")

        # qualify & avg-vol once
        contract = await self.get_contract(symbol)
        avg_vol = await self.get_avg_volume(symbol)
        if contract is None or avg_vol is None:
            logger.warning(f"Cannot screen {symbol} (no contract or avg_vol)")
            return []

        movers: list[tuple[str, date]] = []
        events = await get_events_by_ticker(symbol)
        eps_events = [ev for ev in events if ev.event_type == "wshe_eps" and ev.announce_datetime]

        for ev in eps_events:
            ann_et = ev.announce_datetime.astimezone(ny)
            ev_date = ann_et.date()
            logger.debug(f"Checking {symbol} @ {ann_et}")

            # 1) pre‑market RVOL check
            if not await self.is_high_rel_vol(contract, ev.announce_datetime, avg_vol, rvolFactor):
                logger.debug(f" symbol: {symbol} → RVOL < {rvolFactor} at {ann_et}")
                continue

            # 2) fetch 09:30–11:00 bars on the announce date
            start_dt = ny.localize(datetime.combine(ev_date, time(9,30)))
            end_dt   = ny.localize(datetime.combine(ev_date, time(11,0)))
            aggs = list(self.polygon_client.list_aggs(
                ticker=symbol,
                multiplier=1,
                timespan="minute",
                from_=start_dt,
                to=end_dt,
                adjusted=True,
                sort="asc",
                limit=5000
            ))
            if not aggs:
                logger.warning(f"No minute bars for {symbol} on {ev_date}")
                continue

            open_p, close_p = aggs[0].open, aggs[-1].close
            change = (close_p - open_p) / open_p
            logger.info(f"  → {symbol} moved {change:.2%} from {open_p} to {close_p}")

            if change >= 0.03:
                logger.info(f"If change >= 0.03  → {symbol} moved {change:.2%} from {open_p} to {close_p}")
                movers.append((symbol, ev_date))

        if movers:
            logger.info(f"After‑hours movers: {movers}")
        
        return movers

    async def get_current_ticker(self):

        try:
            logger.info("Retrieving current ticker...")
            contract = []
            tickers = self.ib.tickers()
            for ticker in tickers:
                contract = ticker.contract
                logger.debug(f"Retrieved ticker on {len(contract.symbol)}")
            # cancelled_tickers = self.ib.cancelMktData(contract)
            # logger.info(f"Cancelled market data for {cancelled_tickers} tickers.")
            return tickers

        except Exception as e:

            logger.error(f"Error retrieving current ticker: {e}")
            return None
        
    async def get_wsh_details(self, contract: Contract, limit: int = 100) -> bool:
        startDate = arrow.now().shift(days=-2).format("YYYYMMDD")
        endDate = arrow.now().shift(days=+1).format("YYYYMMDD")
        TODAY = arrow.now().format("YYYYMMDD")

        symbol = contract.symbol
        conId = contract.conId
        logger.info(f"Getting WSH details for {symbol}...for dates {startDate} to {endDate}")
        
      
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
       
    
        avg = None
        events_json = await self.ib.getWshEventDataAsync(data)
        events = json.loads(events_json)
        
        await store_events(events)
        high = False

        #await asyncio.sleep(0.2)
        ny = pytz.timezone("America/New_York")
        now_et = datetime.now(ny)
        day_of_week=now_et.weekday() 

        # figure out “yesterday” for a Mon…Fri calendar, rolling weekend → Friday
        if now_et.weekday() == 0:  # Monday
            yesterday_date = (now_et - timedelta(days=3)).date()
        else:
            yesterday_date = (now_et - timedelta(days=1)).date()

        # build a threshold of Yesterday at 16:00:00 ET
        threshold = ny.localize(datetime.combine(yesterday_date, time(16, 0, 30)))
        end_of_pre_market = ny.localize(datetime.combine(now_et, time(9, 29, 59)))

        ticker_events = await get_events_by_ticker(symbol)
        if not ticker_events:
            
            # no earnings events → do non‑earnings RVOL path
            avg = await self.get_avg_volume(contract.symbol)
            if avg:
                logger.info(f"No earnings events found for {symbol} since {threshold}, got avg volume: {avg} with day_of_week: {day_of_week} and yesterday_date: {yesterday_date}")
                await self.process_non_earnings_rvol(contract, avg)
            return False
        event_number = len(ticker_events)
        logger.info(f"Retrieved {event_number} {symbol} events for threshold {threshold}")
        if event_number != 0:

            for ev in ticker_events:
                # only EPS announcements that happened at-or-after yesterday 4pm ET
                if (
                    ev.event_type == "wshe_eps"
                    and ev.announce_datetime
                    and ev.announce_datetime >= threshold
                ):
                    logger.info(f"Processing earnings event for {ev.ticker} on {ev.announce_datetime}")

                    avg = await self.get_avg_volume(ev.ticker)
                    if avg is None:
                        logger.warning(f"No avg volume for {ev.ticker}, skipping")
                        continue

                    high = await self.is_high_rel_vol(
                        contract,                   # your Contract for ev.ticker
                        ev.announce_datetime,       # start of your 1 min bar window
                        avg,
                        rvolFactor=wsh_rvolFactor
                    )
                    logger.info(f"High rel vol: {high} for {ev.ticker} on {ev.announce_datetime} and avg vol: {avg}")

                    logger.info(
                        "%s: %s pre‑open rel‑vol ≥20%% (%.0f/%d)",
                        ev.ticker,
                        "✔️" if high else "✖️",
                        self.vol_since,
                        avg,
                    )
                else:
                    logger.info(
                        "Skipping %s EPS event at %s (before threshold of %s)",
                        ev.ticker,
                        ev.announce_datetime,
                        threshold,
                    )
            if high:
                logger.warning(f"{ev.ticker} on {ev.announce_datetime} and avg vol: {avg} - is rVol high? {high}")
                return True
            else:
                logger.info(f"{ev.ticker} on {ev.announce_datetime} and avg vol: {avg} - is rVol high? {high}")
                return False
    
    async def get_avg_volume(self, symbol: str) -> float | None:
        """
        Pull the last ~32 calendar days of daily bars from Polygon
        and return sum(vol) / count(bars).
        """
        today = datetime.now().strftime("%Y-%m-%d")
        past_date = (datetime.now() - timedelta(days=32)).strftime("%Y-%m-%d")
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        aggs = list(self.polygon_client.list_aggs(
            symbol,
            1,
            "day",
            past_date,
            yesterday,
            adjusted="true",
            sort="asc",
            limit=250,  # up to ~32 days
        ))
        if not aggs:
            return None

        total_vol = sum(a.volume for a in aggs)
        count = len(aggs)
        return total_vol / count


    async def is_high_rel_vol(
        self,
        contract,
        announce_dt: datetime,
        avg_vol: float,
        rvolFactor
    ) -> bool:
        """
        From announce_dt (tz-aware Eastern) up to one second before
        market open, fetch 1 min bars and see if sum(vol) ≥ threshold * avg_vol.
        """
        # 1) normalize to NY time
        ny = pytz.timezone("America/New_York")
        ann = announce_dt.astimezone(ny)
        logger.debug(f"Checking high relative volume for {contract.symbol} at ann: {ann} and announce_dt: {announce_dt} with avg_vol: {avg_vol} and rvolFactor: {rvolFactor}")

        # 2) decide cutoff = 09:29:59 same day if ann before open, else next day
        cutoff_clock = time(9, 29, 59)
        if ann.time() <= cutoff_clock:
            cut_date = ann.date()
        else:
            cut_date = ann.date() + timedelta(days=1)
        end_dt = ny.localize(datetime.combine(cut_date, cutoff_clock))
        logger.debug(f"Cutoff date: {cut_date} at {cutoff_clock} and end_dt: {end_dt}")
        # 2) grab your announce time (tz‐aware ET)
        announce_et: datetime = announce_dt
        announce_et_str = announce_dt.strftime("%Y-%m-%d %H:%M:%S:%z")
        # 2) convert to UTC for Polygon
        announce_millis = announce_dt.timestamp() * 1000
        announce_millis_int = int(announce_millis)

        

        start_utc = announce_dt.astimezone(pytz.UTC)
        today = datetime.now().strftime("%Y-%m-%d")
        # calculate how many seconds have elapsed from announce → now
        now_utc = datetime.now(pytz.UTC)
        secs = int((now_utc - start_utc).total_seconds())
        logger.debug(f"Start UTC: {start_utc} and now UTC: {now_utc} and secs: {secs} for {contract.symbol}")

        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end_dt,               # empty → “use current time”
            durationStr="2 D",      # span from announce up until now
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=False,                 # include pre‑/post‑market
            formatDate=1                  # tz‑aware datetimes back
        )
        logger.debug(f"Got {len(bars)} bars from {start_utc} to {end_dt} for {contract.symbol}")
        for b in bars:
            bar_date = b.date
            bar_date = bar_date.time()
            volume = b.volume
            #print(bar_date, volume)
       
        aggs_min = list(self.polygon_client.list_aggs(
            contract.symbol,
            1,
            "minute",
            start_utc,
            end_dt,
            adjusted="true",
            sort="asc",
            limit=5000,  
        ))
        if not aggs_min:
            return None
        df_one_min= pd.DataFrame(aggs_min)
        one_min_volume_total = df_one_min['volume'].sum()
        one_min_count = len(aggs_min)
        logger.debug(f"Got {len(aggs_min)} 1 min bars from {announce_et_str} to {end_dt} for {contract.symbol} at one_min_count: {one_min_count} and one_min_volume_total: {one_min_volume_total}")


        total_vol = sum(a.volume for a in aggs_min)
        count = len(aggs_min)
        logger.debug(f"Total volume 1 min: {total_vol} and count: {count} for {contract.symbol} announce_et_str: {announce_et_str} and end_dt: {end_dt} and announce_millis_int: {announce_millis_int}")
        df = util.df(bars)
        # df['date'] dtype == datetime64[ns, America/New_York]

        

        # 3) build your “09:29:59 ET” cutoff, rolling forward to the next day if announce was after that time
        open_threshold = time(9, 29, 59)
        tz = announce_et.tzinfo

        if announce_et.time() > open_threshold:
            # announcement came after today’s open → use tomorrow’s 09:29:59
            open_date = announce_et.date() + timedelta(days=1)
        else:
            # announcement came before (or during) today’s open window
            open_date = announce_et.date()

        open_et = datetime(
            open_date.year,
            open_date.month,
            open_date.day,
            9,
            29,
            59,
            tzinfo=tz
        )
        logger.debug(f"Open date: {open_date} at {open_threshold} and open_et: {open_et} and announce_et: {announce_et}")

        # 4) slice exactly the bars from announce → pre‑open
        df_window = df[(df['date'] >= announce_et) & (df['date'] < open_et)]
        logger.debug(f"Window size: {len(df_window)} bars")
        #print(df_window)

        # 5) sum up that volume
        current_vol = df_window['volume'].sum()
        self.vol_since = current_vol
        logger.debug(f"volume since {announce_et} = {self.vol_since}")

        # 6) compare to your 21‑day avg
        is_heavy = current_vol >= avg_vol * rvolFactor
        logger.info(
                f"RVOL for {contract.symbol}: {current_vol} / {avg_vol} = {current_vol / avg_vol:.2f}  on {announce_et}"
            )
        if is_heavy:
            logger.warning(
                f"is_heavy RVOL for {contract.symbol}: {current_vol} / {avg_vol} = {current_vol / avg_vol:.2f}  is_heavy: {is_heavy} on {announce_et}"
            )

        return is_heavy


    async def get_contract(self, symbol,  wshEvents=False):   
        """Fetch contract details for a given symbol."""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        qualified_contracts = await self.ib.qualifyContractsAsync(contract)
        if not qualified_contracts:
            logger.error(f"Could not qualify contract for {contract.symbol}")
            return None
        qualified_contract = qualified_contracts[0]
        logger.debug(f"Qualified contract: {qualified_contract}")
        if wshEvents:
            logger.debug(f"Getting WSH details for {qualified_contract.symbol}")
            
        logger.debug(f"Getting ticker for {qualified_contract.symbol}")
        # Uncomment the next line if you want to request market data
        #ticker: Ticker = self.ib.reqMktData(contract, genericTickList = "221,236", snapshot=False)
            
        #self.ib.reqMktData(qualified_contract, "", False, False)
        #ticker = self.ib.ticker(qualified_contract)
        #await asyncio.sleep(0.2)
        
        return qualified_contract

    async def get_historical_data(self, contract: Contract):
        """Fetch historical data for a given symbol with contract qualification and error handling."""

        try:
            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr="31 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=False,
            )
        except Exception as e:
            logger.error(f"Error retrieving historical data for {contract.symbol}: {e}")
            return []
        if bars:
            return [
                {
                    "date": bar.date,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                }
                for bar in bars
            ]
        return []

    def get_latest_csv(self):
        """Retrieve the latest CSV file from the specified self.directory."""
        csv_files = list(Path(self.directory).glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in  {self.directory}")
        logger.info(f"Found {len(csv_files)} CSV files in {self.directory}")
        return str(max(csv_files, key=os.path.getctime))

   

    def _filter_premarket(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return only rows where either “Pre-market Change %” or “Pre-market Gap %”
        is ≤ –3.0 or ≥ +3.0.
        """
        # coerce to numeric (in case there's stray “%” or missing values)
        df["Pre-market Change %"] = pd.to_numeric(df["Pre-market Change %"], errors="coerce")
        df["Pre-market Gap %"]    = pd.to_numeric(df["Pre-market Gap %"], errors="coerce")

        mask = (
            (df["Pre-market Change %"].abs() >= 3.0)
            |
            (df["Pre-market Gap %"].abs() >= 3.0)
        )
        return df[mask].copy()

    async def process_csv(self, file_path: str, rvolFactor: float, wshEvents=False):
        logger.info(f"Processing file: {file_path}")
        # 1) Load everything into a single DataFrame
        df = pd.read_csv(file_path)

        # 2) Make sure the new columns exist
        required_columns = [
            "Symbol",
            "Exchange",
            "Average Volume 10 days",
            "Pre-market Change %",
            "Pre-market Gap %",
            "Recent earnings date",
            "Upcoming earnings date"
        ]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                raise KeyError(f"Missing required column: {col}")

        # 3) Convert “Average Volume 10 days” to numeric
        df["Average Volume 10 days"] = pd.to_numeric(
            df["Average Volume 10 days"], errors="coerce"
        )

        # 4) Filter out by Pre‑market Change/GAP first:
        df_pm = self._filter_premarket(df)
        if df_pm.empty:
            logger.info("No symbols passed the Pre-market Change/Gap filter (±3%).")
            return await self.get_current_ticker()

        # 5) Parse earnings‐date columns into actual date objects (Eastern)
        ny = pytz.timezone("America/New_York")
        # coerce to datetime; errors→NaT
        df_pm["Recent earnings date"]   = pd.to_datetime(
            df_pm["Recent earnings date"], errors="coerce"
        ).dt.tz_localize(ny, ambiguous="NaT", nonexistent="NaT").dt.date
        df_pm["Upcoming earnings date"] = pd.to_datetime(
            df_pm["Upcoming earnings date"], errors="coerce"
        ).dt.tz_localize(ny, ambiguous="NaT", nonexistent="NaT").dt.date

        # 6) Determine “today” and “yesterday” in Eastern time
        now_et = datetime.now(ny)
        today   = now_et.date()
        if now_et.weekday() == 0:
            # Monday → yesterday is Friday
            yesterday = (now_et - timedelta(days=3)).date()
        else:
            yesterday = (now_et - timedelta(days=1)).date()

        # 7) Connect to IB
        logger.info("Connecting to IB...")
        await self.ib.connectAsync(
            host=self.ib_host, port=self.ib_port, clientId=self.client_id, timeout=40
        )
        await self.ib.getWshMetaDataAsync()

        # 8) Split into two buckets: earnings_relevant vs. non_earnings
        earnings_rows: list[tuple[str, float]] = []
        non_earnings_rows: list[tuple[str, float]] = []

        for idx, row in df_pm.iterrows():
            sym = row["Symbol"]
            re_date = row["Recent earnings date"]
            ue_date = row["Upcoming earnings date"]
            # if either earnings column is exactly today or yesterday, treat as “earnings” bucket
            if (re_date in (today, yesterday)) or (ue_date in (today, yesterday)):
                # use the WSH‑Rvol threshold (0.2) for these
                earnings_rows.append((sym, self.wsh_rvolFactor))
            else:
                # everything else uses the passed-in rvolFactor (likely 0.3)
                non_earnings_rows.append((sym, rvolFactor))

        # 9) For each bucket, qualify contract → check rVol via get_wsh_details()
        passed_earnings: list[str] = []
        passed_non_earnings: list[str] = []
        ib_volumes = {}
        ib_last_prices = {}

        async def _check_symbol(symbol: str, local_rvol: float):
            """
            1) qualify the contract
            2) fetch history + run get_wsh_details with local_rvol
            3) return True if get_wsh_details(...) is True
            """
            try:
                contract = await self.get_contract(symbol, wshEvents)
                if not contract:
                    logger.error(f"Failed to qualify contract for {symbol}")
                    return False, 0, 0

                hist_data, rvol_ok = await asyncio.gather(
                    self.get_historical_data(contract),
                    self.get_wsh_details(contract),  # uses self.wsh_rvolFactor internally
                )
                # even if get_wsh_details only tests with wsh_rvolFactor, we want to skip if local_rvol > wsh_rvolFactor
                # so only accept if rvol_ok AND local_rvol <= self.wsh_rvolFactor
                # (in our split above, earnings_rows have local_rvol = self.wsh_rvolFactor)
                if not rvol_ok or local_rvol > self.wsh_rvolFactor:
                    logger.warning(f"Symbol {symbol} failed rVol check with local_rvol: {local_rvol} > wsh_rvolFactor: {self.wsh_rvolFactor}")
                    return False, 0, 0

                ib_vol = hist_data[-1]["volume"] if hist_data else 0
                ib_price = hist_data[-1]["close"] if hist_data else 0
                return True, ib_vol, ib_price

            except Exception as e:
                logger.error(f"Error checking {symbol}: {e}")
                return False, 0, 0

        # 10) Run both buckets in parallel (up to semaphore limit)
        tasks = []
        for symbol, local_rvol in earnings_rows + non_earnings_rows:
            # wrap each call in the semaphore if you want to limit concurrency
            await self.semaphore.acquire()
            task = asyncio.create_task(
                _check_symbol(symbol, local_rvol)
            )
            task.add_done_callback(lambda _: self.semaphore.release())
            tasks.append((symbol, local_rvol, task))

        # collect results
        for symbol, local_rvol, task in tasks:
            ok, vol, price = await task
            ib_volumes[symbol] = vol
            ib_last_prices[symbol] = price
            if ok:
                if (symbol, local_rvol) in earnings_rows:
                    passed_earnings.append(symbol)
                else:
                    passed_non_earnings.append(symbol)

        # 11) Build DataFrame of only those that passed rVol
        passed_all = passed_earnings + passed_non_earnings
        if not passed_all:
            logger.info("No symbols passed rVol checks.")
        else:
            result = pd.DataFrame({
                "Symbol": passed_all,
                "IB_Last_Price": [ib_last_prices[s] for s in passed_all],
                "IB_Volume":    [ib_volumes[s]    for s in passed_all],
            })

            # 12) Add “Volume_Ratio” and “Volume*Price” for final sorting:
            # we still need “Average Volume 10 days” from the original df
            avg_vol_map = df.set_index("Symbol")["Average Volume 10 days"].to_dict()
            result["Average Volume 10 days"] = result["Symbol"].map(avg_vol_map)

            result["Volume_Ratio"] = result["IB_Volume"] / result["Average Volume 10 days"]
            result["Volume_Ratio"] = (
                result["Volume_Ratio"].replace([np.inf, -np.inf], np.nan).fillna(0)
            )
            result["Volume*Price"] = result["IB_Volume"] * result["IB_Last_Price"]

            # 13) Write out “earnings” and “non‑earnings” pre‑market lists:
            if passed_earnings:
                out_path = os.path.join(
                    os.path.dirname(file_path),
                    "earnings_pre_market_filtered_tickers.txt"
                )
                with open(out_path, "w") as f:
                    f.write(",".join(
                        f"{row['Exchange']}:{sym}"
                        for sym in passed_earnings
                        for _, row in df[df["Symbol"] == sym].iloc[0:1].iterrows()
                    ))

            if passed_non_earnings:
                out_path = os.path.join(
                    os.path.dirname(file_path),
                    "pre_market_filtered_tickers.txt"
                )
                with open(out_path, "w") as f:
                    f.write(",".join(
                        f"{row['Exchange']}:{sym}"
                        for sym in passed_non_earnings
                        for _, row in df[df["Symbol"] == sym].iloc[0:1].iterrows()
                    ))

            
                logger.info(f"Non‑earnings movers written to {out_path}")

            # 14) Print summary table (sorted by Volume_Ratio desc)
            sorted_df = result.sort_values("Volume_Ratio", ascending=False)
            logger.info("Volume Analysis Results:")
            print(sorted_df.to_string(index=False))
            print(f"\nTotal Stocks: {len(sorted_df)}")
            logger.info(f"Total Stocks: {len(sorted_df)}")

        # 15) Finally, return whatever tickers are currently subscribed
        return await self.get_current_ticker()


    
 
    async def process_backtest_csv(self, file_path, rvolFactor: float,  wshEvents=False):
        logger.info(f"Processing file: {file_path}")
        df = pd.read_csv(file_path)
        rvol_thresh= False

        # Ensure required columns exist
        required_columns = ["Symbol", "Exchange", "Average Volume 10 days"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column: {col}")
                raise KeyError(f"Missing required column: {col}")
        df["Average Volume 10 days"] = pd.to_numeric(
            df["Average Volume 10 days"], errors="coerce"
        )
        result = df[required_columns].copy()

        # Connect to IB
        logger.info("Connecting to IB...")
        await self.ib.connectAsync(
            host=self.ib_host, port=self.ib_port, clientId=self.client_id, timeout=40
        )
        await self.ib.getWshMetaDataAsync()
       
       
       
        for symbol in result["Symbol"]:
            try:
                logger.debug(f"Fetching historical get_contract data for {symbol}...")
                qualified_contract = await self.get_contract(symbol, wshEvents)
                if not qualified_contract:
                    logger.error(f"Failed to get contract for {symbol}")
                    continue
                #rvol_thresh=await self.screen_after_hours_movers(file_path, qualified_contract, rvolFactor)
                #logger.info(f"Fetched historical data for {symbol}: and rvol_thresh is {rvol_thresh}")
                movers=await self.screen_after_hours_movers_all_dates(qualified_contract, rvolFactor)
                
               
                logger.info(f"Fetched historical data for {symbol}: and rvol_thresh is {movers}")
               
               
            except Exception as e:
                logger.error(f"Failed to get data for {symbol}: {e}")
                ib_vol = 0
            
            
        cancel_ticker_contract = await self.get_current_ticker()
        logger.info(f"ticker list sent to: {cancel_ticker_contract}")

        return cancel_ticker_contract
    
    

    
    async def start(self, rvolFactor, wshEvents=False, back_test_route=False):
        #self.directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
        try:
            print(boof_msg)
          
         
            logger.info(f"Starting pre-market volume analysis from main with rvolFactor of {rvolFactor}...")
            latest_csv = self.get_latest_csv()
            latestCsv =await self.process_csv(latest_csv, rvolFactor,   wshEvents=wshEvents)
            
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")

    async def main(self, wshEvents=False):
        #self.directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
        try:
            print(boof_msg)
            global rvolFactor
         
            logger.info(f"Starting pre-market volume analysis from main with rvolFactor of {rvolFactor}...")
            latest_csv = self.get_latest_csv()
            logger.info(f"Latest CSV file found: {latest_csv}")
            await self.process_backtest_csv(latest_csv, rvolFactor,   wshEvents=wshEvents)
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")

if __name__ == "__main__":
    wshEvents = True
    
    ib_manager = IB()
    pre_rvol = PreVolWsH(ib_manager)
    asyncio.run(pre_rvol.start(wshEvents))

