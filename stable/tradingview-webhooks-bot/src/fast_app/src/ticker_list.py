from math import log
import requests, csv, sqlite3, os
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from log_config import log_config, logger

# Set up logging
log_config.setup()

# Configuration
API_KEY = "LX4G4DLW7MKJ9N37"
API_URL = f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={API_KEY}"
DB_FILE = "tickers.db"
ONE_DAY = timedelta(days=1)

class TickerDataManager:
    def __init__(self, api_url: str, db_file: str):
        self.api_url = api_url
        self.db_file = db_file
        self.logger = logger
        self._init_db()
        self._load_cached_data()
        self._schedule_data_refresh()

    def _init_db(self):
        # Connect to SQLite and create tables if they don't exist.
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                exchange TEXT,
                assetType TEXT,
                ipoDate TEXT,
                delistingDate TEXT,
                status TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        self.conn.commit()

    def _get_last_update(self):
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key = 'last_update'")
        row = cur.fetchone()
        if row:
            try:
                return datetime.fromisoformat(row["value"])
            except Exception:
                return None
        return None

    def _set_last_update(self, dt: datetime):
        cur = self.conn.cursor()
        cur.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('last_update', ?)", (dt.isoformat(),))
        self.conn.commit()

    def _load_cached_data(self):
        last_update = self._get_last_update()
        if last_update is None or (datetime.now() - last_update > ONE_DAY):
            self.logger.info(f"{datetime.now()}: Data is stale or not present. Fetching new ticker data.")
            self.fetch_and_store_ticker_data()
        else:
            self.logger.info(f"{datetime.now()}: Loaded ticker data from SQLite cache. Last update: {last_update}.")

    def fetch_and_store_ticker_data(self):
        try:
            response = requests.get(self.api_url, headers={"Accept": "*/*", "Content-Type": "application/json"})
            response.raise_for_status()
            decoded_content = response.content.decode("utf-8")
            csv_lines = decoded_content.splitlines()
            reader = csv.DictReader(csv_lines)
            # Filter rows where assetType equals "Stock" (case-insensitive)
            filtered_rows = [row for row in reader if row["assetType"].strip().lower() == "stock"]

            cur = self.conn.cursor()
            # Clear existing data
            cur.execute("DELETE FROM tickers")
            # Insert new records
            for row in filtered_rows:
                cur.execute("""
                    INSERT INTO tickers (symbol, name, exchange, assetType, ipoDate, delistingDate, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get("symbol"),
                    row.get("name"),
                    row.get("exchange"),
                    row.get("assetType"),
                    row.get("ipoDate"),
                    row.get("delistingDate"),
                    row.get("status")
                ))
            self.conn.commit()
            self._set_last_update(datetime.now())
            self.logger.info(f"{datetime.now()}: Ticker data updated successfully with {len(filtered_rows)} records.")
        except Exception as e:
            self.logger.error(f"{datetime.now()}: Failed to fetch ticker data - {str(e)}")

    def _schedule_data_refresh(self):
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.fetch_and_store_ticker_data, "interval", days=1)
        scheduler.start()

    def query_ticker_data(self, query: str):
        try:
            logger.info(f"{datetime.now()}: Querying ticker data for: {query}")
            query_lower = query.lower()
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM tickers")
            rows = cur.fetchall()
            tickers = [dict(row) for row in rows]
            exact_matches = [{"label": f"{item['symbol']} - {item['name']}", "value": item["symbol"]}
                            for item in tickers if item["symbol"].lower().startswith(query_lower)]
            partial_matches = [{"label": f"{item['symbol']} - {item['name']}", "value": item["symbol"]}
                            for item in tickers if query_lower in item["symbol"].lower() and not item["symbol"].lower().startswith(query_lower)]
            logger.info(f"{datetime.now()}: Found {len(exact_matches)} exact matches and {len(partial_matches)} partial matches.")
            return exact_matches + partial_matches
        except Exception as e:
            self.logger.error(f"{datetime.now()}: Failed to query ticker data - {str(e)}")
            return []

ticker_manager = TickerDataManager(API_URL, DB_FILE)
