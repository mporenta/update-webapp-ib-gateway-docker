# price.py
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
    Ticker
)
from dataclasses import dataclass
from log_config import log_config, logger
from typing import *
import pandas as pd
import os, asyncio, pytz, time
import pandas as pd
import numpy as np
from fastapi.responses import JSONResponse, HTMLResponse
from pathlib import Path
from fastapi import FastAPI, HTTPException
from datetime import datetime
from collections import deque
import math


from pydantic import BaseModel

from dotenv import load_dotenv

env_file = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=env_file)
# --- Configuration ---
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv("FASTAPI_IB_CLIENT_ID", "1111"))
IB_HOST = os.getenv("IB_GATEWAY_HOST", "127.0.0.1")
# Instantiate IB manager

rvolFactor = 0.2


log_config.setup()
async def start_rvol(rvolFactor: float, getTicker=None):
    directory = os.path.dirname(os.path.abspath(__file__))
    try:
        ib_manager = IB()
        pre_rvol = PreVol(ib_manager)
        logger.info("Starting pre-market volume analysis using pre_rvol")
        
        await pre_rvol.start(rvolFactor,  getTicker=None)
    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")

class PreVol:
    def __init__(self, ib_manager=None):
        
        self.ib = ib_manager if ib_manager else IB()
        self.entry_prices = (
            {}
        )  # key: symbol, value: {"entry_long": price, "entry_short": price, "timestamp": time}
        self.timestamp_id = {}
        self.client_id = 11
        self.host = os.getenv("WEBHOOK_IB_HOST", "172.23.49.114")
        self.port = int(os.getenv("TBOT_IBKR_PORT", "4002"))
        self.max_attempts = 300
        self.initial_delay = 1
        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        self.open_price = {}
        self.high_price = {}
        self.low_price = {}
        self.close_price = {}
        self.volume = {}

        self.semaphore = asyncio.Semaphore(10)
        self.active_rtbars = {}  # Store active real-time bar subscriptions

    async def get_current_ticker(self):

        try:
            logger.info("Retrieving current ticker...")
            contract = []
            tickers = self.ib.tickers()
            for ticker in tickers:
                contract = ticker.contract
                logger.info(f"Retrieved ticker on {len(contract.symbol)}")
            # cancelled_tickers = self.ib.cancelMktData(contract)
            # logger.info(f"Cancelled market data for {cancelled_tickers} tickers.")
            return tickers

        except Exception as e:

            logger.error(f"Error retrieving current ticker: {e}")
            return None

    async def get_contract(self, symbol, primary_exchange=None, getTicker=False):   
        """Fetch contract details for a given symbol."""
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        if primary_exchange:
            contract.primaryExchange = primary_exchange
        qualified_contracts = await self.ib.qualifyContractsAsync(contract)
        if not qualified_contracts:
            logger.error(f"Could not qualify contract for {contract.symbol}")
            return None
        qualified_contract = qualified_contracts[0]
        if getTicker:
            logger.info(f"Getting ticker for {qualified_contract.symbol}")
            # Uncomment the next line if you want to request market data
            ticker: Ticker = self.ib.reqMktData(contract, genericTickList = "221,236", snapshot=False)
            
        #self.ib.reqMktData(qualified_contract, "", False, False)
        #ticker = self.ib.ticker(qualified_contract)
        await asyncio.sleep(3)
        hist_data = await self.get_historical_data(qualified_contract)
        return hist_data

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

    def get_latest_csv(self, directory):
        """Retrieve the latest CSV file from the specified directory."""
        csv_files = list(Path(directory).glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError("No CSV files found in directory")
        return str(max(csv_files, key=os.path.getctime))

    def create_ticker_string(self, row):
        """Create a formatted ticker string."""
        return f"{row['Exchange']}:{row['Symbol']}"

    async def process_csv(self, file_path, rvolFactor: float, getTicker=None):
        logger.info(f"Processing file: {file_path}")
        df = pd.read_csv(file_path)

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
        await self.ib.connectAsync('127.0.0.1', 4002, clientId=114, timeout=120)

        ib_volumes = {}
        ib_last_prices = {}
        for symbol in result["Symbol"]:
            try:
                logger.debug(f"Fetching historical get_contract data for {symbol}...")
                hist_data = await self.get_contract(symbol)
                ib_vol = hist_data[-1]["volume"] if hist_data else 0
                ib_price = hist_data[-1]["close"] if hist_data else 0
            except Exception as e:
                logger.error(f"Failed to get data for {symbol}: {e}")
                ib_vol = 0
            ib_volumes[symbol] = ib_vol
            ib_last_prices[symbol] = ib_price
            await asyncio.sleep(0.2)  # Increased delay for better rate limiting

        result["IB_Last_Price"] = result["Symbol"].map(ib_last_prices)
        result["IB_Volume"] = result["Symbol"].map(ib_volumes)
        result["Volume_Ratio"] = result["IB_Volume"] / result["Average Volume 10 days"]
        result["Volume_Ratio"] = (
            result["Volume_Ratio"].replace([np.inf, -np.inf], np.nan).fillna(0)
        )
        result["Volume*Price"] = result["IB_Volume"] * result["IB_Last_Price"]

        # Filter results by rvolFactor and sort in descending order
        result = result[result["Volume_Ratio"] >= rvolFactor].sort_values(
            "Volume_Ratio", ascending=False
        )

        # result = result[result['Volume_Ratio'] >= rvolFactor].sort_values('Volume*Price', ascending=False)
        # result = result[result['Volume_Ratio'] >= rvolFactor].sort_values('Symbol', ascending=False)
        logger.info("Volume Analysis Results:")
        print(result.to_string(index=False))
        total_stocks = len(result)
        print(f"\nTotal Stocks: {total_stocks}")
        logger.info(f"Total Stocks: {total_stocks}")

        # Create formatted ticker string and write to a .txt file
        ticker_string = ",".join(result.apply(self.create_ticker_string, axis=1))
        output_path = os.path.join(
            os.path.dirname(file_path), "pre_market_filtered_tickers.txt"
        )
        with open(output_path, "w") as f:
            f.write(ticker_string)
        logger.info(f"Filtered ticker list written to: {output_path}")
        cancel_ticker_contract = await self.get_current_ticker()
        return cancel_ticker_contract

    async def start(self, rvolFactor: float, getTicker=None):
        directory = os.path.dirname(os.path.abspath(__file__))
        try:
            logger.info("Starting pre-market volume analysis...")
            latest_csv = self.get_latest_csv(directory)
            await self.process_csv(latest_csv, rvolFactor,  getTicker=None)
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")

    async def main(self,  getTicker=None):
        directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
        #directory = os.path.dirname(os.path.abspath(__file__)) latest_csv = self.get_latest_csv(directory)
        try:
            global rvolFactor
         
            logger.info(f"Starting pre-market volume analysis from main with rvolFactor of {rvolFactor}...")
            latest_csv = self.get_latest_csv(directory)
            await self.process_csv(latest_csv, rvolFactor,  getTicker=None)
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")

if __name__ == "__main__":
    log_config.setup()
    ib_manager = IB()
    pre_rvol = PreVol(ib_manager)
    asyncio.run(pre_rvol.main())

