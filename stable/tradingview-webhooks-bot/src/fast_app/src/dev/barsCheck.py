from math import log
import re
import pandas as pd
import numpy as np
from ib_async import *
import asyncio
from types import SimpleNamespace
import datetime
import json
from log_config import log_config, logger
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from dataclasses import dataclass
import os
from timestamps import current_millis, get_timestamp, is_market_hours, is_weekend
from typing import List, Dict
from pandas_ta.overlap import ema 
from pandas_ta.volatility import  atr
from my_util import *
from models import OrderRequest, AccountPnL, TickerResponse, QueryModel, WebhookRequest, TickerSub
from ib_orders import create_parent_order


log_config.setup()


class PriceDataNew:
    def __init__(self, ib_manager=None):
        self.ib_manager = ib_manager
        self.ib = ib_manager.ib if ib_manager else None
        self.client_id = 76
        self.host = "127.0.0.1"
        self.port = 4002
        self.max_attempts = 300
        self.entry_price=0.0  # Initialize entry_price to 0.0
        self.initial_delay = 1
        self.shutting_down = False
        self.qualified_contract_cache = {}
        # Fix: Ensure we have consistent attribute names
        self.close_price: Dict[str, List[float]] = {}  # Not close_price
        self.ask_price: Dict[str, float] = {}
        self.bid_price: Dict[str, float] = {}
        self.ticker = {}
        self.ticker_mk = {}
        self.open_orders: Dict[str, List[Order]] = {}  # Store open orders by symbol
        self.nine_ema: Dict[str, List[float]] = {}
        # Dictionaries for volatility stop and uptrend
        self.vstop_dict: Dict[str, List[float]] = {}
        self.uptrend_dict: Dict[str, List[bool]] = {}
        # Initialize other necessary attributes
        self.timestamp_id = {}
        self.twenty_ema = {}
        self.triggered_bracket_orders= {}
        self.orders_dict_oca_symbol = {}  # Store OCA orders by symbol
        self.ocaGroup ={}
        

    async def connect(self) -> bool:
        """
        Connect to IB Gateway and establish necessary data subscriptions.
        """
        attempt = 0
        while attempt < self.max_attempts:
            try:
                logger.info(f"Connecting to IB at {self.host}:{self.port} : client_id:{self.client_id}...")
                await self.ib.connectAsync(host=self.host, port=self.port, clientId=self.client_id, timeout=40)
        
                if self.ib.isConnected():
                    logger.info("Connected to IB Gateway")
                
                    
                
                    try:
                       
                    
                       # Set up event handlers
                       self.ib.disconnectedEvent += self.on_disconnected
                       self.ib.errorEvent += self.error_code_handler
                       self.ib.pendingTickersEvent += self.onPendingTickers  # Add this to handle ticker updates
                       
                
                    except Exception as e:
                        logger.error(f"Error getting data during connection: {e}")
                        # Even if getting data fails, we're still connected
                
                    return True
                else:
                    logger.warning("Failed to connect to IB Gateway")
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {e}")
        
            attempt += 1
            await asyncio.sleep(self.initial_delay)
    
        logger.error("Max reconnection attempts reached")
        return False
    async def print_prices(self, contract: Contract) -> bool:
        logger.info(f"Current Prices for {contract.symbol}:")
        symbol= contract.symbol if contract else None
        if symbol not in self.close_price:
            logger.warning(f"No close prices available for {contract.symbol}.")
            return False
        logger.warning(f"  Close Prices: {self.close_price[symbol][-1] if symbol in self.close_price else 'N/A'}")
        if symbol not in self.ask_price or symbol not in self.bid_price:
            logger.warning(f"No ask/bid prices available for {contract.symbol}.")
            return False
        logger.warning(f"  Ask Price: {self.ask_price[symbol] if symbol in self.ask_price else 'N/A'}")
        logger.warning(f"  Bid Price: {self.bid_price[symbol] if symbol in self.bid_price else 'N/A'}")

        self.close_price[symbol]
        self.ask_price[symbol]
        self.bid_price[symbol]
        self.ticker[symbol]
        self.nine_ema[symbol]
        
        self.vstop_dict[symbol]
        self.uptrend_dict[symbol]
        logger.info(f"Current Prices for {contract.symbol}:")
        logger.info(f"  Close Prices: {self.close_price[symbol] if symbol in self.close_price else 'N/A'}")
        logger.info(f"  Ask Price: {self.ask_price[symbol] if symbol in self.ask_price else 'N/A'}")
        logger.info(f"  Bid Price: {self.bid_price[symbol] if symbol in self.bid_price else 'N/A'}")
        logger.info(f"  Ticker: {self.ticker[symbol] if symbol in self.ticker else 'N/A'}")
        logger.info(f"  Nine EMA: {self.nine_ema[symbol] if symbol in self.nine_ema else 'N/A'}")
        logger.info(f"  Volatility Stop: {self.vstop_dict[symbol] if symbol in self.vstop_dict else 'N/A'}")
        logger.info(f"  Uptrend: {self.uptrend_dict[symbol] if symbol in self.uptrend_dict else 'N/A'}")
        return True

    async def get_req_id(self):
        try:
            return self.ib.client.getReqId()
        except Exception as e:
            logger.error(f"Error getting request ID: {e}")
            return None



    async def on_disconnected(self):
        try:
            logger.warning("Disconnected from IB.")
            # Check the shutdown flag and do not attempt reconnect if shutting down.
            if self.shutting_down:
                logger.info("Shutting down; not attempting to reconnect.")
                return
            await asyncio.sleep(1)
            if not self.ib.isConnected():
                logger.warning("on disconnected Attempting to reconnect to IB...")
                await self.connect()
        except Exception as e:
            

            logger.error(f"Error in on_disconnected: {e}")
            # Attempt to reconnect if not shutting down
    async def error_code_handler(
        self, reqId: int, errorCode: int, errorString: str, contract: Contract
    ):
        logger.warning(f"Error for reqId {reqId}: {errorCode} - {errorString} - contract: {contract.symbol if contract else 'N/A'}")

    async def atr_ta(self, data, length):
        try:
            
            logger.info(f"Calculating Average True Range (ATR) with pandas_ta at length {length}...")
            
            atr_pandas_ta = atr(data['high'], data['low'], data['close'], length)
            logger.info(f"ATR calculated for {length} periods: {atr_pandas_ta.tail(5)}")  # Debugging line to check ATR values
            
            #logger.info("ATR Array: ", atr_pandas_ta.tail(5))  # Debugging line to check ATR values
            return atr_pandas_ta
        except Exception as e:
            logger.error(f"Error calculating ATR: {e}")
            raise e
        
    async def vol_stop(self, data, atrlen, atrfactor):
        try:
            logger.info("Calculating Volatility Stop...")
            
            # Calculate average of high and low prices
            avg_hl = (data['high'] + data['low']) / 2
            logger.info(f"Average High-Low calculated: {avg_hl.tail(5)}")
            atr_pandas_ta = atr(data['high'], data['low'], data['close'], atrlen)
            nine_ema = ema(data['close'], length=9).tolist() 
            logger.info(f"ATR calculated for {atrlen} periods: {atr_pandas_ta.tail(5)}") 
            
            atrM = atr_pandas_ta * atrfactor
            atrM = atrM.fillna(0)  # Fill NaN values with 0
            #close = data['close']
            close=avg_hl

            #logger.info("atrM: ", atrM.tail(5))  # Debugging line to check ATR values

            max_price = close.copy()
            min_price = close.copy()
            stop = pd.Series(np.nan, index=close.index)
            uptrend = pd.Series(True, index=close.index)

            for i in range(1, len(close)):
                max_price.iloc[i] = max(max_price.iloc[i-1], close.iloc[i])
                min_price.iloc[i] = min(min_price.iloc[i-1], close.iloc[i])
                stop.iloc[i] = stop.iloc[i-1] if not np.isnan(stop.iloc[i-1]) else close.iloc[i]

                if uptrend.iloc[i-1]:
                    stop.iloc[i] = max(stop.iloc[i], max_price.iloc[i] - atrM.iloc[i])
                else:
                    stop.iloc[i] = min(stop.iloc[i], min_price.iloc[i] + atrM.iloc[i])

                uptrend.iloc[i] = close.iloc[i] - stop.iloc[i] >= 0

                if uptrend.iloc[i] != uptrend.iloc[i-1] and i > 1:
                    max_price.iloc[i] = close.iloc[i]
                    min_price.iloc[i] = close.iloc[i]
                    stop.iloc[i] = max_price.iloc[i] - atrM.iloc[i] if uptrend.iloc[i] else min_price.iloc[i] + atrM.iloc[i]

            return stop, uptrend, nine_ema
        except Exception as e:
            logger.error(f"Error calculating Volatility Stop: {e}")
            raise e
    
    async def get_bars_ib(self, contract, barSizeSetting=None, atrFactor=None):
        """
        Fetches historical bars for technical analysis and sets up market data subscription.
        The primary purpose is to get data for vol_stop() calculation and other technical indicators.
        """
        try:
         
            
            logger.info(f"Fetching bars for {contract.symbol} with barSizeSetting={barSizeSetting} and atrFactor={atrFactor}...")
            symbol = contract.symbol
            qualified_contract = await self.qualify_contract(contract)
            entry_price = 0.0  # Initialize entry_price to 0.0
           

    
            # Check if qualified_contract is valid before using it
            if not qualified_contract:
                logger.error(f"Failed to qualify contract for {contract.symbol}.")
                return None, None, None, None
            
            logger.info(f"Qualified contract for {contract.symbol}: {qualified_contract}")

            # Step 1: Get historical bars for technical analysis (vol_stop calculation)
            logger.info(f"Requesting historical data for {contract.symbol}...")
            bars = await self.ib.reqHistoricalDataAsync(
                qualified_contract,
                endDateTime='',  # '' means now
                durationStr='1 D',
                barSizeSetting='1 min' if not barSizeSetting else barSizeSetting,
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
        
            if not bars or len(bars) == 0:
                logger.warning(f"No historical data returned for {contract.symbol}.")
                return None, None, None, None
            
            # Convert bars to DataFrame for technical analysis
            df = util.df(bars)
            if df.empty:
                logger.warning(f"Empty DataFrame for {contract.symbol} after conversion.")
                return None, None, None, None
            logger.debug(f"Fetched {len(df)} historical bars for {contract.symbol} with raw bars from last close: {df}")
            # Store the close prices as a LIST, not a Series
            self.close_price[symbol] = df['close'].tolist()
            entry_price = self.close_price[symbol][-1] if self.close_price[symbol] else None
            logger.info(f"Historical data for {contract.symbol}: {len(df)} bars")
            logger.info(f"Last close price entry_price: {entry_price}")
            self.entry_price=float(entry_price)  # Ensure entry_price is a float for calculations
            logger.info(f"Last close price for {contract.symbol}: {entry_price + 10}")
        
            # Step 2: Set up market data subscription for current prices
            logger.info(f"Requesting market data for {contract.symbol}...")
        
            # Request market data (subscription)
            self.ticker_mk[symbol] = self.ib.reqMktData(qualified_contract, '', False, False)
        
            # Store the ticker for later reference
            self.ticker[symbol] = self.ib.ticker(qualified_contract)
            await asyncio.sleep(1.5)
        
            # Get real-time bar data
            logger.info(f"Creating new real-time bar subscription for {contract.symbol}")
            rtBars = self.ib.reqRealTimeBars(
                contract=qualified_contract,
                barSize=5,
                whatToShow="TRADES",
                useRTH=False,
                realTimeBarsOptions=[],
            )
            await asyncio.sleep(1.5)
            if rtBars:
                logger.info(f"Real-time bars subscription created for {contract.symbol}.")
                df = util.df(rtBars)
                if not df.empty:
                    rtBars.updateEvent += lambda bars, hasNewBar: asyncio.create_task(
                        self.onBarUpdate(bars, hasNewBar, qualified_contract)
                    )
                    #rtBars.updateEvent += self.onBarUpdate
                    
                    logger.debug(f"Real-time bars DataFrame for {contract.symbol}: {df.tail(5)}")
                    self.close_price[symbol] = df['close'].tolist()
                    entry_price = self.close_price[symbol][-1] if self.close_price[symbol] else entry_price

        
            # Update bid and ask prices from the ticker
            if hasattr(self.ticker[symbol], 'bid') and self.ticker[symbol].bid:
                self.bid_price[symbol] = self.ticker[symbol].bid
            if hasattr(self.ticker[symbol], 'ask') and self.ticker[symbol].ask:
                self.ask_price[symbol] = self.ticker[symbol].ask
            if hasattr(self.ticker[symbol], 'last') and self.ticker[symbol].last:
                # If we have a real-time last price, update the last close price
                self.close_price[symbol][-1] = self.ticker[symbol].last
                entry_price = self.ticker[symbol].last
            
            logger.info(f"Market data for {contract.symbol}: Bid={self.bid_price.get(symbol)}, Ask={self.ask_price.get(symbol)}, Last={entry_price}")
        
            # Step 3: Calculate vol_stop and other technical indicators
            factor = 1.5 if atrFactor is None else atrFactor
            vStop, uptrend, nine_ema = await self.vol_stop(df, 20, factor)

            df['vStop'] = vStop
            df['uptrend'] = uptrend
            self.nine_ema[symbol] = nine_ema
        
            # Store the technical indicator values
            self.vstop_dict[symbol] = vStop.tolist()
            self.uptrend_dict[symbol] = uptrend.tolist()

            # Log the technical analysis results
            last_rows = df.tail(5).reset_index()
            formatted_table = (
                "\n" + "="*80 + "\n" +
                "VOLATILITY STOP ANALYSIS (Last 5 bars)\n" +
                "="*80 + "\n" +
                f"{'Timestamp':<20} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'vStop':>8} {'Trend':>8}\n" +
                "-"*80 + "\n"
            )

            for _, row in last_rows.iterrows():
                trend = "⬆️ UP" if row['uptrend'] else "⬇️ DOWN"
                formatted_table += (
                    f"{row['date'].strftime('%Y-%m-%d %H:%M'):<20} "
                    f"{row['open']:>8.2f} {row['high']:>8.2f} {row['low']:>8.2f} {row['close']:>8.2f} "
                    f"{row['vStop']:>8.2f} {trend:>8}\n"
                )
            formatted_table += "="*80
            logger.info("Volatility Stop calculated and added to DataFrame")
            logger.info(formatted_table)
     
            
                
            contract_json = {
                "symbol": symbol,
                "secType": qualified_contract.secType,
                "currency": qualified_contract.currency,
                "exchange": qualified_contract.exchange,
                "localSymbol": qualified_contract.localSymbol,
                "primaryExchange": qualified_contract.primaryExchange,
                "conId": qualified_contract.conId,
                "lastTradeDateOrContractMonth": qualified_contract.lastTradeDateOrContractMonth,
                "vstop": self.vstop_dict[symbol][-1] if symbol in self.vstop_dict else None,
                "uptrend": self.uptrend_dict[symbol][-1] if symbol in self.uptrend_dict else None,
                "nine_ema": self.nine_ema[symbol][-1] if symbol in self.nine_ema else None,
                "close_price":self.entry_price,
                "timestamp": await get_timestamp()
            }
           
            # Return the technical analysis results
            return jsonable_encoder(contract_json)
        
        except Exception as e:
            logger.error(f"Error in get_bars_ib: {e}")
            raise e
    async def onBarUpdate(self, bars, hasNewBar, contract: Contract = None):
        """
        Callback for handling real-time bar updates.
        Updates the close price in our dictionary.
        """
        try:
            symbol= None
            if not hasNewBar or not bars:
                return

            symbol = bars.contract.symbol if hasattr(bars, 'contract') and bars.contract else None
            if not symbol:
                symbol = contract.symbol if contract else None

            df = util.df(bars)
            if df.empty:
                logger.warning(f"Empty DataFrame received for {symbol}.")
                return

            # Update the close price list
            self.close_price[symbol].append(df['close'].iloc[-1])
            logger.debug(f"Updated close price for {symbol}: {self.close_price[symbol][-1]}")

        except Exception as e:
            logger.error(f"Error in onBarUpdate for {symbol}: {e}")
    async def onPendingTickers(self, tickers):
        """
        Callback for handling ticker updates.
        Updates the bid/ask prices and other market data in our dictionaries.
        """
        try:
            if not tickers:
                return
            
            if isinstance(tickers, list):
                for ticker in tickers:
                    logger.debug(f"Processing ticker list: {ticker}")
                    await self._process_ticker(ticker)
            else:
                logger.debug(f"Processing tickers: {tickers}")
                await self._process_ticker(tickers)
        except Exception as e:
            logger.error(f"Error in onPendingTickers: {e}")

    async def _process_ticker(self, ticker):
        """
        Process a single ticker update.
        """
        try:
            symbol= ticker.contract.symbol if hasattr(ticker, 'contract') and ticker.contract else None
            # Log ticker type and representation to help debug
            logger.debug(f"ticker type: {type(ticker)}, ticker object: {ticker}")
        
            # Extract the ticker from a set if necessary
            if isinstance(ticker, set) and len(ticker) == 1:
                ticker = next(iter(ticker))
                logger.debug(f"Extracted ticker from set: {type(ticker)}")
        
            # Now check if ticker has contract attribute
            if not ticker or not hasattr(ticker, 'contract') or not ticker.contract:
                return
        
            # Get the contract directly as an attribute
            contract_t = ticker.contract
            symbol_t = contract_t.symbol
        
            logger.debug(f"Processing ticker for self.close_price[symbol][-1]) {self.close_price[symbol][-1]} ...")
        
            # Update our dictionaries with the latest market data
            if hasattr(ticker, 'bid') and ticker.bid is not None and ticker.bid > 0:
                self.bid_price[symbol] = ticker.bid
                logger.debug(f"Updated bid price for {ticker.contract.symbol}: {ticker.bid}")
        
            if hasattr(ticker, 'ask') and ticker.ask is not None and ticker.ask > 0:
                self.ask_price[symbol] = ticker.ask
                logger.debug(f"Updated ask price for {ticker.contract.symbol}: {ticker.ask}")
        
            if hasattr(ticker, 'last') and ticker.last > 0 and symbol in self.close_price:
                self.close_price[symbol][-1] = ticker.last
                entry_price = self.close_price[symbol][-1]
                logger.debug(f"Updated last price for {ticker.contract.symbol}: {entry_price} ticker.last {ticker.last}")
        
            logger.debug(f"Ticker update for {ticker.contract.symbol}: Bid={self.bid_price.get(symbol, 'N/A')}, Ask={self.ask_price.get(symbol, 'N/A')}, Last={ticker.last if hasattr(ticker, 'last') else 'N/A'}")
        except Exception as e:
            logger.error(f"Error processing ticker {ticker.contract.symbol if hasattr(ticker, 'contract') else 'unknown'}: {e}")
            logger.error(f"Ticker details: {ticker}")

    async def print_prices(self, contract):
        """
        Print current prices in a safe way.
        """
        logger.info(f"Current Prices for {contract.symbol}:")
        symbol= contract.symbol if contract else None
    
        # Check close prices
        if symbol not in self.close_price or not self.close_price[symbol]:
            logger.warning(f"No close prices available for {contract.symbol}.")
        else:
            logger.info(f"  Last Close Price: {self.close_price[symbol][-1]}")
    
        # Check bid/ask prices
        logger.info(f"  Ask Price: {self.ask_price.get(symbol, 'N/A')}")
        logger.info(f"  Bid Price: {self.bid_price.get(symbol, 'N/A')}")
    
        # Check volatility stop data
        if symbol in self.vstop_dict and self.vstop_dict[symbol]:
            logger.info(f"  Last vStop: {self.vstop_dict[symbol][-1]}")
        else:
            logger.warning(f"No volatility stop data for {contract.symbol}")
        
        # Check trend data
        if symbol in self.uptrend_dict and self.uptrend_dict[symbol]:
            trend = "⬆️ UP" if self.uptrend_dict[symbol][-1] else "⬇️ DOWN"
            logger.info(f"  Current Trend: {trend}")
        else:
            logger.warning(f"No trend data for {contract.symbol}")
        
        return True
    
    async def qualify_contract(self, contract: Contract):
        try:
            qualified_contract = None
            symbol = contract.symbol
            logger.info(f"Qualifying contract: {symbol}...")
        
            # Check if symbol exists in the cache dictionary first
            if symbol in self.qualified_contract_cache:
                logger.info(f"Contract {symbol} is in cache, returning...")
                qualified_contract = self.qualified_contract_cache[symbol]
                return qualified_contract
            
            # If we get here, the contract isn't in the cache, so qualify it
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                logger.error(f"Contract qualification failed for {symbol}")
                return None
            
            qualified_contract = qualified[0]
            # Store in cache for future use
            self.qualified_contract_cache[symbol] = qualified_contract
            logger.info(f"Qualified {symbol} and stored in cache")

            return qualified_contract
        except Exception as e:
            logger.error(f"Error qualifying contract {contract.symbol}: {e}")
            return None

    async def graceful_disconnect(self):
        try:
           
            self.shutting_down = True  # signal shutdown
            if self.ib.isConnected():
                logger.info("Unsubscribing events and disconnecting from IB...")
                if hasattr(self.ib, 'disconnectedEvent'):
                    self.ib.disconnectedEvent -= self.on_disconnected
                if hasattr(self.ib, 'errorEvent'):
                    self.ib.errorEvent -= self.error_code_handler
                self.ib.disconnect()
                logger.info("Disconnected successfully.")
            if not self.ib.isConnected():
                logger.info("IB is already disconnected.")
                return True
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
            return False

    async def format_ticker_details_table(self, ticker_details):
        df = pd.DataFrame(
            {"Parameter": ticker_details.keys(), "Value": ticker_details.values()}
        )
        for idx, param in enumerate(df["Parameter"]):
            value = df.loc[idx, "Value"]
            if isinstance(value, (int, float)):
                if "price" in param or "Balance" in param:
                    df.loc[idx, "Value"] = f"${value:.2f}"
                elif "percentage" in param or "Ratio" in param:
                    df.loc[idx, "Value"] = f"{value:.2f}"
        return f"\n{'='*50}\nTICKER DETAILS\n{'='*50}\n{df.to_string(index=False)}\n{'='*50}"

    async def create_order(
        self,
        web_request: OrderRequest,
        contract: Contract,
        submit_cmd: bool,
        meanReversion: bool,
        stopLoss: float,
        prices: Dict[str, float] = None
        
    ):
        trade= await create_parent_order(web_request, contract, self.ib, submit_cmd, meanReversion, stopLoss, prices)
        try:
            order_details = {}
            trade= None
            entry_price= 0.0
            entryPrice = web_request.entryPrice
            stopLoss = web_request.stopLoss if web_request.stopLoss > 0 else 0.0

            reqId= None
            qualified_contract= None
            vstop_dict= None
            uptrend_dict= None
            nine_ema= None
            timeframe= web_request.timeframe
            
            logger.warning( f"web_request  is {web_request}")
            barSizeSetting=None
            logger.warning( f"barSizeSetting  is {barSizeSetting} for  {web_request.ticker} and stop type {web_request.stopType}")
            symbol=contract.symbol if contract else web_request.ticker
            if self.qualified_contract_cache.get(symbol):
                qualified_contract=self.qualified_contract_cache.get(symbol)
            if qualified_contract is None:
                logger.warning(f"Qualified contract not found for {contract.symbol}.")

                qualified_contract = await self.qualify_contract(contract)
            if timeframe is not None:    
                barSizeSetting= await convert_pine_timeframe_to_barsize(timeframe)
            stopType = web_request.stopType if web_request.stopType else "vStop"
            reqId = await self.get_req_id()
            close_price = float(self.close_price[symbol][-1])
            vstop=self.vstop_dict[symbol] 
            uptrend=self.uptrend_dict[symbol]
          

            orderAction = ""
            if web_request.orderAction is not None:
                orderAction = web_request.orderAction.upper()
            if entryPrice > 1.0:
                entry_price = entryPrice
            if orderAction not in ["BUY", "SELL"]:
                orderAction = "BUY" if uptrend[-1] else "SELL"
            logger.info(f"Determined order action for {contract.symbol}: {orderAction} (uptrend: {'⬆️ UP' if uptrend else '⬇️ DOWN'})")     
            if self.ask_price[symbol] >1.0 or self.bid_price[symbol] >1.0:
                logger.warning(f"Ask or Bid price are available for {contract.symbol}")
                
                entry_price = self.ask_price[symbol] if orderAction == "BUY" else  self.bid_price[symbol]

            logger.info(f" Determined entry price for {contract.symbol}: {entry_price} (from ask/bid: {self.ask_price[symbol] if orderAction == 'BUY' else self.bid_price[symbol]})")
            if entry_price <= 1.0:
                logger.info(f"Entry price is None for {contract.symbol}, trying to get from close prices...")
                if close_price is not None:
                    logger.info(f"Using close price for entry: {close_price}")
                    entry_price = self.entry_price
                
                
                if entry_price  <= 1.0:
                    logger.error(f"Failed to get entry price for {contract.symbol}.")
                    raise ValueError("Entry price is None after fetching bars.")
            logger.info(f" Entry price for {contract.symbol}: {entry_price} (from ask/bid: {self.ask_price[symbol] if orderAction == 'BUY' else self.bid_price[symbol]})")  
        
            order=[]
            
            
            unixTimestamp= str(current_millis())
            self.ocaGroup[qualified_contract.symbol] = f"exit_{qualified_contract.symbol}_{unixTimestamp}"
            ocaGroup = self.ocaGroup[qualified_contract.symbol]
            stop_loss_price=0.0
            quantity = 0.0
            symbol = web_request.ticker
            take_profit_price = None
            stopType = web_request.stopType
            kcAtrFactor = web_request.kcAtrFactor
            vstopAtrFactor = web_request.vstopAtrFactor
            atrFactor = vstopAtrFactor if stopType == "vStop" else kcAtrFactor
            action = orderAction
            accountBalance = web_request.accountBalance
            riskPercentage = web_request.riskPercentage
            rewardRiskRatio = web_request.rewardRiskRatio
           
            
        
            try:
                logger.info(f" Setting up entry price and timestamp data for {contract.symbol}...with stopLoss {stopLoss} = web_request.stopLoss {web_request.stopLoss} and stopType {stopType} and action {action}")
                if stopLoss > 0.0:
                    stop_loss_price = stopLoss
                if barSizeSetting is None:
                    barSizeSetting = '1 min'
                else:
                    barSizeSetting=web_request.timeframe
                if vstop is not None and stopType == "vStop" and stop_loss_price <= 0:
                    stop_loss_price = round(vstop[-1] , 2) - 0.02 if orderAction == "BUY" else round(vstop[-1] , 2) + 0.02
                
                

                logger.info(f"Entry price for {qualified_contract.symbol}: {entry_price}")
            except  Exception as e:
                logger.error(f"Error setting up entry price or timestamp data for {contract.symbol}: {e}")
                raise e
            # In price.py, around line 425-430
            try:
                logger.info(f" Calculating position size for {qualified_contract.symbol}...")
                quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk= await compute_position_size(
                        entry_price,
                        stop_loss_price,
                        accountBalance,
                        riskPercentage,
                        rewardRiskRatio,
                        action,
                    )
                

               
                # Use calculated entry price if available
            except Exception as e:
                
                logger.error(f"Error calculating position for {qualified_contract.symbol}: {e}")
                raise e
          
            logger.info(f"Calculated position for {qualified_contract.symbol}:  quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk { quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk}")


            try:
                logger.info(f" Extracting position details for {contract.symbol}...")
                if web_request.quantity > 0:
                    quantity = web_request.quantity
                elif quantity <= 0:
                    quantity=quantity    
                # Validate critical values before proceeding
                if take_profit_price is None:
                    logger.error(f"Take profit price is None for {contract.symbol}")
                    raise ValueError("Take profit price is None")
                if stop_loss_price is None:
                    logger.error(f"Stop loss price is None for {contract.symbol}")
                    raise ValueError("Stop loss price is None")
                if quantity is None:
                    logger.error(f"Quantity is None for {contract.symbol}")
                    raise ValueError("Quantity is None")
                logger.info(
                    f"Calculated position for {contract.symbol}: jengo entry={entry_price}, stop_loss_order={stop_loss_price}, take_profit_order={take_profit_price}, quantity={quantity}"
                )
            
            
                self.timestamp_id[symbol] = {
                    "unixTimestamp": str(current_millis()),
                    "stopType": stopType,
                    "action": action,
                    "riskPercentage": riskPercentage,
                    "atrFactor": atrFactor,
                    "rewardRiskRatio": rewardRiskRatio,
                    "accountBalance": accountBalance,
                    "stopLoss": stop_loss_price,
                    "barSizeSetting": barSizeSetting,  
                }
                logger.info(f" Timestamp data for {contract.symbol}: {self.timestamp_id[symbol]}")
                if action not in ["BUY", "SELL"]:
                    raise ValueError("Order Action is None")
                
                timestamp_data = self.timestamp_id[symbol]
                logger.warning(f"Timestamp data for {contract.symbol}: {timestamp_data}")
            
                logger.info(
                    f"Qualified contract for: {qualified_contract.symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}"
                )
                algo_order = LimitOrder(
                action=action,
                totalQuantity=quantity,
                lmtPrice=round(entry_price, 2),
                tif="GTC",
                transmit=True,
                algoStrategy="Adaptive",
                outsideRth=False,
                orderRef=f"Adaptive - {contract.symbol}",
                algoParams=[TagValue("adaptivePriority", "Urgent")],
             
                )
             

                # Step 3: Place the bracket order
                limit_order = LimitOrder(
                        action,
                        totalQuantity=quantity,
                        lmtPrice=round(entry_price, 2),
                        tif="GTC",
                        outsideRth=True,
                        transmit=True,
                        orderId=reqId,
                
                    )
                logger.info(
                        f"Afterhours limit order for {contract.symbol} at price jengo {entry_price}"
                    )
                
            except Exception as e:

                logger.error(f"Error extracting position details for {contract.symbol}: {e}")
                raise e    
            ################################################################################################ 1
            market_hours = is_market_hours()
            if market_hours:
                logger.info(f"Market Hours Algo Limit order for {contract.symbol} at price jengo {entry_price}")
                order = algo_order
            else:
                logger.info(f"Afterhours limit order for {contract.symbol} at price jengo {entry_price}: {limit_order}")
                order = limit_order


            
            logger.info(
                f"Placing order with orderType {order.orderType} action: {order.action} order: {order.totalQuantity} @ {entry_price}"
            )
           
            
            if submit_cmd:
                p_ord_id = order.orderId
                
                logger.debug(f"jengo bracket order webhook p_ord_id: {p_ord_id}")

                # Determine the type of parent order based on market hours

                logger.debug(
                    f"Market hours - creating MarketOrder for{contract.symbol}"
                )
                parent = order
                logger.debug(f"Parent Order - creating MarketOrder for{parent}")

                # Place the orders

                trade = self.ib.placeOrder(qualified_contract, parent)
                logger.info(f"Placing Parent Order: {parent} for {qualified_contract.symbol}")
                if trade:
                    order_details = {
                        "ticker": qualified_contract.symbol,
                        "orderAction":action,
                        "orderType":trade.order.orderType,
                        "entry_price": entry_price,
                        "stop_loss_price": stop_loss_price,
                        "take_profit_price": take_profit_price,
                        "quantity": quantity,
                        "rewardRiskRatio": web_request.rewardRiskRatio,
                        "riskPercentage": web_request.riskPercentage,
                        "accountBalance": web_request.accountBalance,
                        "stopType": stopType,
                        "atrFactor": atrFactor,
                    }
                   
                    logger.info(
                        f"Parent Order placed for {qualified_contract.symbol}: {parent}"
                    )
                    
                    fill = {
                        "execution": {
                            "avgPrice": entry_price
                        }
                    }
                    logger.info(
                        f"jengo bracket order webhook fill entry_price: {entry_price} "
                    )
                    logger.info(f"Parent Order fill details: {fill} for {qualified_contract.symbol}")
                    trade.fillEvent += self.parent_filled
                    await self.parent_filled(trade, fill)
                    logger.info(format_order_details_table(order_details))
                    await asyncio.sleep(0.1)  # Give some time for the order to be processed
                    open_orders_list= await self.ib.reqAllOpenOrdersAsync()
                
                
                
                    for open_order in open_orders_list:
                
                    
                        symbol_open_order = open_order.contract.symbol
                        self.open_orders[symbol_open_order] = {
                            "symbol": symbol_open_order,
                            "order_id": open_order.order.orderId,
                            "status": open_order.orderStatus.status,
                            "timestamp": await get_timestamp()
                        }
                        #self.ib.cancelOrder(open_order.order)
                        #self.ib.cancelOrderEvent += self.cancel_order # Cancel the order if needed
                        logger.info("Open Orders List:")
                        logger.info(format_order_details_table(order_details=self.open_orders[symbol_open_order]))

                else:
                    logger.error(
                        f"Order placement failed for {contract.symbol}: {trade.order}"
                    )

                trade_check = await self.ib.whatIfOrderAsync(qualified_contract, order)
                logger.info(
                    f"Order details only for {contract.symbol} with action {action} and stop type {stopType } trade_check= {trade_check}"
                )
                
                
                return trade

                
            if not submit_cmd:
                
                order_details = {
                    "ticker": qualified_contract.symbol,
                    "orderAction": orderAction,
                    
                    "entry_price": entry_price,
                    "stop_loss_price": stop_loss_price,
                    "take_profit_price": take_profit_price,
                    "quantity": quantity,
                    "rewardRiskRatio": web_request.rewardRiskRatio,
                    "riskPercentage": web_request.riskPercentage,
                    "accountBalance": web_request.accountBalance,
                    "stopType": stopType,
                    "atrFactor": atrFactor,
                }
                logger.info(format_order_details_table(order_details))
                logger.info(
                    f"jengo bracket order webhook entry_price: {entry_price}"
                )

                return {"status": "order details only", "order_details": order_details}

        except Exception as e:
            logger.error(f"Error creating order: {e}")
    async def cancel_order(self, order):
        try:
            logger.info(f"Cancelling order: {order.contract.symbol} ..")
        except Exception as e:

            logger.error(f"Error cancelling order : {e}")
            return False
        

    async def parent_filled(self, trade: Trade, fill):
        try:
            fill_contract = None
            fill_symbol = None

            # Extract permId early for deduplication check
            permId = trade.order.permId
            

            # Initialize tracking dictionary if not exists
            if not hasattr(self, "triggered_bracket_orders"):
                self.triggered_bracket_orders = {}

            # Initialize lock if not exists
            if not hasattr(self, "fill_lock"):
                self.fill_lock = asyncio.Lock()

            # Use a lock to prevent race conditions with multiple fill events
            async with self.fill_lock:
                # Check if we've already processed this permId
                if permId in self.triggered_bracket_orders:
                    logger.info(
                        f"Bracket order already triggered for permId {permId}, skipping duplicate call."
                    )
                    return

                # Mark this permId as being processed BEFORE any async operations
                # This prevents other concurrent calls from proceeding
                self.triggered_bracket_orders[permId] = True

                logger.info(
                    f"parent_filled Fill received for {trade.contract.symbol} with entry price. Sending Bracket for trade.order {trade.order}"
                )
                logger.info(
                    f"parent_filled Fill received for {trade.contract.symbol} with entry price. Sending Bracket for trade.order {trade.order}"
                )

                if trade.contract is not None:
                    logger.info(
                        f"jengo bracket order webhook trade.contract: {trade.contract.symbol}"
                    )
                    fill_contract = trade.contract
                    fill_symbol = trade.contract.symbol
                if trade.contract is None:
                    logger.info(
                        f"jengo bracket order webhook trade.contract: {trade.contract.symbol}"
                    )
                    fill_contract = trade.contract
                    fill_symbol = trade.contract.symbol

                    logger.info(
                        f"jengo bracket order webhook trade.contract is None, using trade.contract: {fill_contract.symbol}"
                    )
                if fill["execution"]["avgPrice"] > 0:
                    entry_price = float(fill["execution"]["avgPrice"]) > 0
                    logger.info(
                    f"jengo bracket order webhook entry_price: {entry_price} for order {trade.order} and permId {permId}"
                )
                elif self.close_price[fill_symbol][-1] > 0:
                    entry_price = float(self.close_price[fill_symbol][-1])
                    logger.info(
                    f"jengo bracket order webhook entry_price: {entry_price} for order {trade.order} and permId {permId}"
                )
                action = trade.order.action
                order = trade.order
                algoStrategy = trade.order.algoStrategy
                stopType = self.timestamp_id.get(fill_symbol, {}).get("stopType")
                stopLoss = self.timestamp_id.get(fill_symbol, {}).get(
                    "stopLoss", 0.0
                )
                logger.info(
                    f"jengo bracket order webhook entry_price: {entry_price} for order {trade.order} and permId {permId}"
                )

                quantity = float(trade.order.totalQuantity)

                try:
                    logger.info(
                        f"jengo Get latest positions from TWS (only positions with nonzero quantity): stopLoss {self.timestamp_id.get(fill_symbol, {}).get('stopLoss', 0.0)} for permId {permId}"
                    )
                    latest_positions_oca, open_orders_list_id = await asyncio.gather(self.ib.reqPositionsAsync(), self.ib.reqAllOpenOrdersAsync())
                    if algoStrategy == "Adaptive" or not is_market_hours():
                        logger.info(
                            f"Placing oca bracket order for {fill_symbol} with permId {permId} and stopType {stopType} and stopLoss {stopLoss}"
                        )
                        oca_order = await self.add_oca_bracket(
                            fill_contract,
                            order,
                            action,
                            entry_price,
                            quantity,
                            stopType,
                            stopLoss,
                        )
                        return oca_order
                    else:
                        logger.info(
                            f"Skipping {fill_symbol}: algoStrategy {algoStrategy} does not require oca bracket order and stopType {stopType} and stopLoss {stopLoss}"
                        )
                  
                    logger.info(
                        f"jengo Get latest positions from TWS (only positions with nonzero quantity): open_orders_list_id {len(open_orders_list_id)}"
                    )
                    self.orders_dict_oca_symbol = {
                        o.order.permId: o for o in open_orders_list_id
                    }
                    logger.info(f" jengo open_orders_list_id: {self.orders_dict_oca_symbol.keys()}")
                    

                   
                    logger.info(
                        f" jengo parent_filled stopType {stopType} and stopLoss {stopLoss}"
                    )
                    try:
                        logger.info(f" Checking for active positions for {fill_symbol} with stopType {stopType} and stopLoss {stopLoss}")

                        # Now loop through the positions
                        for position in latest_positions_oca:
                            logger.info(
                                f"Position for: {position.contract.symbol} at shares of {position.position}"
                            )
                            if (
                                position.contract.symbol == fill_symbol
                                and position.position != 0
                            ):
                                logger.info(f" Found active position for {fill_symbol}: {position.position} shares")
                                # For Adaptive algoStrategy, trigger the bracket order
                                if algoStrategy == "Adaptive" or not is_market_hours():
                                    logger.info(
                                        f"Placing oca bracket order for {fill_symbol} with permId {permId} and stopType {stopType} and stopLoss {stopLoss}"
                                    )
                                    oca_order = await self.add_oca_bracket(
                                        fill_contract,
                                        order,
                                        action,
                                        entry_price,
                                        quantity,
                                        stopType,
                                        stopLoss,
                                    )
                                    
                                    return oca_order
                                else:
                                    logger.info(
                                        f"Skipping {fill_symbol}: algoStrategy {algoStrategy} does not require oca bracket order and stopType {stopType} and stopLoss {stopLoss}"
                                    )
                    except Exception as e:
                        logger.error(
                            f"Error while checking positions for {fill_symbol}: {e}",
                            exc_info=True,
                        )
                        

                    # If we got here, no matching position was found
                    logger.warning(
                        f"No active position found for {fill_symbol} when placing bracket orders"
                    )

                except Exception as e:
                    logger.error(
                        f"Error in conditions for oca bracket_orders in fill event: {e}"
                    )
                    # If there's an error, we should remove the permId from the triggered dictionary
                    # so that it can be retried
                    if permId in self.triggered_bracket_orders:
                        del self.triggered_bracket_orders[permId]

        except Exception as e:
            logger.error(f"Error updating positions on fill: {e}")
            # Global exception handling should also clean up the triggered dictionary
            if (
                "permId" in locals()
                and hasattr(self, "triggered_bracket_orders")
                and permId in self.triggered_bracket_orders
            ):
                del self.triggered_bracket_orders[permId]    
        except Exception as e:
            logger.error(f"Error in parent_filled: {e}")
            return None
    async def add_oca_bracket(
        self,
        contract: Contract,
        order: Trade,
        action: str,
        entry_price: float,
        quantity: float,
        stopType: str,
        stopLoss: float,
    ):
        try:
            
            stop_loss_price = 0.0
            new_entry_price = 0.0
            # Get symbol and timestamp ID
            symbol = contract.symbol
            logger.info(
                f"Adding OCA bracket for {contract.symbol} with action {action} and stopLoss {stopLoss} : jengo entry_price: {entry_price} and stopType {stopType}"  
            )

            # Get the timestamp ID directly from the dictionary with the symbol as key
            if symbol not in self.timestamp_id:
                logger.error(f"No timestamp ID found for {contract.symbol}")
                return None

            # Get timestamp data for this symbol
            timestamp_data = self.timestamp_id[symbol]

            priceType = None
            
            new_entry_price= self.close_price.get(symbol)[-1]
            bracket_orders = None
            if stopLoss > 1.0:
                stop_loss_price = stopLoss

            # Get current price from new_entry_price if available
            current_price = 0.0
            if entry_price > 1.0:
                current_price = entry_price
                new_entry_price= self.close_price.get(symbol)[-1]  # Get the last close price if available
                priceType = "arg entry_price"
                logger.debug(f"Using provided entry price for {contract.symbol}: {current_price}")
            elif current_price <= 1.0 and new_entry_price > 1.0:
                entry_price = new_entry_price
                if new_entry_price ==0.0:
                    logger.warning(f"Entry prices not available for {contract.symbol}")
                    entry_price =  await self.last_price_five_sec(contract)
                    current_price = entry_price
                    
                else:
                    priceType = "ib.postions"
                    latest_positions_oca = await self.ib.reqPositionsAsync()
                    for pos in latest_positions_oca:
                        if pos.contract.symbol == symbol:
                            new_entry_price = {
                                "entry_long": pos.avgCost if action == "BUY" else None,
                                "entry_short": pos.avgCost if action == "SELL" else None,
                                "timestamp": datetime.now().isoformat(),
                            }
                            current_price = new_entry_price["entry_long"] if action == "BUY" else new_entry_price["entry_short"]
                            break
                        if not pos:
                            logger.error(f"Could not calculate position for {contract.symbol}")
                            return None
                 
            logger.debug(f"Timestamp ID for {contract.symbol}: {timestamp_data}")

            if current_price is None:
                logger.error(f"Current price is None for {contract.symbol} after checking entry prices")
                return None
               

            if current_price is not None:
               

                logger.info(
                    f"Entry price for {contract.symbol}: {entry_price} current price: {current_price} with stopType {stopType}"
                )
                barSizeSetting= timestamp_data["barSizeSetting"]
                rewardRiskRatio=timestamp_data["rewardRiskRatio"]
                riskPercentage=timestamp_data["riskPercentage"]
                accountBalance=timestamp_data["accountBalance"]
                atrFactor=timestamp_data["atrFactor"]

                # Calculate position details
             
                quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk= await compute_position_size(
                    entry_price,
                    stop_loss_price,
                    timestamp_data["accountBalance"],
                    timestamp_data["riskPercentage"],
                    timestamp_data["rewardRiskRatio"],
                    action,
                )
                

                

                
                stop_loss_price=self.vstop_dict[symbol][-1] - 0.02 if action == "BUY" else self.vstop_dict[symbol][-1] + 0.02
                logger.info(f"jengo here Calculated position for {contract.symbol}: quantity={quantity}, take_profit_price={take_profit_price}, stop_loss_price={stop_loss_price}, brokerComish={brokerComish}, perShareRisk={perShareRisk}, toleratedRisk={toleratedRisk}")
               

                # Place the bracket order
                p_ord_id = order.orderId
                reverseAction = "BUY" if order.action == "SELL" else "SELL"

                # Create the take profit (limit) and stop loss orders
                take_profit_order = LimitOrder(
                    reverseAction, order.totalQuantity, take_profit_price
                )
                stop_loss_order = StopOrder(reverseAction, order.totalQuantity, stop_loss_price)

                if take_profit_price is None:
                    logger.error(f"Take profit price is None for {contract.symbol}")
                    raise ValueError("Take profit price is None")
                if stop_loss_price is None:
                    logger.error(f"Stop loss price is None for {contract.symbol}")
                    raise ValueError("Stop loss price is None")

                logger.info(
                    f"Creating bracket orders for {contract.symbol}: entry={entry_price}, stop_loss_order={stop_loss_price}, take_profit_order={take_profit_price}, quantity={order.totalQuantity}"
                )

                orders = [take_profit_order, stop_loss_order]
                # Group them in an OCA group using oneCancelsAll
                ocaGroup = self.ocaGroup[symbol]
                

                # Create the OCA bracket orders
                try:
                    bracket_orders = self.ib.oneCancelsAll(orders, ocaGroup, 1)
                    logger.debug(
                        f"Created OCA bracket orders for {contract.symbol} with group {ocaGroup}"
                    )
                except Exception as e:
                    logger.error(f"Failed to create OCA bracket orders: {e}")
                    return None

            # Place each order
            if bracket_orders is not None:
                for bracket_order in bracket_orders:
                    logger.info(
                        f"Placing order: {contract.symbol} {bracket_order.action} {bracket_order.totalQuantity} @ {bracket_order.lmtPrice if hasattr(bracket_order, 'lmtPrice') else bracket_order.auxPrice}"
                    )
                   
                    
                    trade = self.ib.placeOrder(contract, bracket_order)
                    if trade:
                        order_details = {
                            "ticker": symbol,
                            "orderAction":action,
                            "orderType":trade.order.orderType,
                            "entry_price": entry_price,
                            "stop_loss_price": stop_loss_price,
                            "take_profit_price": take_profit_price,
                            "quantity": quantity,
                            "rewardRiskRatio":  rewardRiskRatio,
                            "riskPercentage":  riskPercentage,
                            "accountBalance":  accountBalance,
                            "atrFactor": atrFactor,
                            "stopType": stopType,

                        }
                        
                        logger.info(format_order_details_table(order_details))
                        
                        # Remove the timestamp ID data after successful order placement
                        if symbol in self.timestamp_id:
                            del self.timestamp_id[symbol]

                        logger.info(f"Order placed for {contract.symbol}: {bracket_order} priceType = {priceType}")
                        trade.fillEvent += self.parent_filled
                    else:
                        logger.error(
                            f"Order placement failed for {contract.symbol}: {bracket_order}"
                        )

                return bracket_orders
            else:
                logger.error(f"No bracket orders created for {contract.symbol}")
                return None

        except Exception as e:
            logger.error(f"Error in add oca bracket: {e}")
            return None 
        
    async def last_price_five_sec(self, contract: Contract) -> float:
        """
        Asynchronously fetch the last price for a given symbol every 5 seconds.

        Args:
            symbol (str): The stock symbol to fetch the last price for.

        Returns:
            float: The last price of the stock.
        """
        try:
            symbol = contract.symbol

            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime='',  # '' means now
                durationStr='60 S',
                barSizeSetting='5 secs',
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
            
            if not bars or len(bars) == 0:
                logger.error(f"No historical data returned for {contract.symbol}.")
                raise ValueError(f"No historical data returned for {contract.symbol}.")
                
            # Convert bars to DataFrame for technical analysis
            df = util.df(bars)
            if df.empty:
                logger.error(f"Empty DataFrame for {contract.symbol} after conversion.")
                raise ValueError(f"No historical data returned for {contract.symbol}.")
            logger.info(f"Fetched {len(df)} historical bars for {contract.symbol} with raw bars from last close: {self.close_price[symbol][-1]}")
            # Store the close prices as a LIST, not a Series
            self.close_price[symbol] = df['close'].tolist()
            entry_price = float(self.close_price[symbol][-1]) if self.close_price[symbol] else None
            logger.info(f"Last price for {contract.symbol}: {entry_price} from historical data.")
            return entry_price
        
        except Exception as e:
            logger.error(f"Error fetching last price for {contract.symbol}: {e}")
            entry_price = None

    