# price.py
from tracemalloc import stop
from ib_async import Contract, Order, Trade, LimitOrder, StopOrder, TagValue, util, IB, MarketOrder
from dataclasses import dataclass
from log_config import log_config, logger
from typing import *
import pandas as pd
import os, asyncio, pytz, time
import pandas as pd
import numpy as np
from fastapi.responses import JSONResponse, HTMLResponse

from fastapi import FastAPI, HTTPException
from datetime import datetime
from collections import deque
import math
import talib as ta


from pydantic import BaseModel

from indicators import  calculate_keltner_channels_ta_lib,  volatility_stop, get_ema, detect_pivot_points, daily_volatility
from timestamps import current_millis, get_timestamp, is_market_hours
from dotenv import load_dotenv
load_dotenv()
# --- Configuration ---
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv('FASTAPI_IB_CLIENT_ID', '1111'))
IB_HOST = os.getenv('IB_GATEWAY_HOST', '127.0.0.1')


env_file = os.path.join(os.path.dirname(__file__), '.env')
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Setup logging and environment
log_config.setup()




                        

class PriceData:
    def __init__(self, ib: IB):
        self.ib = ib
        self.entry_prices = {}  # key: symbol, value: {"entry_long": price, "entry_short": price, "timestamp": time}
        self.timestamp_id ={}
        self.client_id = int(os.getenv('FASTAPI_IB_CLIENT_ID', '1111'))
        self.host = '127.0.0.1'
        self.port = int(os.getenv('TBOT_IBKR_PORT', '4002'))
        self.max_attempts = 300
        self.initial_delay = 1
        self.current_positions = {}  # key: symbol, value: position details
        self.open_orders = {}
        self.open_price= {}
        self.high_price= {}
        self.low_price= {}
        self.close_price= {}
        self.volume= {}
        
        self.active_rtbars = {}  # Store active real-time bar subscriptions


    
    def format_trade_details_table(self, trade_details):
        """
        Format trade details as a nice pandas DataFrame table for terminal display.
    
        Args:
            trade_details: Dictionary containing trade details
        
        Returns:
            Formatted string representation of the table
        """
        # Convert dictionary to a DataFrame with 'Parameter' and 'Value' columns
        df = pd.DataFrame({
            'Parameter': trade_details.keys(),
            'Value': trade_details.values()
        })
    
        # Format numerical values
        for idx, param in enumerate(df['Parameter']):
            value = df.loc[idx, 'Value']
            if isinstance(value, (int, float)):
                if 'price' in param or 'Balance' in param:
                    df.loc[idx, 'Value'] = f"${value:.2f}"
                elif 'percentage' in param or 'Ratio' in param:
                    df.loc[idx, 'Value'] = f"{value:.2f}"
    
        # Return formatted table string
        return f"\n{'='*50}\nORDER DETAILS\n{'='*50}\n{df.to_string(index=False)}\n{'='*50}"
    async def error_code_handler(self, reqId: int, errorCode: int, errorString: str, contract: Contract):
        logger.error(f"Error for reqId {reqId}: {errorCode} - {errorString}")
        
    async def connect(self) -> bool:
        attempt = 0
        while attempt < self.max_attempts:
            try:
                logger.info(f"Connecting to IB at {self.host}:{self.port} : client_id:{self.client_id}...")
                await self.ib.connectAsync(host=self.host, port=self.port, clientId=self.client_id, timeout=40)
                if self.ib.isConnected():
                    logger.info("Connected to IB Gateway")
                    # Use a lambda to bind self
                    #self.ib.disconnectedEvent += self.on_disconnected
                    #self.ib.errorEvent += self.error_code_handler
                    return True
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {e}")
            attempt += 1
            await asyncio.sleep(self.initial_delay)
        logger.error("Max reconnection attempts reached")
        return False
 
            
    async def graceful_disconnect(self):
        try:
            if self.ib.isConnected():
                logger.info("Unsubscribing events and disconnecting from IB...")
                self.ib.pnlEvent.clear()
                self.ib.orderStatusEvent.clear()
                self.ib.updatePortfolioEvent.clear()
                self.ib.disconnect()
                logger.info("Disconnected successfully.")
                return True
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
            return False
        
                
    async def ohlc_vol(self, contract, durationStr, barSizeSetting, whatToShow, useRTH, return_df=False):
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
        logger.info(f"Getting historical data for {contract.symbol} with duration {durationStr}, bar size {barSizeSetting}, and data type {whatToShow}")
        # Get historical data for indicator calculations
        bars = await self.ib.reqHistoricalDataAsync(
            contract=contract,
            endDateTime='',
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow=whatToShow,
            useRTH=useRTH
        )
    
        if not bars:
            logger.error(f"No historical data available for {contract.symbol}")
            return None
        
        # Convert bars to DataFrame
        df = util.df(bars)
        logger.debug(f"Retrieved {len(df)} bars for {contract.symbol}")

        # Return raw DataFrame if requested
        if return_df:
            return df
    
        # Check if we have enough data
        if len(df) < 10:
            logger.warning(f"Not enough data to calculate OHLC averages. Need at least 10 bars, but got {len(df)}")
            return None
    
        open_prices = df['open']
        high = df['high']
        low = df['low']
        close = df['close']
        vol = df['volume']
        hl2 = (high + low) / 2
        hlc3 = (high + low + close) / 3
        ohlc4 = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        logger.info(f"Calculated OHLCV data for {contract.symbol}")

        return ohlc4, hlc3, hl2, open_prices, high, low, close, vol

    async def on_disconnected(self):
        logger.warning("Disconnected from IB. Reconnecting...")
        await asyncio.sleep(1)
        if not self.ib.isConnected():
            await self.connect()

    
    async def get_price(self, contract, action: str, max_wait_seconds=5, keep_subscription=False):
        symbol = contract.symbol
        logger.info(f"Getting price from real-time bars for {symbol}")
        result_container = {"price": None, "source": None}

        # Get or create the real-time bar subscription.
        rtBars = await self._get_rt_bars_subscription(contract, symbol, keep_subscription, action)
    
        # If this is a new subscription (i.e. not stored already), attach the update callback.
        if symbol not in self.active_rtbars:
            logger.info(f"Attaching update event for {symbol}")
            rtBars.updateEvent += lambda bars, hasNewBar: asyncio.create_task(
                self._on_bar_update(bars, symbol, action, result_container)
            )

        # Wait for a price update (via callback or direct check) up to max_wait_seconds.
        await self._wait_for_price(result_container, rtBars, symbol, action, max_wait_seconds)
    
        # Fallback: if no price yet and we're not keeping the subscription persistently.
        if result_container["price"] is None and not keep_subscription:
            logger.warning(f"Failed to get real-time price for {symbol}; using fallback")
            fallback_price, source = await self._fallback_price(contract, symbol, action)
            result_container["price"] = fallback_price
            result_container["source"] = source

        # Cancel the temporary subscription if needed.
        if not keep_subscription and symbol not in self.active_rtbars and rtBars:
            self.ib.cancelRealTimeBars(rtBars)
            logger.info(f"Cancelled temporary real-time bar subscription for {symbol}")

        logger.info(f"Final entry price for {symbol}: {result_container['price']} (source: {result_container['source']})")
        return result_container["price"]

    async def _get_rt_bars_subscription(self, contract, symbol, keep_subscription, action):
        whatToShow = 'MIDPOINT'
        if action == 'BUY' or action == 'SELL':
            whatToShow = 'ASK' if action == 'BUY' else 'BID'
        if symbol in self.active_rtbars:
            logger.info(f"Using existing real-time bar subscription for {symbol}")
            return self.active_rtbars[symbol]
        else:
            logger.info(f"Creating new real-time bar subscription for {symbol}")
            rtBars = self.ib.reqRealTimeBars(
                contract=contract,
                barSize=5,
                whatToShow=whatToShow,
                useRTH=False,
                realTimeBarsOptions=[]
            )
            if keep_subscription:
                self.active_rtbars[symbol] = rtBars
                logger.info(f"Created and stored new real-time bar subscription for {symbol}")
            return rtBars

    async def _on_bar_update(self, bars, symbol, action, result_container):
        try:
            df = util.df(bars)
            if df is None or df.empty:
                logger.warning(f"Empty dataframe received in onBarUpdate for {symbol}")
                return
            if 'close' not in df.columns or 'open_' not in df.columns:
                logger.warning(f"Missing required columns in dataframe for {symbol}")
                return

            # Calculate entry prices using the existing helper.
            entry_long, entry_short = self.calculate_entry_prices(df)
            self.entry_prices[symbol] = {
                "entry_long": entry_long,
                "entry_short": entry_short,
                "timestamp": datetime.now().isoformat()
            }
            price = entry_long if action == 'BUY' else entry_short
            if price is not None:
                
                result_container["price"] = price
                result_container["source"] = "real_time"
                logger.info(f"self.entry_prices is price for {symbol}: {price}")
        except Exception as e:
            logger.error(f"Error in _on_bar_update for {symbol}: {e}")

    async def _wait_for_price(self, result_container, rtBars, symbol, action, max_wait_seconds):
        start_time = time.time()
        while result_container["price"] is None and time.time() - start_time < max_wait_seconds:
            await asyncio.sleep(0.2)
            try:
                df = util.df(rtBars)
                if df is not None and not df.empty and 'open_' in df.columns and 'close' in df.columns:
                    logger.debug(f"Got dataframe with {len(df)} rows for {symbol}")
                    entry_long, entry_short = self.calculate_entry_prices(df)
                    self.entry_prices[symbol] = {
                        "entry_long": entry_long,
                        "entry_short": entry_short,
                        "timestamp": datetime.now().isoformat()
                    }
                    result_container["price"] = entry_long if action == 'BUY' else entry_short
                    result_container["source"] = "direct"
                    logger.info(f"Got self.entry_prices directly for {symbol}: {result_container['price']}")
                    break
            except Exception as e:
                logger.warning(f"Error getting data from rtBars for {symbol}: {e}")

    async def _fallback_price(self, contract, symbol, action):
        """
        Provides a fallback price using cached entry prices (if available) or historical data.
        Returns a tuple: (price, source)
        """
        # Use cached prices if available.
        cached_prices = self.entry_prices.get(symbol)
        if cached_prices:
            logger.debug(f"Using cached entry price for {symbol}: {cached_prices}")
            return (cached_prices["entry_long"] if action == 'BUY' else cached_prices["entry_short"], "cached")
        else:
            try:
                whatToShow = 'ASK' if action == 'BUY' else 'BID'
                df = await self.ohlc_vol(
                    contract=contract,
                    durationStr='60 S',
                    barSizeSetting='5 secs',
                    whatToShow=whatToShow,
                    useRTH=False,
                    return_df=True
                )
                if df is not None and not df.empty:
                    last_price = float(df.close.iloc[-1])
                    entry_long = last_price
                    entry_short = last_price
                    self.entry_prices[symbol] = {
                        "entry_long": entry_long,
                        "entry_short": entry_short,
                        "timestamp": datetime.now().isoformat(),
                        "source": "historical_fallback"
                    }
                    logger.info(f"Using historical fallback price for {symbol}: {last_price}")
                    return (entry_long if action == 'BUY' else entry_short, "historical_fallback")
                else:
                    logger.error(f"No historical data available for {symbol}")
            except Exception as e:
                logger.error(f"Error getting historical fallback for {symbol}: {e}")
        return (None, None)


        
    async def _get_initial_ema(self, contract):
        """
        Get initial 2-D historical data (1-min bars) and compute EMA.
        Returns the DataFrame (df_1) and the latest EMA.
        """
        durationStr = '2 D'
        barSizeSetting = '1 min'
        df_1 = await self.ohlc_vol(
            contract=contract,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow='TRADES',
            useRTH=False,
            return_df=True
        )
        if df_1 is None:
            logger.error(f"No historical data available for {contract.symbol}")
            return None, None
        df_1['ema'] = ta.EMA(df_1['close'].values, timeperiod=20)
        ema = df_1.iloc[-1]['ema']
        return df_1, ema

    async def _compute_stop_loss(self, contract, orderAction, stopType, atrFactor, entry_price, provided_stopLoss, df_1):
        """
        Compute the stop loss price if a valid one was not provided.
        Returns a tuple: (stop_loss_price, lower, upper, updated_stopType)
        """
        # Use provided stop loss if greater than zero.
        if provided_stopLoss > 0:
            return round(provided_stopLoss, 2), None, None, stopType

        # Get additional historical data for indicator calculations.
        durationStr = '35 D'
        barSizeSetting = '1 day'
        ohlc_data = await self.ohlc_vol(
            contract=contract,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow='TRADES',
            useRTH=True,
            return_df=False
        )
        if ohlc_data is None:
            logger.error(f"No extended historical data for {contract.symbol}")
            return None, None, None, stopType

        # Unpack the data
        ohlc4_dv, hlc3_dv, hl2_dv, open_prices_dv, high_dv, low_dv, close_dv, vol_dv = ohlc_data
        ib_connection = self.ib

        # Ensure that contract is a Contract object (not a dict)
        if not hasattr(contract, 'symbol'):
            logger.warning(f"Contract is not a Contract instance; attempting to convert it.")
            try:
                contract = Contract(**contract)
            except Exception as e:
                logger.error(f"Failed to convert contract dict to Contract: {e}")
                return None, None, None, stopType

        # Calculate daily volatility (this may adjust bar size internally)
        try:
            _ = await daily_volatility(ib_connection, contract, high_dv, low_dv, close_dv, stopType)
        except Exception as e:
            logger.error(f"Error in Daily Volatility: {e}")
            return None, None, None, stopType

        # Calculate indicators concurrently.
        keltner, vstop_df, pivot_points = await asyncio.gather(
            calculate_keltner_channels_ta_lib(
                high=df_1['high'], 
                low=df_1['low'], 
                close=df_1['close'], 
                ema_period=20, 
                atr_period=10, 
                multiplier=atrFactor
            ),
            volatility_stop(df_1, length=20, atr_multiplier=atrFactor), 
            detect_pivot_points(df_1, left_bars=5, right_bars=5)
        )
        if isinstance(vstop_df, np.ndarray):
            vstop_df = pd.DataFrame(vstop_df, columns=["vstop", "uptrend"])

        # Add Keltner Channel bands to the data.
        df_1['KC_Middle'] = keltner['middle_band']
        df_1['KC_Upper'] = keltner['upper_band']
        df_1['KC_Lower'] = keltner['lower_band']

        latest = df_1.iloc[-1]
        lower = latest['KC_Lower']
        upper = latest['KC_Upper']

        # Determine stop loss from volatility stop.
        latest_vstop = vstop_df.iloc[-1]
        vstop_val = latest_vstop['vstop']
        uptrend = latest_vstop['uptrend']
        stop_loss_price = None
        if stopType == "vStop":
            if orderAction == "BUY" and uptrend:
                stop_loss_price = vstop_val
            elif orderAction == "SELL" and not uptrend:
                stop_loss_price = vstop_val
            elif (orderAction == "SELL" and uptrend) or (orderAction == "BUY" and not uptrend):
                stopType = "kcStop"
                stop_loss_price = lower if orderAction == "BUY" else upper

        # Adjust using pivot points if pivot stop is selected.
        high_pivots = pivot_points.get("high_pivots", [])
        low_pivots = pivot_points.get("low_pivots", [])
        if orderAction == "BUY" and stopType == "pivot" and stop_loss_price is None:
            if low_pivots:
                stop_loss_price = low_pivots[0]['value'] - 0.02
            else:
                stop_loss_price = entry_price * 0.98
        elif orderAction == "SELL" and stopType == "pivot" and stop_loss_price is None:
            if high_pivots:
                stop_loss_price = high_pivots[0]['value'] + 0.02

        # Fallback if still undefined.
        if stop_loss_price is None:
            if stopType == "kcStop":
                stop_loss_price = lower if orderAction == "BUY" else upper
            else:
                stop_loss_price = entry_price * 0.99 if orderAction == "BUY" else entry_price * 1.01

        stop_loss_price = round(float(stop_loss_price), 2)
        return stop_loss_price, lower, upper, stopType

    def _compute_position_size(self, entry_price, stop_loss_price, accountBalance, riskPercentage, rewardRiskRatio, orderAction):
        """
        Given the entry and stop loss prices, compute the broker commission,
        per-share risk, tolerated risk, quantity and take profit price.
        """
        brokerComish = max(round((accountBalance / entry_price) * 0.005), 1.0)
        perShareRisk = abs(entry_price - stop_loss_price)
        if perShareRisk == 0 or perShareRisk < 0.01:
            logger.warning(f"Per share risk ({perShareRisk}) is too small; defaulting to 1% of entry price")
            perShareRisk = entry_price * 0.01
        toleratedRisk = abs((riskPercentage / 100 * accountBalance) - brokerComish)
        quantity = round(min(toleratedRisk / perShareRisk, math.floor((accountBalance * 0.95) / entry_price)))
        if quantity <= 0:
            logger.warning(f"Calculated quantity ({quantity}) is invalid; using minimum quantity of 1")
            quantity = 1
        if orderAction == "BUY":
            take_profit_price = round(entry_price + (perShareRisk * rewardRiskRatio), 2)
        else:
            take_profit_price = round(entry_price - (perShareRisk * rewardRiskRatio), 2)
        return quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk

    async def calculate_position(self, contract, entry_price: float, orderAction: str, stopType: str,
                                 atrFactor: float, riskPercentage: float, accountBalance: float,
                                 rewardRiskRatio: float, stopLoss: float):
        """
        Main function to calculate a position.
        It gathers initial EMA data, computes a stop loss (if not provided),
        and then calculates the proper position size.
        """
        try:
            logger.info(f"Calculating position for {contract.symbol} with entry {entry_price}, action {orderAction}, "
                        f"stopType {stopType}, atrFactor {atrFactor}, riskPercentage {riskPercentage}, "
                        f"accountBalance {accountBalance}, rewardRiskRatio {rewardRiskRatio}, stopLoss {stopLoss}")
            
            # Step 1: Get initial historical data and EMA.
            df_1, ema = await self._get_initial_ema(contract)
            if df_1 is None:
                return None

            # Step 2: Compute stop loss (if not provided).
            computed_stop_loss, lower, upper, updated_stopType = await self._compute_stop_loss(
                contract, orderAction, stopType, atrFactor, entry_price, stopLoss, df_1
            )
            if computed_stop_loss is None:
                logger.error("Failed to compute stop loss")
                return None

            # Step 3: Compute position size.
            quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = self._compute_position_size(
                entry_price, computed_stop_loss, accountBalance, riskPercentage, rewardRiskRatio, orderAction
            )

            logger.info(f"Position for {contract.symbol}: entry={entry_price}, stop_loss={computed_stop_loss}, "
                        f"take_profit={take_profit_price}, quantity={quantity}, brokerComish={brokerComish}, "
                        f"toleratedRisk={toleratedRisk}, perShareRisk={perShareRisk}, ema={ema}")

            return {
                "stop_loss_price": computed_stop_loss,
                "quantity": quantity,
                "take_profit_price": take_profit_price,
                "ema": ema,
                "upper": upper,
                "lower": lower,
                "stopType": updated_stopType
            }
        except Exception as e:
            logger.error(f"Error calculating position: {e}", exc_info=True)
            return None
   
    async def req_mkt_data(self, contract: Contract):
        """
        Request market data for a contract and set up real-time price monitoring.
        This populates the entry_prices dictionary with long and short entry prices.
        """
        try:
            if not self.ib.isConnected():
                logger.info(f"IB not connected in req_mkt_data, attempting to reconnect")
                await self.ib_manager.connect()
                if not self.ib.isConnected():
                    raise ValueError("Unable to connect to IB")
                
            logger.info(f"Getting market data for {contract.symbol}")
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                logger.error(f"Contract qualification failed for {contract.symbol}")
                return None
                
            # Use the qualified contract instance
            qualified_contract = qualified[0]
            symbol = qualified_contract.symbol
            logger.info(f"Qualified contract for: {symbol} {qualified_contract.secType} {qualified_contract.exchange} {qualified_contract.currency} conID: {qualified_contract.conId}")
            ticker = self.ib.reqMktData(qualified_contract, '', True, False)
            
            # Start real-time bar subscriptions to populate entry prices if not already active
            if symbol not in self.active_rtbars:
                # This will trigger get_price to populate the entry_prices dictionary with both entry_long and entry_short
                # We don't await the result because we want to set up the subscription without blocking
                asyncio.create_task(self.get_price(qualified_contract, "BUY", keep_subscription=True))

            # Prepare contract details for return
            contract_json = {
                "symbol": symbol,
                "secType": qualified_contract.secType,
                "currency": qualified_contract.currency,
                "exchange": qualified_contract.exchange,
                "localSymbol": qualified_contract.localSymbol,
                "primaryExchange": qualified_contract.primaryExchange,
                "conId": qualified_contract.conId,
                "lastTradeDateOrContractMonth": qualified_contract.lastTradeDateOrContractMonth,
            }
            
            # Check if we already have entry prices for this symbol
            entry_prices = await self.get_entry_prices(symbol)
            if entry_prices:
                # Add entry prices to the return data
                contract_json["entry_long"] = entry_prices["entry_long"]
                contract_json["entry_short"] = entry_prices["entry_short"]
                contract_json["timestamp"] = entry_prices["timestamp"]
                logger.info(f"Returning entry prices for {symbol}: {entry_prices}")
            else:
                # If we don't have prices yet, wait a bit and check again
                for _ in range(10):  # Try for up to 2 seconds
                    await asyncio.sleep(0.2)
                    entry_prices = await self.get_entry_prices(symbol)
                    if entry_prices:
                        contract_json["entry_long"] = entry_prices["entry_long"]
                        contract_json["entry_short"] = entry_prices["entry_short"]
                        contract_json["timestamp"] = entry_prices["timestamp"]
                        logger.info(f"Returning entry prices for {symbol} after wait: {entry_prices}")
                        break
                
                if "entry_long" not in contract_json:
                    # Still no prices, just return what we have
                    logger.warning(f"No entry prices available yet for {symbol}")
                    contract_json["entry_prices_status"] = "pending"
            
            return contract_json
            
        except Exception as e:
            logger.error(f"Error in req_mkt_data: {e}")
            if not self.ib.isConnected():
                logger.info("Reconnecting to IB...")
                await self.ib_manager.connect()
            return {"error": str(e)}

    async def get_entry_prices(self, symbol):
        """
        Get the latest entry prices for a given symbol.
        Returns a dictionary with entry_long and entry_short, or None if not available.
        """
        if symbol in self.entry_prices:
            return self.entry_prices[symbol]
        return None

    

    def calculate_entry_prices(self, df_all: pd.DataFrame):
        """
        Calculate dynamic entry prices for long and short positions.
        Handles various edge cases including missing data.
        """
        if df_all is None or df_all.empty:
            logger.warning("Empty dataframe passed to calculate entry prices")
            return None, None
    
        # Check if required columns exist
        if 'close' not in df_all.columns:
            logger.warning("DataFrame missing 'close' column in calculate entry prices")
            return None, None
    
        if 'whatToShow' not in df_all.columns:
            # For real time bars without whatToShow column
            try:
                last_close = df_all.iloc[-1]['close']
                # Add a small buffer (0.1%) for long and short entries
                entry_long = last_close * 1.001
                entry_short = last_close * 0.999
                return entry_long, entry_short
            except Exception as e:
                logger.error(f"Error processing real-time bars without whatToShow: {e}")
                return None, None

        try:
            bid_df = df_all[df_all['whatToShow'] == 'BID']
            ask_df = df_all[df_all['whatToShow'] == 'ASK']
            
        
            # Handle cases where bid or ask data might be missing
            if bid_df.empty and ask_df.empty:
                # Fall back to using the last close price
                if len(df_all) > 0:
                    last_close = df_all.iloc[-1]['close']
                    # Add a small buffer (0.1%) for long and short entries
                    entry_long = last_close * 1.001  
                    entry_short = last_close * 0.999
                    return entry_long, entry_short
                else:
                    logger.warning("No data available in calculate entry prices")
                    return None, None
        
            # If one of bid or ask is missing, use the available one with an approximated spread
            if bid_df.empty and not ask_df.empty:
                latest_ask = ask_df.iloc[-1]['close']
                estimated_spread = latest_ask * 0.001  # Use 0.1% as an estimated spread
                latest_bid = latest_ask - estimated_spread
            elif not bid_df.empty and ask_df.empty:
                latest_bid = bid_df.iloc[-1]['close']
                estimated_spread = latest_bid * 0.001  # Use 0.1% as an estimated spread
                latest_ask = latest_bid + estimated_spread
            else:
                latest_bid = bid_df.iloc[-1]['close']
                latest_ask = ask_df.iloc[-1]['close']
        
            latest_spread = max(0.01, latest_ask - latest_bid)  # Ensure positive spread
            buffer = 0.1 * latest_spread
            entry_long = latest_ask + buffer
            entry_short = latest_bid - buffer
            logger.info(f"Calculated entry prices: long={entry_long}, short={entry_short} based on bid={latest_bid}, ask={latest_ask}, spread={latest_spread} buffer={buffer}")
            return entry_long, entry_short
        except Exception as e:
            logger.error(f"Error in calculate entry prices: {e}")
            return None, None
            
    
   
daily_volatility_rule= None


async def detect_pivot_points(df, left_bars=5, right_bars=5):
    """
    Detect pivot high and pivot low points in price data, including most recent highs/lows
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with price data (must contain 'high' and 'low' columns)
    left_bars : int, optional
        Number of bars to check to the left, default is 5
    right_bars : int, optional
        Number of bars to check to the right, default is 5
        
    Returns:
    --------
    dict : Dictionary containing the latest confirmed pivot points and most recent extremes
    """
    global daily_volatility_rule
    if daily_volatility_rule is not None:
        logger.info(f"Using daily volatility rule: {daily_volatility_rule} to determine left and right bars for pivot detection")
        if float(daily_volatility_rule) >= 30:
            left_bars = 1
            right_bars = 3
            logger.info(f"Using 1 left bar and 3 right bars for pivot detection due to high volatility")
    logger.info(f"Detecting pivot points with {left_bars} left bars and {right_bars} right bars with daily_volatility_rule: {daily_volatility_rule}")
    
    if len(df) < left_bars + 1:  # Only need left bars to find recent extremes
        logger.warning(f"Not enough data to detect pivot points. Need at least {left_bars + 1} bars, but got {len(df)}")
        return {"high_pivots": [], "low_pivots": []}
    
    # Initialize empty lists for pivot points
    high_pivots = []
    low_pivots = []
    
    # Step 1: Find confirmed pivot points (traditional method)
    scan_limit = len(df) - right_bars if len(df) > right_bars else 0
    
    for i in range(left_bars, scan_limit):
        # Current values
        current_high = df.at[i, 'high']
        current_low = df.at[i, 'low']
        current_date = df.at[i, 'date']
        
        # Check for pivot high (higher than all bars to the left and right)
        is_pivot_high = True
        for j in range(i - left_bars, i + right_bars + 1):
            if j == i or j >= len(df):
                continue  # Skip the current bar or out of bounds
            
            if df.at[j, 'high'] > current_high:
                is_pivot_high = False
                break
        
        if is_pivot_high:
            high_pivots.append({
                'index': i,
                'date': current_date,
                'value': current_high,
                'confirmed': True
            })
        
        # Check for pivot low (lower than all bars to the left and right)
        is_pivot_low = True
        for j in range(i - left_bars, i + right_bars + 1):
            if j == i or j >= len(df):
                continue  # Skip the current bar or out of bounds
            
            if df.at[j, 'low'] < current_low:
                is_pivot_low = False
                break
        
        if is_pivot_low:
            low_pivots.append({
                'index': i,
                'date': current_date,
                'value': current_low,
                'confirmed': True
            })
    
    # Step 2: Find the most recent high/low within lookback window
    # This captures developing pivots that might not have enough right bars yet
    lookback = min(10, len(df))  # Look at last 10 bars or all available data
    recent_section = df.iloc[-lookback:]
    
    recent_high_idx = recent_section['high'].argmax()
    recent_high_value = recent_section['high'].max()
    recent_high_date = recent_section.iloc[recent_high_idx]['date']
    recent_high_abs_idx = len(df) - lookback + recent_high_idx
    
    recent_low_idx = recent_section['low'].argmin()
    recent_low_value = recent_section['low'].min()
    recent_low_date = recent_section.iloc[recent_low_idx]['date']
    recent_low_abs_idx = len(df) - lookback + recent_low_idx
    
    # Check if these recent extremes are not already in our pivots list
    high_values = [p['value'] for p in high_pivots]
    low_values = [p['value'] for p in low_pivots]
    
    if recent_high_value not in high_values:
        # Check if it's a potential pivot (higher than bars to the left)
        is_potential_high = True
        for j in range(max(0, recent_high_abs_idx - left_bars), recent_high_abs_idx):
            if df.at[j, 'high'] > recent_high_value:
                is_potential_high = False
                break
        
        if is_potential_high:
            high_pivots.append({
                'index': recent_high_abs_idx,
                'date': recent_high_date,
                'value': recent_high_value,
                'confirmed': False  # Mark as unconfirmed/developing
            })
    
    if recent_low_value not in low_values:
        # Check if it's a potential pivot (lower than bars to the left)
        is_potential_low = True
        for j in range(max(0, recent_low_abs_idx - left_bars), recent_low_abs_idx):
            if df.at[j, 'low'] < recent_low_value:
                is_potential_low = False
                break
        
        if is_potential_low:
            low_pivots.append({
                'index': recent_low_abs_idx,
                'date': recent_low_date,
                'value': recent_low_value,
                'confirmed': False  # Mark as unconfirmed/developing
            })
    
    # Sort pivot points by index (time) in descending order
    high_pivots.sort(key=lambda x: x['index'], reverse=True)
    low_pivots.sort(key=lambda x: x['index'], reverse=True)
    
    # Get the latest 2 pivot highs and lows (or fewer if not enough found)
    latest_high_pivots = high_pivots[:2] if len(high_pivots) >= 2 else high_pivots
    latest_low_pivots = low_pivots[:2] if len(low_pivots) >= 2 else low_pivots
    
    logger.info(f"Found {len(high_pivots)} high pivots and {len(low_pivots)} low pivots")
    if latest_high_pivots:
        logger.info(f"Latest high pivots: {[p['value'] for p in latest_high_pivots]}")
    if latest_low_pivots:
        logger.info(f"Latest low pivots: {[p['value'] for p in latest_low_pivots]}")
    
    # Return the results
    return {
        "high_pivots": latest_high_pivots,
        "low_pivots": latest_low_pivots
    }




async def get_ema(df, length):
    """
    Calculate Exponential Moving Average (EMA) for a dataframe
    
    Parameters:
    df: DataFrame with OHLC data including 'close' column
    length: EMA period length
    
    Returns:
    DataFrame with added 'ema' column
    """
    try:
        if df is None:
            logger.error("DataFrame is None in get_ema")
            raise ValueError("DataFrame is None")
            
        if isinstance(df, dict):
            logger.error("get_ema received a dictionary instead of a DataFrame")
            raise TypeError("Expected DataFrame, got dictionary")
            
        if len(df) == 0:
            logger.error("DataFrame is empty in get_ema")
            raise ValueError("DataFrame is empty")
            
        if 'close' not in df.columns:
            logger.error("DataFrame missing 'close' column in get_ema")
            raise ValueError("DataFrame missing 'close' column")
        
        # Calculate EMA
        df_copy = df.copy()
        df_copy['ema'] = df_copy['close'].ewm(span=length, adjust=False).mean()
        logger.debug(f"EMA calculated with length {length}, first few values: {df_copy['ema'].head(3).tolist()}")
        
        return df_copy
        
    except Exception as e:
        logger.error(f"Error calculating EMA: {e}", exc_info=True)
        # Return original dataframe with NaN in ema column to prevent failures downstream
        df_copy = df.copy() if df is not None else pd.DataFrame()
        df_copy['ema'] = np.nan
        return df_copy
    
async def calculate_keltner_channels_ta_lib(high, low, close, ema_period, atr_period, multiplier):
    """
    Calculate Keltner Channels using EMA and ATR to match TradingView implementation.
    
    Parameters:
    -----------
    high : pandas.Series or numpy.ndarray
        Series or array of high prices
    low : pandas.Series or numpy.ndarray
        Series or array of low prices
    close : pandas.Series or numpy.ndarray
        Series or array of closing prices
    ema_period : int, optional
        The time period for the EMA calculation, default is 20
    atr_period : int, optional
        The time period for the ATR calculation, default is 14
    multiplier : float, optional
        Multiplier for the ATR to set channel width, default is 2.0
        
    Returns:
    --------
    dict : Dictionary containing the middle band (EMA), upper band, and lower band
    """
    # Calculate the EMA of the close prices for the middle band
    middle_band = ta.EMA(close, timeperiod=ema_period)
    
    # Calculate the ATR - TradingView uses RMA (Wilder's Smoothing)
    atr = ta.ATR(high, low, close, timeperiod=atr_period)
    
    
    # Log the most recent ATR value for debugging
    last_atr = atr.iloc[-1] if len(atr) > 0 else None
    
    ATR_df = pd.DataFrame(atr)
    ATR_df.columns = ['atr']
    ATR_df['atr'] = ATR_df['atr'].fillna(0)
    logger.warning(f"kc middle_band: {middle_band} Most recent ATR value: {last_atr} and multiplier: {multiplier}")
    # Calculate the upper and lower bands
    upper_band = middle_band + (multiplier * atr)
    lower_band = middle_band - (multiplier * atr)
    
    return {
        'middle_band': middle_band,
        'upper_band': upper_band,
        'lower_band': lower_band,
        'atr': atr  # Include the ATR values in the return
    }


#volatilityCalc=ta.tr(true)*100/math.abs(low)
async def daily_volatility(ib, contract, high, low, close, stopType):
     try:
        global daily_volatility_rule
        df = None
        logger.debug(f"Calculating Daily Volatility with (high, low, close) {(high, low, close)}")
            
        truRange = ta.TRANGE(high, low, close)
        Volatility=truRange*100/abs(low)
        if Volatility is not None:
            daily_volatility_rule = Volatility.iloc[-1]  # Get the latest volatility value
            
            logger.info(f"Daily volatility for {contract}: is {daily_volatility_rule} and rule is: {daily_volatility_rule >= 20}, using {'15-second' if daily_volatility_rule >= 20 else '1-minute'} bars")
            bar_size = '15 secs' if daily_volatility_rule >= 20 and stopType == 'kcStop' else '1 min'
            logger.info(f"Using {bar_size} bars for volatility calculation")
            bars = await ib.reqHistoricalDataAsync(
                    contract=contract,
                    endDateTime='',
                    durationStr='1 D',
                    barSizeSetting=bar_size,  # Explicitly request 15-second bars
                    whatToShow='MIDPOINT',
                    useRTH=False
                    
                )
            df = util.df(bars)
            latest = df.iloc[-1]
            close_price = latest['close']
            logger.debug(f"Latest close price: {close_price} Using {bar_size} bars for volatility calculation")
            
        

            # Check for valid data
            if df is None:
                logger.error(f"No historical data available for {contract}")
                return None
            else:
                logger.info(f"15-second bars: {len(df)}") 
        return df
     except Exception as e:
        logger.error(f"Error in Daily Volatility: {e}")
        return None


async def volatility_stop(df, length, atr_multiplier):
    """
    Calculate Volatility Stop indicator matching the TradingView implementation
    
    Parameters:
    df: DataFrame with OHLC data
    length: ATR length
  
    
    Returns:
    DataFrame with volatility stop values and trend direction
    """
    logger.debug("Calculating volatility stop")
    global daily_volatility_rule
    if daily_volatility_rule is not None:
        atr_multiplier = 3 if float(daily_volatility_rule) >= 30  else 1.5
        logger.info(f"Using ATR multiplier of {atr_multiplier} for volatility stop")
    
    # Calculate ATR using TA-Lib
    df['atr'] = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=length)
    #df['atr'] = pine_atr(df['high'].values, df['low'].values, df['close'].values, length)
    # Create a copy of the dataframe to avoid modifying the original
    result_df = df.copy()
    
    # Initialize variables
    result_df['max'] = result_df['close'].copy()
    result_df['min'] = result_df['close'].copy()
    result_df['uptrend'] = True
    result_df['vstop'] = np.nan
    
    # Calculate initial vstop
    atrM = result_df.at[0, 'atr'] * atr_multiplier if not np.isnan(result_df.at[0, 'atr']) else result_df.at[0, 'high'] - result_df.at[0, 'low']
    result_df.at[0, 'vstop'] = result_df.at[0, 'close'] - atrM
    
    # Calculate Volatility Stop iteratively (matching TradingView logic)
    for i in range(1, len(result_df)):
        src = result_df.at[i, 'close']
        prev_max = result_df.at[i-1, 'max']
        prev_min = result_df.at[i-1, 'min']
        prev_uptrend = result_df.at[i-1, 'uptrend']
        prev_stop = result_df.at[i-1, 'vstop']
        
        # Calculate ATR multiplier
        atrM = result_df.at[i, 'atr'] * atr_multiplier
        if np.isnan(atrM):  # Fallback if ATR is NaN
            atrM = result_df.at[i, 'high'] - result_df.at[i, 'low']
        
        # Update max and min
        result_df.at[i, 'max'] = max(prev_max, src)
        result_df.at[i, 'min'] = min(prev_min, src)
        
        # Calculate stop level based on trend
        if prev_uptrend:
            stop = max(prev_stop, result_df.at[i, 'max'] - atrM)
        else:
            stop = min(prev_stop, result_df.at[i, 'min'] + atrM)
        
        result_df.at[i, 'vstop'] = stop
        
        # Determine trend direction
        uptrend = src - stop >= 0.0
        result_df.at[i, 'uptrend'] = uptrend
        
        # Reset max and min if trend changes
        if uptrend != prev_uptrend:
            result_df.at[i, 'max'] = src
            result_df.at[i, 'min'] = src
            if uptrend:
                result_df.at[i, 'vstop'] = src - atrM
            else:
                result_df.at[i, 'vstop'] = src + atrM
    
    return result_df

