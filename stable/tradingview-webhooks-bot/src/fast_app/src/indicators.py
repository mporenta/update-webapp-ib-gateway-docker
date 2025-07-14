# indicators.py 
from functools import cache
from ib_async import *
from dataclasses import dataclass
from log_config import log_config, logger
from typing import *
import pandas as pd
import numpy as np
import os, asyncio, pytz, time
from fastapi import FastAPI, HTTPException
from datetime import datetime
from collections import deque
import math
#import pandas_ta as ta

import re
import sys

import pandas_ta as ta
from pandas_ta.volatility import true_range, atr
from pandas_ta.momentum import rsi, cci, stoch, macd, willr, squeeze_pro
from pydantic import BaseModel
from ib_async import *
from timestamps import current_millis, get_timestamp, is_market_hours
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))
client_id = int(os.getenv('FASTAPI_IB_CLIENT_ID', '1111'))
IB_HOST = os.getenv('IB_GATEWAY_HOST', '127.0.0.1')
# Instantiate IB manager

daily_volatility_rule= None

class CachedATR:
    """
    Caches the ATR values to avoid recalculating them for each request.
    This can help improve performance when multiple requests are made in quick succession.
    """
    def __init__(self, length=10):
        self.length = length
        self.atr_cache = {}


    async def atr_update(self, symbol, high, low, close, static_length=None):
        # Calculate ATR using TA-Lib and cache it
        try: 
            if static_length is not None:
                self.length = static_length

            self.atr_cache = atr(high, low, close, length=self.length)
            logger.info(f"Calculating ATR for {symbol} with length {self.length}. ATR values: {self.atr_cache.tail(5)}")  # Log the last 5 values for debugging
            return self.atr_cache
        
        except Exception as e:
            logger.error(f"Error calculating ATR for {symbol}: {e}", exc_info=True)
            # Return an empty DataFrame or handle the error as needed
            return pd.Series(dtype=float)
        
cached_atr_instance = CachedATR(length=10)
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

def calculate_traditional_pivots(high: float, low: float, close: float) -> dict:
    """
    Calculate Traditional (Classic) Pivot Points and support/resistance levels.

    Formulas:
      - Pivot Point (PP) = (High + Low + Close) / 3
      - Resistance 1 (R1) = (2 * PP) - Low
      - Resistance 2 (R2) = PP + (High - Low)
      - Support 1 (S1) = (2 * PP) - High
      - Support 2 (S2) = PP - (High - Low)
    """
    logger.debug("Calculating Traditional Pivot Points")
    pivot = (high + low + close) / 3
    resistance1 = (2 * pivot) - low
    resistance2 = pivot + (high - low)
    support1 = (2 * pivot) - high
    support2 = pivot - (high - low)
    logger.debug(f"Pivot: {pivot}, Resistance1: {resistance1}, Resistance2: {resistance2}, Support1: {support1}, Support2: {support2}")
    return {'pivot': pivot, 'support1': support1, 'support2': support2, 'resistance1': resistance1, 'resistance2': resistance2}

def calculate_camarilla_pivots(high: float, low: float, close: float) -> dict:
    """
    Calculate Camarilla Pivot Points for support and resistance.

    Common formulas for Camarilla levels are:
      - R4 = Close + (High - Low) * 1.1 / 2
      - R3 = Close + (High - Low) * 1.1 / 4
      - R2 = Close + (High - Low) * 1.1 / 6
      - R1 = Close + (High - Low) * 1.1 / 12
      - S1 = Close - (High - Low) * 1.1 / 12
      - S2 = Close - (High - Low) * 1.1 / 6
      - S3 = Close - (High - Low) * 1.1 / 4
      - S4 = Close - (High - Low) * 1.1 / 2
    """
    range_val = high - low
    r4 = close + (range_val * 1.1 / 2)
    r3 = close + (range_val * 1.1 / 4)
    r2 = close + (range_val * 1.1 / 6)
    r1 = close + (range_val * 1.1 / 12)
    s1 = close - (range_val * 1.1 / 12)
    s2 = close - (range_val * 1.1 / 6)
    s3 = close - (range_val * 1.1 / 4)
    s4 = close - (range_val * 1.1 / 2)
    return {'resistance4': r4, 'resistance3': r3, 'resistance2': r2, 'resistance1': r1, 'support1': s1, 'support2': s2, 'support3': s3, 'support4': s4}



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
async def calculate_keltner_channels_ta_lib(high, low, close, ema_period, atr_period, multiplier, contract):
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
    middle_band = ta.ema(close=close, length=ema_period)
    symbol = contract.symbol if hasattr(contract, 'symbol') else 'unknown'  # Get the symbol from the contract object for logging
    # Calculate the ATR - TradingView uses RMA (Wilder's Smoothing)
    atr = await cached_atr_instance.atr_update(symbol, high, low, close, timeperiod=atr_period)
    
    
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
        if isinstance(contract, dict):
            contract_obj = Contract(
                symbol=contract.get('symbol'),
                exchange=contract.get('exchange', 'SMART'),
                secType=contract.get('secType', 'STK'),
                currency=contract.get('currency', 'USD')
            )
        else:
            contract_obj = contract
        global daily_volatility_rule
        df = None
        logger.debug(f"Calculating Daily Volatility with (high, low, close) {(high, low, close)}")
            
        truRange = true_range(high, low, close)
        logger.debug(f"True Range calculated: {truRange}")  # Log the last 5 values for debugging
        Volatility=truRange*100/abs(low)
        if Volatility is not None:
            daily_volatility_rule = Volatility.iloc[-1]  # Get the latest volatility value
            
            logger.info(f"Daily volatility for {contract_obj}: is {daily_volatility_rule} and rule is: {daily_volatility_rule >= 20}, using {'15-second' if daily_volatility_rule >= 20 else '1-minute'} bars")
            bar_size = '15 secs' if daily_volatility_rule >= 20 and stopType == 'kcStop' else '1 min'
            logger.info(f"Using {bar_size} bars for volatility calculation")
            bars = await ib.reqHistoricalDataAsync(
                    contract=contract_obj,
                    endDateTime='',
                    durationStr='1 D',
                    barSizeSetting=bar_size,  # Explicitly request 15-second bars
                    whatToShow='MIDPOINT',
                    useRTH=False
                    
                )
            df = util.df(bars)
            #print("Printing historical data for debugging:")
            #print(df)
            latest = df.iloc[-1]
            close_price = latest['close']
            logger.info(f"Latest close price: {close_price} Using {bar_size} bars for volatility calculation")
            
        

            # Check for valid data
            if df is None:
                logger.error(f"No historical data available for {contract_obj}")
                return None
            else:
                logger.info(f" {bar_size} bars: {len(df)}") 
        return df
     except Exception as e:
        logger.error(f"Error in Daily Volatility: {e}")
        return None
async def volatility_stop(bars, length, atr_multiplier):
    """
    Calculate Volatility Stop indicator matching the TradingView implementation
    
    Parameters:
    bars: OHLC bars data (can be DataFrame or list of bars)
    length: ATR length
    atr_multiplier: Multiplier for ATR to determine stop distance
    
    Returns:
    DataFrame with volatility stop values and trend direction
    """
    try:
        uptrend = None
        vstop = None
        length =  20
        vstop_df={}
        # Convert bars to DataFrame if needed
        if not isinstance(bars, pd.DataFrame):
            df = pd.DataFrame(bars)
        else:
            df = bars.copy()
            
        # Validate DataFrame
        if df.empty:
            logger.warning(f"Empty dataframe passed to volatility_stop")
            return pd.DataFrame({'vstop': [], 'uptrend': []})
            
        # Ensure required columns exist
        required_cols = ['high', 'low', 'close']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in dataframe: {missing_cols}")
            return pd.DataFrame({'vstop': [], 'uptrend': []})
            
        # Check for null values and potentially replace them
        for col in required_cols:
            if df[col].isnull().any():
                logger.warning(f"Null values found in {col}, filling with appropriate values")
                if col == 'close' and df['close'].isnull().all():
                    return pd.DataFrame({'vstop': [], 'uptrend': []})
                # Fill nulls with appropriate values
                if col == 'high':
                    df[col] = df[col].fillna(df['close'])
                elif col == 'low':
                    df[col] = df[col].fillna(df['close'])
                elif col == 'close':
                    df[col] = df[col].fillna(method='ffill')
        
        # Convert all to numeric to ensure calculations work
        for col in required_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # Calculate True Range safely
        try:
            # First calculate the true range components
            high_low = df['high'] - df['low']
            high_close = (df['high'] - df['close'].shift(1)).abs()
            low_close = (df['low'] - df['close'].shift(1)).abs()
            
            # Calculate true range, handling NaNs
            df['tr'] = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            
            # Calculate ATR with safe handling of NaNs
            df['atr'] = df['tr'].rolling(window=20).mean()
            df['atr'] = df['atr'].fillna(df['tr'])  # Fill initial NaNs with TR values
            logger.info(f"ATR calculated with length {length}, last 5 values: {df['atr'].tail(5).tolist()}")
        except Exception as e:
            logger.error(f"Error calculating ATR: {e}")
            # Create a simple fallback ATR
            df['atr'] = (df['high'] - df['low']).rolling(window=3).mean()
            df['atr'] = df['atr'].fillna(df['high'] * 0.01)  # Default to 1% of price
        
        # Initialize result columns
        df['vstop'] = df['close'].copy()
        df['uptrend'] = True
        df['max'] = df['close'].copy() 
        df['min'] = df['close'].copy()
        
        # Calculate initial vstop with error handling
        if len(df) > 0:
            try:
                first_idx = df.index[0]
                atrM = df.at[first_idx, 'atr'] * atr_multiplier
                if pd.isna(atrM) or atrM == 0:
                    atrM = (df.at[first_idx, 'high'] - df.at[first_idx, 'low']) or df.at[first_idx, 'close'] * 0.01
                df.at[first_idx, 'vstop'] = df.at[first_idx, 'close'] - atrM
            except Exception as e:
                logger.error(f"Error initializing vstop: {e}")
                df.at[df.index[0], 'vstop'] = df.at[df.index[0], 'close'] * 0.99  # Default 1% below
        
        # Process remaining bars with more robust iteration
        for i in range(1, len(df)):
            try:
                current_idx = df.index[i]
                prev_idx = df.index[i-1]
                
                src = df.at[current_idx, 'close']
                prev_max = df.at[prev_idx, 'max']
                prev_min = df.at[prev_idx, 'min']
                prev_uptrend = df.at[prev_idx, 'uptrend']
                prev_stop = df.at[prev_idx, 'vstop']
                
                # Calculate ATR multiplier with fallback
                atrM = df.at[current_idx, 'atr'] * atr_multiplier
                if pd.isna(atrM) or atrM == 0:
                    atrM = (df.at[current_idx, 'high'] - df.at[current_idx, 'low']) or src * 0.01
                
                # Update max and min
                df.at[current_idx, 'max'] = max(prev_max, src)
                df.at[current_idx, 'min'] = min(prev_min, src)
                
                # Calculate stop level based on trend
                if prev_uptrend:
                    stop = max(prev_stop, df.at[current_idx, 'max'] - atrM)
                else:
                    stop = min(prev_stop, df.at[current_idx, 'min'] + atrM)
                
                df.at[current_idx, 'vstop'] = stop
                
                # Determine trend direction
                uptrend = src - stop >= 0.0
                df.at[current_idx, 'uptrend'] = uptrend
                
                # Reset max and min if trend changes
                if uptrend != prev_uptrend:
                    df.at[current_idx, 'max'] = src
                    df.at[current_idx, 'min'] = src
                    if uptrend:
                        df.at[current_idx, 'vstop'] = src - atrM
                    else:
                        df.at[current_idx, 'vstop'] = src + atrM
            except Exception as e:
                logger.error(f"Error in volatility stop calculation at bar {i}: {e}")
                # Copy previous values if there's an error
                if i > 0:
                    df.iloc[i, df.columns.get_loc('vstop')] = df.iloc[i-1, df.columns.get_loc('vstop')]
                    df.iloc[i, df.columns.get_loc('uptrend')] = df.iloc[i-1, df.columns.get_loc('uptrend')]
                    vstop_df_up= df[['vstop', 'uptrend']].copy()
                    vstop_df=  {df['vstop'].iloc[-1] if not df.empty else 'N/A'}
                    uptrend = df.iloc[i, df.columns.get_loc('uptrend')]
        
        logger.info(f"Volatility Stop calculated with length {length} and ATR multiplier {atr_multiplier}. uptrend: {uptrend}")
        # Extract the value from vstop_df, which is a set-like object
        if vstop_df and isinstance(next(iter(vstop_df), 'N/A'), (int, float)):
            vstop = round(float(next(iter(vstop_df))), 2)
        else:
            # Get the last vstop value directly from the DataFrame as fallback
            vstop = round(float(df['vstop'].iloc[-1] if not df.empty else 0), 2)
        logger.info(f"Final uptrend status Last vstop value: {vstop} ")
        logger.info(f"Final uptrend status vstop_df: {vstop_df}")
         # Round to 2 decimal places for better readability
        return vstop, uptrend
    
    except Exception as e:
        logger.error(f"Error in volatility_stop: {e}", exc_info=True)
        # Return an empty DataFrame as fallback
        return pd.DataFrame({'vstop': [], 'uptrend': []})
async def calculate_keltner_channels(high, low, close, ema_period=20, atr_period=10, multiplier=2.0):
    """
    Calculate Keltner Channels using EMA and ATR.
    
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
    middle_band = ta.ema(close=close, length=ema_period)
    print(f"Middle band: {middle_band}")
    
    # Calculate the ATR - this already returns a DataFrame with 'atr' column
    atr_df = await calculate_atr(high, low, close, atr_period)
    
    # Extract the ATR values directly
    atr = atr_df['atr']
    
    # Calculate the upper and lower bands
    upper_band = middle_band + (multiplier * atr)
    lower_band = middle_band - (multiplier * atr)
    
    return {
        'middle_band': round(middle_band, 2),
        'upper_band': round(upper_band,2),
        'lower_band': round(lower_band,2),
        'atr': atr
    }


async def calculate_atr(high, low, close, length=10):
    """
    Calculate Average True Range (ATR) for pandas Series data.
    
    Parameters:
    -----------
    high : pandas.Series
        Series of high prices
    low : pandas.Series
        Series of low prices
    close : pandas.Series
        Series of closing prices
    length : int, optional
        ATR period, default is 10
        
    Returns:
    --------
    pandas.Series : ATR values
    """
    # Create a DataFrame to hold our calculations
    df = pd.DataFrame({'high': high, 'low': low, 'close': close})
    
    # Calculate the true range using element-wise operations
    # True Range = max(high-low, abs(high-prev_close), abs(low-prev_close))
    df['prev_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['prev_close']).abs()
    df['tr3'] = (df['low'] - df['prev_close']).abs()
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    
    # Calculate ATR (Simple Moving Average of True Range for first 'length' periods,
    # then Exponential Moving Average)
    df['atr'] = df['tr'].rolling(window=length).mean()
    
    # Fill first values using simple average (not completely accurate but better than NaN)
    first_valid = df['atr'].first_valid_index()
    if first_valid is not None and first_valid > 0:
        df.loc[:first_valid, 'atr'] = df.loc[:first_valid, 'tr'].mean()
    
    logger.debug(f"ATR calculated with length {length}")
    
    return df[['atr']]

