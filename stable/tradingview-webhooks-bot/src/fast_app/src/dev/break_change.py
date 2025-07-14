# break_change.py
import os
from pathlib import Path
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
from log_config import log_config, logger

class VolatilityScanner:
    def __init__(self):
        self.logger = logger
        self.matches = []
        self.NY_TZ = pytz.timezone("America/New_York")
        
    def scan_csv(self, csv_path):
        """Scans a TradingView CSV file for opening volatility patterns"""
        try:
            self.logger.info(f"Processing CSV file: {csv_path}")
            
            # Read the CSV
            df = pd.read_csv(csv_path)
            
            # Process the DataFrame
            return self.scan_dataframe(df)
            
        except Exception as e:
            self.logger.error(f"Error in CSV volatility scan: {e}")
            raise
            
    def scan_dataframe(self, df):
        """Scans a DataFrame for opening volatility patterns"""
        try:
            self.logger.info(f"Processing DataFrame with {len(df)} rows")
            self.matches = []  # Reset matches list
            
            # Ensure we have the time column
            if 'time' in df.columns and 'datetime' not in df.columns:
                df["datetime"] = pd.to_datetime(df["time"])
            
            # Calculate volume moving average (10-period window)
            if 'volume' in df.columns:
                df["Volume_MA"] = df["volume"].rolling(window=10).mean()
            else:
                self.logger.warning("No volume column found in data")
                return []
            
            # Process each row
            for i in range(1, len(df)):
                # Get current row and previous row
                row = df.iloc[i]
                prev_row = df.iloc[i-1]
                
                # Handle datetime conversion properly
                dt = None
                try:
                    # First, check if datetime is already a datetime object
                    if isinstance(row["datetime"], pd.Timestamp):
                        # If timestamp is already timezone-aware, convert it
                        if row["datetime"].tz is not None:
                            dt = row["datetime"].tz_convert(self.NY_TZ)
                        # If timestamp is naive, localize it
                        else:
                            dt = row["datetime"].tz_localize(pytz.UTC).tz_convert(self.NY_TZ)
                    else:
                        # If it's a string, parse it
                        dt = pd.to_datetime(row["datetime"]).tz_localize(pytz.UTC).tz_convert(self.NY_TZ)
                except Exception as e:
                    # If any error occurs, just use the original datetime
                    dt = row["datetime"]
                    self.logger.debug(f"Time conversion issue: {e}, using original datetime")
                
                # Check if time is between 9:30 AM and 9:36 AM
                is_target_time = False
                if hasattr(dt, 'hour') and hasattr(dt, 'minute'):
                    is_target_time = (dt.hour == 9 and 30 <= dt.minute <= 36)
                
                if is_target_time:
                    o = row["open"]
                    c = row["close"]
                    c1 = prev_row["close"]
                    volume = row["volume"] if "volume" in row else 0
                    volMA = row["Volume_MA"] if "Volume_MA" in row and not pd.isna(row["Volume_MA"]) else 0
                    
                    # Skip if open is zero
                    if o == 0:
                        self.logger.warning(f"Open price is zero at {dt}, skipping...")
                        continue
                    
                    # Calculate percent change
                    change = abs((c - c1) / c1) * 100
                    
                    # Check conditions (3% change and volume > volume MA)
                    if change >= 1 and volume > volMA:
                        self.logger.warning(f"symbol: {row['symbol']} MATCH at {dt} | Open: {o:.2f} Close: {c:.2f} Change: {change:.2f}% volume: {volume} > {volMA}")
                        self.matches.append({'datetime': dt, 'open': float(o), 'close': float(c), 'change_pct': round(change, 2), 'volume': float(volume), "symbol": row['symbol'] if 'symbol' in row else 'N/A'})
            
            return self.matches
        
        except Exception as e:
            self.logger.error(f"Error in DataFrame volatility scan: {e}")
            raise