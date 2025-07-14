import backtrader as bt
import numpy as np
import pandas as pd
import talib as ta
from ib_async import IB, Contract, util
from datetime import datetime, timedelta
import sys

class MPSignalStrategy(bt.Strategy):
    """
    Backtrader implementation of MP Signal Strategy, converted from TradingView Pine Script
    """
    params = (
        # Strategy Settings
        ('accountBalance', 7500),
        ('rewardRiskRatio', 2.0),
        ('riskPercentage', 1.0),
        ('kcAtrFactor', 1.5),
        ('atrFactor', 2.0),
        ('vstopAtrFactor', 3.0),
        ('stopType', 'vStop'),
        ('meanReversion', False),
        
        # Technical Indicators
        ('leftBars', 5),
        ('rightBars', 5),
        ('ocsThresh', 20.0),
        ('rsiLn', 8),
        ('emaLn', 21),
        ('rsiFastLn', 3),
        ('volLen', 10),
        ('period', 5),  # TSI Length
        ('periodThresLong', 0.85),
        ('periodThresShort', -0.85),
    )

    def __init__(self):
        # Initialize variables
        self.newDay = False
        self.newShortSig = False
        self.newLongSig = False
        self.volCondSig = False
        self.shortTrendSig = False
        self.longTrendSig = False
        self.longLevelBreakSig = False
        self.shortLevelBreakSig = False
        self.r2 = None
        self.s2 = None
        self.longE = False
        self.shortE = False
        self.volumeVar = False
        self.prevLongSig = False
        self.prevShortSig = False
        self.totalTrades = 0
        
        # Arrays for storing data
        self.close_array = []
        self.open_array = []
        self.high_array = []
        self.low_array = []
        self.volume_array = []
        self.date_array = []
        
        # Store current day
        self.currentDay = None

    def get_pivot_high(self, high_data, left_bars, right_bars):
        """
        Simulate TradingView pivothigh function
        Returns the high value if a pivot high is found, otherwise None
        """
        if len(high_data) < left_bars + right_bars + 1:
            return None
        
        center_idx = left_bars
        center_val = high_data[center_idx]
        
        # Check if center bar is higher than all bars to its left and right
        is_higher_than_left = all(center_val > high_data[i] for i in range(center_idx-left_bars, center_idx))
        is_higher_than_right = all(center_val > high_data[i] for i in range(center_idx+1, center_idx+right_bars+1))
        
        if is_higher_than_left and is_higher_than_right:
            return center_val
        
        return None
    
    def get_pivot_low(self, low_data, left_bars, right_bars):
        """
        Simulate TradingView pivotlow function
        Returns the low value if a pivot low is found, otherwise None
        """
        if len(low_data) < left_bars + right_bars + 1:
            return None
        
        center_idx = left_bars
        center_val = low_data[center_idx]
        
        # Check if center bar is lower than all bars to its left and right
        is_lower_than_left = all(center_val < low_data[i] for i in range(center_idx-left_bars, center_idx))
        is_lower_than_right = all(center_val < low_data[i] for i in range(center_idx+1, center_idx+right_bars+1))
        
        if is_lower_than_left and is_lower_than_right:
            return center_val
        
        return None

    def next(self):
        # Check for new day
        currentDate = self.datas[0].datetime.date(0)
        if self.currentDay is None:
            self.currentDay = currentDate
        
        self.newDay = currentDate != self.currentDay
        if self.newDay:
            self.currentDay = currentDate
            self.shortTrendSig = False
            self.longTrendSig = False
            self.shortLevelBreakSig = False
            self.longLevelBreakSig = False
            self.r2 = None
            self.s2 = None
            self.volumeVar = False
            self.totalTrades = 0
        
        # Update data arrays
        self.date_array.append(currentDate)
        self.close_array.append(self.datas[0].close[0])
        self.open_array.append(self.datas[0].open[0])
        self.high_array.append(self.datas[0].high[0])
        self.low_array.append(self.datas[0].low[0])
        self.volume_array.append(self.datas[0].volume[0])
        
        # Keep arrays at a reasonable size
        max_lookback = 100
        if len(self.close_array) > max_lookback:
            self.close_array = self.close_array[-max_lookback:]
            self.open_array = self.open_array[-max_lookback:]
            self.high_array = self.high_array[-max_lookback:]
            self.low_array = self.low_array[-max_lookback:]
            self.volume_array = self.volume_array[-max_lookback:]
            self.date_array = self.date_array[-max_lookback:]
        
        # Need enough data for calculations
        if len(self.close_array) <= max(21, self.params.emaLn, self.params.leftBars + self.params.rightBars):
            return
        
        # Convert to numpy arrays for TA-Lib
        close_np = np.array(self.close_array)
        open_np = np.array(self.open_array)
        high_np = np.array(self.high_array)
        low_np = np.array(self.low_array)
        volume_np = np.array(self.volume_array)
        
        # === EMAs and RMAs ===
        ema9 = ta.EMA(close_np, timeperiod=9)[-1]
        rma9 = ta.SMA(close_np, timeperiod=9)[-1]  # Approximating RMA with SMA
        emaSlow = ta.EMA(close_np, timeperiod=self.params.emaLn)[-1]
        rmaSlow = ta.SMA(close_np, timeperiod=self.params.emaLn)[-1]
        
        # === RSI ===
        RSI = ta.RSI(close_np, timeperiod=self.params.rsiLn)[-1]
        RSIFast = ta.RSI(close_np, timeperiod=self.params.rsiFastLn)[-1]
        
        # === Volume Analysis ===
        mean = ta.SMA(volume_np, timeperiod=self.params.volLen)[-1]
        std = ta.STDDEV(volume_np, timeperiod=self.params.volLen)[-1]
        volStdTwo = std * 2 + mean
        volStdOneFive = std * 1.5 + mean
        
        # === Volume Oscillator ===
        short_vol = ta.EMA(volume_np, timeperiod=5)[-1]
        long_vol = ta.EMA(volume_np, timeperiod=10)[-1]
        osc = 100 * (short_vol - long_vol) / long_vol if long_vol > 0 else 0
        
        # === TSI (Correlation) ===
        if len(close_np) >= self.params.period:
            price_series = close_np[-self.params.period:]
            index_series = np.array(range(self.params.period))
            tsi = np.corrcoef(price_series, index_series)[0, 1]
        else:
            tsi = 0
        
        # === Pivot Points ===
        if len(high_np) > self.params.leftBars + self.params.rightBars:
            high_window = high_np[-(self.params.leftBars + self.params.rightBars + 1):]
            low_window = low_np[-(self.params.leftBars + self.params.rightBars + 1):]
            
            ph2 = self.get_pivot_high(high_window, self.params.leftBars, self.params.rightBars)
            pl2 = self.get_pivot_low(low_window, self.params.leftBars, self.params.rightBars)
            
            if ph2 is not None:
                self.r2 = ph2
            
            if pl2 is not None:
                self.s2 = pl2
        
        # === Conditions ===
        # Volume conditions
        belowMean = volume_np[-1] < mean
        aboveMean = volume_np[-1] > mean
        above2strDev = volStdTwo <= volume_np[-1]
        
        if self.newDay:
            self.volumeVar = False
        elif above2strDev:
            self.volumeVar = True
        elif not above2strDev:
            self.volumeVar = False
        
        volCond = (self.volumeVar or (self.volumeVar and aboveMean) or 
                  osc > self.params.ocsThresh or volStdOneFive < volume_np[-1])
        volSig = aboveMean or volCond
        
        # EMA conditions
        emaL = (close_np[-1] > ema9 and ema9 > rma9 and 
                close_np[-1] > emaSlow and emaSlow > rmaSlow)
        
        emaS = (close_np[-1] < ema9 and ema9 < rma9 and 
                close_np[-1] < emaSlow and emaSlow < rmaSlow)
        
        # TSI conditions
        tsiLong = (tsi >= self.params.periodThresLong)
        tsiShort = (tsi <= self.params.periodThresShort)
        
        # RSI conditions
        rsiLong = (RSI < RSIFast and RSI > 50)
        rsiShort = (RSI > RSIFast and RSI < 50)
        
        # Level break conditions
        levelBreakS = (self.s2 is not None and 
                      close_np[-1] < self.s2 and 
                      not(open_np[-1] - close_np[-1] < high_np[-1] - open_np[-1]) and 
                      close_np[-1] >= 4.0)
        
        levelBreakL = (self.r2 is not None and 
                      close_np[-1] > self.r2 and 
                      not(open_np[-1] - low_np[-1] > close_np[-1] - open_np[-1]))
        
        # Trend conditions
        longTrend = emaL and tsiLong and rsiLong
        shortTrend = emaS and tsiShort and rsiShort and close_np[-1] >= 4.0
        
        # === Signal Generation ===
        if shortTrend:
            self.shortTrendSig = True
        if longTrend:
            self.longTrendSig = True
        
        if levelBreakL:
            self.longLevelBreakSig = True
        if levelBreakS:
            self.shortLevelBreakSig = True
        
        if volSig:
            self.volCondSig = True
        if belowMean:
            self.volCondSig = False
        
        # Calculate signals
        self.newLongSig = (longTrend and self.volCondSig and 
                          self.longLevelBreakSig and self.longTrendSig)
        self.newShortSig = (shortTrend and self.volCondSig and 
                           self.shortLevelBreakSig and self.shortTrendSig)
        
        if emaS or belowMean:
            self.newLongSig = False
            self.longTrendSig = False
        if emaL or belowMean:
            self.newShortSig = False
            self.shortTrendSig = False
        
        # === Entry Conditions ===
        longCondition = self.newLongSig and not self.prevLongSig and self.totalTrades == 0
        shortCondition = self.newShortSig and not self.prevShortSig and self.totalTrades == 0
        
        # Place trades using 100% of account balance as requested
        if longCondition:
            size = self.broker.getcash() / close_np[-1]
            self.buy(size=size)
            self.totalTrades += 1
            print(f"LONG Signal @ {self.data.datetime.datetime(0)} | Price: {close_np[-1]:.2f} | Size: {size:.2f}")
        
        if shortCondition:
            size = self.broker.getcash() / close_np[-1]
            self.sell(size=size)
            self.totalTrades += 1
            print(f"SHORT Signal @ {self.data.datetime.datetime(0)} | Price: {close_np[-1]:.2f} | Size: {size:.2f}")
        
        # Additional entry conditions from the original script
        if (len(close_np) > 1 and 
            close_np[-1] != close_np[-2] and 
            close_np[-1] > high_np[-2]):
            size = self.broker.getcash() / close_np[-1]
            self.buy(size=size)
            self.totalTrades += 1
            print(f"LONG Breakout @ {self.data.datetime.datetime(0)} | Price: {close_np[-1]:.2f} | Size: {size:.2f}")
        
        if (len(close_np) > 1 and 
            close_np[-1] != close_np[-2] and 
            close_np[-1] < low_np[-2]):
            size = self.broker.getcash() / close_np[-1]
            self.sell(size=size)
            self.totalTrades += 1
            print(f"SHORT Breakdown @ {self.data.datetime.datetime(0)} | Price: {close_np[-1]:.2f} | Size: {size:.2f}")
        
        # Update signal states
        self.prevLongSig = self.newLongSig
        self.prevShortSig = self.newShortSig


def get_ib_data(symbol, start_date, end_date, exchange='SMART', currency='USD'):
    """
    Get historical 1-minute OHLC data from Interactive Brokers
    
    Note: IB limits how much 1-minute data you can fetch in a single request,
    so we may need to make multiple requests for longer time periods
    """
    ib = IB()
    try:
        # Convert string dates to datetime if needed
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        
        # Add one day to end_date to include the full day
        end_date = end_date + timedelta(days=1)
        
        # Connect to IB TWS/Gateway
        print("Connecting to Interactive Brokers...")
        ib.connect('127.0.0.1', 4002, clientId=1)
        
        # Define the contract
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = exchange
        contract.currency = currency
        
        print(f"Fetching 1-minute data for {symbol} from {start_date.date()} to {end_date.date()}...")
        
        # For 1-minute data, we need to break up requests into smaller chunks
        # IB typically allows up to 1-2 weeks of 1-minute data per request
        
        all_bars = []
        current_date = start_date
        
        while current_date < end_date:
            # Set the end date for this chunk (7 days later or end_date, whichever is earlier)
            chunk_end = min(current_date + timedelta(days=7), end_date)
            
            # Format the endDateTime for IB API
            end_str = chunk_end.strftime('%Y%m%d %H:%M:%S')
            
            # Calculate duration string based on the difference
            days_diff = (chunk_end - current_date).days + 1
            duration_str = f"{days_diff} D"
            
            print(f"  Requesting chunk: {current_date.date()} to {chunk_end.date()}")
            
            # Request historical data
            bars = ib.reqHistoricalData(
                contract=contract,
                endDateTime=end_str,
                durationStr=duration_str,
                barSizeSetting='1 min',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1,
                keepUpToDate=False
            )
            
            if bars:
                all_bars.extend(bars)
                print(f"  Received {len(bars)} bars")
            else:
                print(f"  No data received for this chunk")
            
            # Move to the next chunk
            current_date = chunk_end
        
        # Convert to Pandas DataFrame
        df = util.df(all_bars) if all_bars else pd.DataFrame()
        
        if df.empty:
            print(f"No data returned for {symbol}")
            return None
        
        # Format for Backtrader
        df['datetime'] = pd.to_datetime(df['date'])
        df.set_index('datetime', inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume']]
        
        print(f"Successfully retrieved {len(df)} bars of 1-minute data")
        return df
    
    except Exception as e:
        print(f"Error fetching data from IB: {e}")
        return None
    
    finally:
        ib.disconnect()
        print("Disconnected from Interactive Brokers")


def validate_date(date_str):
    """Validate and convert date string to datetime object"""
    try:
        return pd.to_datetime(date_str)
    except:
        return None


def run_strategy(symbol, start_date, end_date, cash=7500):
    """
    Run the MP Signal Strategy with specified parameters
    """
    # Initialize Cerebro
    cerebro = bt.Cerebro()
    
    # Add the strategy
    cerebro.addstrategy(MPSignalStrategy)
    
    # Get data from Interactive Brokers
    df = get_ib_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date
    )
    
    if df is None or df.empty:
        print("Failed to retrieve data or no data available. Exiting.")
        return
    
    # Create data feed and add to Cerebro
    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data)
    
    # Set starting cash
    cerebro.broker.setcash(cash)
    
    # Set commission (0.1%)
    cerebro.broker.setcommission(commission=0.001)
    
    # Print starting conditions
    print(f'Starting Portfolio Value: ${cerebro.broker.getvalue():.2f}')
    
    # Run the strategy
    print("\nRunning strategy...")
    cerebro.run()
    
    # Print results
    final_value = cerebro.broker.getvalue()
    profit_loss = final_value - cash
    percent_return = (profit_loss / cash) * 100
    
    print("\n==== STRATEGY RESULTS ====")
    print(f'Final Portfolio Value: ${final_value:.2f}')
    print(f'Profit/Loss: ${profit_loss:.2f} ({percent_return:.2f}%)')
    
    # Plot the results
    print("\nGenerating plot...")
    try:
        cerebro.plot(style='candlestick')
    except Exception as e:
        print(f"Error generating plot: {e}")
        print("This might be due to missing dependencies or too many data points.")
        print("Consider reducing the date range for better visualization.")


if __name__ == "__main__":
    print("\n==== MP Signal Strategy Backtester ====\n")
    
    # Get ticker symbol from user
    symbol = input("Enter ticker symbol (e.g., AAPL): ").strip().upper()
    if not symbol:
        print("Invalid ticker symbol. Exiting.")
        sys.exit(1)
    
    # Get start date from user
    start_date_str = input("Enter start date (YYYY-MM-DD): ").strip()
    start_date = validate_date(start_date_str)
    if start_date is None:
        print("Invalid start date format. Exiting.")
        sys.exit(1)
    
    # Get end date from user
    end_date_str = input("Enter end date (YYYY-MM-DD): ").strip()
    end_date = validate_date(end_date_str)
    if end_date is None:
        print("Invalid end date format. Exiting.")
        sys.exit(1)
    
    # Validate date range
    if end_date <= start_date:
        print("End date must be after start date. Exiting.")
        sys.exit(1)
    
    # Get account size (optional)
    account_size_str = input("Enter account size in $ (default: 7500): ").strip()
    try:
        account_size = float(account_size_str) if account_size_str else 7500
    except:
        print("Invalid account size. Using default value of $7500.")
        account_size = 7500
    
    print(f"\nBacktesting {symbol} from {start_date.date()} to {end_date.date()} with ${account_size:.2f}")
    print("Using 1-minute bars for higher resolution analysis")
    print("\nPreparing data. This may take some time depending on the date range...")
    
    # Run the strategy
    run_strategy(symbol, start_date, end_date, cash=account_size)