# scalping_strategy_async.py

import backtrader as bt
import pandas as pd
import numpy as np
from contextlib import asynccontextmanager

import datetime
import asyncio
from log_config import log_config, logger
# Import your PriceData class from ib_data.py.
# It is assumed that PriceData contains your async ohlc_vol function.
from ib_price_data import PriceData
from ib_async import *  # Ensure your IB contract import is available
log_config.setup()
ib=IB()
contract = Contract(
    symbol='GDHG',
    secType='STK',
    exchange='SMART',
    currency='USD'
)

###############################################################################
# Define the scalping strategy using Backtrader
###############################################################################
class ScalpingStrategy(bt.Strategy):
    params = dict(
        premarket_start=datetime.time(4, 00),    # assumed premarket period start
        market_open=datetime.time(9, 30),
        exit_time=datetime.time(15, 55),         # time to close positions
        volume_ma_period=10,                     # period for volume moving average and std dev
        volume_spike_multiplier=1.5,             # threshold: either MA + 1.5*STD or 1.5x MA
        risk_reward=2.0,                         # target is 2x risk
        max_position=30000,                      # maximum $ exposure per trade
        trade_once=True                         # one trade per day per stock
    )

    def __init__(self):
        # Flag to ensure only one trade per day
        self.trade_executed_today = False

        # Container to hold our trade details
        self.trade_log = []

        # Calculate rolling volume average and standard deviation
        self.vol_ma = bt.indicators.SimpleMovingAverage(self.data.volume, period=self.p.volume_ma_period)
        self.vol_std = bt.indicators.StandardDeviation(self.data.volume, period=self.p.volume_ma_period)

        # Variables for premarket high/low for each day
        self.premarket_high = None
        self.premarket_low = None
        self.current_day = None

    def next(self):
        # Get current bar datetime information
        current_datetime = self.data.datetime.datetime(0)
        current_time = current_datetime.time()
        current_date = current_datetime.date()

        # Reset daily flags on a new day
        if self.current_day != current_date:
            self.current_day = current_date
            self.trade_executed_today = False
            self.premarket_high = None
            self.premarket_low = None

        # --- Premarket Setup ---
        # Before market open, update premarket high/low from available bars.
        if current_time < self.p.market_open:
            if self.data.high[0] is not None:
                if self.premarket_high is None or self.data.high[0] > self.premarket_high:
                    self.premarket_high = self.data.high[0]
            if self.data.low[0] is not None:
                if self.premarket_low is None or self.data.low[0] < self.premarket_low:
                    self.premarket_low = self.data.low[0]
            return  # Do not evaluate further until regular trading

        # --- Entry Conditions (9:30 AM to 9:45 AM) ---
        market_open_dt = datetime.datetime.combine(current_date, self.p.market_open)
        eval_end_dt = market_open_dt + datetime.timedelta(minutes=15)
        if market_open_dt <= current_datetime <= eval_end_dt:
            if not self.trade_executed_today and self.premarket_high is not None and self.premarket_low is not None:
                current_vol = self.data.volume[0]
                ma = self.vol_ma[0]
                std = self.vol_std[0]

                # Volume spike detection: check if current volume exceeds either MA + multiplier * STD or multiplier * MA
                if current_vol > (ma + self.p.volume_spike_multiplier * std) or current_vol > (self.p.volume_spike_multiplier * ma):
                    # LONG condition: Price breaks above premarket high
                    if self.data.close[0] > self.premarket_high:
                        entry_price = self.data.close[0]
                        # Set stop-loss a few ticks below premarket high (adjust as needed)
                        stop_loss = self.premarket_high - 0.05  
                        risk = entry_price - stop_loss
                        take_profit = entry_price + risk * self.p.risk_reward
                        position_size = self.p.max_position / entry_price

                        self.trade_log.append({
                            'date': current_date,
                            'time': current_time.strftime('%H:%M:%S'),
                            'side': 'LONG',
                            'entry_price': entry_price,
                            'stop_loss': stop_loss,
                            'take_profit': take_profit,
                            'position_size': position_size,
                            'entry_datetime': current_datetime
                        })
                        self.trade_executed_today = True
                        self.log(f"LONG entry at {entry_price:.2f} | SL: {stop_loss:.2f} | TP: {take_profit:.2f} | Volume: {current_vol}")

                    # SHORT condition: Price breaks below premarket low
                    elif self.data.close[0] < self.premarket_low:
                        entry_price = self.data.close[0]
                        stop_loss = self.premarket_low + 0.05  
                        risk = stop_loss - entry_price
                        take_profit = entry_price - risk * self.p.risk_reward
                        position_size = self.p.max_position / entry_price

                        self.trade_log.append({
                            'date': current_date,
                            'time': current_time.strftime('%H:%M:%S'),
                            'side': 'SHORT',
                            'entry_price': entry_price,
                            'stop_loss': stop_loss,
                            'take_profit': take_profit,
                            'position_size': position_size,
                            'entry_datetime': current_datetime
                        })
                        self.trade_executed_today = True
                        self.log(f"SHORT entry at {entry_price:.2f} | SL: {stop_loss:.2f} | TP: {take_profit:.2f} | Volume: {current_vol}")

        # --- Exit Conditions ---
        # Time-based exit: exit trades at or after exit_time
        if current_time >= self.p.exit_time and self.trade_executed_today:
            if 'exit_price' not in self.trade_log[-1]:
                exit_price = self.data.close[0]
                self.trade_log[-1]['exit_price'] = exit_price
                self.trade_log[-1]['exit_datetime'] = current_datetime
                self.log(f"Exiting trade at {exit_price:.2f} due to end-of-day exit.")

    def log(self, txt, dt=None):
        dt = dt or self.data.datetime.datetime(0)
        logger.info(f'{dt.isoformat()} | {txt}')


###############################################################################
# Asynchronous part: Retrieve historical data using your ohlc_vol function.
###############################################################################
price_data = PriceData(ib)

    
async def get_ib_connect():
    logger.info("Starting application; connecting to IB..")
    if not await price_data.connect():
        raise RuntimeError("Failed to connect to IB Gateway")
    df = await get_price_data(contract)
    return df
    
    
async def get_price_data(contract: Contract) -> pd.DataFrame:
    # Shutdown: Gracefully disconnect
    
    # Request historical data using your async ohlc_vol method.
    # Adjust parameters as desired. Here we use '1 D' duration, '1 min' bars, 'TRADES' data, and regular trading hours.
    durationStr='1 D'  # Duration for historical data,
    barSizeSetting= '15 secs'
    df = await price_data.ohlc_vol(
        contract=contract,
        durationStr=durationStr,
        barSizeSetting=barSizeSetting,
        whatToShow='TRADES',
        useRTH=True,
        return_df=True
    )
    if df is None:
        raise ValueError("Failed to retrieve historical data from IB")
    return df

###############################################################################
# Main block: Get IB historical data asynchronously, then run Backtrader.
###############################################################################
if __name__ == '__main__':
    # Define your IB contract. Adjust the parameters as needed.
  
    # Retrieve data from IB asynchronously.
    try:
        df = asyncio.run(get_ib_connect())
    except Exception as e:
        logger.info(f"Error retrieving data: {e}")
        exit(1)

    # Make sure the DataFrame has a datetime column and set it as index.
    # IB data via util.df should already include a datetime column.
    if 'date' in df.columns:
        df.rename(columns={'date': 'datetime'}, inplace=True)
    elif 'datetime' not in df.columns:
        # If missing, try to create one from index (customize as needed)
        df['datetime'] = pd.to_datetime(df.index)
    df.set_index('datetime', inplace=True)

    # Display some information about the loaded data
    logger.info(f"Loaded {len(df)} bars of data from IB.")

    # Create a Backtrader data feed from the DataFrame.
    data = bt.feeds.PandasData(dataname=df)

    # Initialize Cerebro and add the strategy.
    cerebro = bt.Cerebro()
    cerebro.addstrategy(ScalpingStrategy)
    cerebro.adddata(data)

    # Set initial cash for backtest simulation.
    cerebro.broker.setcash(25000.0)

    logger.info('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    logger.info('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Retrieve the trade log from the strategy for analysis.
    # Since Backtrader does not directly expose strategy instances from cerebro.run(),
    # one common approach is to grab it from the runstrats list.
    strat_instance = cerebro.runstrats[0][0]
    trade_log_df = pd.DataFrame(strat_instance.trade_log)
    logger.info(trade_log_df)

    # Optionally, save the trade log to CSV.
    trade_log_df.to_csv('scalping_trade_log.csv', index=False)
