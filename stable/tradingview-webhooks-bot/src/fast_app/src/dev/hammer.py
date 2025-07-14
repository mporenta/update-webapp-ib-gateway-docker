import backtrader as bt
from log_config import log_config, logger
import pytz
import talib as ta
from talib import CDLHAMMER
import numpy as np

log_config.setup()
NY_TZ = pytz.timezone("America/New_York")

class HammerBacktest(bt.Strategy):
    def __init__(self):
        self.logger = logger
        self.hammer_detected = []

        # Keep rolling lists of candles
        self.opens = []
        self.highs = []
        self.lows = []
        self.closes = []

    def next(self):
        # Pull latest candle data
        o, h, l, c = self.data.open[0], self.data.high[0], self.data.low[0], self.data.close[0]
        self.opens.append(o)
        self.highs.append(h)
        self.lows.append(l)
        self.closes.append(c)

        # Limit to last 100 bars
        if len(self.opens) > 100:
            self.opens.pop(0)
            self.highs.pop(0)
            self.lows.pop(0)
            self.closes.pop(0)

        # Wait until we have enough data
        if len(self.opens) < 5:
            return

        open_arr = np.array(self.opens, dtype=np.float64)
        high_arr = np.array(self.highs, dtype=np.float64)
        low_arr = np.array(self.lows, dtype=np.float64)
        close_arr = np.array(self.closes, dtype=np.float64)

        hammer_signal = CDLHAMMER(open_arr, high_arr, low_arr, close_arr)

        if hammer_signal[-1] == 100 and (h == c or o == h):
            dt_utc = self.data.datetime.datetime(0)
            dt_ny = dt_utc.replace(tzinfo=pytz.utc).astimezone(NY_TZ)
            self.logger.info(f"HAMMER DETECTED (TA-Lib) at {dt_ny} | Open: {o:.2f} High: {h:.2f} Low: {l:.2f} Close: {c:.2f}")
            

                
class TVOpeningVolatilityScan(bt.Strategy):
    def __init__(self):
        self.logger = logger
        self.matches = []

    def next(self):
        try:
            
            dt = self.data.datetime.datetime(0).replace(tzinfo=pytz.utc).astimezone(NY_TZ)
            if dt.hour == 9 and 30 <= dt.minute <= 36:
                #self.logger.info(f"Processing data for {dt}...")
                o = self.data.open[0]
                c = self.data.close[0]
                c1 = self.data.close[-1]  # Previous close for comparison
                volume= self.data.volume[0]
                volMA = self.data.volumeMA[0]  # Volume moving average for comparison
                if o == 0:
                    logger.warning(f"Open price is zero at {dt}, skipping...")
                    return
                change = abs((c - c1) / c1) * 100
                if change >= 3 and volume > volMA :
                    self.logger.warning(f"TV CSV MATCH at {dt} | Open: {o:.2f} Close: {c:.2f} Change: {change:.2f}%")
                    self.matches.append({'datetime': dt, 'open': o, 'close': c, 'change_pct': round(change, 2)})
                
        except Exception as e:
            self.logger.error(f"Error in TVOpeningVolatilityScan: {e}")
            raise