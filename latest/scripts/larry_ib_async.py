from ib_async import *
import time
import os
import sys
do_trade = False
start_time= time.perf_counter_ns()
ib = IB()
ib.connect('127.0.0.1', 4002, clientId=8884488)
ib_conn = ib.isConnected()
print(f"IB connected: {ib_conn}")

stock = Stock('NVDA', 'SMART', 'USD')
contact = ib.qualifyContracts(stock)
print(contact[0])
bars = ib.reqHistoricalData(contact[0], endDateTime='', durationStr='4 D',
                         barSizeSetting='1 day', whatToShow='TRADES', useRTH=True,
                         formatDate=1)
if not bars:
    print("No bars received")
    ib.disconnect()
    # System exit to avoid hanging
    sys.exit(0)
print(bars)    
df = util.df(bars)
print(df)
if not df.empty:
    print(df['close'].iloc[-1])
    print("disconnecting...")
    ib.disconnect()
    # System exit to avoid hanging
    sys.exit(0)
end_time = time.perf_counter_ns()
ib.run()