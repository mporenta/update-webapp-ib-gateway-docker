import asyncio
import datetime
import json
import re
from typing import Dict, Optional
from collections import defaultdict
from math import log
import xml.etree.ElementTree as ET
from ib_async import *                                    
from ib_async.objects import ScannerSubscription, TagValue    
from matplotlib.pyplot import sca
from tabulate import tabulate                                 # Table formatting :contentReference[oaicite:11]{index=11}
from models import subscribed_contracts, is_coming_soon
from log_config import log_config, logger
log_config.setup()  # Setup logging configuration


ib = IB()
subscribed_contracts.set_ib(ib)
async def connnect_ib():
    # Connect to TWS (Async)
    logger.info("Connecting to TWS...from pol_wsh")
    
    await ib.connectAsync('127.0.0.1', 4002, clientId=1111)
    #await scan_parms_xml()
    await ib.getWshMetaDataAsync()

    #await wh_horizons()
    
    
    
    
                     
async def scan_parms_xml():
    scan_parms = await ib.reqScannerParametersAsync()  # Get scanner parameters :contentReference[oaicite:8]{index=8}
    scanner_root = ET.fromstring(scan_parms)  # Parse XML response
    scanner_xml = ET.ElementTree(scanner_root)
    

    scanner_xml.write('scanner_parameters.xml')  # Save to file for reference
    logger.info("Scanner parameters saved to 'scanner_parameters.xml'")
    logger.info("Available scanner parameters:")
    await scan_and_log()
async def larry_scan():
    allParams = await ib.reqScannerParametersAsync()
    #logger.info(allParams)

    subscription = ScannerSubscription(instrument='STK', locationCode='STK.US.MAJOR', scanCode='SCAN_currYrETFFYDividendYield_DESC')

    scanData = await ib.reqScannerDataAsync(subscription)

    for scan in scanData:
        #logger.info(scan)
        logger.info(scan.contractDetails.contract.symbol) 
    await scan_and_log()     
async def wh_horizons():
    datWsha = WshEventData(
        filter = '''{
          "country": "All",
          "watchlist": ["500289391"],
          "limit_region": 10,
          "limit": 10,
          "wshe_ed": "true",
          "wshe_bod": "true"
        }''')
   
    contract = Stock('RGTI', 'SMART', 'USD')
    qualify_contract=await ib.qualifyContractsAsync(contract)  
    conId=qualify_contract[0].conId
    
    #logger.info(meta)
    #wsh= ib.reqWshMetaData()
    #logger.info(wsh)
    
    logger.info(f"ConId: {conId} for {contract.symbol}")
    data = WshEventData()
    data.conId = conId
    data.startDate = "20250101"
    data.totalLimit = 100
    
    #ib.reqWshEventData(datWsha)
    #events = await ib.getWshEventDataAsync(data)
    #logger.info(events)
    await process_earnings_dates(qualify_contract[0], conId)
    ib.disconnect()
    
    
   
    
    
async def scan_and_log():
    # Connect to TWS (Async)
    # 1. Scanner setup — use subscription fields instead of TagValue for market cap & volume
    sub = ScannerSubscription(
        instrument='STK',
        locationCode='STK.US.MAJOR',
        scanCode='MOST_ACTIVE_AVG_USD',
        abovePrice=2.0,            # Last price > $2.00
        #marketCapAbove=50_000_000, # Market cap > $50 M
        #aboveVolume=600_000        # Avg. 10‑day volume > 600 K
    )

    # 2. One‑shot scan
    scan_data = await ib.reqScannerDataAsync(sub)
    logger.info(f"Found {scan_data} stocks matching criteria")

    # 3. Twitter client
    

    results = []
    tz_et = datetime.timezone(datetime.timedelta(hours=-4))

    for cd in scan_data:
        c   = cd.contract
        sym = c.symbol

        # Snapshot for last price
        [tkr]       = await ib.reqTickersAsync(c)
        last_price  = tkr.last or cd.rank

        # 10‑day daily bars for prev_close & avg vol
        dly = await ib.reqHistoricalDataAsync(
            c, '', '10 D', '1 day', 'TRADES', True, 1
        )
        prev_close = dly[-1].close
        avg10d_vol = sum(b.volume for b in dly) / len(dly)

        # Pre‑market 1‑min bars (04:00–09:30 ET)
        pm = await ib.reqHistoricalDataAsync(
            c, '', '5 H', '1 min', 'TRADES', False, 1
        )
        opens = [b.open for b in pm if b.date.astimezone(tz_et).hour == 4]
        open04 = opens[0] if opens else None
        pm_vol = sum(
            b.volume for b in pm
            if 4 <= b.date.astimezone(tz_et).hour < 9
               or (b.date.astimezone(tz_et).hour == 9 and b.date.astimezone(tz_et).minute <= 30)
        )

        # Gap & vol filter
        if not open04 or open04 <= abs(prev_close * 1.03) or pm_vol <= 120_000:
            continue

        #
     
  
        results.append({
            'Symbol':       sym,
            'LastPrice':    last_price,
            'Avg10DayVol':  int(avg10d_vol),
            'PreMarketVol': pm_vol,
            '04Open':       open04,
            'PrevClose':    prev_close
           
        })

    # 4. Log the table
    if results:
        headers = results[0].keys()
        rows    = [list(r.values()) for r in results]
        table   = tabulate(rows, headers=headers, tablefmt='grid')
        logger.info(f"\n{table}")                   # neat grid output :contentReference[oaicite:15]{index=15}

    ib.disconnect()

async def get_earnings_dates(symbol: str, limit=10) -> Optional[str]:
    """
    Fetch and return the raw JSON of earnings‑date events for a single symbol.
    """
    tasks=[]
    contract=None
    conId=None
    logger.info(f"Fetching earnings dates for {symbol}…")
    if not ib.isConnected():
        await connnect_ib()

    contracts = await subscribed_contracts.get(symbol)
    contract = contracts[0]
    if not contract:
        logger.error(f"Couldn’t qualify contract for {symbol}")
        return None
    conId = contract.conId
    if conId:
                    
                    
        tasks.append(process_earnings_dates(contract, conId))

    else:
        logger.info(
            f"Skipping {symbol}: already has a pending order/trade."
        )

    # Execute all close orders simultaneously
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Process results to check for errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error executing close order: {result}")
        logger.info(
            f"Close orders have been placed for {len(tasks)} positions"
        )
        return tasks
    else:
        logger.info(
            "No positions eligible for closure; pending orders/trades exist for all positions."
        )
        return None


async def get_earnings_dates_batch(
    symbols: list[str],
    limit: int = 200
) -> Dict[str, Optional[str]]:
    """
    Batch‑fetch earnings dates for all `symbols`. Returns a dict mapping
    symbol → raw JSON (or None on failure).
    """
    # 1. Connect once
    if not ib.isConnected():
        await connnect_ib()

    # 2. Spin up one get_earnings_dates task per symbol
    logger.info(f"Fetching earnings dates for {len(symbols)} symbols…")
    tasks = {
        symbol: asyncio.create_task(get_earnings_dates(symbol, limit))
        for symbol in symbols
    }
    logger.info(f"Tasks: {tasks}")

    # 3. Wait for them all to finish
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    # 4. Clean up your connection
    await ib.disconnect()

    # 5. Zip them back up into a dict
    return {
        sym: (res if not isinstance(res, Exception) else None)
        for sym, res in zip(tasks.keys(), results)
    }
     
async def process_earnings_dates(contract: Contract, conId: int, limit=10):  
    try:  
        data = WshEventData(
                filter = f'''{{
                    "country": "All",
                    "watchlist": ["{conId}"],
                    "limit": {limit},
                    "wshe_ed": "true"
                }}'''
            )
    
        # Get earnings date events
   
        events_json = await ib.getWshEventDataAsync(data)
        events = json.loads(events_json)
        d = None
        earnings_date = None
        status      = None
        time_of_day   = None


        logger.info(f"ConId: {conId} for {contract.symbol}")
        logger.info(f"Earnings data for {contract.symbol}:")
        #logger.info(earnings_data)

        # Print out the earnings dates (corrected parsing)
        #logger.info("Upcoming and recent earnings dates:")
        earnings_dates_found = False
        future_earnings_dates = False
        for event in events:
            if event.get('event_type') != 'wshe_ed':
                continue
            try:
                d = event.get('data', {})
                earnings_date_full = d.get('earnings_date')          # e.g. "20250512"
                earnings_date = datetime.datetime.strptime(d.get('earnings_date'), "%Y%m%d").date()              # e.g. "20250512"
                status        = d.get('wshe_earnings_date_status')  # "CONFIRMED", etc.
                time_of_day   = d.get('time_of_day')                # "Before Market", etc.
                future_date=is_coming_soon(earnings_date_full, 3)
                #logger.info(f"ConId: {conId} for {contract.symbol} has earnings date: {earnings_date_full} and status: {status} and time_of_day: {time_of_day} future_date: {future_date}")
               
        

                # parse YYYYMMDD into a date object
        
            
            except Exception:
                continue

            # only care about CONFIRMED
            if status != "CONFIRMED":
                continue

            # get today and previous trading day (skip weekends)
            today = datetime.date.today()
            
            day_of_week = today.weekday()  # Monday=0, Sunday=6
            is_weekend = day_of_week >= 5  # Saturday or Sunday
            if future_date:
                
                future_earnings_dates = True
        


            prev_day = today - datetime.timedelta(days=1)
            trading_day_tomorrow = today + datetime.timedelta(days=1)
            saturday= today.weekday() == 5
            sunday= today.weekday() == 6
            if is_weekend:
                if saturday:
                    trading_day_tomorrow = today + datetime.timedelta(days=2)
                elif sunday:
                    trading_day_tomorrow = today + datetime.timedelta(days=1)   

            
            if prev_day.weekday() == 6:      # Sunday → back up to Friday
                prev_day -= datetime.timedelta(days=2)
            elif prev_day.weekday() == 5:    # Saturday → back up to Friday
                prev_day -= datetime.timedelta(days=1)
            elif is_weekend:  # Today is Saturday or Sunday
                prev_day = today - datetime.timedelta(days=2)
            # condition 1: today & (Before or During Market)
            if earnings_date == today and time_of_day in ("Before Market", "During Market"):
                earnings_dates_found = True

            # condition 2: prev trading day & After Market
            elif earnings_date == prev_day and time_of_day == "After Market":
                earnings_dates_found = True
            # condition 3: future date & Before Market
            elif is_weekend and time_of_day == "Before Market" and earnings_date == trading_day_tomorrow:
                earnings_dates_found = True
                logger.info(f"Weekend: {today} and time_of_day: {time_of_day}")
            elif earnings_date == trading_day_tomorrow and time_of_day == "Before Market":
                logger.info(f"earnings_date: {earnings_date} is tomorrow")
                earnings_dates_found = True
            


        if earnings_dates_found:
            logger.info(f"ConId: {conId} for {contract.symbol} has earnings date: {earnings_date} and status: {status} and time_of_day: {time_of_day}")
        if future_earnings_dates:
            logger.info(f"ConId: {conId} for {contract.symbol} has future earnings date: {earnings_date} and status: {status} and time_of_day: {time_of_day}")
        else:
            logger.info("No earnings dates found")
        
        ib.disconnect()  
        return events_json
    except Exception as e:
        logger.info(f"Error retrieving earnings dates: {e}")
        raise
if __name__ == '__main__':
    asyncio.run(connnect_ib())
