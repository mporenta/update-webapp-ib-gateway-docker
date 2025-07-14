import csv
import asyncio
from datetime import datetime
from math import log
from ib_async import IB
from ib_async.objects import RealTimeBar, RealTimeBarList
from ib_async.contract import Contract
from ib_async.util import isNan
from collections import defaultdict, deque
from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, HTTPException
from pathlib import Path
import os
from log_config import log_config, logger
import httpx
import json
log_config.setup()

class SimulatedIB:
    def __init__(self):
        self.bars= RealTimeBarList()
        self.sim_bars  =defaultdict(RealTimeBar)
        

    async def reqRealTimeBars(
        self,
        contract: Contract,
        barSize: int,
        whatToShow: str,
        useRTH: bool,
        realTimeBarsOptions=None,
        csv_path: str = None,       # path to your exported CSV
        interval: float = 5.0,       # seconds between bars
        reqId=None  # optional request ID, if you want to specify one
    ) -> RealTimeBarList:
        # 1) Mirror IB’s real-time bar list
        
        
        self.bars.contract             = contract
        self.bars.barSize              = barSize
        self.bars.whatToShow           = whatToShow
        self.bars.useRTH               = useRTH
        self.bars.realTimeBarsOptions  = realTimeBarsOptions or []

        # 2) Start asyncio task to feed from CSV
        if csv_path:
            logger.info(f"Starting real-time bar simulation with CSV for: {self.bars}")
            
            asyncio.create_task(self._simulate_bars(self.bars, csv_path, interval))
            logger.info(f"Looping real-time bar simulation with CSV for: {self.bars}")

        return self.bars

    async def _simulate_bars(
        self,
        bars: RealTimeBarList,
        csv_path: str,
        interval: float
    ):
        logger.info(f"Simulating real-time bars from CSV: {csv_path}")
        latest_csv = self.get_latest_csv(csv_path)
        symbol= bars.contract.symbol if bars.contract else "Unknown"
        
        # Read CSV rows
        with open(latest_csv, newline='') as f:
            reader = csv.DictReader(f)
            logger.info(f"Reading CSV file: {latest_csv}")
            for row in reader:
               
                ts = int(row['time'])
                

                bar = RealTimeBar(
                    time    = ts,
                    endTime = ts+5,
                    open_   = float(row['open']),
                    high    = float(row['high']),
                    low     = float(row['low']),
                    close   = float(row['close']),
                    volume  = float(row['Volume']),
                    wap     = float(row.get('wap', 0.0)),
                    count   = int(row.get('count', 0))
                )
                
               

                self.bars.append(bar)
                self.sim_bars[symbol] = RealTimeBar(bar) 
                logger.info(f"Simulated bar: {bar}")
                #logger.info(f"Simulated self.sim_bars: {self.sim_bars[symbol]}")
                #logger.info(f"Simulated self.bars: {self.bars}")
                await post_bars(bar)
                
                await asyncio.sleep(interval)
                
                

    def get_latest_csv(self, directory):
        """Retrieve the latest CSV file from the specified directory."""

        csv_files = list(Path(directory).glob('*.csv'))
        if not csv_files:
            raise FileNotFoundError("No CSV files found in directory")
        
        return str(max(csv_files, key=os.path.getctime))
    


    async def start_rt_bars(self, contract: Contract = None, reqId= None):
        directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
        
     

        
        self.bars = await self.reqRealTimeBars(
            contract,
            barSize=5,
            whatToShow='TRADES',
            useRTH=False,
            realTimeBarsOptions=[],
            csv_path=directory,
            interval=5.0,
            reqId= ""
        )

        # subscribe your handler to the simulated updateEvent
        

        # keep the event loop running while simulation runs…
        await asyncio.Event().wait()
        logger.info("Real-time bar simulation started.")
        return self.bars
  
   
ib_sim = SimulatedIB()   


# ——— Usage with CSV-loaded objects ———
async def post_bars(bars) -> None:
    """
    Forwards the incoming Yelp webhook payload to Zapier via an HTTP POST request.

    Args:
        json_data (dict): The webhook payload data to forward.
    """
    headers = {"Content-Type": "application/json"}
    try:
        logger.info(f"Forwarding request to bars...{bars}")
        url ="http://localhost:5011/rt-bars"
        
        # Floor the bar time to a 5-second interval.
        last_bar = bars

        logger.info(f"Last bar: {last_bar}")
        # Build the confirmed_bar dictionary.
        # Only update if the incoming value is not NaN; otherwise preserve (or leave as None).
        

    
        
        
        # Ensure datetime objects are properly serialized
        serializable_data = json.loads(
            json.dumps(bars, default=lambda obj: obj.isoformat() if isinstance(obj, datetime) else str(obj))
        )
        payload=jsonable_encoder(bars)
        logger.info(f"Forwarding request to Zapier with data: {payload}")
        
        response = httpx.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            logger.error(f"Forwarding to Zapier failed with status: {response.status_code}")
        else:
            logger.info("Successfully forwarded request to Zapier.")
    except Exception as e:
        logger.error(f"Error forwarding request to Zapier: {str(e)}")

