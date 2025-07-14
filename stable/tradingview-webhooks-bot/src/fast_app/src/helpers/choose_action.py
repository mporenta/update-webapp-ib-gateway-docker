from typing import *
from math import *
import talib as ta  
import pandas as pd
from ib_async import Contract, IB
import talib as ta  
import pandas as pd
from log_config import log_config, logger
from ib_async import *
from tv_ticker_price_data import price_data_dict
from dataclasses import asdict
import numpy as np
from my_util import  clean_nan
log_config.setup()


from models import PriceSnapshot, OrderRequest
    
async def choose_action(req: OrderRequest, contract: Contract, snapshot: PriceSnapshot, uptrend: bool, barSizeSetting, ib=None) -> str:
    """
    Choose an action based on the current price snapshot.
    
    :param snapshot: The PriceSnapshot containing market data.
    :param ib: The IB instance (optional).
    :return: "BUY" or "SELL" based on the snapshot data.

    """
    action=  ""
    emaCheck = None
    snapshot=await price_data_dict.get_snapshot(contract.symbol)
    ema =await ema_check(contract, ib, barSizeSetting)  # Ensure EMA is checked for the contract
    emaCheck, _ = ema
    if emaCheck is None:
        
        logger.error(f"emaCheck order Action for {contract.symbol} is {action} emaCheck {emaCheck}"
        )
        raise ValueError(f"emaCheck is None for {contract.symbol}, cannot determine action")
   
        
    if emaCheck =="LONG"  and uptrend:
        action= "BUY"
    elif emaCheck =="SHORT" and not uptrend:
        action= "SELL"
    else:
        action=  ""
    return action
    
async def ema_check(contract: Contract, ib: IB=None, barSizeSetting=None) -> str:
    """
    Check if the last close is above the EMA for the given period.
    """
    try:
        if barSizeSetting is None:
            barSizeSetting = "1 min"
        df_d, df_m, daily_bars, minute_bars=await ib_data(contract, ib, barSizeSetting)  # Ensure df is populated with historical data
        if df_m.empty:
            raise ValueError("No historical data available for the contract")
        logger.info(f"Checking EMA for {contract.symbol} with df_m length {len(df_m)}")
        emaCheck = ""
        
        logger.debug(f"Checking EMA for {contract.symbol} ")
        
        snapshot=await price_data_dict.get_ticker(contract.symbol)
        snapshot_json_cleaned = {}
       
        try:
        
            # turn the dataclass into a dict…
            data = asdict(snapshot)
            # drop the Ticker object (not serializable) and any others you don’t want
            data.pop("ticker", None)
            # serialize datetime → ISO string
            if data.get("time") is not None:
                data["time"] = data["time"].isoformat()
            # now clean out NaNs/inf
            snapshot_json_cleaned = clean_nan(data)

        except Exception as e:
            logger.error(f"Error processing json ticker data for {contract.symbol}: {e}")
            return None


        # Calculate the EMA
        close_m = df_m["close"].values
        last_close = close_m[-1]
        logger.debug(f"Checking EMA for {contract.symbol} with close_m  {close_m}")
        df_m["EMA9"] = ta.EMA(close_m, timeperiod=9)
        current_EMA9 = safe_float(df_m["EMA9"].iloc[-1])

        logger.debug(f"Checking EMA for {contract.symbol} with current_EMA9  {current_EMA9}")
        

        # Get the last close and last EMA value
        
        

        logger.debug(f"Checking EMA for {contract.symbol} with last_close  {last_close}")
        if current_EMA9 < safe_float(last_close):
            emaCheck = "LONG"
        elif current_EMA9 > safe_float(last_close):
            emaCheck = "SHORT" 
        else:
            emaCheck = None

        # Check if the last close is above the EMA
        logger.info(f"EMA check for {contract.symbol}: last_close={last_close}, current_EMA9={current_EMA9}, result={emaCheck}")
        return emaCheck, snapshot_json_cleaned
    except Exception as e:
        logger.error(f"Error in ema_check for {contract.symbol}: {e}")
        return None
async def ib_data(contract: Contract, ib: IB = None, barSizeSetting=None, durationStr_min = None) -> dict:
    """
    Pull daily + 1-min history, enrich with TA-Lib indicators, and return JSON-ready dict.
    """
    try:
        if durationStr_min is None:
            durationStr_min = "2700 S"
        daily_bars = []
        df_d = pd.DataFrame()
        logger.info(f"ib_data for {contract.symbol} started, will return data asynchronously for contract : {contract}")
        if barSizeSetting is None:
            barSizeSetting = "1 min"
        
        minute_bars = await ib.reqHistoricalDataAsync(
            contract=contract, 
            endDateTime="", 
            durationStr=durationStr_min,
            barSizeSetting=barSizeSetting, 
            whatToShow="TRADES", 
            useRTH=False, 
            formatDate=1,
        )
       

        # Fixed: Use util.df correctly from ib_insync
        logger.info(f"fetched ib_data for {contract.symbol} with barSizeSetting {barSizeSetting} started")
        df_m = util.df(minute_bars)
        
        
        if df_m.empty:
            logger.warning(f"Empty dataframes for {contract.symbol}")
            return {"error": "No historical data available"}

        logger.info(f"fetched ib_data for {contract.symbol} with barSizeSetting {barSizeSetting} and df_m length {len(df_m)}")

        return df_d, df_m, daily_bars, minute_bars
          

    except Exception as e:
        logger.error(f"Error in gpt_data for {contract.symbol}: {e}")
        return {"error": str(e)}
    
def safe_float(x):
    try:
        v = float(x)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except Exception:
        return None