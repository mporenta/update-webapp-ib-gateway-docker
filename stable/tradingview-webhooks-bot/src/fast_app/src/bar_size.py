from log_config import log_config, logger
from models import OrderRequest
from tv_ticker_price_data import  barSizeSetting_dict
log_config.setup()
def parse_timeframe_to_seconds(tf: str) -> int:
    tf = tf.strip().lower()
    if tf.endswith("secs") or tf.endswith("sec"):
        return int(tf.split()[0])
    if tf.endswith("mins") or tf.endswith("min"):
        return int(tf.split()[0]) * 60
    if tf.endswith("s"):
        return int(tf[:-1])
    if tf.endswith("m"):
        return int(tf[:-1]) * 60
    raise ValueError(f"Unrecognized timeframe: {tf}")

async def convert_pine_timeframe_to_barsize(timeframe: str=None, post_log=True,tv_hook: bool= False, req: OrderRequest= None) -> tuple[str, int]:
    """
    Convert Pine Script timeframe string to TWS API barSizeSetting

    Args:
        timeframe (str): Pine Script timeframe string (e.g., "10S", "30", "1D")

    Returns:
        str: Corresponding TWS API barSizeSetting
    """
    # Direct mapping of Pine Script timeframes to TWS API barSizeSetting
    try:
        symbol  = req.symbol 
        
        if req is None and timeframe is None:
            raise ValueError("timeframe cannot be None when req is None")

        if req is not None:
            symbol     = req.symbol
            timeframe  = req.barSizeSetting_tv if tv_hook else req.barSizeSetting_web

        
        barSizeSetting = None
        if tv_hook:
            logger.debug(f"Resolving contract for TradingView hook with symbol: {req.symbol} and barSizeSetting_tv: {req.barSizeSetting_tv}")
            timeframe = req.barSizeSetting_tv
           
        else:
            logger.debug(f"Resolving contract for symbol: {symbol} and barSizeSetting_web: {req.barSizeSetting_web}")
            timeframe = req.barSizeSetting_web
           

        is_valid_timeframe = "secs" in timeframe or "min" in timeframe  or "mins" in timeframe
        bar_sec_int = None
        
        if not is_valid_timeframe and post_log:
            logger.debug(f"Converting Pine timeframe '{timeframe}' to barSizeSetting...")
        if is_valid_timeframe:
            barSizeSetting = timeframe
            
            bar_sec_int=parse_timeframe_to_seconds(barSizeSetting)
            if post_log:
                logger.debug(f"Converted timeframe '{timeframe}' to barSizeSetting: {barSizeSetting} and bar_sec_int: {bar_sec_int}")

            return barSizeSetting, bar_sec_int

        # Seconds
        if timeframe == "1S":
            barSizeSetting = "1 secs"
        elif timeframe == "5S":
            barSizeSetting = "5 secs"
        elif timeframe == "10S":
            barSizeSetting = "10 secs"
        elif timeframe == "15S":
            barSizeSetting = "15 secs"
        elif timeframe == "30S":
            barSizeSetting = "30 secs"

        # Minutes (Pine uses numbers without units)
        elif timeframe == "1":
            barSizeSetting = "1 min"
        elif timeframe == "2":
            barSizeSetting = "2 mins"
        elif timeframe == "3":
            barSizeSetting = "3 mins"
        elif timeframe == "5":
            barSizeSetting = "5 mins"
        elif timeframe == "10":
            barSizeSetting = "10 mins"
        elif timeframe == "15":
            barSizeSetting = "15 mins"
        elif timeframe == "20":
            barSizeSetting = "20 mins"
        elif timeframe == "30":
            barSizeSetting = "30 mins"

        # Hours (Pine expresses hours in minutes)
        elif timeframe == "60":
            barSizeSetting = "1 hour"
        elif timeframe == "120":
            barSizeSetting = "2 hours"
        elif timeframe == "180":
            barSizeSetting = "3 hours"
        elif timeframe == "240":
            barSizeSetting = "4 hours"
        elif timeframe == "480":
            barSizeSetting = "8 hours"

        # Days, Weeks, Months
        elif timeframe == "1D":
            barSizeSetting = "1 day"
        elif timeframe == "1W":
            barSizeSetting = "1 week"
        elif timeframe == "1M":
            barSizeSetting = "1 month"

        # Default to 1 minute if timeframe not recognized
        else:
            
            barSizeSetting = timeframe
            barSizeSetting_dict[req.symbol]= barSizeSetting
            if post_log:
                logger.debug(f"Unrecognized timeframe '{timeframe}', defaulting to '1 min'")
        bar_sec_int=parse_timeframe_to_seconds(barSizeSetting)
        if post_log:
            logger.debug(f"Converted timeframe '{timeframe}' to barSizeSetting: {barSizeSetting} and bar_sec_int: {bar_sec_int}")
        return barSizeSetting, bar_sec_int

    except Exception as e:
        logger.error(f"Error converting Pine timeframe '{timeframe}' to barSizeSetting: {e}")
        raise ValueError(f"Unrecognized timeframe: {timeframe}") from e