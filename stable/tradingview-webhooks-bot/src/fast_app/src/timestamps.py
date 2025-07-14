import asyncio, os, time, pytz
from datetime import datetime
def current_millis() -> int:
    return int(time.time_ns() // 1_000_000)

async def get_timestamp() -> str:
    # Adjust timestamp (subtracting 4 hours) if needed for
    ts = current_millis() - (4 * 60 * 60 * 1000)
    return str(ts)

def is_market_hours():
    ny_tz = pytz.timezone('America/New_York')
    now = datetime.now(ny_tz)
    if now.weekday() >= 5:
        return False
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close
def is_weekend():
    ny_tz = pytz.timezone('America/New_York')
    now = datetime.now(ny_tz)
    # weekday() returns 0-6 where 0 is Monday and 6 is Sunday
    # So 5 is Saturday and 6 is Sunday
    return now.weekday() >= 5