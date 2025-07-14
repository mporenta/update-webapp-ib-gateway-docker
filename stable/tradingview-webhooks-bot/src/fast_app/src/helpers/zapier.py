import asyncio
import httpx
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from log_config import  logger
from models import (
    OrderRequest,
       
)
from my_util import  clean_nan,  ticker_to_dict,  is_market_hours, delete_orders_db, format_order_details_table
from tv_ticker_price_data import  tv_store_data


async def zapier_forward(symbol: OrderRequest, snapshot=None):
    
    try:
        symbol= symbol.symbol 
        
        form=await tv_store_data.get(symbol)
        
        logger.debug(f"Zapier forward form data for {symbol}: {form}")
        client = httpx.Client()

        reqUrl = "https://hooks.zapier.com/hooks/catch/10447300/2n8ncic/"

        headersList = {
        
        "Content-Type": "application/json",
        "Accept": "application/json" 
        }
        if form is None:
            form=symbol.model_dump()
        result_raw = {
            "OrderRequest": form,   
            "snapshot":        snapshot
        }
        payload=jsonable_encoder(result_raw)
        payload = clean_nan(payload)

        logger.debug(f"Zapier payload: {payload}")
        
        

        data = client.post(reqUrl, data=payload, headers=headersList)

        logger.debug(data.text)
        return None
    except Exception as e:
        logger.error(f"Error forwarding to Zapier: {e}")
        return None
    
async def zapier_relay(symbol: OrderRequest, snapshot=None):
    
    try:
        asyncio.create_task(zapier_forward(symbol,snapshot))
        return JSONResponse(content="", status_code=200)
        
    except Exception as e:
        logger.error(f"Error forwarding to Zapier: {e}")
        return None