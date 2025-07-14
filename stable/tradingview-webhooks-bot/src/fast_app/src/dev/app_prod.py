# app.py
from typing import *
import os, asyncio, time
import re
import threading
from turtle import pos
import pytz
import datetime
import math
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Response, Query, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import signal
import sys
from ib_async import *
from dotenv import load_dotenv
from models import OrderRequest, AccountPnL, TickerResponse, QueryModel, WebhookRequest, TickerSub, TickerRequest

# from pnl import IBManager
from new_pnl import IBManager

from timestamps import current_millis
from log_config import log_config, logger
from ticker_list import ticker_manager
from price import PriceData

from indicators import daily_volatility
from p_rvol import PreVol
from break_change import VolatilityScanner  # Import our new scanner
from trade_dict import trade_to_dict
from my_util import convert_pine_timeframe_to_barsize, extract_order_fields
global_close_prices = {}
ema9_dict = {}
# from hammer import HammerBacktest, TVOpeningVolatilityScan
load_dotenv()

# --- Configuration ---
PORT = int(os.getenv("FAST_API_PORT", "80"))
HOST = os.getenv("FAST_API_HOST", "localhost")
tbotKey = os.getenv("TVWB_UNIQUE_KEY", "WebhookReceived:1234")
risk_amount = float(os.getenv("WEBHOOK_PNL_THRESHOLD", "-300"))

boofMsg = os.getenv("HELLO_MSG")
subscribed_contracts: dict[str, Contract] = {}
# Instantiate IB manager
ib = IB()
ib_manager = IBManager(ib)
from vol_stop_price import PriceDataNew
ib_vol_data = PriceDataNew(ib_manager)
#ib_vol_data = PriceData(ib_manager)

pre_rvol = PreVol(ib_manager)




# --- FastAPI Setup ---
env_file = os.path.join(os.path.dirname(__file__), ".env")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))

 # Ensure the web_request is JSON serializable
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application lifespan...")
    logger.info(f"boof message {boofMsg}")
    # Create DB handler at top level

    shutdown_event = asyncio.Event()

    # Create a task that will respond to the shutdown event
    async def cleanup_task():
        await shutdown_event.wait()
        logger.info("Cleanup task triggered")

        # First disconnect from IB
        try:
            await ib_manager.graceful_disconnect()
            logger.info("IB disconnected successfully")
        except Exception as e:
            logger.error(f"Error during IB disconnect: {e}")

        # Then close the database connection
        try:
            # await ib_manager.db_handler.close()
            logger.info("Database connection closed.")
        except Exception as e:
            logger.error(f"Error closing database: {e}")

    # Start the cleanup task
    cleanup = asyncio.create_task(cleanup_task())

    try:
        # Startup: Connect to IB and other initialization
        logger.info(f"Starting application; connecting to IB...{risk_amount}")
        if not await ib_manager.connect():
            raise RuntimeError("Failed to connect to IB Gateway")
        ib_manager.ib.newOrderEvent += ib_manager.on_order_status_event
        #
    
   
        
       
        
        logger.info("Connected to IB Gateway")
        await asyncio.sleep(1)

        yield  # Application runs during this yield

    finally:
        # Signal shutdown and let the cleanup task handle the details
        logger.info("graceful disconnect - Shutting down application...")
        shutdown_event.set()

        try:
            # Wait for cleanup task with a timeout
            await asyncio.wait_for(cleanup, timeout=5.0)
            logger.info("Cleanup completed successfully")
        except asyncio.TimeoutError:
            logger.warning("Cleanup timed out - forcing shutdown")
        except asyncio.CancelledError:
            logger.info("Shutdown process was cancelled")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

        logger.info("Shutdown complete.")


app = FastAPI(lifespan=lifespan)
app.mount(
    "/static", StaticFiles(directory=os.path.join(CURRENT_DIR, "static")), name="static"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://portfolio.porenta.us", "https://tv.porenta.us"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    try:
        return templates.TemplateResponse("tbot_dashboard.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cancel-all-orders")
async def cancel_aal_orders():

    status = False
    # Just call the method without assigning the unused result

    orders = await ib_manager.ib.reqAllOpenOrdersAsync()
    status = await on_cancel_order(orders)
    if status:
        return JSONResponse(
            content={"status": "success", "message": "All orders cancelled"}
        )
    # Ensure we always return a response
    return JSONResponse(
        content={"status": "error", "message": "Failed to cancel all orders"}
    )


async def on_cancel_order(trades) -> bool:
    all_cancelled = False
    if trades:
        for trade in trades:
            if trade.orderStatus.status != "Cancelled" and not all_cancelled:
                ib_manager.ib.reqGlobalCancel()
                logger.info(f"Canceling orders: {trade.orderStatus.status}")
                await asyncio.sleep(2)
                return True
            else:
                logger.info(f"Processing order: {trade.orderStatus.status}")
                all_cancelled = True
                logger.info("No orders to cancel")
                return all_cancelled


@app.post("/api/cancel-order")
async def cancel_orders(order: Request):
    try:
        # Get raw JSON data from request
        raw_data = await order.json()
        logger.debug(f"Canceling order raw_data: {raw_data}")

        # Create a standardized response format
        parsed_data = {
            "timestamp": current_millis(),
            "request_data": raw_data,
            "parsed_fields": {
                "top_level_keys": (
                    list(raw_data.keys()) if isinstance(raw_data, dict) else []
                ),
                "data_types": (
                    {key: str(type(value).__name__) for key, value in raw_data.items()}
                    if isinstance(raw_data, dict)
                    else {}
                ),
            },
        }

        # Extract the needed fields
        order_fields = extract_order_fields(parsed_data)
        logger.debug(f"Extracted order fields: {order_fields}")
        
        # Get symbol and order data for logging
        symbol = order_fields.get("symbol")
        orderId = order_fields.get("orderId")
        conId = order_fields.get("conId")
        permId = order_fields.get("permId")
        account = order_fields.get("account")
        order_data = order_fields.get("order")
        
        logger.info(
            f"Canceling order for symbol: {symbol}, orderId: {orderId}, conId: {conId}, permId: {permId}, account: {account}"
        )
        
        # Initialize order_obj to None
        order_obj = None
        
        if order_data and isinstance(order_data, dict):
            # Only create Order object if we have valid order data
            order_obj = Order(**order_data)
            # Cancel the order
            ib_manager.ib.cancelOrder(order=order_obj, manualCancelOrderTime="")
            logger.info(f"Order cancellation sent for {symbol}, orderId: {orderId}")
            
            return JSONResponse(
                content={
                    "status": "success", 
                    "message": f"Order for {symbol} cancelled"
                }
            )
        elif orderId or permId:
            # If we have orderId or permId but no valid order object,
            # try to look up the order and cancel by ID
            if orderId:
                ib_manager.ib.cancelOrder(orderId=orderId)
                logger.info(f"Cancelled by orderId: {orderId}")
            elif permId:
                all_orders = await ib_manager.ib.reqAllOpenOrdersAsync()
                for o in all_orders:
                    if o.order.permId == permId:
                        ib_manager.ib.cancelOrder(order=o.order)
                        logger.info(f"Cancelled by permId: {permId}")
                        
            return JSONResponse(
                content={
                    "status": "success",
                    "message": f"Order for {symbol or 'unknown'} cancelled"
                }
            )
        else:
            # No valid order data or IDs found
            return JSONResponse(
                content={
                    "status": "error", 
                    "message": "No valid order data found to cancel"
                },
                status_code=400
            )
            
    except Exception as e:
        logger.error(f"Error processing cancel order request: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)},
            status_code=500
        )

@app.post("/close_positions")
async def close_positions_post(webhook_data: WebhookRequest):
    """
    Webhook endpoint that receives order event notifications from Fast API Webapp 

    This endpoint immediately returns a 200 OK response to Fast API Webapp  and then processes
    the webhook payload asynchronously to fetch order details, log events, and forward
    the data to ib_async functions.

    Args:
        webhook (WebhookRequest): The validated webhook payload received from Fast API Webapp .

    Returns:
        JSONResponse: An empty response with status code 200.
    """
    # Immediately return a 200 response and process the webhook asynchronously.
    try:
        logger.info(f"Received webhook request: {webhook_data}")

        # Start  task 
        logger.info(f"Processing webhook request for {webhook_data.ticker} asynchronously.")
        task1 = asyncio.create_task(close_positions(webhook_data))

        return JSONResponse(content="", status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook request: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )

async def close_positions(webhook_data: WebhookRequest):
    try:
        qty = next((m.value for m in webhook_data.metrics if m.name == "qty"), None)
        action = webhook_data.direction
        symbol = webhook_data.ticker
        ib_contract = {}
        logger.info(f"Closing position for {symbol}")
        # This endpoint is used for closing positions (triggered by webhook alerts)
        contract_data = {
            "symbol": symbol,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        
         
        ib_contract = Contract(**contract_data)
        # Store the contract instance for later cancellation
        global subscribed_contracts
        if subscribed_contracts.get(symbol) is not None:
            logger.info(f"Already subscribed to {symbol}, using existing contract.")
            ib_contract = subscribed_contracts[symbol]
        subscribed_contracts[symbol] = ib_contract
        price = await ib_vol_data.req_mkt_data(ib_contract)
        await asyncio.sleep(1)  # Give some time for market data to be fetched

        trade = await ib_vol_data.create_close_order(ib_contract, action, qty)
        if trade:
            return JSONResponse(
                content={
                    "status": "success",
                    "message": f"Close order placed for {symbol}",
                    "trade": trade,
                }
            )
        else:
            return JSONResponse(
                content={
                    "status": "no action",
                    "message": f"No active position found for {symbol}",
                }
            )
    except Exception as e:
        logger.error(f"Error closing position: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/account_pnl")
async def account_pnl():
    try:
        pnl = ib_manager.account_pnl()
        logger.info(f"Fetched account PnL: {pnl}")
        data = {"pnl": jsonable_encoder(pnl)}
        logger.info(f"Account PnL data: {data}")
    except Exception as e:
        logger.error(f"Error fetching account PnL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pnl-data")
async def get_current_pnl():
    try:
        logger.info(f"boof message {boofMsg}")
        completed_order = []
        new_get_position_update = await ib_manager.get_position_update()
        logger.info(f"New get_position_update: {new_get_position_update}")

        # Unpack the values from app_pnl_event()
        orders, position_json, pnl, close_trades = await ib_manager.app_pnl_event()
        net_liquidation = ib_manager.net_liquidation
        buying_power = ib_manager.buying_power
        get_positions_from_db = await ib_manager.get_positions_from_db()
        for close_trade in close_trades:
            if close_trade.orderStatus.status != "Cancelled":
                completed_order = close_trade
       

        data = {
            "get_positions_from_db": jsonable_encoder(get_positions_from_db),
            "trades": jsonable_encoder(orders),
            "completed_orders": jsonable_encoder(completed_order),
            "pnl": jsonable_encoder(pnl),
            "positions": jsonable_encoder(position_json),
            
            "net_liquidation": net_liquidation,
            "buying_power": buying_power
            
        }
        
        logger.debug(f"Got them for jengo - get_current_pnl {data}")
        logger.info(f"Got them for jengo - positions {jsonable_encoder(position_json)}")
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error fetching pnl data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/hard-data")
async def get_hard_data():
    data = await ib_manager.fast_get_ib_data()
    if data is None:
        data = {
            "positions": [],
            "pnl": [
                {
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "total_pnl": 0.0,
                    "net_liquidation": 0.0,
                }
            ],
            "orders": [],
            "trades": [],
        }
    return JSONResponse(content=data)


@app.post("/tvscan_ib")
async def tv_volatility_scan_ib(ticker: Request, barSizeSetting: str = "1 min"):
    """
    Scan market data from TWS for volatility patterns.

    Parameters:
    - ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
    - barSizeSetting: Time period of one bar (default: '1 min')
                      Must be one of: '1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
                      '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
                      '20 mins', '30 mins', '1 hour', '2 hours', '3 hours',
                      '4 hours', '8 hours', '1 day', '1 week', '1 month'
    """
    raw_data = await ticker.json()
    logger.info(f"Canceling order raw_data: {raw_data}")
    symbol = raw_data.get("ticker")
    contract = {
        "symbol": symbol,
        "exchange": "SMART",
        "secType": "STK",
        "currency": "USD",
    }
    try:
        logger.info(
            f"Processing TWS market data for {symbol} with bar size {barSizeSetting}"
        )

        # Validate bar size setting
        valid_bar_sizes = [
            "1 secs",
            "5 secs",
            "10 secs",
            "15 secs",
            "30 secs",
            "1 min",
            "2 mins",
            "3 mins",
            "5 mins",
            "10 mins",
            "15 mins",
            "20 mins",
            "30 mins",
            "1 hour",
            "2 hours",
            "3 hours",
            "4 hours",
            "8 hours",
            "1 day",
            "1 week",
            "1 month",
        ]

        if barSizeSetting not in valid_bar_sizes:
            logger.error(f"Invalid bar size setting: {barSizeSetting}")
            return {
                "error": f"Invalid bar size setting. Must be one of: {', '.join(valid_bar_sizes)}"
            }

        # Create a stock contract for the requested ticker
        contract = Stock(symbol, "SMART", "USD")

        # Determine durations based on bar size to capture enough data
        # for 9:30-9:36 AM analysis
        if "secs" in barSizeSetting:
            duration_str = "1 D"  # 1 day for seconds data
        elif "min" in barSizeSetting:
            duration_str = "5 D"  # 5 days for minute data
        elif "hour" in barSizeSetting:
            duration_str = "2 W"  # 2 weeks for hourly data
        else:
            duration_str = "1 M"  # 1 month for daily or larger

        # Request historical data from TWS
        start_time = time.time()
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",  # Current time
            durationStr=duration_str,
            barSizeSetting=barSizeSetting,
            whatToShow="TRADES",
            useRTH=True,
        )
        logger.info(
            f"Data fetch took {time.time() - start_time:.2f} seconds for {len(bars) if bars else 0} bars"
        )

        if not bars or len(bars) < 2:
            logger.error(f"Insufficient data received for {ticker}")
            return {"error": f"Insufficient data received for {ticker}"}

        # Convert IB bars to pandas DataFrame with proper timezone
        df = pd.DataFrame(
            {
                "datetime": [pd.to_datetime(bar.date) for bar in bars],
                "symbol": symbol,
                "open": [bar.open for bar in bars],
                "high": [bar.high for bar in bars],
                "close": [bar.close for bar in bars],
                "low": [bar.low for bar in bars],
                "volume": [bar.volume for bar in bars],
            }
        )

        # Use our scanner to analyze the data directly
        scanner = VolatilityScanner()
        start_time = time.time()
        matches = scanner.scan_dataframe(df)
        logger.info(
            f"Scan took {time.time() - start_time:.2f} seconds, found {len(matches)} matches"
        )

        # Prepare result data
        match_data = []
        for match in matches:
            # Handle different datetime formats
            try:
                dt_str = (
                    match["datetime"].strftime("%Y-%m-%d %H:%M:%S")
                    if hasattr(match["datetime"], "strftime")
                    else str(match["datetime"])
                )

                match_data.append(
                    {
                        "datetime": dt_str,
                        "symbol": match["symbol"],
                        "open": float(match["open"]),
                        "close": float(match["close"]),
                        "change_pct": float(match["change_pct"]),
                        "volume": float(match["volume"]),
                    }
                )
                logger.info(
                    f"Added match: {dt_str}, change: {match['change_pct']}%, volume: {match['volume']}"
                )
            except Exception as e:
                logger.error(f"Error formatting match data: {e}")
                # Continue processing other matches even if one fails

        # Build the response with useful information
        response = {
            "symbol": symbol,
            "bar_size": barSizeSetting,
            "data_points": len(df),
            "matches_found": len(matches),
            "matches": match_data,
        }

        # Add time range info if available
        if not df.empty:
            response["time_range"] = {
                "start": df["datetime"].min().strftime("%Y-%m-%d %H:%M:%S"),
                "end": df["datetime"].max().strftime("%Y-%m-%d %H:%M:%S"),
            }

        return response
    except Exception as e:
        logger.error(f"Error in tv_volatility_scan_ib: {e}")
        return {"error": str(e), "type": str(type(e).__name__)}


@app.get("/tvscan")
async def tv_csv_volatility_scan(barSizeSetting: str = "1 min"):
    """
    Scan CSV files for symbols, fetch data from IB, and analyze for volatility patterns.

    Parameters:
    - barSizeSetting: Time period of one bar (default: '1 min')
                      Must be one of: '1 secs', '5 secs', '10 secs', '15 secs', '30 secs',
                      '1 min', '2 mins', '3 mins', '5 mins', '10 mins', '15 mins',
                      '20 mins', '30 mins', '1 hour', '2 hours', '3 hours',
                      '4 hours', '8 hours', '1 day', '1 week', '1 month'
    """
    try:
        # Find CSV files in the directory
        directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
        csv_files = list(Path(directory).glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError("No CSV files found in the directory.")

        latest_csv = str(max(csv_files, key=os.path.getctime))
        logger.info(f"Processing TradingView CSV: {latest_csv}")

        # Read the CSV file
        try:
            df = pd.read_csv(latest_csv)

            # Figure out which column contains symbols
            possible_symbol_columns = [
                "Symbol",
                "Ticker",
                "ticker",
                "symbol",
                "stock",
                "Stock",
            ]
            symbol_column = None
            for col in possible_symbol_columns:
                if col in df.columns:
                    symbol_column = col
                    break

            if not symbol_column:
                # If we can't find a clear symbol column, log an error
                columns_str = ", ".join(df.columns.tolist())
                logger.error(
                    f"Could not determine symbol column. Available columns: {columns_str}"
                )
                return {
                    "error": f"Could not determine symbol column. Available columns: {columns_str}"
                }

            # Get unique symbols
            symbols = df[symbol_column].dropna().unique()
            logger.info(
                f"Found {len(symbols)} unique symbols in the CSV under column '{symbol_column}'"
            )

            # Validate bar size setting
            valid_bar_sizes = [
                "1 secs",
                "5 secs",
                "10 secs",
                "15 secs",
                "30 secs",
                "1 min",
                "2 mins",
                "3 mins",
                "5 mins",
                "10 mins",
                "15 mins",
                "20 mins",
                "30 mins",
                "1 hour",
                "2 hours",
                "3 hours",
                "4 hours",
                "8 hours",
                "1 day",
                "1 week",
                "1 month",
            ]

            if barSizeSetting not in valid_bar_sizes:
                logger.error(f"Invalid bar size setting: {barSizeSetting}")
                return {
                    "error": f"Invalid bar size setting. Must be one of: {', '.join(valid_bar_sizes)}"
                }

            # Initialize aggregate data
            all_matches = []
            total_data_points = 0
            time_range_start = None
            time_range_end = None

            # Process each symbol
            for symbol in symbols:
                logger.info(f"Processing symbol: {symbol}")

                # Create a stock contract for the symbol

                contract_data = {
                    "symbol": symbol,
                    "exchange": "SMART",
                    "secType": "STK",
                    "currency": "USD",
                }
                ib_contract = Contract(**contract_data)
                qualified_contract = await ib_manager.qualify_contract(ib_contract)
                # Determine durations based on bar size
                if "secs" in barSizeSetting:
                    duration_str = "1 D"  # 1 day for seconds data
                elif "min" in barSizeSetting:
                    duration_str = "5 D"  # 5 days for minute data
                elif "hour" in barSizeSetting:
                    duration_str = "2 W"  # 2 weeks for hourly data
                else:
                    duration_str = "1 M"  # 1 month for daily or larger

                # Request historical data from TWS
                start_time = time.time()
                try:
                    bars = await ib.reqHistoricalDataAsync(
                        qualified_contract,
                        endDateTime="",  # Current time
                        durationStr=duration_str,
                        barSizeSetting=barSizeSetting,
                        whatToShow="TRADES",
                        useRTH=True,
                    )
                    logger.info(
                        f"Data fetch took {time.time() - start_time:.2f} seconds for {len(bars) if bars else 0} bars"
                    )

                    if not bars or len(bars) < 2:
                        logger.warning(f"Insufficient data received for {symbol}")
                        continue

                    # Convert IB bars to pandas DataFrame with proper timezone
                    symbol_df = pd.DataFrame(
                        {
                            "datetime": [pd.to_datetime(bar.date) for bar in bars],
                            "symbol": symbol,  # Include symbol in the DataFrame
                            "open": [bar.open for bar in bars],
                            "high": [bar.high for bar in bars],
                            "close": [bar.close for bar in bars],
                            "low": [bar.low for bar in bars],
                            "volume": [bar.volume for bar in bars],
                        }
                    )
                    total_data_points += len(symbol_df)

                    # Update time range
                    df_start = symbol_df["datetime"].min()
                    df_end = symbol_df["datetime"].max()
                    if time_range_start is None or df_start < time_range_start:
                        time_range_start = df_start
                    if time_range_end is None or df_end > time_range_end:
                        time_range_end = df_end

                    # Use our scanner to analyze the data
                    scanner = VolatilityScanner()
                    scan_start_time = time.time()
                    matches = scanner.scan_dataframe(symbol_df)
                    logger.info(
                        f"Scan took {time.time() - scan_start_time:.2f} seconds, found {len(matches)} matches for {symbol}"
                    )

                    # Add matches to combined list
                    for match in matches:
                        try:
                            dt_str = (
                                match["datetime"].strftime("%Y-%m-%d %H:%M:%S")
                                if hasattr(match["datetime"], "strftime")
                                else str(match["datetime"])
                            )

                            all_matches.append(
                                {
                                    "datetime": dt_str,
                                    "open": float(match["open"]),
                                    "close": float(match["close"]),
                                    "change_pct": float(match["change_pct"]),
                                    "volume": float(match["volume"]),
                                    "symbol": match[
                                        "symbol"
                                    ],  # Include symbol in the match data
                                }
                            )
                        except Exception as e:
                            logger.error(
                                f"Error formatting match data for {symbol}: {e}"
                            )

                except Exception as e:
                    logger.error(f"Error processing symbol {symbol}: {e}")
                    # Continue with next symbol even if one fails

            # Build the response in the same format as tv_volatility_scan_ib
            response = {
                "symbol": "MULTI_SYMBOLS",  # Indicate multiple symbols
                "bar_size": barSizeSetting,
                "data_points": total_data_points,
                "matches_found": len(all_matches),
                "matches": all_matches,
                "source_csv": os.path.basename(latest_csv),
                "symbols_processed": len(symbols),
                "symbols_with_matches": len(
                    set(match["ticker"] for match in all_matches)
                ),
            }

            # Add time range if available
            if time_range_start and time_range_end:
                response["time_range"] = {
                    "start": time_range_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "end": time_range_end.strftime("%Y-%m-%d %H:%M:%S"),
                }

            return response

        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    except Exception as e:
        logger.error(f"Error in tv_csv_volatility_scan: {e}")
        return {"error": str(e), "type": str(type(e).__name__)}


# --- Main Endpoint ---
@app.post("/place_tv_order")
async def place_tv_web_order(web_request: OrderRequest):
    """
    Webhook endpoint that receives order event notifications from Tradingview.com.

    This endpoint immediately returns a 200 OK response to Tradingview.com and then processes
    the webhook payload asynchronously to fetch order details, log events, and forward
    the data to ib_async functions.

    Args:
        webhook (OrderRequest): The validated webhook payload received from Tradingview.com.

    Returns:
        JSONResponse: An empty response with status code 200.
    """
    # Immediately return a 200 response and process the webhook asynchronously.
    try:
        logger.info(f"Received webhook request: {web_request}")
        positions_db = await ib_manager.get_positions_from_db()
        if positions_db and positions_db.get("positions") and web_request.ticker.upper() in positions_db["positions"]:
            logger.info(f"Ticker {web_request.ticker} already exists in the database. No order will be placed.")
            return None

        # Start both tasks concurrently
        logger.info(f"Processing webhook request for {web_request.ticker} asynchronously.")
        task1 = asyncio.create_task(process_tv_web_order(web_request))

        return JSONResponse(content="", status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook request: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )


async def process_tv_web_order(web_request: OrderRequest):
    try:
        barSizeSetting = await convert_pine_timeframe_to_barsize(web_request.timeframe)
        
        #ib_manager.ib.newOrderEvent += ib_vol_data.on_order_status_event
        

        if web_request.entryPrice <= 0:
            logger.error(f"entryPrice is Less than or equal to 0")
            raise ValueError("entryPrice is Less than or equal to 0")
        if web_request.entryPrice is None:
            logger.error(f"entryPrice is None")
            raise ValueError("entryPrice is None")
        if web_request.quantity == 0 or web_request.quantity is None:
            logger.error(f"Quantity is Less than or equal to 0")
            raise ValueError("Quantity is Less than or equal to 0")
        if web_request.stopLoss is None or web_request.stopLoss == 0:
            logger.error(f"stopLoss is None or 0")
            raise ValueError("stopLoss is None or 0")

        # Step 1: Connect to IB and retrieve market data/entry price
        contract = {
            "symbol": web_request.ticker,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract)
        qualified_contract = await ib_vol_data.qualify_contract(ib_contract)
        logger.info(f"New Order request for  for {web_request.ticker}")
        # Step 2: Calculate position details
        # if ib_vol_data.entry_prices.ema9[web_request.ticker] is not None:
        # ema=ib_vol_data.entry_prices.ema9[web_request.ticker]

        order = OrderRequest(
            entryPrice=web_request.entryPrice,
            quantity=web_request.quantity,  # Use the quantity from the webhook
            rewardRiskRatio=web_request.rewardRiskRatio,
            riskPercentage=web_request.riskPercentage,
            timeframe = barSizeSetting,
            vstopAtrFactor=web_request.vstopAtrFactor,
            kcAtrFactor=web_request.kcAtrFactor,
            atrFactor=web_request.atrFactor,
            accountBalance=web_request.accountBalance,
            ticker=web_request.ticker,
            orderAction=web_request.orderAction,
            stopType=web_request.stopType,
            submit_cmd=web_request.submit_cmd,
            meanReversion=web_request.meanReversion,
            stopLoss=float(web_request.stopLoss),
        )
        submit_cmd = web_request.submit_cmd
        meanReversion = web_request.meanReversion
        stopLoss = float(web_request.stopLoss)
        logger.info(
            f"New Order request for  for {web_request.ticker} with stopLoss {stopLoss} and web_request.stopLoss {web_request.stopLoss} and stopLoss+5 {stopLoss+5}"
        )

        orderEntry = await ib_vol_data.create_order(
            order, qualified_contract, submit_cmd, meanReversion, stopLoss, prices=None, timestamp_id=None
        )
        
        

        trade_dict = await trade_to_dict(orderEntry)
        logger.info(
            f"Order Entry for {web_request.ticker} successfully placed with order {trade_dict}"
        )
        return JSONResponse(content={"status": "success", "order_details": trade_dict})
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")


@app.post("/place_order")
async def place_web_order(web_request: OrderRequest):
    """
    Webhook endpoint that receives order event notifications from HTML Template.

    This endpoint immediately returns a 200 OK response to HTML Template and then processes
    the webhook payload asynchronously to fetch order details, log events, and forward
    the data to ib_async functions.

    Args:
        webhook (OrderRequest): The validated webhook payload received from HTML Template.

    Returns:
        JSONResponse: An empty response with status code 200.
    """
    # Immediately return a 200 response and process the webhook asynchronously.
    try:
        

        logger.info(f"Received webhook request: {web_request}")
        asyncio.create_task(process_web_order(web_request))
        return JSONResponse(content="", status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook request: {e}")
        return JSONResponse(
            content={"status": "error", "message": str(e)}, status_code=500
        )


async def process_web_order(web_request: OrderRequest):
    try:
        barSizeSetting = await convert_pine_timeframe_to_barsize(web_request.timeframe)
        global subscribed_contracts
        logger.info(f"Processing webhook request for {web_request.ticker} asynchronously subscribed_contracts[ticker.ticker] {subscribed_contracts[web_request.ticker]}.")
        ticker= TickerSub(ticker=web_request.ticker)
        symbol=web_request.ticker  # Ensure the ticker symbol is in uppercase
        
       
        t = ib_vol_data.ticker
        if not t:
            await sub_to_ticker(ticker)
            t = ib_vol_data.ticker
           
           

        # If ticker is stored as a dictionary (e.g., {"TSLA": Ticker(...)}), get one ticker value.
        if t and isinstance(t, dict):
            t_value = next(iter(t.values()))
        else:
            t_value = t

        logger.info(f"Ticker data: {t_value}")
        data = {
            "symbol": t_value.contract.symbol,
            "bidSize": t_value.bidSize,
            "bid": t_value.bid,
            "ask": t_value.ask,
            "askSize": t_value.askSize,
            "high": t_value.high,
            "low": t_value.low,
            "close": t_value.close,
        }
        logger.info(f"Market data for {web_request.ticker}: {data}")

        # Step 1: Connect to IB and retrieve market data/entry price
        contract = {
            "symbol": web_request.ticker,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract)
        qualified_contract = await ib_vol_data.qualify_contract(ib_contract)
        logger.info(f"New Order request for  for {web_request.ticker}")
        # Step 2: Calculate position details
        # if ib_vol_data.entry_prices.ema9[web_request.ticker] is not None:
        # ema=ib_vol_data.entry_prices.ema9[web_request.ticker]

        #ema = ib_vol_data.nine_ema[t_value.contract.symbol]
        uptrend=ib_vol_data.uptrend_dict[symbol][-1]
        
        if uptrend is None:
            logger.error(f"uptrend is None for {web_request.ticker}")
            raise ValueError("uptrend is None")
        logger.info(
            f"New Order request for {web_request.ticker} with jengo and uptrend {uptrend}"
        )

        uptrend_orderAction = "BUY" if uptrend  else "SELL"
        logger.info(
            f"Uptrend order Action for {web_request.ticker} is {uptrend_orderAction} and entryPrice {t_value.close}"
        )
        order = OrderRequest(
            entryPrice=t_value.close,  # Use the latest market price as entry price
            rewardRiskRatio=web_request.rewardRiskRatio,
            quantity=web_request.quantity,  # Use the quantity from the webhook
            riskPercentage=web_request.riskPercentage,
            vstopAtrFactor=web_request.vstopAtrFactor,
            timeframe = barSizeSetting,
            kcAtrFactor=web_request.kcAtrFactor,
            atrFactor=web_request.atrFactor,
            accountBalance=web_request.accountBalance,
            ticker=web_request.ticker,
            orderAction=uptrend_orderAction,
            stopType=web_request.stopType,
            submit_cmd=web_request.submit_cmd,
            meanReversion=web_request.meanReversion,
            stopLoss=float(web_request.stopLoss),
        )
        logger.warning( f"order  is {order}")
        logger.warning( f"barSizeSetting  is {barSizeSetting} for  {web_request.ticker} and stop type {web_request.stopType}")
        submit_cmd = web_request.submit_cmd
        meanReversion = web_request.meanReversion
        stopLoss = float(web_request.stopLoss)

        logger.info(
            f"New Order request for  for {web_request.ticker} with stopLoss {stopLoss} and web_request.stopLoss {web_request.stopLoss} and stopLoss+5 {stopLoss+5}"
        )

        orderEntry = await ib_vol_data.create_order(
            order, qualified_contract, submit_cmd, meanReversion, stopLoss
        )

        trade_dict = await trade_to_dict(orderEntry)
        logger.info(
            f"Order Entry for {web_request.ticker} successfully placed with order {trade_dict}"
        )
        return JSONResponse(content={"status": "success", "order_details": trade_dict})
    except Exception as e:
        logger.error(f"Error placing order: {e}")
        raise HTTPException(status_code=500, detail="Error placing order")


@app.post("/check_order")
async def check_web_order(web_request: OrderRequest):
    try:

        order = web_request
        action = web_request.orderAction
        stopType = web_request.stopType
        meanReversion = web_request.meanReversion
        stopLoss = web_request.stopLoss

        if action not in ["BUY", "SELL"]:
            raise ValueError("Order Action is None")
        # Step 1: Connect to IB and retrieve market data/entry price
        contract = {
            "symbol": web_request.ticker,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract)
        logger.info(f"New Order check for {web_request.ticker}")
        # Step 2: Calculate position details
        durationStr = "35 D"
        barSizeSetting = "1 day"
        ohlc4, hlc3, hl2, open_prices, high, low, close, vol = await get_ohlc_vol(
            ib_contract, durationStr, barSizeSetting, return_df=False
        )
        df, orderEntry = await asyncio.gather(
            daily_volatility(ib_manager.ib, contract, high, low, close, stopType),
            ib_vol_data.create_order(
                order, ib_contract, False, meanReversion, stopLoss
            ),
        )

        logger.info(f"Order Check for {web_request.ticker}: {orderEntry}")

    except Exception as e:
        logger.error(f"Error checking order: {e}")
        raise HTTPException(status_code=500, detail="Error checking order")


@app.post("/tv_close")
async def tv_close(ticker: TickerSub):
    try:
        qty = None
        action = None
        symbol = ticker.ticker
        logger.info(f"Closing position for {symbol}")
        # This endpoint is used for closing positions (triggered by webhook alerts)
        contract_data = {
            "symbol": symbol,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract_data)

        trade = await ib_vol_data.create_close_order(
            ib_contract, action=None, qty=None
        )
        if trade:
            return JSONResponse(
                content={
                    "status": "success",
                    "message": f"Close order placed for {symbol}",
                    "trade": trade,
                }
            )
        else:
            return JSONResponse(
                content={
                    "status": "no action",
                    "message": f"No active position found for {symbol}",
                }
            )
    except Exception as e:
        logger.error(f"Error closing position: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/get_contracts")
async def get_contracts(ticker: TickerSub):
    logger.info(f"getting ticker for BT Ticker Tradingview: {ticker.ticker}")
    contract_data = {
        "symbol": ticker.ticker,
        "exchange": "SMART",
        "secType": "STK",
        "currency": "USD",
    }
    ib_contract = Contract(**contract_data)

    subscribed_contracts[ticker.ticker] = ib_contract
    await get_contract_bars(ticker)

    # price = await ib_vol_data.req_mkt_data(ib_contract)

    qualified_contract = await ib_vol_data.qualify_contract(ib_contract)
    logger.info(f"qualified_contract: {qualified_contract}")
    json_qualified_contract = {
        "symbol": qualified_contract.symbol,
        "exchange": qualified_contract.primaryExchange,
    }

    return JSONResponse(content=json_qualified_contract)


@app.get("/autocomplete/")
async def autocomplete(query: str = Query(...)):
    results = ticker_manager.query_ticker_data(query)
    return JSONResponse(content=results)


@app.get("/tickers/")
async def get_all_tickers():
    cur = ticker_manager.conn.cursor()
    cur.execute("SELECT * FROM tickers")
    rows = cur.fetchall()
    tickers = [dict(row) for row in rows]
    return JSONResponse(content=tickers)


@app.get("/query-tickers/")
async def query_tickers(query_model: QueryModel = Depends()):
    results = ticker_manager.query_ticker_data(query_model.query)
    return JSONResponse(content=results)


@app.get("/active-tickers")
async def list_active_tickers():
    try:
        active_tickers = await ib_manager.get_active_tickers()

        # Handle NaN values in the response to make it JSON compliant
        def clean_nan(value):
            if isinstance(value, dict):
                return {k: clean_nan(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [clean_nan(item) for item in value]
            elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                return None
            else:
                return value

        cleaned_tickers = clean_nan(active_tickers)
        return JSONResponse(content={"status": "success", "tickers": cleaned_tickers})
    except Exception as e:
        logger.error(f"Error retrieving tickers: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})


async def get_contract_bars(ticker: TickerSub):
    try:
        action = "BUY"
        contract = {
            "symbol": ticker.ticker,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract)
        price = await ib_vol_data.req_mkt_data(ib_contract)
        logger.info(f"Price for {ticker.ticker}: {price} now sleeping for 3 seconds")
        await asyncio.sleep(3)
        bars = await ib_vol_data.get_rt_price(
            ib_contract,
            action,
            max_wait_seconds=5,
            keep_subscription=True,
            whatToShow="TRADES",
        )
        global ema9_dict
        await asyncio.sleep(1)
        close_price = ib_vol_data.close_price

        # Retrieve ema9 value stored under the given symbol.
        ema_entry = ib_vol_data.entry_prices.get(ticker.ticker, {})
        ema9_value = ema_entry.get("ema9", None)
        ema9_dict[ticker.ticker] = ema9_value
        logger.info(f"ema9 for {ticker.ticker}: {ema9_value}")

        # Store close_price in the global dictionary using the ticker as key
        global global_close_prices
        global_close_prices[ticker.ticker] = close_price

        logger.info(f"close_price df jengo for {ticker.ticker}: {close_price}")
        return JSONResponse(content={"close_price": close_price})
    except Exception as e:
        logger.error(f"Error in get_contract_bars: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})


@app.post("/boof")
async def sub_to_ticker(ticker: TickerSub):
    try:
        logger.info(f"Subscribing to ticker: {ticker.ticker}")
        contract_data = {
            "symbol": ticker.ticker,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract_data)
        # Store the contract instance for later cancellation
        global subscribed_contracts
        subscribed_contracts[ticker.ticker] = ib_contract
        price = await ib_vol_data.get_bars_ib(ib_contract, barSizeSetting= '15 secs', atrFactor=1.5)
        await asyncio.sleep(3)
        action = "BUY"
        contract_json = jsonable_encoder(ib_contract)
        price_json = jsonable_encoder(price) if price else None
        logger.info(f"Subscribed to {contract_json} with price: {price_json}")
       
        
        return JSONResponse(
            content={"status": "success", "contract": contract_json, "price": price_json}
        )
    except Exception as e:
        logger.error(f"Error in subscribing to ticker: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})


@app.post("/un_boof")
async def unsub_from_ticker(ticker: TickerSub):
    try:
        contract_data = {
            "symbol": ticker.ticker,
            "exchange": "SMART",
            "secType": "STK",
            "currency": "USD",
        }
        ib_contract = Contract(**contract_data)
        symbol=None
        global subscribed_contracts
        qualified_contract = await ib_vol_data.qualify_contract(ib_contract)
        # Retrieve the original contract instance
        logger.info(subscribed_contracts[ticker.ticker])
        contract=subscribed_contracts[ticker.ticker]
        # Get the symbol value from the contract which is a dictionary 
        if isinstance(contract, dict):
            ticker.ticker = contract.get("symbol", ticker.ticker)
            symbol = contract.get("symbol", ticker.ticker)
        if ticker.ticker == symbol:
            logger.info(f"Unsubscribing from ticker: {ticker.ticker} with symbol: {symbol}")
            await ib_manager.unsub_mkt_data(ib_contract)
            del subscribed_contracts[ticker.ticker]

        

        
        elif not qualified_contract:
            raise HTTPException(status_code=404, detail="Ticker not subscribed")
           
        # Optionally remove the contract from the cache
           
        json_ticker = {jsonable_encoder(contract_data)}
        return JSONResponse(
            content={"status": "success", "contract": {"symbol": json_ticker}}
        )
    except Exception as e: 
        logger.error(f"Error in unsubscribing from ticker: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)})


@app.get("/hello")
async def hello():
    unique_ts = str(current_millis() - (4 * 60 * 60 * 1000))
    return {"message": f"Hello World time:{unique_ts}"}


@app.post("/test_bars")
async def test_bars(ticker: TickerSub):
    contract_data = {
        "symbol": ticker.ticker,
        "exchange": "SMART",
        "secType": "STK",
        "currency": "USD",
    }
    ib_contract = Contract(**contract_data)
    qualified_contract = await ib_manager.qualify_contract(ib_contract)
    positions, df = await asyncio.gather(
        ib_manager.ib.reqPositionsAsync(),
        ib_manager.ohlc_vol(
            contract=qualified_contract,
            durationStr="1 D",
            barSizeSetting="5 secs",
            whatToShow="TRADES",
            useRTH=False,
            return_df=True,
        ),
    )


async def get_ohlc_vol(contract, durationStr, barSizeSetting, return_df):
    try:
        if return_df:
            df = await ib_vol_data.ohlc_vol(
                contract=contract,
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow="TRADES",
                useRTH=True,
                return_df=return_df,
            )
            logger.info(f"return_df Got them for jengo - get_ohlc_vol ")
            return df

        if not return_df:
            ohlc4, hlc3, hl2, open_prices, high, low, close, vol = (
                await ib_vol_data.ohlc_vol(
                    contract=contract,
                    durationStr=durationStr,
                    barSizeSetting=barSizeSetting,
                    whatToShow="TRADES",
                    useRTH=True,
                    return_df=return_df,
                )
            )
            logger.info(
                f"ohlc4, hlc3, hl2, open_prices, high, low, close, vol Got them for jengo - get_ohlc_vol "
            )
            return ohlc4, hlc3, hl2, open_prices, high, low, close, vol
    except Exception as e:
        logger.error(f"Error in get_ohlc_vol: {e}")





@app.get("/price", response_model=TickerResponse)
async def get_ticker_price(ticker: str = Query(..., description="Stock ticker symbol")):
    global subscribed_contracts
    logger.info(f"Processing GET request for ticker {ticker}")
    
    # Ensure the ticker symbol is in uppercase
    symbol = ticker
    contract = subscribed_contracts.get(symbol)
    
    if not contract:
        raise HTTPException(status_code=404, detail=f"Contract for ticker {symbol} not found")
    
    # Access the ticker data from your price_data instance
    if symbol not in ib_vol_data.ticker:
        raise HTTPException(status_code=404, detail=f"Ticker {symbol} not found or not subscribed")
    
    ticker_obj = ib_vol_data.ticker[symbol]
    order_prices = await ib_vol_data.order_prices(contract)
    logger.info(f"Order prices for {symbol}: {order_prices}")
    
    # Safe extraction of values with default
    def safe_float(value, default=0.0):
        if value is None or math.isnan(value) or not math.isfinite(value):
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    # Extract relevant data from the ticker object with safety checks
    response = {
        "symbol": symbol,
        "bid": safe_float(getattr(ticker_obj, 'bid', None)),
        "ask": safe_float(getattr(ticker_obj, 'ask', None)),
        "last": safe_float(getattr(ticker_obj, 'last', None)),
        "high": safe_float(getattr(ticker_obj, 'high', None)),
        "low": safe_float(getattr(ticker_obj, 'low', None)),
        "close": order_prices.get('close_price', 0.0),
        "tick_close": safe_float(getattr(ticker_obj, 'close', None)),
        "volume": safe_float(getattr(ticker_obj, 'volume', None), 0),
        "timestamp": str(ticker_obj.time) if hasattr(ticker_obj, "time") and ticker_obj.time else datetime.datetime.now().isoformat()
    }
    
    return response
 

async def new_on_bar_update(bars, hasNewBar):

    try:
        if hasNewBar:
            # Process the new bar data
            ib_time = await ib_manager.ib.reqCurrentTimeAsync()
            df = pd.DataFrame(
                {
                    "datetime": [pd.to_datetime(bar.time) for bar in bars],
                    "open": [bar.open_ for bar in bars],
                    "high": [bar.high for bar in bars],
                    "close": [bar.close for bar in bars],
                    "low": [bar.low for bar in bars],
                    "volume": [bar.volume for bar in bars],
                    "ib_time": ib_time,
                }
            )
            logger.info(f"New bar update time_stamp: {ib_time} - {df}")
            # Add your processing logic here
    except Exception as e:
        logger.error(f"Error processing new bar update: {e}")


def time_stamp():
    ny_tz = pytz.timezone("America/New_York")
    now = datetime.now(ny_tz)

    return now.strftime("%Y-%m-%d %H:%M:%S")


@app.get("/rvol", response_class=HTMLResponse)
async def rvol(rvol: float):
    try:
        rvol_percent = float(rvol / 10)
        logger.info(f"Starting rvol from fastapi")
        await pre_rvol.start(rvol_percent)

        return JSONResponse(
            content={
                "status": "success",
                "message": f"rvol started with {rvol_percent}",
            }
        )
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    log_config.setup()

    # Create a force exit mechanism
    def force_exit():
        logger.warning("Forcing application exit after timeout")
        # Use os._exit instead of sys.exit to force termination
        os._exit(0)

    # Add signal handlers that include a force-exit failsafe
    def handle_exit(sig, frame):
        logger.info(f"Received signal {sig} - initiating graceful shutdown")

        # Set a timeout to force exit if needed
        timer = threading.Timer(5.0, force_exit)
        timer.daemon = True  # Make sure the timer doesn't prevent exit
        timer.start()

        # Let the normal shutdown process continue

    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # Run the application
    uvicorn.run("app:app", host=HOST, port=PORT, log_level="info")
