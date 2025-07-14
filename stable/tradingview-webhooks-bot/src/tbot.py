#!/usr/bin/env python3
"""
TradingBoat helper views – FastAPI edition matching Flask exactly
"""
import os
import sqlite3
from threading import Lock
from typing import Any, List, Dict
from contextvars import ContextVar

from dotenv import load_dotenv
from fastapi import Request
from fastapi.templating import Jinja2Templates

from utils.log import get_logger

logger = get_logger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# .env handling ─ same logic as before
DEFAULT_ENV_FILE_PATH = os.path.expanduser("~/.env")
ENV_FILE_PATH = (
    DEFAULT_ENV_FILE_PATH
    if os.path.isfile(DEFAULT_ENV_FILE_PATH)
    else "/home/tbot/.env"
)
load_dotenv(dotenv_path=ENV_FILE_PATH, override=True)
# ──────────────────────────────────────────────────────────────────────────────

# Jinja templates
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Context variable to store request (like Flask's g)
_request_context: ContextVar[Request | None] = ContextVar('request_context', default=None)

def set_request_context(request: Request):
    """Set the current request context"""
    _request_context.set(request)

def get_request():
    """Get the current request context"""
    return _request_context.get()

# ────────────────
# SQLite utilities - matching Flask exactly
# ────────────────
_db_lock: Lock = Lock()
_database: sqlite3.Connection | None = None

def get_db() -> sqlite3.Connection:
    """Get the database - matching Flask g._database pattern"""
    global _database
    with _db_lock:
        if _database is None:
            path = os.environ.get("TBOT_DB_OFFICE", "/run/tbot/tbot_sqlite3")
            _database = sqlite3.connect(path, check_same_thread=False)
            _database.row_factory = sqlite3.Row
        return _database

def query_db(query: str, args: tuple = ()) -> List[Dict[str, Any]]:
    """Query database - matching Flask version exactly"""
    try:
        cur = get_db().execute(query, args)
        rows = cur.fetchall()
        unpacked = [{k: item[k] for k in item.keys()} for item in rows]
        cur.close()
    except Exception as err:
        logger.error(f"Failed to execute: {err}")
        return []
    return unpacked

# ────────────────
# View functions - matching Flask signatures exactly
# ────────────────

def get_orders():
    """Get IBKR Orders - matches Flask exactly"""
    request = get_request()
    return templates.TemplateResponse("orders.html", {"request": request, "title": "IBKR Orders"})

def get_orders_data():
    """Get IBKR Orders for AJAX - matches Flask exactly"""
    rows = query_db("select * from TBOTORDERS")
    return {"data": rows}

def get_alerts():
    """Get TradingView alerts - matches Flask exactly"""
    request = get_request()
    return templates.TemplateResponse(
        "alerts.html", {"request": request, "title": "TradingView Alerts to TBOT"}
    )

def get_alerts_data():
    """Get TradingView alerts for AJAX - matches Flask exactly"""
    rows = query_db("select * from TBOTALERTS")
    return {"data": rows}

def get_errors():
    """Get TradingView alerts - matches Flask exactly"""
    request = get_request()
    return templates.TemplateResponse(
        "error.html", {"request": request, "title": "TradingView Errors to TBOT"}
    )

def get_errors_data():
    """Get TradingView errors for AJAX - matches Flask exactly"""
    rows = query_db("select * from TBOTERRORS")
    return {"data": rows}

def get_tbot():
    """Get the holistic view of Tbot - matches Flask exactly"""
    request = get_request()
    return templates.TemplateResponse(
        "alerts_orders.html",
        {"request": request, "title": " TradingView Alerts and IBKR Orders on TBOT"},
    )

def get_ngrok():
    """Get NGROK Address - matches Flask exactly"""
    addr = os.environ.get("TBOT_NGROK", "#")
    return {"data": {"address": addr}}

def get_tbot_data():
    """Get inner join between TBOTORDERS and TBOTALERTS - matches Flask exactly"""
    query = (
        "SELECT "
        "TBOTORDERS.timestamp, "
        "TBOTORDERS.uniquekey, "
        "TBOTALERTS.tv_timestamp, "
        "TBOTALERTS.ticker, "
        "TBOTALERTS.tv_price, "
        "TBOTORDERS.avgprice, "
        "TBOTALERTS.direction, "
        "TBOTORDERS.action, "
        "TBOTORDERS.ordertype, "
        "TBOTORDERS.qty, "
        "TBOTORDERS.position, "
        "TBOTALERTS.orderref, "
        "TBOTORDERS.orderstatus "
        "FROM TBOTORDERS INNER JOIN TBOTALERTS "
        "ON TBOTALERTS.orderref = TBOTORDERS.orderref "
        "AND TBOTALERTS.uniquekey = TBOTORDERS.uniquekey "
        "ORDER BY TBOTORDERS.uniquekey DESC "
    )
    rows = query_db(query)
    return {"data": rows}

def get_main():
    """Get entry point for TradingBoat - matches Flask exactly"""
    request = get_request()
    if request and request.method == "GET":
        # Make the open-gui mode in favor of the system-level firewall.
        try:
            os.remove(".gui_key")
        except FileNotFoundError:
            pass

        return templates.TemplateResponse("tbot_dashboard.html", {"request": request})

def close_connection(exception):
    """Close connection - matches Flask exactly"""
    global _database
    with _db_lock:
        if _database is not None:
            _database.close()
            _database = None