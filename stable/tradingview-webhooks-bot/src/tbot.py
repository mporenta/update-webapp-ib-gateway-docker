#!/usr/bin/env python3
"""
TradingBoat helper views – FastAPI edition
"""
import os
import sqlite3
from threading import Lock
from typing import Any, List, Dict

from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import JSONResponse
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

# ────────────────
# SQLite utilities
# ────────────────
_db_lock: Lock = Lock()
_database: sqlite3.Connection | None = None


def get_db() -> sqlite3.Connection:
    """Open or reuse a singleton SQLite connection (thread-safe)."""
    global _database
    with _db_lock:
        if _database is None:
            path = os.environ.get("TBOT_DB_OFFICE", "/run/tbot/tbot_sqlite3")
            _database = sqlite3.connect(path, check_same_thread=False)
            _database.row_factory = sqlite3.Row
        return _database


def query_db(query: str, args: tuple = ()) -> List[Dict[str, Any]]:
    """Run SELECT and return list-of-dict rows; swallow & log errors like Flask version."""
    try:
        cur = get_db().execute(query, args)
        rows = cur.fetchall()
        cur.close()
        return [{k: row[k] for k in row.keys()} for row in rows]
    except Exception as err:  # noqa: BLE001
        logger.error(f"Failed to execute query: {err}")
        return []


# ────────────────
# View helpers
# ────────────────
def get_orders(request: Request):
    return templates.TemplateResponse(
        "orders.html", {"request": request, "title": "IBKR Orders"}
    )


def get_orders_data():
    rows = query_db("SELECT * FROM TBOTORDERS")
    return JSONResponse({"data": rows})


def get_alerts(request: Request):
    return templates.TemplateResponse(
        "alerts.html", {"request": request, "title": "TradingView Alerts to TBOT"}
    )


def get_alerts_data():
    rows = query_db("SELECT * FROM TBOTALERTS")
    return JSONResponse({"data": rows})


def get_errors(request: Request):
    return templates.TemplateResponse(
        "error.html", {"request": request, "title": "TradingView Errors to TBOT"}
    )


def get_errors_data():
    rows = query_db("SELECT * FROM TBOTERRORS")
    return JSONResponse({"data": rows})


def get_tbot(request: Request):
    return templates.TemplateResponse(
        "alerts_orders.html",
        {"request": request, "title": "TradingView Alerts and IBKR Orders on TBOT"},
    )


def get_ngrok():
    addr = os.environ.get("TBOT_NGROK", "#")
    return JSONResponse({"data": {"address": addr}})


def get_tbot_data():
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
    return JSONResponse({"data": rows})


def get_main(request: Request):
    """
    Entry point – identical semantics to Flask version:
    • On GET, delete .gui_key file (open-GUI mode)
    • Render tbot_dashboard.html
    """
    # Make the open-gui mode in favour of the system-level firewall.
    try:
        os.remove(".gui_key")
    except FileNotFoundError:
        pass

    return templates.TemplateResponse("tbot_dashboard.html", {"request": request})


# called from lifespan in app_fastapi.py
def close_connection(_: object | None = None):
    global _database
    if _database is not None:
        _database.close()
        _database = None
 