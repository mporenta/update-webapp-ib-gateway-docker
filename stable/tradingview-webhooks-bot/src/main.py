# app_fastapi.py
import os
import uvicorn
import logging
from contextlib import asynccontextmanager
import signal
from dotenv import load_dotenv
import threading
from log_config import log_config
import tbot
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates

from commons import VERSION_NUMBER, LOG_LOCATION
from components.actions.base.action import am
from components.events.base.event import em
from components.logs.log_event import LogEvent
from components.schemas.trading import Order, Position
from utils.log import get_logger
from utils.register import register_action, register_event, register_link
from settings import REGISTERED_ACTIONS, REGISTERED_EVENTS, REGISTERED_LINKS
# Set the default path to the .env file in the user's home directory
log_config.setup()
logger = get_logger(__name__)
# Load the environment variables from the chosen .env file
HOST = os.getenv("FAST_API_HOST", "127.0.0.1")
boof = os.getenv("HELLO_MSG")
logger.info(f"Hello {boof} from FastAPI!")

# ───── paths / templates ───────────────────────────────────────────────────────
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
templates   = Jinja2Templates(directory=os.path.join(CURRENT_DIR, "templates"))

# ───── logging ────────────────────────────────────────────────────────────────

# ───── registration ───────────────────────────────────────────────────────────
registered_actions = [register_action(a) for a in REGISTERED_ACTIONS]
registered_events  = [register_event(e) for e in REGISTERED_EVENTS]
registered_links   = [register_link(l, em, am) for l in REGISTERED_LINKS]

schema_list = {"order": Order().as_json(), "position": Position().as_json()}

# ───── lifespan (startup / shutdown) ──────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting TBOT API…")
    # (e.g.) open DB connection / preload caches / warm-up
    yield
    logger.info("Shutting down TBOT API…")
    tbot.close_connection(None)

# ───── app instance ───────────────────────────────────────────────────────────
app = FastAPI(lifespan=lifespan)

# static files
app.mount("/static", StaticFiles(directory=os.path.join(CURRENT_DIR, "static")), name="static")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://nyc.porenta.us",
        "https://zt.porenta.us",
        "https://porenta.us",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ───── routes mapped to existing tbot views ───────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    # delegate completely to helper
    return tbot.get_main(request)

@app.get("/orders", response_class=HTMLResponse)
def orders(request: Request):
    return tbot.get_orders(request)

@app.get("/alerts", response_class=HTMLResponse)
def alerts(request: Request):
    return tbot.get_alerts(request)



@app.get("/errors", response_class=HTMLResponse)
def errors(request: Request):
    return tbot.get_errors(request)

@app.get("/tbot", response_class=HTMLResponse)
def tbot_page(request: Request):
    return tbot.get_tbot(request)

# data endpoints (JSON) stay unchanged
@app.get("/orders/data")
def orders_data():
    return tbot.get_orders_data()

@app.get("/alerts/data")
def alerts_data():
    return tbot.get_alerts_data()
@app.get("/errors/data")
def errors_data():
    return tbot.get_errors_data()
@app.get("/tbot/data")
def tbot_data():
    return tbot.get_tbot_data()



# ───── custom endpoints previously in Flask ───────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, guiKey: str | None = None):
    try:
        with open(".gui_key") as f:
            if f.read().strip() != guiKey:
                return PlainTextResponse("Access Denied", status_code=401)
    except FileNotFoundError:
        logger.warning("GUI key file not found. Open GUI mode detected.")

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "schema_list": schema_list,
            "action_list": am.get_all(),
            "event_list": registered_events,
            "version": VERSION_NUMBER,
        },
    )

@app.post("/webhook")
async def webhook(request: Request):
    payload = await request.json()
    if not payload or "key" not in payload:
        logger.warning(f"Bad payload: {payload}")
        return Response(status_code=415)

    logger.debug(f"Request Data: {payload}")
    fired = []
    for e in em.get_all():
        if e.webhook and e.key == payload["key"]:
            logger.info(f"Triggered events: and e.key {e.key} ")
            e.trigger(data=payload)
            fired.append(e.name)

    if not fired:
        logger.warning(f"No events triggered for webhook request {payload}")
    else:
        logger.info(f"Triggered events: {fired}")
        logger.info(f"client IP: {request.client.host}")
        ip = request.headers.get("X-Forwarded-For", request.client.host)
        logger.info(f"client IP: {ip}")


    return Response(status_code=200)
@app.get("/fast_dashboard", response_class=HTMLResponse)
async def fast_dashboard(request: Request):
    try:
        return templates.TemplateResponse("fast_api_dashboard.html", {"request": request})
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/logs")
def get_logs():
    with open(LOG_LOCATION) as f:
        logs = [LogEvent().from_line(line).as_json() for line in f]
    return logs

@app.post("/event/active")
def activate_event(event: str | None = None, active: str = "true"):
    if not event:
        raise HTTPException(status_code=404, detail="Event name cannot be empty")
    try:
        evt = em.get(event)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Cannot find event '{event}'")

    evt.active = (active == "true")
    logger.info(f"Event {evt.name} active set to {evt.active}")
    return {"active": evt.active}

# ───── entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log_config.setup()
    port = 5000
    logger.info(f"1 Starting FastAPI with log level 'info' and HOST={HOST} and port={port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
