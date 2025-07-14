# tests/test_buy_bracket.py
import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import MagicMock

import app_dev as app  # <-- your FastAPI app module
import pandas as pd
from models import OrderRequest

@pytest.fixture(autouse=True)
def stub_ib_and_db(monkeypatch):
    # 1) Qualify just echoes back
    async def fake_qualify(c): return c
    monkeypatch.setattr(app, "qualify_contract", fake_qualify)

    # 2) IB‐connection fakes
    monkeypatch.setattr(app.ib, "isConnected", lambda: True)
    monkeypatch.setattr(app.ib, "connectAsync", lambda *a, **k: asyncio.Future().set_result(True))
    monkeypatch.setattr(app.ib.client, "getReqId", lambda: 42)
    monkeypatch.setattr(app, "is_market_hours", lambda: True)

    
    fake_existing = MagicMock()
    fake_existing.contract = MagicMock(symbol="NVDA")
    fake_existing.order    = MagicMock(orderId=999)

    async def fake_req_all_open():
        return [fake_existing]

    async def fake_req_completed(flag):
        return [fake_existing]

    monkeypatch.setattr(app.ib, "reqAllOpenOrdersAsync", fake_req_all_open)
    monkeypatch.setattr(app.ib, "reqCompletedOrdersAsync", fake_req_completed)

    # 4) Fake placeOrder → our fake_trade
    fake_trade = MagicMock()
    fake_trade.contract    = MagicMock(symbol="NVDA")
    fake_trade.order       = MagicMock(orderRef="Parent Adaptive - NVDA", orderId=999, action="BUY", totalQuantity=2)
    fake_trade.orderStatus = MagicMock(orderId=999, permId=None, status="Filled", filled=0, remaining=0, avgFillPrice=0.0)
    callbacks = []
    class FakeEvent:
        def __iadd__(self, cb):
            callbacks.append(cb)
            return self
    fake_trade.filledEvent = FakeEvent()
    monkeypatch.setattr(app.ib, "placeOrder", lambda c,o: fake_trade)

    

    # 5) Stub out DB lookup for web_orders
    async def fake_get_web_orders_by_ticker(ticker):
        return [ OrderRequest(
            entryPrice=101.1,
            rewardRiskRatio=1,
            quantity=2,
            riskPercentage=1,
            vstopAtrFactor=1.5,
            timeframe="15S",
            kcAtrFactor=2,
            atrFactor=1.5,
            accountBalance=300,
            ticker="NVDA",
            orderAction="BUY",
            stopType="vStop",
            submit_cmd=True,
            meanReversion=False,
            stopLoss=98.13
        )]
    monkeypatch.setattr(app.order_db, "get_web_orders_by_ticker", fake_get_web_orders_by_ticker)

    # 6) No‐ops for insert_trade/get_orders
    async def fake_insert_trade(t): return None
    async def fake_get_orders(*a,**k): return []
    monkeypatch.setattr(app, "insert_trade", fake_insert_trade)
    monkeypatch.setattr(app, "get_orders", fake_get_orders)

    # 7) Inject a price snapshot
    app.price_data["NVDA"] = MagicMock(ask=102.0, bid=100.0, last=101.27)

    # 8) Fake vStop & uptrend series so .iloc[-1] works
    monkeypatch.setitem(app.vstop_data,   "NVDA", pd.Series([101.30, 101.3155]))
    monkeypatch.setitem(app.uptrend_data, "NVDA", pd.Series([False, False]))

    # 9) Fake position sizing
    async def fake_compute_ps(limit_p, stop_p, acc, risk, rr, action):
        return 2, limit_p + 1.0, 0.1, 0.5, 1.0
    monkeypatch.setattr(app, "compute_position_size_order", fake_compute_ps)

    # 10) Wrap real bracket_oca_order to count calls
    counter = {"calls": 0}
    real_br = app.bracket_oca_order
    async def counting_br(contract, action, qty, trade, **kw):
        counter["calls"] += 1
        return await real_br(contract, action, qty, trade, **kw)
    monkeypatch.setattr(app, "bracket_oca_order", counting_br)

    # 11) Stub oneCancelsAll to just echo back the two child orders
    monkeypatch.setattr(app.ib, "oneCancelsAll", lambda orders, **kw: orders)

    return {"fake_trade": fake_trade, "callbacks": callbacks, "counter": counter}


@pytest.mark.asyncio
async def test_buy_order_triggers_bracket(stub_ib_and_db):
    payload = {
        "entryPrice": 101.10,
        "rewardRiskRatio": 1,
        "timeframe": "15S",
        "quantity": 2,
        "riskPercentage": 1,
        "kcAtrFactor": 2,
        "atrFactor": 1.5,
        "vstopAtrFactor": 1.5,
        "accountBalance": 300,
        "ticker": "NVDA",
        "orderAction": "BUY",
        "stopType": "vStop",
        "meanReversion": False,
        "stopLoss": 98.13,
        "submit_cmd": True
    }

    transport = ASGITransport(app=app.app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        # 1) POST to /place_order → should return 200
        r = await client.post("/place_order", json=payload)
        assert r.status_code == 200

        # 2) give the background‐launched process_web_order() a moment
        await asyncio.sleep(0.1)

        # 3) now manually fire our fake_trade.filledEvent callbacks
        ft = stub_ib_and_db["fake_trade"]
        for cb in stub_ib_and_db["callbacks"]:
            await cb(ft, fill=MagicMock())

        # 4) give bracket_oca_order a moment to run
        await asyncio.sleep(0.1)

    # 5) we should have invoked bracket_oca_order exactly once
    assert stub_ib_and_db["counter"]["calls"] == 1
