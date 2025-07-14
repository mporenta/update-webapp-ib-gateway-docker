import pytest
import pandas as pd
from unittest.mock import MagicMock, ANY

import app_dev as app
from ib_async import Contract, LimitOrder
from models import OrderRequest

@pytest.fixture(autouse=True)
def setup_environment(monkeypatch):
    # 1) Prepare a fake market snapshot for symbol "TEST"
    symbol = "TEST"
    snapshot = MagicMock()
    snapshot.bid = 100.0
    snapshot.ask = 102.0
    snapshot.last = 101.0
    snapshot.bar_close = 101.0
    snapshot.ticker = MagicMock()
    snapshot.ticker.hasBidAsk.return_value = True
    app.price_data[symbol] = snapshot

    # 2) Inject simple vstop and uptrend Series so .iloc[-1] works
    app.vstop_data[symbol] = pd.Series([5.0])
    app.uptrend_data[symbol] = pd.Series([True])

    # 3) Force market‐hours path
    monkeypatch.setattr(app, "is_market_hours", lambda: True)

    # 4) Stub out compute_position_size_order
    async def fake_compute_position_size_order(
        entry_price, stop_loss_price,
        account_balance, risk_percentage,
        reward_risk_ratio, action
    ):
        return 10, 110.0, 0.0, 1.0, 0.01
    monkeypatch.setattr(
        app,
        "compute_position_size_order",
        fake_compute_position_size_order
    )

    # 5) Stub out the IB client reqId generator
    fake_client = MagicMock()
    fake_client.getReqId.return_value = 12345
    app.ib.client = fake_client

@pytest.mark.asyncio
async def test_place_limit_order_success(monkeypatch):
    # Build a minimal OrderRequest
    req = OrderRequest(
        entryPrice=0.0,
        rewardRiskRatio=1.0,
        quantity=None,
        riskPercentage=1.0,
        vstopAtrFactor=1.5,
        timeframe="1 min",
        kcAtrFactor=1.0,
        atrFactor=1.0,
        accountBalance=10_000.0,
        ticker="TEST",
        orderAction="BUY",
        stopType="vStop",
        submit_cmd=True,
        meanReversion=False,
        stopLoss=0.0,
    )

    # Prepare a dummy Contract
    contract = Contract(symbol="TEST", exchange="SMART", secType="STK", currency="USD")

    # Spy on ib.placeOrder and return a fake Trade, so we can assert calls
    fake_trade = MagicMock()
    fake_trade.order = MagicMock(orderRef="Parent Adaptive - TEST")
    mock_place = MagicMock(return_value=fake_trade)
    monkeypatch.setattr(app.ib, "placeOrder", mock_place)

    # Call the function under test
    trade = await app.place_limit_order(contract, "BUY", None, order_details=req)

    # Assertions
    assert trade is fake_trade, "Should return the Trade object from ib.placeOrder"
    # Verify placeOrder was called exactly once with our contract and *any* LimitOrder
    mock_place.assert_called_once_with(contract, ANY)
    _, built_order = mock_place.call_args[0]
    assert isinstance(built_order, LimitOrder)
    assert built_order.action == "BUY"
    assert built_order.totalQuantity == 10
    assert built_order.lmtPrice == 102.0
