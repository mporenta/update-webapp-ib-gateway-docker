from ib_async import *
from ib_async.util import isNan
from tv_ticker_price_data import ib_open_orders



async def app_pnl_event(order_db, ib=None):
       
        orders = ib.openTrades()
        close_trades = await ib.reqCompletedOrdersAsync(False)
        for trade_close in close_trades:
            await order_db.insert_trade(trade_close)
       
    

        return orders, close_trades