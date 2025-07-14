import asyncio
from ib_async import *
from ib_async.contract import Stock
from ib_async.order import LimitOrder
from matplotlib import ticker


def wait_for_fill(trade, fill):
    
    print(f'Order status: trade {trade} {fill}')

    
ib = IB()
ib.connect('127.0.0.1', 4002, clientId=16)

positions =  ib.portfolio()


symbol = "NVDA"
qty = 10
marketPrice = 141.90
print(f'Symbol: {symbol}, Qty: {qty}, Avg Price: {marketPrice}')

# Only place a sell order for long positions


action =  'BUY'
contract_data = {
    "symbol": symbol,
    "exchange": "SMART",
    "secType": "STK",
    "currency": "USD",
}
ib_contract = Contract(**contract_data)
#contract_q=ib.qualifyContracts(ib_contract)
ticker = ib.reqMktData(ib_contract, '', False, False)
ib.sleep(2)
tick=ib.ticker(ib_contract)
limit_price = marketPrice
print(tick)


#order = LimitOrder('SELL', qty, limit_price)
order = LimitOrder(
        action,
        totalQuantity=abs(qty),
        lmtPrice=round(limit_price, 2),
        tif="GTC",
        outsideRth=True,
        transmit=True,
    

    )

#print(f'Placing SELL order: {symbol}, Qty: {qty}, Limit: {limit_price}')
trade = ib.placeOrder(ib_contract, order)

if trade:
    trade.filledEvent += wait_for_fill
    

                # Optionally wait for fill or status
            


    # Disconnect from IB
   
ib.run()
