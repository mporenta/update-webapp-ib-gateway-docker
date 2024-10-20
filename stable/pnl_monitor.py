import asyncio
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from threading import Thread

class AsyncIBApi(EWrapper, EClient):
    def __init__(self, loop):
        EClient.__init__(self, self)
        self.loop = loop
        self.nextOrderId = None
        self.positions = {}
        self.account = ''  # Replace with your account ID if necessary
        self.reqId = 1

        # Variables to store account values
        self.starting_equity = None
        self.current_pnl = 0.0

        # Events for asyncio synchronization
        self.pnl_event = asyncio.Event()
        self.positions_event = asyncio.Event()
        self.account_value_event = asyncio.Event()

    def error(self, reqId, errorCode, errorString):
        print(f"Error {errorCode}: {errorString}")

    def nextValidId(self, orderId):
        """Receives next valid order ID."""
        self.nextOrderId = orderId
        print(f"NextValidId: {orderId}")

        # Request account summary to get starting equity
        self.reqAccountSummary(self.reqId, "All", "NetLiquidation")

    def accountSummary(self, reqId, account, tag, value, currency):
        """Receives account summary updates."""
        if tag == "NetLiquidation":
            print(f"Account Summary - NetLiquidation: {value} {currency}")
            self.starting_equity = float(value)
            self.account = account  # Store the account ID if not set
            # Once starting equity is obtained, start requesting PnL and positions
            self.reqPnL(self.reqId, self.account, '')
            self.reqPositions()
            # Signal that account value is received
            self.loop.call_soon_threadsafe(self.account_value_event.set)

    def accountSummaryEnd(self, reqId):
        """Called when account summary request is complete."""
        print("Account Summary End")

    def pnl(self, reqId, dailyPnL, unrealizedPnL, realizedPnL):
        """Receives PnL updates."""
        print(f"PnL Update - DailyPnL: {dailyPnL}, UnrealizedPnL: {unrealizedPnL}, RealizedPnL: {realizedPnL}")
        self.current_pnl = dailyPnL
        # Signal that new PnL data is available
        self.loop.call_soon_threadsafe(self.pnl_event.set)

    def position(self, account, contract, position, avgCost):
        """Receives position updates."""
        print(f"Position - Account: {account}, Symbol: {contract.symbol}, Position: {position}, AvgCost: {avgCost}")
        self.positions[contract.conId] = (contract, position)

    def positionEnd(self):
        """Called when all positions have been received."""
        print("Position End")
        # Signal that positions have been fully received
        self.loop.call_soon_threadsafe(self.positions_event.set)

    def close_all_positions(self):
        """Closes all open positions."""
        for conId, (contract, position) in self.positions.items():
            if position != 0:
                order = Order()
                # Determine action to close the position
                if position > 0:
                    order.action = 'SELL'
                else:
                    order.action = 'BUY'
                order.orderType = 'MKT'
                order.totalQuantity = abs(position)
                self.placeOrder(self.nextOrderId, contract, order)
                print(f"Placed order to close position for {contract.symbol}")
                self.nextOrderId += 1

    def global_cancel(self):
        """Cancels all open orders globally."""
        print("Sending global cancel request")
        self.reqGlobalCancel()

def start_ibapi_loop(ibapi_client):
    """Starts the ibapi client loop in a separate thread."""
    ibapi_client.run()

async def monitor_pnl(ibapi_client):
    """Monitors PnL and triggers position closing and order cancellation."""
    # Wait until starting equity is available
    await ibapi_client.account_value_event.wait()
    print(f"Starting Equity: {ibapi_client.starting_equity}")

    pnl_threshold_percent = 1.0  # Set the PnL loss percentage threshold (1%)

    while True:
        await ibapi_client.pnl_event.wait()
        ibapi_client.pnl_event.clear()
        # Calculate PnL loss percentage
        pnl_loss_percent = (-ibapi_client.current_pnl / ibapi_client.starting_equity) * 100
        print(f"Current PnL Loss Percentage: {pnl_loss_percent:.2f}%")

        if pnl_loss_percent >= pnl_threshold_percent:
            print("PnL loss threshold reached. Closing positions and cancelling orders.")
            ibapi_client.close_all_positions()
            ibapi_client.global_cancel()
            break  # Exit after closing positions
        await asyncio.sleep(1)  # Adjust the sleep interval as needed

async def main():
    """Main asynchronous function."""
    loop = asyncio.get_event_loop()
    ibapi_client = AsyncIBApi(loop)
    ibapi_client.connect('127.0.0.1', 4002, clientId=7)

    # Start the ibapi client in a separate thread
    ibapi_thread = Thread(target=start_ibapi_loop, args=(ibapi_client,), daemon=True)
    ibapi_thread.start()

    # Wait for positions to be received
    await ibapi_client.positions_event.wait()

    # Start monitoring PnL
    await monitor_pnl(ibapi_client)

    # Disconnect after operations are complete
    ibapi_client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
