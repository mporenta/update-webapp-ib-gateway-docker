from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time
import pandas as pd
from threading import Event
from datetime import datetime
import logging

class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)
        self.pos_df = pd.DataFrame(columns=['Account', 'Symbol', 'SecType',
                                            'Currency', 'Position', 'Avg cost'])
        self.position_updates_received = False
        self.starting_net_liq = None
        self.current_unrealized_pnl = 0
        self.current_realized_pnl = 0
        self.stop_loss_triggered = False
        self.exit_event = Event()
        
    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
        
    def position(self, account, contract, position, avgCost):
        super().position(account, contract, position, avgCost)
        dictionary = {"Account":account, "Symbol": contract.symbol, "SecType": contract.secType,
                      "Currency": contract.currency, "Position": position, "Avg cost": avgCost}
        if self.pos_df["Symbol"].str.contains(contract.symbol).any():
            self.pos_df.loc[self.pos_df["Symbol"]==contract.symbol,"Position"] = position
            self.pos_df.loc[self.pos_df["Symbol"]==contract.symbol,"Avg cost"] = avgCost
        else:
            self.pos_df = pd.concat((self.pos_df, pd.DataFrame([dictionary])),ignore_index=True)
            
    def positionEnd(self):
        super().positionEnd()
        self.position_updates_received = True
        print("All positions received")

    def updatePortfolio(self, contract: Contract, position: float, marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str):
        """Real-time portfolio updates"""
        self.current_unrealized_pnl = unrealizedPNL
        self.current_realized_pnl = realizedPNL
        
        # Calculate total P&L
        total_pnl = unrealizedPNL + realizedPNL
        
        # Check if we have starting net liquidation value
        if self.starting_net_liq is not None and not self.stop_loss_triggered:
            loss_threshold = -0.01 * self.starting_net_liq  # -1% of starting value
            
            if total_pnl <= loss_threshold:
                print(f"\nStop loss triggered!")
                print(f"Total P&L: ${total_pnl:.2f}")
                print(f"Loss threshold: ${loss_threshold:.2f}")
                print(f"Starting Net Liq: ${self.starting_net_liq:.2f}")
                self.stop_loss_triggered = True
                # Trigger trade_positions in a separate thread
                threading.Thread(target=trade_positions, args=(self,)).start()

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """Called when account values update"""
        if key == "NetLiquidation" and currency == "USD" and self.starting_net_liq is None:
            self.starting_net_liq = float(val)
            print(f"Starting Net Liquidation Value: ${self.starting_net_liq:.2f}")

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        """Handle errors"""
        print(f"Error {errorCode}: {errorString}")
        if errorCode == 1100:  # Connectivity between IB and TWS has been lost
            print("Connection lost to TWS")
        elif errorCode == 1102:  # Connectivity between IB and TWS has been restored
            print("Connection restored to TWS")
        elif errorCode == 506:   # Requested market data is not subscribed
            print("Market data subscription error")

    def connectionClosed(self):
        """Called when connection is closed"""
        print("Connection closed")
        self.exit_event.set()

def websocket_con():
    app.run()
    
def usStk(symbol, sectype="STK", currency="USD", exchange="NASDAQ"):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sectype
    contract.currency = currency
    contract.exchange = exchange
    return contract

def mktOrder(direction, quantity):
    order = Order()
    order.action = direction
    order.orderType = "MKT"
    order.totalQuantity = quantity
    order.eTradeOnly = ""
    order.firmQuoteOnly = ""
    return order

def trade_positions(app_instance):
    """Close all positions"""
    app_instance.position_updates_received = False
    app_instance.reqPositions()
    
    # Wait until positions are received
    timeout = 10  # seconds
    start_time = time.time()
    while not app_instance.position_updates_received:
        time.sleep(0.1)
        if time.time() - start_time > timeout:
            print("Timeout waiting for positions")
            return
    
    # Process each position
    for index, row in app_instance.pos_df.iterrows():
        position = float(row['Position'])
        symbol = row['Symbol']
        
        if position != 0:  # Only process non-zero positions
            # Determine order direction and quantity
            if position > 0:  # Long position
                direction = "SELL"
                quantity = abs(position)
                print(f"Closing long position in {symbol}: {quantity} shares")
            else:  # Short position
                direction = "BUY"
                quantity = abs(position)
                print(f"Covering short position in {symbol}: {quantity} shares")
            
            # Place the order
            app_instance.placeOrder(
                app_instance.nextValidOrderId,
                usStk(symbol),
                mktOrder(direction, quantity)
            )
            app_instance.nextValidOrderId += 1
            time.sleep(1)  # Add delay between orders
    
    # After closing positions, trigger exit
    app_instance.exit_event.set()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'ibkr_connection_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('IBKR_Connection')

if __name__ == "__main__":
    app = TradingApp()
    
    try:
        logger.info("Attempting to connect to IB Gateway at 127.0.0.1:4002")
        app.connect("127.0.0.1", 4002, clientId=7)
        
        # Start websocket connection
        logger.info("Starting websocket connection thread")
        con_thread = threading.Thread(target=websocket_con, daemon=True)
        con_thread.start()
        
        logger.info("Waiting for connection to establish...")
        time.sleep(1)  # Allow time for connection to establish
        
        if app.isConnected():
            logger.info("Successfully connected to IB Gateway")
        else:
            logger.error("Failed to establish connection to IB Gateway")
            raise ConnectionError("Could not connect to IB Gateway")

        # Request account updates
        account_id = 'DU7397764'  # Replace with your account number
        logger.info(f"Requesting account updates for account {account_id}")
        app.reqAccountUpdates(True, account_id)
        
        # Wait for exit event instead of polling
        logger.info("Waiting for exit event...")
        app.exit_event.wait()
        
    except KeyboardInterrupt:
        logger.warning("Received keyboard interrupt, initiating shutdown...")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        logger.info("Starting cleanup process...")
        try:
            logger.info(f"Stopping account updates for account {account_id}")
            app.reqAccountUpdates(False, account_id)
            
            logger.info("Disconnecting from IB Gateway")
            app.disconnect()
            
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
