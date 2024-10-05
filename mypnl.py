
import sys
import logging
import requests
import time
import json
from loguru import logger
from datetime import datetime
import pytz
from ib_insync import IB, MarketOrder, util, Stock



# Set up loguru logging configuration at the start of the script
logger.remove()  # Remove default handler to avoid duplicate logs
logger.add("trading_strategy.log", level="INFO", format="{time} {level} {message}")  # Log to a file
logger.add(sys.stderr, level="INFO")  # Also log to the console

logger.info("Logging is configured.")


class SimplePnLStrategy:
    def __init__(self, host='127.0.0.1', port=4002, client_id=2, account_balance=30000.0):
        self.host = host
        self.port = port
        self.client_id = client_id
      

        self.ib = None
        self.pnl = None
        self.account_balance = account_balance  # User-provided account balance
        self.loss_threshold = -0.01* self.account_balance  # 1% of the account balance

        logger.info("SimplePnLStrategy initialized.")

    

    def run(self):
        logger.info("Running the strategy...")

        # Establish connection to IB
        self.connect_to_ib()

        # Request all open orders asynchronously
        self.ib.reqAllOpenOrdersAsync()

        # Subscribe to order events
        self.subscribe_to_events()

        # Wait until there is at least one open position
        self.wait_for_initial_position()

        # Subscribe to PnL updates
        self.request_pnl_updates()

        logger.info("Entering main update loop...")

        # Enter the event-driven loop to monitor PnL updates
        self.ib.run()

    def get_current_time(self):
        local_tz = pytz.timezone('America/New_York')
        return datetime.now(local_tz).strftime('%Y-%m-%d %H:%M:%S')

    def connect_to_ib(self):
        try:
            self.ib = IB()
            self.ib.connect(self.host, self.port, clientId=self.client_id)
            logger.info("Connected to IB at {}", self.get_current_time())
            logger.info("Connected to IB at {}", self.get_current_time())
        except Exception as e:
            logger.error("Failed to connect to IB: {}", str(e))
            logger.error("Failed to connect to IB: {}", str(e))

    def subscribe_to_events(self):
        self.ib.newOrderEvent += self.on_new_order
        logger.info("Subscribed to newOrderEvent.")

        self.ib.orderModifyEvent += self.on_order_modify
        logger.info("Subscribed to orderModifyEvent.")

        self.ib.cancelOrderEvent += self.on_cancel_order
        logger.info("Subscribed to cancelOrderEvent.")

        self.ib.openOrderEvent += self.on_open_order
        logger.info("Subscribed to openOrderEvent.")

        self.ib.orderStatusEvent += self.on_order_status
        logger.info("Subscribed to orderStatusEvent.")

        logger.info("All order events subscribed.")

    def on_new_order(self, trade):
        logger.info("New order placed: {}", trade)

    def on_order_modify(self, trade):
        logger.info("Order modified: {}", trade)

    def on_cancel_order(self, trade):
        logger.info("Order cancelled: {}", trade)

    def on_open_order(self, trade):
        logger.info("Open order: {}", trade)

    def on_order_status(self, trade):
        logger.info("Order status changed: {}", trade)

    def wait_for_initial_position(self):
        logger.info("Waiting for initial position...")
        while not self.ib.positions():
            self.ib.sleep(1)
        logger.info("Initial position detected. Starting strategy.")
        logger.info("Initial position detected. Starting strategy.")

    def request_pnl_updates(self):
        try:
            account = self.ib.managedAccounts()[0]
            self.ib.reqPnL(account)
            self.ib.pnlEvent += self.on_pnl
            logger.info("Subscribed to PnL updates.")
        except Exception as e:
            logger.error("Failed to subscribe to PnL updates: {}", str(e))
            logger.error("Failed to subscribe to PnL updates: {}", str(e))

    def on_pnl(self, pnl):
        self.pnl = pnl
      

        if self.check_loss_threshold():
            self.close_all_positions()
            logger.info("Loss threshold exceeded, positions closed. Exiting strategy.")
            logger.info("Loss threshold exceeded, positions closed. Exiting strategy.")
            self.ib.disconnect()

    def check_loss_threshold(self):
        if self.pnl and self.pnl.dailyPnL <= self.loss_threshold:
            logger.warning(
                "Loss threshold of {:.2f} (1%% of account balance) exceeded with daily PnL: {:.2f} at {}",
                self.loss_threshold, self.pnl.dailyPnL, self.get_current_time()
            )
            return True
        return False

  

    
        
    def close_all_positions(self):
        logging.info("Attempting to close all positions at {}", self.get_current_time())
        url = "https://tv.porenta.us/webhook"
        for position in self.ib.positions():
            contract = position.contract
            qty = position.position
            symbol = contract.symbol
            client_id = self.client_id

            # Use the avgCost as the last price
            last_price = position.avgCost

            # Generate the current timestamp in Unix format
            current_time = int(time.time())

            # Prepare the payload for the POST request
            payload = {
                "timestamp": current_time,
                "ticker": symbol,
                "currency": "USD",
                "timeframe": "S",
                "clientId": "1",
                "key": "WebhookReceived:fcbd3d",
                "contract": "stock",
                "orderRef": "close_all",
                "direction": "strategy.close_all",
                "metrics": [
                    {"name": "entry.limit", "value": 0},
                    {"name": "entry.stop", "value": 0},
                    {"name": "exit.limit", "value": 0},
                    {"name": "exit.stop", "value": 0},
                    {"name": "qty", "value": -10000000000},
                    {"name": "price", "value": last_price}
                ]
            }

            headers = {'Content-Type': 'application/json'}

            # Convert payload to JSON format
            payload_json = json.dumps(payload)

            try:
                # Send the POST request to the webhook
                response = requests.post(url, headers=headers, data=payload_json)

                # Log the response status
                logging.info("Sent POST request to close position for {}: {} - Response: {}", symbol, response.status_code, response.text)

            except Exception as e:
                logging.error("Failed to send POST request for {}: {}", symbol, str(e))

        logging.info("Completed position closing attempt at {}", self.get_current_time())

if __name__ == "__main__":
    logger.info("Script is starting...")
    # User can set their account balance here
    account_balance = 30000.0  # Example: $100,000
    strategy = SimplePnLStrategy(account_balance=account_balance)
    strategy.run()
    logger.info("Script has finished.")
    logger.info("Script has finished.")