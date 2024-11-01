import logging 
from ib_async import IB, MarketOrder, LimitOrder, PnL, PortfolioItem, AccountValue
from typing import List, Dict, Optional
from datetime import datetime
import time
import pytz

RISK_PERCENT = 0.015  # 1.5% risk threshold       

class IBPortfolioTracker:
    def __init__(self):
        self.ib = IB()
        self.accountV = {}
        self.total_realized_pnl = 0.0
        self.total_unrealized_pnl = 0.0
        self.daily_pnl = 0.0
        self.position_updates_received = False
        self.should_close_positions = False
        self.net_liquidation = 0.0
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Connect to IB Gateway
        try:
            self.ib.connect("127.0.0.1", 4002, clientId=7)
            self.logger.info("Connected successfully to IB Gateway")
            
            # Wait for managed accounts to be populated
            self.ib.waitOnUpdate(timeout=2)

            # Get account
            accounts = self.ib.managedAccounts()
            self.logger.info(f"Managed accounts: {accounts}")
            if not accounts:
                raise Exception("No managed accounts available")
                
            self.account = accounts[0]
            self.logger.info(f"Using account {self.account}")
            
            # Set up callbacks
            self.ib.accountSummaryEvent += self.on_account_summary
            self.ib.connectedEvent += self.onConnected

            # Request initial account summary
            self.request_account_summary()
            
            # Wait briefly for initial account data
            self.ib.sleep(2)
            
            # Subscribe to PnL updates
            self.pnl = self.ib.reqPnL(self.account)
            if not self.pnl:
                raise Exception("Failed to subscribe to PnL updates")
                
            self.logger.info(f"Successfully subscribed to PnL updates for account {self.account}")
            self.ib.pnlEvent += self.on_pnl_update
            
        except Exception as e:
            self.logger.error(f"Initialization failed: {str(e)}")
            raise

    def request_account_summary(self):
        """Request account summary update"""
        try:
            # Request account summary
            self.ib.reqAccountSummary()
            self.logger.info("Requested account summary update")
        except Exception as e:
            self.logger.error(f"Error requesting account summary: {str(e)}")

    def on_account_summary(self, value: AccountValue):
        """Handle account summary updates"""
        try:
            if value.tag == 'NetLiquidation':
                try:
                    new_value = float(value.value)
                    if new_value > 0:
                        if new_value != self.net_liquidation:
                            self.logger.info(f"Net liquidation changed: ${self.net_liquidation:,.2f} -> ${new_value:,.2f}")
                        self.net_liquidation = new_value
                    else:
                        self.logger.warning(f"Received non-positive net liquidation value: {new_value}")
                except ValueError:
                    self.logger.error(f"Invalid net liquidation value received: {value.value}")
        except Exception as e:
            self.logger.error(f"Error processing account summary: {str(e)}")

    def get_net_liquidation(self) -> float:
        """Get the current net liquidation value"""
        if self.net_liquidation <= 0:
            # Request a fresh update if the value is invalid
            self.request_account_summary()
            self.ib.sleep(1)  # Give time for update to arrive
        return self.net_liquidation

    def check_pnl_conditions(self) -> bool:
        """Check if PnL has exceeded risk threshold"""
        try:
            net_liq = self.get_net_liquidation()
            if net_liq <= 0:
                self.logger.warning(f"Invalid net liquidation value: ${net_liq:,.2f}")
                return False
                
            self.risk_amount = net_liq * RISK_PERCENT
            #is_threshold_exceeded = self.daily_pnl <= -self.risk_amount  # Note the negative sign
            is_threshold_exceeded = 100 <= self.risk_amount  # Note the negative sign
            self.logger.info(f"Risk amount is ${self.risk_amount:,.2f} ")
            
            if is_threshold_exceeded:
                self.logger.info(f"Risk threshold reached: Daily PnL ${self.daily_pnl:,.2f} <= -${self.risk_amount:,.2f}")
                self.logger.info(f"Threshold is {RISK_PERCENT:.1%} of ${net_liq:,.2f}")
                self.should_close_positions = True
                
            return is_threshold_exceeded
            
        except Exception as e:
            self.logger.error(f"Error checking PnL conditions: {str(e)}")
            return False

    def on_pnl_update(self, pnl: PnL):
        """Handle PnL updates from IB"""
        try:
            # Convert PnL values to float to ensure they're numeric
            self.daily_pnl = float(pnl.dailyPnL) if pnl.dailyPnL is not None else 0.0
            self.total_unrealized_pnl = float(pnl.unrealizedPnL) if pnl.unrealizedPnL is not None else 0.0
            self.total_realized_pnl = float(pnl.realizedPnL) if pnl.realizedPnL is not None else 0.0
            
            self.logger.info(f"""
PnL Update:
- Daily P&L: ${self.daily_pnl:,.2f}
- Unrealized P&L: ${self.total_unrealized_pnl:,.2f}
- Realized P&L: ${self.total_realized_pnl:,.2f}
- Current Net Liquidation: ${self.get_net_liquidation():,.2f}
""")
            
            # Check PnL conditions and get positions if needed
            if self.check_pnl_conditions():
                positions = [pos for pos in self.ib.positions() if pos.position != 0]
                existing_trades = self.ib.trades()
                
                # Check if all positions have pending orders
                all_have_orders = all(
                    any(
                        trade.contract.conId == position.contract.conId and 
                        trade.orderStatus.status not in ['Filled', 'Cancelled', 'Inactive']
                        for trade in existing_trades
                    )
                    for position in positions
                )
                
                if not all_have_orders:
                    self.logger.info("Some positions don't have pending orders - action required")
                else:
                    self.logger.info("All positions already have pending orders")
                    
        except Exception as e:
            self.logger.error(f"Error processing PnL update: {str(e)}")

    def onConnected(self):
        self.logger.info("Connected to IB Gateway.")
        # Request account summary when connected
        self.request_account_summary()

    def run(self):
        """Start the event loop"""
        try:
            self.logger.info("Starting IB event loop...")
            
            while True:
                self.ib.sleep(1)
                if self.should_close_positions:
                    # Here you would implement your position closing logic
                    self.logger.info("Should close positions flag is set")
                    self.should_close_positions = False  # Reset flag
                    
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
        finally:
            self.ib.disconnect()

if __name__ == "__main__":
    portfolio_tracker = IBPortfolioTracker()
    portfolio_tracker.run()
