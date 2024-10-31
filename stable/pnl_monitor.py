import logging
from ib_async import IB, MarketOrder, LimitOrder, PnL, PortfolioItem, Watchdog
from typing import List, Dict, Optional
from datetime import datetime
import time
import pytz
risk_amount = (0.01)  # 1% risk threshold       
class IBPortfolioTracker:
    def __init__(self):
        self.ib = IB()
        self.positions: Dict[str, PortfolioItem] = {}
        self.total_realized_pnl = 0.0
        self.total_unrealized_pnl = 0.0
        self.daily_pnl = 0.0
        self.position_updates_received = False
        self.should_close_positions = False
        self.net_liquidation = None


        
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
            
            # Get account
            self.account = self.ib.wrapper.accounts[0]
            
            # Request portfolio updates
            self.ib.reqAllOpenOrders()
            self.ib.reqPositions()

            # Get Account Values
            self.account = self.ib.wrapper.accounts[0]
            # Set up callbacks
            self.ib.accountValueEvent += self.on_account_value
            # Request initial account updates
            self.ib.reqAccountUpdates(self.account)
            # Subscribe to PnL updates
            self.pnl = self.ib.reqPnL(self.account)  # Request PnL subscription
            if self.pnl:
                self.logger.info(f"Successfully subscribed to PnL updates for account {self.account}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect: {str(e)}")
            raise

    


    def on_account_value(self, account_value):
        """Callback for account value updates"""
        if account_value.tag == "NetLiquidation" and account_value.currency == "USD":
            self.net_liquidation = float(account_value.value)
            self.logger.info(f"Net Liquidation updated: ${self.net_liquidation:,.2f}")
    
    def get_net_liquidation(self) -> Optional[float]:
        """Get the current net liquidation value"""
        return self.net_liquidation
        
    def check_pnl_conditions(self) -> bool:
        current_net_liq = self.get_net_liquidation()
        condition_met = (self.daily_pnl <= risk_amount * current_net_liq)
        if condition_met:
            self.logger.info(f"Risk threshold reached: ${self.daily_pnl:.2f} <= ${risk_amount:.2f}")
            self.should_close_positions = True  # Set flag instead of calling directly
        return condition_met
        
    
    def close_all_positions(self):
        """Close all positions with market or limit orders based on time"""
        trades = []
        try:
            positions = [pos for pos in self.ib.positions() if pos.position != 0]
            
            if not positions:
                self.logger.info("No positions to close")
                return trades
                
            # Get current NY time
            ny_tz = pytz.timezone('America/New_York')
            ny_time = datetime.now(ny_tz)
            current_hour = ny_time.hour
            
            # Determine if we're in after-hours (16:00-20:00 NY time)
            is_after_hours = 16 <= current_hour < 23
            
            self.logger.info(f"""
    Attempting to close {len(positions)} positions
    Time: {ny_time.strftime('%H:%M:%S')} NY
    Order Type: {'LIMIT' if is_after_hours else 'MARKET'}
    """)
            
            # Get existing trades
            existing_trades = self.ib.trades()
            all_positions_have_orders = True  # Flag to track if all positions have orders
            
            for position in positions:
                # Check if we already have a pending order for this contract
                has_pending_order = any(
                    trade.contract.conId == position.contract.conId and 
                    trade.orderStatus.status not in ['Filled', 'Cancelled', 'Inactive']
                    for trade in existing_trades
                )
                
                if has_pending_order:
                    self.logger.info(f"Already have pending order for {position.contract.symbol}, skipping...")
                    continue
                
                # If we get here, at least one position doesn't have an order
                all_positions_have_orders = False
                
                # Rest of your existing order placement code...
                action = 'SELL' if position.position > 0 else 'BUY'
                quantity = abs(position.position)
                
                # Get price data
                bars = self.ib.reqHistoricalData(
                    position.contract,
                    durationStr='60 S',
                    endDateTime='',
                    barSizeSetting='5 secs',
                    whatToShow='ASK',
                    useRTH=True
                )
                self.ib.sleep(1)
                
                if bars and bars[-1].close:
                    current_price = bars[-1].close
                    # Your existing order creation code...
                    if is_after_hours:
                        order = LimitOrder(
                            action=action,
                            totalQuantity=quantity,
                            lmtPrice=current_price,
                            account=self.account,
                            tif='GTC',
                            outsideRth=True
                        )
                        order_type = "Limit"
                    else:
                        order = MarketOrder(
                            action=action,
                            totalQuantity=quantity,
                            account=self.account,
                            tif='GTC'
                        )
                        order_type = "Market"
                    
                    # Your existing logging and order placement...
                    trade = self.ib.placeOrder(position.contract, order)
                    trades.append(trade)
                    
            # Check if all positions have pending orders
            if all_positions_have_orders:
                self.logger.info(f"""
    All {len(positions)} positions have pending orders.
    Waiting 30 seconds before resuming normal operations...
    """)
                self.ib.sleep(30)
                return trades
                
            return trades
                
        except Exception as e:
            self.logger.error(f"Error closing positions: {str(e)}")
            return trades

    def on_pnl_update(self, pnl: PnL):
        """Handle PnL updates from IB"""
        try:
            self.daily_pnl = pnl.dailyPnL
            self.total_unrealized_pnl = pnl.unrealizedPnL
            self.total_realized_pnl = pnl.realizedPnL
            
            self.logger.info(f"""
    PnL Update:
    - Daily P&L: ${pnl.dailyPnL:,.2f}
    - Unrealized P&L: ${pnl.unrealizedPnL:,.2f}
    - Realized P&L: ${pnl.realizedPnL:,.2f}
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
                    self.close_all_positions()
                else:
                    self.logger.info("All positions already have pending orders")
                    
        except Exception as e:
            self.logger.error(f"Error processing PnL update: {str(e)}")

    def on_portfolio_update(self, item: PortfolioItem):
        """
        Handle portfolio updates from IB
        Args:
            item: PortfolioItem containing position information
        """
        try:
            symbol = item.contract.symbol
            self.positions[symbol] = item
            
            self.logger.info(
                f"My updatePortfolio: {item}"
            )
            
            # Log summary of position
            self.logger.info(f"""
Position Update for {symbol}:
- Position: {item.position}
- Market Price: ${item.marketPrice:.2f}
- Market Value: ${item.marketValue:.2f}
- Average Cost: ${item.averageCost:.2f}
- Unrealized P&L: ${item.unrealizedPNL:.2f}
- Realized P&L: ${item.realizedPNL:.2f}
""")
            
        except Exception as e:
            self.logger.error(f"Error processing portfolio update: {str(e)}")
    
    def onConnected(self):
        self.logger.info("Initial PnL values:")
    

    def setup_events(self):
        """Setup event handlers"""
        # Connect event handlers
        self.ib.updatePortfolioEvent += self.on_portfolio_update
        #self.ib.accountSummaryEvent += self.on_account_summary
        self.ib.pnlEvent += self.on_pnl_update  # Add PnL event handler
        self.ib.connectedEvent += self.onConnected
        # Request initial data
        
        # Get current PnL values
        pnl_list = self.ib.pnl(account=self.account)  # Get current PnL values
        if pnl_list:
            self.logger.info("Initial PnL values:")
            for pnl in pnl_list:
                self.on_pnl_update(pnl)

    def run(self):
        """Start the event loop"""
        try:
            self.setup_events()
            self.logger.info("Starting IB event loop...")
            
            while True:
                self.ib.sleep(1)  # Give time for events to process
                current_net_liq = self.get_net_liquidation()
                # Check if we should close positions
                if self.should_close_positions:
                    self.logger.info("Initiating position closing...")
                    trades = self.close_all_positions()
                    self.should_close_positions = False  # Reset flag
                if current_net_liq is not None:
                    self.logger.info(f"Current Net Liquidation: ${current_net_liq:,.2f}")
                    
                    
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
        finally:
            self.ib.disconnect()

if __name__ == "__main__":
    portfolio_tracker = IBPortfolioTracker()
    portfolio_tracker.close_all_positions()
    portfolio_tracker.run()
