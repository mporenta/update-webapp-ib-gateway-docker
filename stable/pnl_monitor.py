import os
from ib_insync import *
import pandas as pd
from loguru import logger

PNL_THRESHOLD = float(os.environ.get('PNL_THRESHOLD', '-0.05'))  # Default to -5%
account = os.environ.get('ACCOUNT', 'DU7397764')  # If not set, will use the first account
beginning_balance = None

class IBPortfolioMonitor:
    def __init__(self):
        self.ib = IB()
        self.pnl = PnL()
        self.account = account
        self.portfolio_items = {}
        self.beginning_balance = beginning_balance
        self.total_unrealized_pnl = 0
        self.total_realized_pnl = 0
        self.total_market_value = 0
        self.loss_threshold = PNL_THRESHOLD
        self.action_taken = False  # Initialize action_taken flag

    def connect(self):
        """Connect to IB Gateway"""
        try:
            self.ib.connect('127.0.0.1', 4002, clientId=1)
            print("Successfully connected to IB Gateway")
        except Exception as e:
            print(f"Failed to connect: {e}")
            raise

    def pnl_update(self, account):
        self.ib.pnlEvent += self.pnl_update
        logger.info(f"PnL Update: {self.ib.pnl}")
        return self.ib.pnl(account)

    def fetch_net_liquidation(self):
        account_values = self.ib.accountSummary(self.account)
        net_liquidation = next(
            (float(item.value) for item in account_values
             if item.tag == 'NetLiquidation' and item.currency == 'USD'),
            None
        )
        #self.ib.pnlEvent += self.fetch_net_liquidation
       
        #logger.info(self.fetch_net_liquidation)
        return net_liquidation

    def fetch_beginning_balance(self):
        self.beginning_balance = self.fetch_net_liquidation()
        if self.beginning_balance is not None:
            self.close_positions()
            self.action_taken = True
            logger.info(f"Beginning account balance: {self.beginning_balance}")
        else:
            logger.error("Failed to retrieve the beginning account balance.")

    def setup_portfolio_handlers(self):
        """Set up portfolio update handlers"""
        def on_portfolio_update(portfolio_item):
            """Handle portfolio updates"""
            symbol = portfolio_item.contract.symbol
            self.portfolio_items[symbol] = {
                'position': portfolio_item.position,
                'market_price': portfolio_item.marketPrice,
                'market_value': portfolio_item.marketValue,
                'average_cost': portfolio_item.averageCost,
                'unrealized_pnl': portfolio_item.unrealizedPNL,
                'realized_pnl': portfolio_item.realizedPNL
            }
            
            # Update totals
            self.update_portfolio_totals()
            
            # Print position details
            self.print_position_details(portfolio_item)

        # Subscribe to portfolio updates
        self.ib.updatePortfolioEvent += on_portfolio_update

    def on_pnl_update(self):
        if self.beginning_balance is None:
            self.fetch_beginning_balance()
        current_net_liquidation = self.fetch_net_liquidation()
        if current_net_liquidation is not None and self.beginning_balance is not None:
            # Calculate percentage change
            percentage_change = ((current_net_liquidation - self.beginning_balance) / self.beginning_balance) * 100
            logger.info(f"Current P&L: {percentage_change:.2f}%")
            if percentage_change <= self.loss_threshold:
                logger.warning(f"P&L threshold reached: {percentage_change:.2f}%")
                # Take action to close positions and cancel orders
                self.close_positions()
                self.action_taken = True
        else:
            logger.error("Failed to retrieve current net liquidation value.")

    def close_positions(self):
        positions = self.ib.positions()
        for position in positions:
            contract = position.contract
            pos_size = position.position
            if pos_size == 0:
                continue  # Nothing to close
            action = 'SELL' if pos_size > 0 else 'BUY'
            order = Order(
                action=action,
                totalQuantity=abs(pos_size),
                orderType='MKT'
            )
            trade = self.ib.placeOrder(contract, order)
            
            print(f"Placed order to {action} {abs(pos_size)} of {contract.symbol}")

    def get_positions(self):
        """Get current positions"""
        my_positions = self.ib.positions()
        print("My positions:")
        print(my_positions)
        return my_positions

    def get_positions_event(self):
        """Get positions event"""
        self.ib.positionEvent += self.get_positions

    def update_portfolio_totals(self):
        """Update portfolio totals"""
        self.total_unrealized_pnl = sum(item['unrealized_pnl'] for item in self.portfolio_items.values())
        self.total_realized_pnl = sum(item['realized_pnl'] for item in self.portfolio_items.values())
        self.total_market_value = sum(item['market_value'] for item in self.portfolio_items.values())

        print("\nPortfolio Totals:")
        print(f"Total Market Value: ${self.total_market_value:,.2f}")
        print(f"Total Unrealized P&L: ${self.total_unrealized_pnl:,.2f}")
        print(f"Total Realized P&L: ${self.total_realized_pnl:,.2f}")
        print(f"Total P&L: ${(self.total_unrealized_pnl + self.total_realized_pnl):,.2f}")

    def print_position_details(self, portfolio_item):
        """Print detailed position information"""
        print(f"\nPosition Update for {portfolio_item.contract.symbol}:")
        print(f"Position: {portfolio_item.position:,.0f} shares")
        print(f"Market Price: ${portfolio_item.marketPrice:,.2f}")
        print(f"Market Value: ${portfolio_item.marketValue:,.2f}")
        print(f"Average Cost: ${portfolio_item.averageCost:,.2f}")
        print(f"Unrealized P&L: ${portfolio_item.unrealizedPNL:,.2f}")
        print(f"Realized P&L: ${portfolio_item.realizedPNL:,.2f}")
        
        # Calculate and print additional metrics
        if portfolio_item.position != 0:
            pnl_percentage = (portfolio_item.unrealizedPNL / 
                              (portfolio_item.averageCost * abs(portfolio_item.position))) * 100
            print(f"P&L %: {pnl_percentage:.2f}%")

    def get_portfolio_summary_df(self):
        """Create a pandas DataFrame with portfolio summary"""
        df = pd.DataFrame.from_dict(self.portfolio_items, orient='index')
        df['pnl_percentage'] = (df['unrealized_pnl'] / 
                               (df['average_cost'] * abs(df['position']))) * 100
        return df

    def run(self):
        """Main run method"""
        try:
            self.connect()
            # Request initial portfolio data
            portfolio = self.ib.portfolio()
            positions = self.ib.positions()
            PnL()
           

            self.setup_portfolio_handlers()
            self.on_pnl_update()
            self.get_positions_event()
            self.get_positions()
            self.pnl_update(account)
            currentPnL= self.pnl_update(account)
            print("Current Positions:")
            print(positions)
            print("Current PnL Updates:")
            print(currentPnL)
            
            
            # Keep the connection alive
            self.ib.run()
            
        except KeyboardInterrupt:
            print("\nShutting down...")
        except Exception as e:
            print(f"Error in main loop: {e}")
        finally:
            self.ib.disconnect()

if __name__ == "__main__":
    monitor = IBPortfolioMonitor()
    monitor.run()

