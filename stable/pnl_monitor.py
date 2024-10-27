import os
from ib_insync import *
import pandas as pd
from loguru import logger

PNL_THRESHOLD = float(os.environ.get('PNL_THRESHOLD', '-0.05'))  # Default to -5%
account = os.environ.get('ACCOUNT', 'DU7397764')  # If not set, will use the first account
beginning_balance = None

class IBPortfolioMonitor:
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

