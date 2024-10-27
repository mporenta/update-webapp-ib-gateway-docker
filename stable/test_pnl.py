from pnl_monitor import TradingApp
import unittest
from unittest.mock import MagicMock, patch
import time

class TestStopLossFunctionality(unittest.TestCase):

    @patch('threading.Thread')
    def test_stop_loss_triggers_position_closure(self, MockThread):
        # Create an instance of the TradingApp
        app = TradingApp()

        # Mock essential methods to prevent actual API calls
        app.placeOrder = MagicMock()
        app.reqPositions = MagicMock()

        # Mock threading to avoid actually starting threads in test
        MockThread.return_value.start = MagicMock()

        # Set initial starting_net_liq and loss threshold for testing
        app.starting_net_liq = 100000  # Assume starting net liquidity is 100,000
        loss_threshold = -0.01 * app.starting_net_liq  # -1% of starting value = -1000

        # Simulate portfolio update with total P&L below the threshold
        app.updatePortfolio(Contract(), 10, 100, 1000, 105, -1100, 0, 'DU7397764')

        # Check if the stop loss was triggered
        self.assertTrue(app.stop_loss_triggered, "Stop loss should be triggered when P&L goes below threshold.")

        # Ensure trade_positions (closing positions) is initiated
        MockThread.assert_called_once()
        app.reqPositions.assert_called_once()

        # Additional checks can confirm behavior like order placement or event setting

# Run the tests
if __name__ == "__main__":
    unittest.main()
