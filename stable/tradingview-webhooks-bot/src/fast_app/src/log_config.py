from loguru import logger 
import sys
import os
from typing import *
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
env_file = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_file)
log_level = os.getenv("TBOT_LOGLEVEL", "INFO")


#logger.add(sys.stdout, colorize=True)

class LogConfig:
    """Centralized logging configuration for the application"""
    
    def __init__(self, root_dir: str = None):
        self._configured: bool = False
        self.root_dir = root_dir or str(Path(__file__).parent.parent)
        self._configured: bool = False
        
        self.log_level = log_level
        
        # Common format for all logs
        self.log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )
        
        # Format for detailed debugging
        self.debug_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "Process: {process} | Thread: {thread} | "
            "<level>{message}</level>"
        )
        
        # Pretty format for structured data
        self.pretty_format = lambda record: self._format_record(record)

    def _format_record(self, record: Dict[str, Any]) -> str:
        """Custom formatter for structured data"""
        def format_value(v):
            if isinstance(v, (dict, list)):
                return f"\n{str(v)}"
            elif isinstance(v, (int, float)):
                return f"{v:,}"
            return str(v)
            
        msg = record["message"]
        
        # Format any structured data
        if isinstance(msg, dict):
            formatted_dict = "\n".join(
                f"    {k}: {format_value(v)}" for k, v in msg.items()
            )
            msg = f"\n{formatted_dict}"
        elif isinstance(msg, (list, tuple)):
            formatted_list = "\n".join(
                f"    - {format_value(item)}" for item in msg
            )
            msg = f"\n{formatted_list}"
            
        return msg

    def setup(self):
        """Set up logging configuration"""
        if self._configured:
            return
        # Remove any existing handlers
        logger.remove()
        
        # Add console logging with standard format
        logger.add(
            sink=sys.stderr,
            level=self.log_level,
            format=self.log_format,
            enqueue=True,  # Thread-safe logging
            colorize=True
        )
        
        # Add file logging
        logger.add(
            sink=os.path.join(self.root_dir, "app.log"),
            level=self.log_level,
            format=self.log_format,
            rotation="10 MB",
            retention="1 week",
            compression="zip",
            enqueue=True
        )
        
        # Error-only logs with more detail
        logger.add(
            sink=os.path.join(self.root_dir, "error.log"),
            level="ERROR",
            format=self.debug_format,
            rotation="10 MB",
            retention="1 month",
            compression="zip",
            backtrace=True,
            diagnose=True,
            enqueue=True
        )
        
        # Debug logs with full detail
        if self.log_level == "DEBUG":
            logger.add(
                sink=os.path.join(self.root_dir, "debug.log"),
                level="DEBUG",
                format=self.debug_format,
                rotation="10 MB",
                retention="3 days",
                compression="zip",
                enqueue=True
            )
        self._configured = True


    def format_positions(self, positions) -> str:
        """Format position data into a readable string"""
        if not positions:
            return "No active positions"
            
        formatted = "\n"
        for pos in positions:
            if hasattr(pos, 'position') and pos.position != 0:
                formatted += (
                    f"    {pos.contract.symbol:<6} | "
                    f"Pos: {pos.position:>8,.0f} | "
                    f"Avg Cost: ${pos.avgCost:>10,.2f} | "
                   
                )
        return formatted
    
    def format_portfolio(self, portfolio_items) -> str:
        """Format position data into a readable string"""
        if not portfolio_items:
            return "No active portfolio_items"
            
        formatted = "\n"
        for pos in portfolio_items:
            if hasattr(pos, 'position') and pos.position != 0:
                formatted += (
                    f"    {pos.contract.symbol:<6} | "
                    f"Pos: {pos.position:>8,.0f} | "
                    f"Avg Cost: ${pos.averageCost:>10,.2f} | "
                    f"unrealized PnL: ${getattr(pos, 'unrealizedPNL', 0):>10,.2f} | "
                    f"realized PnL: ${getattr(pos, 'realizedPNL', 0):>10,.2f}\n"
                )
        return formatted

    def format_account_values(self, values: dict) -> str:
        """Format account values into a readable string"""
        return (
            f"\n    Net Liquidation: ${values('net_liquidation', 0):,.2f}\n"
            f"    Buying Power:    ${values('buying_power', 0):,.2f}\n"
            f"    Settled Cash:    ${values('settled_cash', 0):,.2f}\n"
        )

# Create global instance
log_config = LogConfig()
if __name__ == "__main__":
    # Initialize logging
    log_config.setup()
"""
# Usage example:
if __name__ == "__main__":
    # Initialize logging
    log_config.setup()
    
    # Example usage
    logger.info("Application started")
    logger.debug("Debug message")
    logger.error("Error occurred", exc_info=True)
    
    # Example of structured logging
    positions = [...]  # Your position data
    logger.info(f"Current Positions:{log_config.format_positions(positions)}")
    
    account_values = {...}  # Your account values
    logger.info(f"Account Values:{log_config.format_account_values(account_values)}")
    """    