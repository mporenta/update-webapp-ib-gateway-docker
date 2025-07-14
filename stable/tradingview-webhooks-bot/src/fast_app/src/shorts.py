# shorts.py
from ftplib import FTP
from datetime import datetime, timedelta
import pytz
from typing import Dict, Tuple, Any
import csv
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import feedparser
import asyncio
import xml.etree.ElementTree as ET
import json
import aiohttp 
# Set up logging
from log_config import log_config, logger
log_config.setup()

class ShortStockManager:
    def __init__(self):
        self.availability_dict: Dict[str, int] = {}
        self.last_updated: datetime = None
        self.scheduler = AsyncIOScheduler()
        self.halts_dict: Dict[str, Dict] = {}
        self.rss_base_url = 'https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts'


    def get_rss_url(self):
        """
        Constructs the RSS URL based on the current day.
        Returns base URL for weekdays, adds haltdate parameter for weekends.
        """
        now = datetime.now(pytz.timezone('America/New_York'))
        if now.weekday() in [5, 6]:  # 5 = Saturday, 6 = Sunday
            # Calculate previous Friday because halts are only reported on weekdays
            days_to_friday = now.weekday() - 4 if now.weekday() > 4 else 0
            previous_friday = now - timedelta(days=days_to_friday)
            haltdate = previous_friday.strftime('%m%d%Y')
            url = f"{self.rss_base_url}&haltdate={haltdate}"
        else:
            url = 'https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts'
    
        logger.info(f"Constructed RSS URL: {url} for now.weekday() of {now.weekday()}")
        return url
        
    async def fetch_and_parse_short_availability(self):
        """
        Fetches and parses the IB short stock availability data.
        Updates internal dictionary and timestamp.
        """
        try:
            logger.info("Starting short stock data update")
            
            # Connect to IB FTP
            ftp = FTP('ftp2.interactivebrokers.com')
            ftp.login('shortstock', '')
            
            # Download usa.txt content
            content = []
            def handle_line(line):
                content.append(line)
            
            ftp.retrlines('RETR usa.txt', handle_line)
            ftp.quit()
            
            # Find the BOF line for timestamp and convert to UTC
            ny_tz = pytz.timezone('America/New_York')
            new_availability: Dict[str, int] = {}
            
            for line in content:
                if line.startswith('#BOF'):
                    bof_parts = line.split('|')
                    date_str = bof_parts[1]
                    time_str = bof_parts[2]
                    
                    # Parse the timestamp in NY timezone
                    ny_time = ny_tz.localize(
                        datetime.strptime(f"{date_str} {time_str}", "%Y.%m.%d %H:%M:%S")
                    )
                    # Convert to UTC
                    self.last_updated = ny_time.astimezone(pytz.UTC)
                    continue
                
                if not line.startswith('#'):
                    fields = line.split('|')
                    if len(fields) >= 8:
                        symbol = fields[0]
                        available = fields[7]
                        
                        if available.startswith('>'):
                            available = available[1:]
                        try:
                            shares = int(available.replace(',', ''))
                            new_availability[symbol] = shares
                        except ValueError:
                            continue
            
            # Update the dictionary atomically
            self.availability_dict = new_availability
            
            logger.info(f"Short stock data updated successfully. Total symbols: {len(self.availability_dict)}")
            
            # Export to CSV if needed
            await self.short_availability_export_to_csv()
            
        except Exception as e:
            logger.error(f"Error updating short stock data: {str(e)}")
    
    async def short_availability_export_to_csv(self) -> None:
        """Exports the current data to a CSV file."""
        try:
            if not self.availability_dict or not self.last_updated:
                return
            
            timestamp = self.last_updated.strftime("%Y%m%d_%H%M%S")
            filename = f"short_availability_{timestamp}.csv"
            
            # Get script directory and create full path
            script_dir = Path(__file__).parent
            filepath = script_dir / filename
            
            # Write to CSV with ISO format timestamp
            with open(filepath, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Symbol', 'Available_Shares', 'Last_Updated'])
                for symbol, shares in sorted(self.availability_dict.items()):
                    writer.writerow([symbol, shares, self.last_updated.isoformat()])
            
            logger.info(f"CSV exported to: {filepath}")
        except Exception as e:
            logger.error(f"Error exporting CSV: {str(e)}")
    
    def get_availability(self, symbol: str) -> int:
        """Get the number of shares available for a symbol."""
        return self.availability_dict.get(symbol.upper())
    
    def get_last_updated(self) -> datetime:
        """Get the timestamp of the last update."""
        return self.last_updated
    
    async def start_scheduler(self):
        """Start the background scheduler for data updates."""
        self.scheduler.add_job(
            self.fetch_and_parse_short_availability,
            CronTrigger(minute='*/15'),  # Run every 15 minutes
            id='short_stock_updater',
            name='Update short stock availability',
            replace_existing=True
        )
        self.scheduler.start()
        logger.info("Short stock update scheduler started")
    
    async def stop_scheduler(self):
        """Stop the background scheduler."""
        self.scheduler.shutdown()
        logger.info("Short stock update scheduler stopped")

    async def fetch_and_parse_trade_halts(self):
       

       try:
           # Fetch RSS Feed
           xml_content = await self.fetch_rss_feed()
           logger.info("RSS feed fetched successfully.")

           # Parse RSS and convert to JSON
           rss_json = self.parse_rss_to_json(xml_content)
           logger.info("RSS feed parsed and converted to JSON successfully.")

           # Pretty-print the JSON
           json_output = json.dumps(rss_json, indent=4)

           # Optionally, save to a file
           with open('tradehalts.json', 'w', encoding='utf-8') as f:
               f.write(json_output)
               logger.info("JSON output saved to 'tradehalts.json'.")

           # Print JSON to console (optional)
           logger.debug(json_output)

       except Exception as e:
           logger.error(str(e))

    

    async def fetch_rss_feed(self):
        """
        Fetches the RSS feed from the constructed URL using aiohttp.
        """
        try:
            url = self.get_rss_url()
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status >= 400:
                        logger.error(f"Error fetching RSS feed: HTTP {response.status}")
                        return None
                    return await response.read()
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching RSS feed: {e}")
            return None

    def parse_rss_to_json(self,xml_content):
        """
        Parses the RSS XML content and converts it to a JSON object, including all fields.

        Args:
            xml_content (str): The XML content of the RSS feed.

        Returns:
            dict: A dictionary representing the JSON structure of the RSS feed.
        """
        # Define namespaces
        namespaces = {
            'ndaq': 'http://www.nasdaqtrader.com/'
        }

        # Parse XML
        root = ET.fromstring(xml_content)

        # Initialize JSON structure
        rss_json = {}

        # Extract channel information
        channel = root.find('channel')
        if channel is None:
            raise Exception("Invalid RSS feed: 'channel' element not found.")

        # Extract standard channel fields
        rss_json['channel'] = {
            'title': channel.findtext('title'),
            'link': channel.findtext('link'),
            'copyright': channel.findtext('copyright'),
            'pubDate': channel.findtext('pubDate'),
            'ttl': channel.findtext('ttl')
        }

        # Extract namespaced channel fields (e.g., ndaq:numItems)
        for ns_prefix, ns_uri in namespaces.items():
            for elem in channel.findall(f'{ns_prefix}:*', namespaces):
                tag = elem.tag.replace(f'{{{ns_uri}}}', '')
                rss_json['channel'][tag] = elem.text

        # Initialize items list
        rss_json['items'] = []

        # Iterate over each item in the channel
        for item in channel.findall('item'):
            item_json = {}

            # Extract standard item fields
            for field in ['title', 'pubDate']:
                item_json[field] = item.findtext(field)

            # Extract namespaced item fields
            for ns_prefix, ns_uri in namespaces.items():
                for elem in item.findall(f'{ns_prefix}:*', namespaces):
                    tag = elem.tag.replace(f'{{{ns_uri}}}', '')
                    item_json[tag] = elem.text if elem.text else ""

            # Handle 'description' field if further parsing is needed
            # Currently, it's kept as a raw HTML string within CDATA
            # If you need to parse the HTML table inside, consider using BeautifulSoup

            rss_json['items'].append(item_json)

        return rss_json



    async def export_to_csv_halts(self):
        """Exports the current trade halt data to a CSV file."""
        try:
            if not self.halts_dict:
                logger.warning("No data to export")
                return

            timestamp = datetime.now(pytz.UTC).strftime("%Y%m%d_%H%M%S")
            filename = f"trade_halts_{timestamp}.csv"

            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Symbol', 'Company', 'Halt Time', 'Link'])
                for symbol, details in self.halts_dict.items():
                    writer.writerow([symbol, details['company'], details['halt_time'], details['link']])

            logger.info(f"CSV exported successfully: {filename}")

        except Exception as e:
            logger.error(f"Error exporting CSV: {str(e)}")
    async def validate_short_trade(self, ticker: str, price: float, quantity: int, action=None) -> Tuple[bool, Dict[str, Any], int]:
        """
        Validates a short trade by checking trade halts and short availability.
    
        Args:
            ticker: The stock symbol
            price: The current price of the stock
            quantity: The requested quantity to short
        
        Returns:
            Tuple containing:
            - bool: Whether the request is valid
            - Dict: Response content if invalid (None if valid)
            - int: Status code if invalid (200 if valid)
        """
        # Check if price is below threshold and fetch trade halts if needed
        if price is not None and price < 10.0:
            logger.info(f"Price {price} is below 10.0, checking trade halts for {ticker} with action {action}")
        
            # Fetch current trade halts
            await self.fetch_and_parse_trade_halts()
            rss_data =  self.parse_rss_to_json(await self.fetch_rss_feed())
        
            # Check if ticker is in halts
            ticker_halt = next((item for item in rss_data.get("items", []) 
                              if item.get("IssueSymbol") == ticker), None)
        
            if ticker_halt:
                logger.info(f"Found halt data for {ticker}")
            
                # Check ResumptionDate and ResumptionTradeTime
                if not ticker_halt.get("ResumptionDate"):
                    logger.warning(f"No resumption date for {ticker}, blocking webhook")
                    return False, {
                        "status": "rejected",
                        "message": f"Trading halted for {ticker} with no resumption date"
                    }, 403
            
                # Check if enough time has passed since resumption
                # In validate_short_trade function in shorts.py
                # Replace the try block around line 330 with this:

                try:
                    ny_tz = pytz.timezone('America/New_York')
                    now = datetime.now(ny_tz)
    
                    # Check if ResumptionTradeTime exists and is not empty
                    resumption_date = ticker_halt.get('ResumptionDate', '').strip()
                    resumption_time = ticker_halt.get('ResumptionTradeTime', '').strip()
    
                    # Check if there's no resumption information at all
                    if not resumption_date:
                        logger.warning(f"No resumption date for {ticker}, blocking webhook")
                        return False, {
                            "status": "rejected",
                            "message": f"Trading halted for {ticker} with no resumption date"
                        }, 403
    
                    # If we have a resumption date but no time, use the quote time or default
                    if not resumption_time:
                        # Try to use ResumptionQuoteTime instead, or default to market open
                        quote_time = ticker_halt.get('ResumptionQuoteTime', '').strip()
                        if quote_time:
                            logger.info(f"Using ResumptionQuoteTime ({quote_time}) for {ticker} since ResumptionTradeTime is empty")
                            resumption_time = quote_time
                        else:
                            # Default to market open (9:30 AM)
                            logger.info(f"Using default market open time for {ticker} since no resumption times available")
                            resumption_time = "09:30:00"
    
                    # Now we should have both a date and time to use
                    resumption_datetime = datetime.strptime(
                        f"{resumption_date} {resumption_time}", 
                        "%m/%d/%Y %H:%M:%S"
                    )
                    resumption_datetime = ny_tz.localize(resumption_datetime)
    
                    time_since_resumption = (now - resumption_datetime).total_seconds()
    
                    if time_since_resumption < 300:  # Less than 5 minutes
                        logger.warning(f"Not enough time passed since resumption for {ticker}")
                        return False, {
                            "status": "rejected",
                            "message": f"Only {int(time_since_resumption)} seconds have passed since trading resumed for {ticker}. Required: 300 seconds"
                        }, 403
                except Exception as e:
                    logger.error(f"Error processing resumption time: {str(e)}")
                    # Provide more detailed error in response
                    return False, {
                        "status": "rejected",
                        "message": f"Error processing halt data for {ticker}: {str(e)}"
                    }, 403
    
        # Check short availability
        required_shares = None
        shares = None
        if action == "SELL":
            shares = self.get_availability(ticker)
            if shares is None:
                return False, {
                    "status": "rejected",
                    "message": f"No short availability data found for {ticker}"
                }, 403
    
            required_shares = quantity * 5
            if shares < required_shares:
                return False, {
                    "status": "rejected",
                    "message": f"Insufficient shares available to short. Required: {required_shares}, Available: {shares}",
                    "symbol": ticker,
                    "available_shares": shares,
                    "required_shares": required_shares
                }, 403
    
        logger.info(f"Short availability check passed for {ticker}. Required: {required_shares}, Available: {shares}")
        return True, None, 200

short_stock_manager = ShortStockManager()


  
        

    