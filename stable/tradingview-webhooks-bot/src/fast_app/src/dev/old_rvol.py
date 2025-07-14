import os
import re
import pandas as pd
import numpy as np
from pathlib import Path
import asyncio
from ib_async import IB, Contract
from dotenv import load_dotenv
import logging
from log_config import log_config, logger

# Set up logging

# Load environment variables
env_file = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_file)

# Configuration variables
rvolFactor = 0.1
ib=IB()

async def get_contract(symbol, primary_exchange=None):
    """Fetch contract details for a given symbol."""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'
    if primary_exchange:
        contract.primaryExchange = primary_exchange
    qualified_contracts = await ib.qualifyContractsAsync(contract)
    if not qualified_contracts:
        logger.error(f"Could not qualify contract for {contract.symbol}")
        return None
    qualified_contract = qualified_contracts[0]
    #ticker = ib.reqMktData(qualified_contract, '', True, False)
    await asyncio.sleep(0.5) 
    hist_data = await get_historical_data(ib, qualified_contract)
    await asyncio.sleep(0.5)  # Rate limiting
    return hist_data
    


async def get_historical_data(ib, contract: Contract):
    """Fetch historical data for a given symbol with contract qualification and error handling."""
    
    try:
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',
            durationStr='31 D',
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=False
        )
    except Exception as e:
        logger.error(f"Error retrieving historical data for {contract.symbol}: {e}")
        return []
    if bars:
        return [{'date': bar.date, 'open': bar.open, 'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume} for bar in bars]
    return []


def get_latest_csv(directory):
    """Retrieve the latest CSV file from the specified directory."""
    csv_files = list(Path(directory).glob('*.csv'))
    if not csv_files:
        raise FileNotFoundError("No CSV files found in directory")
    return str(max(csv_files, key=os.path.getctime))

def create_ticker_string(row):
    """Create a formatted ticker string."""
    return f"{row['Exchange']}:{row['Symbol']}"

async def process_csv(file_path):
    logger.info(f"Processing file: {file_path}")
    df = pd.read_csv(file_path)
    
    # Ensure required columns exist
    required_columns = ['Symbol', 'Exchange', 'Average Volume 10 days']
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            raise KeyError(f"Missing required column: {col}")
    df['Average Volume 10 days'] = pd.to_numeric(df['Average Volume 10 days'], errors='coerce')
    result = df[required_columns].copy()

    # Connect to IB
    logger.info("Connecting to IB...")
    await ib.connectAsync('127.0.0.1', 4002, clientId=14, timeout=120)

    ib_volumes = {}
    ib_last_prices = {}
    for symbol in result['Symbol']:
        try:
            hist_data = await get_contract(symbol)
            ib_vol = hist_data[-1]['volume'] if hist_data else 0
            ib_price = hist_data[-1]['close'] if hist_data else 0
        except Exception as e:
            logger.error(f"Failed to get data for {symbol}: {e}")
            ib_vol = 0
        ib_volumes[symbol] = ib_vol
        ib_last_prices[symbol] = ib_price
        await asyncio.sleep(0.5)  # Increased delay for better rate limiting

    result['IB_Last_Price'] = result['Symbol'].map(ib_last_prices)
    result['IB_Volume'] = result['Symbol'].map(ib_volumes)
    result['Volume_Ratio'] = result['IB_Volume'] / result['Average Volume 10 days']
    result['Volume_Ratio'] = result['Volume_Ratio'].replace([np.inf, -np.inf], np.nan).fillna(0)
    result['Volume*Price']= result['IB_Volume'] * result['IB_Last_Price']
    

    # Filter results by rvolFactor and sort in descending order
    result = result[result['Volume_Ratio'] >= rvolFactor].sort_values('Volume_Ratio', ascending=False)

    #result = result[result['Volume_Ratio'] >= rvolFactor].sort_values('Volume*Price', ascending=False)
    #result = result[result['Volume_Ratio'] >= rvolFactor].sort_values('Symbol', ascending=False)
    logger.info("Volume Analysis Results:")
    print(result.to_string(index=False))
    total_stocks = len(result)
    print(f"\nTotal Stocks: {total_stocks}")
    logger.info(f"Total Stocks: {total_stocks}")

    # Create formatted ticker string and write to a .txt file
    ticker_string = ','.join(result.apply(create_ticker_string, axis=1))
    output_path = os.path.join(os.path.dirname(file_path), 'pre_market_filtered_tickers.txt')
    with open(output_path, 'w') as f:
        f.write(ticker_string)
    logger.info(f"Filtered ticker list written to: {output_path}")

    ib.disconnect()

async def main():
    # Update this directory as needed
    directory = "C:\\Users\\mikep\\OneDrive\\Downloads Chrome"
    try:
        latest_csv = get_latest_csv(directory)
        await process_csv(latest_csv)
    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")

if __name__ == "__main__":
    log_config.setup()

    asyncio.run(main())
