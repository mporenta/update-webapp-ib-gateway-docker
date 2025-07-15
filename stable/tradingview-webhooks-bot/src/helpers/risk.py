# risk.py

from ib_async import *
from ib_async.util import isNan
import math

from log_config import log_config, logger

from tv_ticker_price_data import  price_data_dict, barSizeSetting_dict
from my_util import  shortable_shares
from helpers.derive_stop_loss import volatility_stop_calc



from models import (
    OrderRequest,
   
    PriceSnapshot,
    VolatilityStopData,
)


async def compute_risk_position_size(
    contract: Contract,
    stop_loss_price: float,
    accountBalance: float,
    riskPercentage: float,
    rewardRiskRatio: float,
    action: str,
    req: OrderRequest,
    snapshot: PriceSnapshot,
    ib: IB = None
):
    """
    Compute the position size, take profit, and risk metrics.
    Only used when quantity is not provided or is 0.0.
    """
    try:
        snapshot=await price_data_dict.get_snapshot(contract.symbol)
        logger.info(f"compute_risk_position_size for {contract.symbol} with action: {action}, stop_loss_price: {stop_loss_price}, accountBalance: {accountBalance}, riskPercentage: {riskPercentage}, rewardRiskRatio: {rewardRiskRatio}")
        brokerComish = 0.0
        quantity = 0.0
        limit_price = 0.0
        shortableShares = -1.0
        price_source = "None 0.0"
        hasBidAsk = snapshot.hasBidAsk if snapshot else None
        last = snapshot.last if (snapshot.last is not None and not isNan(snapshot.last) and snapshot.last >1.1) else 0.0
        if req.limit_price is not None and req.limit_price > 0.0:
            logger.debug(f"compute_risk_position_size for {contract.symbol} using req.limit_price: {req.limit_price}")
            limit_price = req.limit_price
            price_source = "req.limit_price"
        
        
        elif hasBidAsk and req.limit_price <= 0:
            logger.debug(f"compute_risk_position_size for {contract.symbol} hasBidAsk: {hasBidAsk}")
            ask_price = snapshot.ask if (snapshot.ask is not None and not isNan(snapshot.ask) and snapshot.ask > 1.1) else 0.0
            bid_price = snapshot.bid if (snapshot.bid is not None and not isNan(snapshot.bid) and snapshot.bid > 1.1) else 0.0
           
            if action == "BUY" and ask_price > 2.0:
                limit_price = round(ask_price, 2)
                price_source ="snapshot.ask"
            elif action == "SELL" and bid_price > 2.0:
                limit_price = round(bid_price, 2)
                price_source ="snapshot.bid"
            elif not isNan(snapshot.last):
                limit_price = round(snapshot.last, 2)
                price_source = "snapshot.last"
            elif not isNan(snapshot.markPrice) and snapshot.markPrice > 0.0:
                limit_price = round(snapshot.markPrice, 2)   
                price_source = "snapshot.markPrice"
            elif not isNan(last) and last > 0.0:
                limit_price = round(last, 2)
                price_source = "snapshot.last"      
            else:
                limit_price = 0.0
                price_source = "None 0.0"
        

            last_bar_close = (
                snapshot.last_bar_close           # set by add_bar() / add_rt_bar()
                or snapshot.close                 # same value – both safe
            )

            yesterday_price = snapshot.yesterday_close_price
        elif last is not None and last > 0.0 and not isNan(last) and req.limit_price <= 0:
            limit_price =last
            price_source = "snapshot.last"
        else:
            return 0.0, 0.0, 0.0, 0.0, 0.0   
        
        if req.quantity is not None and req.quantity > 0:
            logger.debug(f"compute_risk_position_size for {contract.symbol} using req.quantity: {req.quantity}")
            quantity= req.quantity

        # Validate inputs to prevent calculation errors
        if limit_price <= 0.0:
            raise ValueError(f"Invalid entry price: {limit_price}")

        if accountBalance <= 0.0:
            raise ValueError(f"Invalid account balance: {accountBalance}")

       
        logger.debug(f"Calculating so far {contract.symbol} with limit_price: {limit_price} and quantity: {quantity}")
        stop_loss_price_adj = await last_bar_and_stop(action, snapshot, contract, stop_loss_price, barSizeSetting_dict[contract.symbol], ib)
        if stop_loss_price_adj is None or isNan(stop_loss_price_adj):
            logger.error(f"Invalid stop loss price: {stop_loss_price_adj} for {contract.symbol}")
            return 0.0, 0.0, 0.0, 0.0, 0.0  

        stop_loss_price = stop_loss_price_adj
        logger.debug(f"Adjusted stop loss price: {stop_loss_price} for {contract.symbol}")
        # Calculate per-share risk with validation
        perShareRisk = abs(limit_price - stop_loss_price)
        logger.debug(f"Per share risk: {perShareRisk} with limit_price: {limit_price} and stop_loss_price: {stop_loss_price}")

        # Ensure we have a reasonable risk per share
        if perShareRisk < 0.01:
            logger.debug(
                f"Per share risk ({perShareRisk}) is too small; defaulting to 1% of entry price"
            )
            perShareRisk = limit_price * 0.01
            logger.debug(f"Adjusted per share risk: {perShareRisk}")

        # Calculate tolerated risk
        toleratedRisk = abs((riskPercentage / 100 * accountBalance) - (max(round((accountBalance / limit_price) * 0.005), 1.0)))
        logger.debug(f"Tolerated risk: {toleratedRisk} with riskPercentage: {riskPercentage} and accountBalance: {accountBalance}")

        # Calculate quantity based on risk and max capital allocation
        if quantity == 0.0:
            quantity = round(
                min(
                    toleratedRisk / perShareRisk,
                    math.floor((accountBalance) / limit_price),
                )
            )

            # Ensure valid quantity
        if quantity <= 0:
            logger.debug(
                f"Calculated quantity ({quantity}) is invalid; using minimum quantity of 1"
            )
            quantity = 0.0
        logger.debug(f"Calculated quantity: {quantity}")
        logger.debug(f"Per share risk: {perShareRisk}")
        logger.debug(f"Tolerated risk: {toleratedRisk}")
        logger.debug(f"Account balance: {accountBalance}")
        logger.debug(f"Entry price: {limit_price}")
        logger.debug(f"Stop loss price: {stop_loss_price}")
        logger.debug(f"Risk percentage: {riskPercentage}")
        logger.debug(f"Reward risk ratio: {rewardRiskRatio}")
        logger.debug(f"Order action: {action}")
        #perShareRisk, toleratedRisk
        if quantity != 0:
        
            brokerComish = max(round(quantity * 0.005), 1.0)
            
        shortableShares = snapshot.shortableShares if snapshot.shortableShares is not None else 0.0
        shortable= False
        if action == "SELL" :
            logger.info(f"Checking if {contract.symbol} is shortable...")
            is_shortable=shortable_shares(contract.symbol, action, quantity, shortableShares)
            logger.info(f"{contract.symbol} is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")

            if shortable_shares:
                logger.info(f"{contract.symbol} is shortable, is_shortable: {is_shortable} with shortableShares: {shortableShares} and quantity: {quantity}")
                shortable = True

            if not shortable:
                logger.error(f"{contract.symbol} is not shortable, cannot proceed with SELL action.")
                return 0.0, 0.0, 0.0, 0.0, 0.0
            

        # Calculate take profit price
        if action == "BUY":
            take_profit_price = round(limit_price + (perShareRisk * rewardRiskRatio), 2)
            logger.debug(f"Take profit price: {take_profit_price} with action: {action}")
        else:
            take_profit_price = round(limit_price - (perShareRisk * rewardRiskRatio), 2)
            logger.debug(f"Take profit price: {take_profit_price} with action: {action}")
        logger.info(f"compute_position_size={stop_loss_price}, limitPrice={limit_price},  quantity={quantity} take_profit_price={take_profit_price}, brokerComish={brokerComish}, perShareRisk={perShareRisk}, toleratedRisk={toleratedRisk}")
        return quantity, limit_price, take_profit_price
    except Exception as e:  
        logger.error(f"Error in compute_risk_position_size for {contract.symbol}: {e}")
        return 0.0, 0.0, 0.0, 0.0, 0.0

       

 
async def last_bar_and_stop(
        action: str,
    snapshot: PriceSnapshot,
    contract: Contract,
    stop_loss: float,  # your vStop value
    barSizeSetting: str,
    ib: IB=None
) -> float:
    """
    Calculate an adjusted stop‐loss price based on the last 30s bar.

    Parameters
    ----------
    contract : Contract
        The IB contract.
    stop_loss : float
        Initial vStop value.
    barSizeSetting : str
        Bar size (e.g. "15 secs").
    ib : IB
        IB client instance.

    Returns
    -------
    float
        Adjusted stop‐loss level:
          • If gap ≥ 0.4%, keep vStop.
          • Otherwise, use close × 0.99 (long) or close × 1.01 (short).
    """
    try:
        barSizeSetting = barSizeSetting_dict.get(contract.symbol)
        uptrend_bool = None
        vStop_float=None
        vol_stop_data = await volatility_stop_calc.get(contract.symbol)
        logger.debug(f"vol_stop_data for {contract.symbol}: {vol_stop_data}")
        vStop_float,uptrend_bool, atr_float, vstopAtrFactor_float=vol_stop_data
        logger.debug(f"last_bar_and_stop for {contract.symbol} with stop_loss: {stop_loss} and barSizeSetting: {barSizeSetting}")
        if stop_loss is None or isNan(stop_loss) or stop_loss < 1.0:
            logger.warning(f"stop_loss is None or NaN or less than 1.0 for {contract.symbol}, using vStop_float: {vStop_float}")
            
            stop_loss = vStop_float
            logger.debug(f"vStop_float: {vStop_float}, uptrend_bool: {uptrend_bool}, vstopAtrFactor_float: {vstopAtrFactor_float} for {contract.symbol}")
        
            
        stop_loss_price_raw = stop_loss
        stop_loss_price_adj= stop_loss
        logger.debug(f"stop_loss_price_raw: {stop_loss_price_raw} for {contract.symbol}")
       
        logger.info(f"stop_loss raw {stop_loss_price_adj} and barSizeSetting {barSizeSetting} for {contract.symbol}")
        
 
        close=snapshot.bar.close
        open=snapshot.bar.open
        high=snapshot.bar.high
        low=snapshot.bar.low
        logger.debug(f"last bar for {contract.symbol}: {snapshot.bar}")
        logger.debug(f"last bar close:  {close}, open: {open}, high: {high}, low: {low} and stop_loss {stop_loss} for {contract.symbol}")



        if  action == "BUY":
            gap_pct = abs(low - stop_loss) / abs(stop_loss) * 100
            if gap_pct < 0.04:
                stop_loss = max(close - (atr_float*(2*vstopAtrFactor_float)), vStop_float)
                stop_loss_price_adj= stop_loss
                logger.info(f"stop_loss_price_raw {stop_loss_price_raw} ohlc stop_loss clean {stop_loss_price_adj} for {contract.symbol}, and atr_float*(2*vstopAtrFactor_float) = {atr_float} * {(2*vstopAtrFactor_float)} = {atr_float*(2*vstopAtrFactor_float)}")
                logger.debug(f"gap_pct: {gap_pct} for {contract.symbol} and close - (atr_float*(2*vstopAtrFactor_float)): {close - (atr_float*(2*vstopAtrFactor_float))} for {contract.symbol}")
                logger.debug(f"gap_pct: {gap_pct} for {contract.symbol} and close * 0.995: {close * 0.995} for {contract.symbol}")
        elif  action == "SELL":
            gap_pct = (stop_loss - high) / high * 100
            if gap_pct < 0.04:
                logger.info(f"stop_loss_price_raw {stop_loss_price_raw} ohlc stop_loss clean {stop_loss_price_adj} for {contract.symbol}, and atr_float*(2*vstopAtrFactor_float) = {atr_float} * {(2*vstopAtrFactor_float)} = {atr_float*(2*vstopAtrFactor_float)}")
                logger.debug(f"gap_pct: {gap_pct} for {contract.symbol} and close + (atr_float*(2*vstopAtrFactor_float)): {close + (atr_float*(2*vstopAtrFactor_float))} for {contract.symbol}")
                logger.debug(f"gap_pct: {gap_pct} for {contract.symbol} and close * 1.005: {close * 1.005} for {contract.symbol}")
                stop_loss = min(close + (atr_float*(2*vstopAtrFactor_float)), vStop_float)
                stop_loss_price_adj= stop_loss
        else:
            logger.error(f"stop_loss_price_raw {stop_loss_price_raw} ohlc stop_loss clean {stop_loss_price_adj} for {contract.symbol}, no adjustment needed")
            raise ValueError(f"Invalid uptrend_bool: {uptrend_bool} for {contract.symbol}")
        logger.info(f"stop_loss clean {stop_loss_price_adj}")
        
        return stop_loss_price_adj

    except Exception as e:
        logger.error(f"Error in last_bar_and_stop for {contract.symbol}: {e}")
        return stop_loss_price_adj