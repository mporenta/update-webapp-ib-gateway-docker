
from ib_async import *
from typing import Dict, List, Optional
from models import OrderRequest
import math
import datetime
from log_config import log_config, logger
from my_util import convert_pine_timeframe_to_barsize, current_millis, is_market_hours, is_weekend, get_timestamp, format_order_details_table, compute_position_size
import asyncio
log_config.setup()
class OrderManager:
    def __init__(self, ib: IB):
        self.ib = ib
        self.triggered_bracket_orders = {}
        self.fill_lock = asyncio.Lock()
        self.orders_dict_oca_symbol = {}
        self.ticker= {}
        self.close_price = {}
        self.open_price: Dict[str, List[float]] = {}
        self.high_price: Dict[str, List[float]] = {}
        self.low_price: Dict[str, List[float]] = {}
        self.volume: Dict[str, List[float]] = {}
        self.ask_price = {}
        self.bid_price = {}
        self.open_orders = {}
        self.timestamp_id = {}
        self.ocaGroup  = {}  # To store OCA group names for each symbol
        self.uptrend_dict = {}  # To store uptrend status for each symbol
        self.vstop_dict= {}  # To store vStop values for each symbol     


    async def create_parent_order(self,
    web_request: OrderRequest,
    contract: Contract,
    ib,
    submit_cmd: bool,
    meanReversion: bool,
    stopLoss: float,
    prices: Dict[str, float] = None):
        
        try:
            
            logger.info(f"Creating parent order for {web_request.ticker} with request: {web_request}")
            reqId= None
            qualified_contract= None
            vstop_dict= 0.0
            uptrend= 0.0
            vstop = 0.0
            nine_ema= 0.0
            #logger.info(f"Creating parent order with timestamp_id {timestamp_id}")
            #timestamp_id = self.timestamp_id[contract.symbol] if contract and contract.symbol in self.timestamp_id else timestamp_data
            
            order_details = {}
            trade = None
            symbol = contract.symbol if contract else web_request.ticker
            unixTimestamp= str(current_millis())

            self.ocaGroup[contract.symbol] = f"exit_{contract.symbol}_{unixTimestamp}"
            ocaGroup = self.ocaGroup[contract.symbol]

            symbol = web_request.ticker
            take_profit_price = None
            stopType = web_request.stopType
            kcAtrFactor = web_request.kcAtrFactor
            vstopAtrFactor = web_request.vstopAtrFactor
            atrFactor = vstopAtrFactor if stopType == "vStop" else kcAtrFactor
            orderAction = ""
            action = ""
            accountBalance = web_request.accountBalance
            riskPercentage = web_request.riskPercentage
            rewardRiskRatio = web_request.rewardRiskRatio
                
            # Validate contract
            if contract:
                logger.info(f"Valid contract found: {contract.symbol} ({contract.secType})")
                qualified_contract = contract
            if qualified_contract is None:
                logger.error(f"Qualified contract not found for {symbol}.")
                raise ValueError("Qualified contract is None")
                
            # Get timeframe
            timeframe = web_request.timeframe
            barSizeSetting = await convert_pine_timeframe_to_barsize(timeframe) if timeframe else "1 min"
            logger.info(f"Converted timeframe '{timeframe}' to barSizeSetting '{barSizeSetting}' for {symbol}")
                
            # Get market data
            reqId = await self.get_req_id(ib)
            close_price = float(self.close_price[symbol][-1]) if symbol in self.close_price and isinstance(self.close_price[symbol], list) and self.close_price[symbol] else 0.0
            vstop_value = self.vstop_dict[symbol][-1] if isinstance(self.vstop_dict[symbol], list) else self.vstop_dict[symbol]
            uptrend_value = self.uptrend_dict[symbol][-1] if isinstance(self.uptrend_dict[symbol], list) else self.uptrend_dict[symbol]
            vstop = float(vstop_value)
            uptrend = float(uptrend_value)
            if close_price > 1.0 or math.isnan(close_price):
                logger.info(f"Using close price for {symbol}: {close_price} and uptrend value: {uptrend} and vstop value: {vstop}")
            
            # Handle order action - use provided value if valid
            
            if web_request.orderAction:
                logger.info(f" Using provided order action: {web_request.orderAction}")
                orderAction = web_request.orderAction.upper()
                action = orderAction
            if orderAction not in ["BUY", "SELL"]:
                orderAction = "BUY" if uptrend else "SELL"
                action = orderAction
                logger.info(f"Determined order action: {orderAction} (uptrend: {'⬆️ UP' if uptrend else '⬇️ DOWN'})")
                
            
            # Handle entry price - ONLY use fallbacks if entryPrice is invalid or missing
            entry_price = 0.0
            if web_request.entryPrice > 0.0 and not math.isnan(web_request.entryPrice):
                # Use provided entry price if it's valid (greater than 0 and not NaN)
                entry_price = web_request.entryPrice
                logger.info(f"Using provided entry price: {entry_price}")
            else:
                # Fallback logic for entry price
                logger.info(f"Request entry price invalid ({web_request.entryPrice}), checking fallbacks...")
    
                # Check if ask/bid prices are valid (must be > 1.0 and not NaN)
                ask_valid = symbol in self.ask_price and self.ask_price[symbol] > 1.0 and not math.isnan(self.ask_price[symbol])
                bid_valid = symbol in self.bid_price and self.bid_price[symbol] > 1.0 and not math.isnan(self.bid_price[symbol])
    
                if (orderAction == "BUY" and ask_valid) or (orderAction == "SELL" and bid_valid):
                    entry_price = self.ask_price[symbol] if orderAction == "BUY" else self.bid_price[symbol]
                    logger.info(f"Using market price for {orderAction}: {entry_price}")
                else:
                    logger.warning(f"No valid ask/bid prices for {symbol} (ask: {self.ask_price.get(symbol, 'N/A')}, bid: {self.bid_price.get(symbol, 'N/A')})")
        
                # If market prices invalid, try close price
                if entry_price <= 1.0 or math.isnan(entry_price):
                    close_valid = symbol in self.close_price and len(self.close_price[symbol]) > 0 and self.close_price[symbol][-1] > 0.0 and not math.isnan(self.close_price[symbol][-1])
        
                    if close_valid:
                        entry_price = float(self.close_price[symbol][-1])
                        logger.info(f"Using close price for entry: {entry_price}")
                    else:
                        logger.warning(f"No valid close price for {symbol}")
    
                # Final validation check
                if entry_price <= 1.0 or math.isnan(entry_price):
                    logger.error(f"Failed to get valid entry price for {symbol} - all fallbacks exhausted")
                    raise ValueError(f"Cannot determine valid entry price for {symbol} - all values invalid or missing")
                
            # Handle stop loss - ONLY use fallbacks if stopLoss is 0.0
            stop_loss_price = 0.0
            stopType = web_request.stopType if web_request.stopType else "vStop"
                
            if web_request.stopLoss > 0.0:
                # Use provided stop loss if it's greater than 0
                stop_loss_price = web_request.stopLoss
                logger.info(f"Using provided stop loss: {stop_loss_price}")
            else:
                # Fallback logic for stop loss
                if vstop is not None and stopType == "vStop":
                    vstop_value = self.vstop_dict[symbol][-1] if isinstance(self.vstop_dict[symbol], list) else self.vstop_dict[symbol]
                    stop_loss_price = vstop_value - 0.02 if action == "BUY" else vstop_value + 0.02

                    
                    logger.info(f"Using vStop-based stop loss: {stop_loss_price}")
                
            # Initialize parameters for position calculation
            quantity = 0.0
            take_profit_price = None
                
            # Set risk parameters - directly use provided values
            accountBalance = web_request.accountBalance
            riskPercentage = web_request.riskPercentage
            rewardRiskRatio = web_request.rewardRiskRatio
            atrFactor = web_request.vstopAtrFactor if stopType == "vStop" else web_request.kcAtrFactor
                
            # Calculate position size ONLY if quantity is 0.0
            if web_request.quantity <= 0.0:
                logger.info(f"Calculating position size for {symbol}...")
                try:
                    quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = await compute_position_size(
                        entry_price,
                        stop_loss_price,
                        accountBalance,
                        riskPercentage,
                        rewardRiskRatio,
                        orderAction
                    )
                    logger.info(f"Calculated position: quantity={quantity}, take_profit_price={take_profit_price}")
                except Exception as e:
                    logger.error(f"Error calculating position for {symbol}: {e}")
                    raise e
            else:
                # Use provided quantity directly
                quantity = web_request.quantity
                logger.info(f"Using provided quantity: {quantity}")
                    
                # Since quantity was provided but we still need take_profit, calculate it
                perShareRisk = abs(entry_price - stop_loss_price)
                if orderAction == "BUY":
                    take_profit_price = round(entry_price + (perShareRisk * rewardRiskRatio), 2)
                else:
                    take_profit_price = round(entry_price - (perShareRisk * rewardRiskRatio), 2)
                
            # Final validation
            if take_profit_price is None:
                logger.error(f"Take profit price is None for {symbol}")
                raise ValueError("Take profit price is None")
                
            if stop_loss_price <= 0.0:
                logger.error(f"Stop loss price is None or invalid for {symbol}")
                raise ValueError("Stop loss price is invalid")
                
            if quantity <= 0.0:
                logger.error(f"Quantity is None or invalid for {symbol}")
                raise ValueError("Quantity is invalid")

            

            
            logger.info(
                f"Calculated position for {contract.symbol}: boof/jengo entry={entry_price}, stop_loss_order={stop_loss_price}, take_profit_order={take_profit_price}, quantity={quantity}"
            )
        
        
            
            #logger.info(f" Timestamp data for {contract.symbol}: {self.timestamp_id[symbol]}")
            if action not in ["BUY", "SELL"]:
                raise ValueError("Order Action is None")
            
            #timestamp_data = self.timestamp_id[symbol]
            self.timestamp_id[contract.symbol] = {
                "unixTimestamp": str(current_millis()),
                "stopType": stopType,
                "action": action,
                "riskPercentage": riskPercentage,
                "atrFactor": atrFactor,
                "rewardRiskRatio": rewardRiskRatio,
                "accountBalance": accountBalance,
                "prices": prices,
                "barSizeSetting": web_request.timeframe,  
            }
        
            logger.info(
                f"Qualified contract for: {contract.symbol} {contract.secType} {contract.exchange} {contract.currency} conID: {contract.conId}"
            )
            algo_order = LimitOrder(
            action=action,
            totalQuantity=quantity,
            lmtPrice=round(entry_price, 2),
            tif="GTC",
            transmit=True,
            algoStrategy="Adaptive",
            outsideRth=False,
            orderRef=f"Adaptive - {contract.symbol}",
            algoParams=[TagValue("adaptivePriority", "Urgent")],
        
            )
        

            # Step 3: Place the bracket order
            limit_order = LimitOrder(
                    action,
                    totalQuantity=quantity,
                    lmtPrice=round(entry_price, 2),
                    tif="GTC",
                    outsideRth=True,
                    transmit=True,
                    orderId=reqId,
            
                )
            logger.info(
                    f"Afterhours limit order for {contract.symbol} at price jengo {entry_price}"
                )
                
        except Exception as e:

            logger.error(f"Error extracting position details for {contract.symbol}: {e}")
            raise e    
        ################################################################################################ 
        try:
            market_hours = is_market_hours()
            if market_hours:
                logger.info(f"Market Hours Algo Limit order for {contract.symbol} at price jengo {entry_price}")
                order = algo_order
            else:
                logger.info(f"Afterhours limit order for {contract.symbol} at price jengo {entry_price}: {limit_order}")
                order = limit_order


            
            logger.info(
                f"Placing order with orderType {order.orderType} action: {order.action} order: {order.totalQuantity} @ {entry_price}"
            )

            
            if submit_cmd:
                p_ord_id = order.orderId
                
                logger.info(f"jengo bracket order webhook p_ord_id: {p_ord_id}")

                # Determine the type of parent order based on market hours

                logger.info(
                    f"Market hours - creating MarketOrder for{contract.symbol}"
                )
                parent = order
                logger.info(f"Parent Order - creating MarketOrder for{parent}")

                # Place the orders

                trade = self.ib.placeOrder(qualified_contract, parent)
                logger.info(f"Placing Parent Order: {parent} for {contract.symbol}")
                if trade:
                    self.ib.cancelOrderEvent += self.cancel_order

                    order_details = {
                        "ticker": contract.symbol,
                        "orderAction":action,
                        "orderType":trade.order.orderType,
                        "entry_price": entry_price,
                        "stop_loss_price": stop_loss_price,
                        "take_profit_price": take_profit_price,
                        "quantity": quantity,
                        "rewardRiskRatio": web_request.rewardRiskRatio,
                        "riskPercentage": web_request.riskPercentage,
                        "accountBalance": web_request.accountBalance,
                        "stopType": stopType,
                        "atrFactor": atrFactor,
                    }
                
                    logger.info(
                        f"Parent Order placed for {contract.symbol}: {parent} order_details.ticker = {order_details['ticker']} order_details.entry_price = {order_details['entry_price']}"
                    )
                    
                    fill = {
                        "execution": {
                            "avgPrice": entry_price
                        }
                    }
                    logger.info(
                        f"jengo bracket order webhook fill entry_price: {entry_price} "
                    )
                    logger.info(f"Parent Order fill details: {fill} for {contract.symbol}")
                    #trade.fillEvent += self.parent_filled
                    trade.fillEvent += lambda trade, fill: asyncio.create_task(
                        self.parent_filled(trade, fill, order_details)
                    )
                    await self.parent_filled(trade, fill, order_details)
                    
                    logger.info(format_order_details_table(order_details))
                    await asyncio.sleep(0.1)  # Give some time for the order to be processed
                    open_orders_list= await self.ib.reqAllOpenOrdersAsync()
                
                
                
                    for open_order in open_orders_list:
                
                    
                        symbol_open_order = open_order.contract.symbol
                        self.open_orders[symbol_open_order] = {
                            "symbol": symbol_open_order,
                            "order_id": open_order.order.orderId,
                            "status": open_order.orderStatus.status,
                            "timestamp": await get_timestamp()
                        }
                        #self.ib.cancelOrder(open_order.order)
                        #self.ib.cancelOrderEvent += cancel_order # Cancel the order if needed
                        logger.info("Open Orders List:")
                        logger.info(format_order_details_table(order_details=self.open_orders[symbol_open_order]))

                else:
                    logger.error(
                        f"Order placement failed for {contract.symbol}: {trade.order}"
                    )

                trade_check = await self.ib.whatIfOrderAsync(qualified_contract, order)
                logger.info(
                    f"Order details only for {contract.symbol} with action {action} and stop type {stopType } trade_check= {trade_check}"
                )
                
                
                    

                
                if not submit_cmd:
                    
                    order_details = {
                        "ticker": contract.symbol,
                        "orderAction": orderAction,
                        
                        "entry_price": entry_price,
                        "stop_loss_price": stop_loss_price,
                        "take_profit_price": take_profit_price,
                        "quantity": quantity,
                        "rewardRiskRatio": web_request.rewardRiskRatio,
                        "riskPercentage": web_request.riskPercentage,
                        "accountBalance": web_request.accountBalance,
                        "stopType": stopType,
                        "atrFactor": atrFactor,
                    }
                    logger.info(format_order_details_table(order_details))
                    logger.info(
                        f"jengo bracket order webhook entry_price: {entry_price}"
                    )

                    return {"status": "order details only", "order_details": order_details}
        except Exception as e:
            logger.error(f"Error creating order: {e}")

    async def get_req_id(self, ib):
        try:
            return self.ib.client.getReqId()
        except Exception as e:
            logger.error(f"Error getting request ID: {e}")
            return None
    async def parent_filled(self, trade: Trade, fill,  order_details=None):
        try:
            accountBalance = order_details["accountBalance"] if order_details and "accountBalance" in order_details else None
            riskPercentage = order_details["riskPercentage"] if order_details and "riskPercentage" in order_details else None
            rewardRiskRatio = order_details["rewardRiskRatio"] if order_details and "rewardRiskRatio" in order_details else None
            
            fill_contract = None
            fill_symbol = None

            # Extract permId early for deduplication check
            permId = trade.order.permId
                

            # Initialize tracking dictionary if not exists
            if not hasattr(self, "triggered_bracket_orders"):
                self.triggered_bracket_orders = {}

            # Initialize lock if not exists
            if not hasattr(self, "fill_lock"):
                self.fill_lock = asyncio.Lock()

            # Use a lock to prevent race conditions with multiple fill events
            async with self.fill_lock:
                # Check if we've already processed this permId
                if permId in self.triggered_bracket_orders:
                    logger.info(
                        f"Bracket order already triggered for permId {permId}, skipping duplicate call."
                    )
                    return

                # Mark this permId as being processed BEFORE any async operations
                # This prevents other concurrent calls from proceeding
                self.triggered_bracket_orders[permId] = True

                logger.info(
                    f"parent_filled Fill received for {trade.contract.symbol} with entry price. Sending Bracket for trade.order {trade.order}"
                )
                logger.info(
                    f"parent_filled Fill received for {trade.contract.symbol} with entry price. Sending Bracket for trade.order {trade.order}"
                )

                if trade.contract is not None:
                    logger.info(
                        f"jengo bracket order webhook trade.contract: {trade.contract.symbol}"
                    )
                    fill_contract = trade.contract
                    fill_symbol = trade.contract.symbol
                if trade.contract is None:
                    logger.info(
                        f"jengo bracket order webhook trade.contract: {trade.contract.symbol}"
                    )
                    fill_contract = trade.contract
                    fill_symbol = trade.contract.symbol

                    logger.info(
                        f"jengo bracket order webhook trade.contract is None, using trade.contract: {fill_contract.symbol}"
                    )
                if fill["execution"]["avgPrice"] > 0:
                    entry_price = float(fill["execution"]["avgPrice"]) > 0
                    logger.info(
                    f"jengo bracket order webhook entry_price: {entry_price} for order {trade.order} and permId {permId}"
                )
                elif self.close_price[fill_symbol][-1] > 0:
                    entry_price = float(self.close_price[fill_symbol][-1])
                    logger.info(
                    f"jengo bracket order webhook entry_price: {entry_price} for order {trade.order} and permId {permId}"
                )
                elif order_details["entry_price"] > 0:
                    
                    entry_price = float(order_details["entry_price"])
                    logger.info(
                        f"jengo bracket order webhook entry_price: {entry_price} for order {trade.order} and permId {permId} from order_details"
                    )

                action = trade.order.action
                order = trade.order
                algoStrategy = trade.order.algoStrategy
                stopType = self.timestamp_id.get(fill_symbol, {}).get("stopType")
                stopLoss = self.timestamp_id.get(fill_symbol, {}).get(
                    "stopLoss", 0.0
                )
                logger.info(
                    f"jengo bracket order webhook entry_price: {entry_price} for order {trade.order} and permId {permId}"
                )

                quantity = float(trade.order.totalQuantity)

                try:
                    logger.info(
                        f"jengo Get latest positions from TWS (only positions with nonzero quantity): stopLoss {self.timestamp_id.get(fill_symbol, {}).get('stopLoss', 0.0)} for permId {permId}"
                    )
                    if is_weekend:
                        logger.info("It's weekend, skipping bracket order placement logic for testing")
                        # For Adaptive algoStrategy, trigger the bracket order
                        if algoStrategy == "Adaptive" or not is_market_hours():
                            logger.info(
                                f"Placing oca bracket order for {fill_symbol} with permId {permId} and stopType {stopType} and stopLoss {stopLoss}"
                            )
                            oca_order = await self.add_oca_bracket(
                                fill_contract,
                                order,
                                action,
                                entry_price,
                                quantity,
                                stopType,
                                stopLoss,
                                order_details
                            )
                                        
                            return oca_order
                        
                        
                    latest_positions_oca, open_orders_list_id = await asyncio.gather(self.ib.reqPositionsAsync(), self.ib.reqAllOpenOrdersAsync())
                    if algoStrategy == "Adaptive" or not is_market_hours():
                        logger.info(
                            f"Placing oca bracket order for {fill_symbol} with permId {permId} and stopType {stopType} and stopLoss {stopLoss}"
                        )
                        oca_order = await self.add_oca_bracket(
                            fill_contract,
                            order,
                            action,
                            entry_price,
                            quantity,
                            stopType,
                            stopLoss,
                            order_details
                        )
                        return oca_order
                    else:
                        logger.info(
                            f"Skipping {fill_symbol}: algoStrategy {algoStrategy} does not require oca bracket order and stopType {stopType} and stopLoss {stopLoss}"
                        )
                    
                    logger.info(
                        f"jengo Get latest positions from TWS (only positions with nonzero quantity): open_orders_list_id {len(open_orders_list_id)}"
                    )
                    self.orders_dict_oca_symbol = {
                        o.order.permId: o for o in open_orders_list_id
                    }
                    logger.info(f" jengo open_orders_list_id: {self.orders_dict_oca_symbol.keys()}")
                        

                    
                    logger.info(
                        f" jengo parent_filled stopType {stopType} and stopLoss {stopLoss}"
                    )
                    try:
                        logger.info(f" Checking for active positions for {fill_symbol} with stopType {stopType} and stopLoss {stopLoss}")

                        # Now loop through the positions
                        for position in latest_positions_oca:
                            logger.info(
                                f"Position for: {position.contract.symbol} at shares of {position.position}"
                            )
                            if (
                                position.contract.symbol == fill_symbol
                                and position.position != 0
                            ):
                                logger.info(f" Found active position for {fill_symbol}: {position.position} shares")
                                # For Adaptive algoStrategy, trigger the bracket order
                                if algoStrategy == "Adaptive" or not is_market_hours():
                                    logger.info(
                                        f"Placing oca bracket order for {fill_symbol} with permId {permId} and stopType {stopType} and stopLoss {stopLoss}"
                                    )
                                    oca_order = await self.add_oca_bracket(
                                        fill_contract,
                                        order,
                                        action,
                                        entry_price,
                                        quantity,
                                        stopType,
                                        stopLoss,
                                    )
                                        
                                    return oca_order
                                else:
                                    logger.info(
                                        f"Skipping {fill_symbol}: algoStrategy {algoStrategy} does not require oca bracket order and stopType {stopType} and stopLoss {stopLoss}"
                                    )
                    except Exception as e:
                        logger.error(
                            f"Error while checking positions for {fill_symbol}: {e}",
                            exc_info=True,
                        )
                            

                    # If we got here, no matching position was found
                    logger.warning(
                        f"No active position found for {fill_symbol} when placing bracket orders"
                    )

                except Exception as e:
                    logger.error(
                        f"Error in conditions for oca bracket_orders in fill event: {e}"
                    )
                    # If there's an error, we should remove the permId from the triggered dictionary
                    # so that it can be retried
                    if permId in self.triggered_bracket_orders:
                        del self.triggered_bracket_orders[permId]

        except Exception as e:
            logger.error(f"Error updating positions on fill: {e}")
            # Global exception handling should also clean up the triggered dictionary
            if (
                "permId" in locals()
                and hasattr(self, "triggered_bracket_orders")
                and permId in self.triggered_bracket_orders
            ):
                del self.triggered_bracket_orders[permId]    
        except Exception as e:
            logger.error(f"Error in parent_filled: {e}")
            return None
    async def add_oca_bracket(
        self,
        contract: Contract,
        order: Trade,
        action: str,
        entry_price: float,
        quantity: float,
        stopType: str,
        stopLoss: float,
        order_details = None 
    ):
        try:
            stop_loss_price = 0.0
            new_entry_price = 0.0
            take_profit_price=0.0
            stop_loss_price=order_details["stop_loss_price"] if order_details and "stop_loss_price" in order_details else 0.0
            take_profit_price=order_details["take_profit_price"] if order_details and "take_profit_price" in order_details else 0.0
            accountBalance = order_details["accountBalance"] if order_details and "accountBalance" in order_details else None
            riskPercentage = order_details["riskPercentage"] if order_details and "riskPercentage" in order_details else None
            rewardRiskRatio = order_details["rewardRiskRatio"] if order_details and "rewardRiskRatio" in order_details else None
            
            
            
            
            # Get symbol and timestamp ID
            symbol = contract.symbol
            logger.info(
                f"Adding OCA bracket for {contract.symbol} with action {action} and stopLoss {stopLoss} : jengo entry_price: {entry_price} and stopType {stopType}"  
            )

            # Get the timestamp ID directly from the dictionary with the symbol as key
            if symbol not in self.timestamp_id:
                logger.error(f"No timestamp ID found for {contract.symbol}")
                return None

            # Get timestamp data for this symbol
            #timestamp_data = self.timestamp_id[symbol]
            vstop_value = self.vstop_dict[symbol][-1] if isinstance(self.vstop_dict[symbol], list) else self.vstop_dict[symbol]
            stop_loss_price = vstop_value - 0.02 if action == "BUY" else vstop_value + 0.02

            priceType = None
            perShareRisk = abs(entry_price - stop_loss_price)
            quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk = await compute_position_size(
                entry_price,
                stop_loss_price,
                accountBalance,
                riskPercentage,
                rewardRiskRatio,
                action
            )
            if action == "BUY":
                take_profit_price = round(entry_price + (perShareRisk * rewardRiskRatio), 2)
            else:
                take_profit_price = round(entry_price - (perShareRisk * rewardRiskRatio), 2)
            
            new_entry_price= self.close_price.get(symbol)[-1]
            bracket_orders = None
            if stopLoss > 1.0:
                stop_loss_price = stopLoss
            
            
            # Get current price from new_entry_price if available
            current_price = 0.0
            
            if entry_price > 1.0:
                current_price = entry_price
                new_entry_price= self.close_price.get(symbol)[-1]  # Get the last close price if available
                priceType = "arg entry_price"
                logger.info(f"Using provided entry price for {contract.symbol}: {current_price}")
            elif current_price <= 1.0 and new_entry_price > 1.0:
                entry_price = new_entry_price
                if new_entry_price ==0.0:
                    logger.warning(f"Entry prices not available for {contract.symbol}")
                    entry_price =  await self.last_price_five_sec(contract)
                    current_price = entry_price
                    
                else:
                    priceType = "ib.postions"
                    latest_positions_oca = await self.ib.reqPositionsAsync()
                    for pos in latest_positions_oca:
                        if pos.contract.symbol == symbol:
                            new_entry_price = {
                                "entry_long": pos.avgCost if action == "BUY" else None,
                                "entry_short": pos.avgCost if action == "SELL" else None,
                                "timestamp": datetime.now().isoformat(),
                            }
                            current_price = new_entry_price["entry_long"] if action == "BUY" else new_entry_price["entry_short"]
                            break
                        if not pos:
                            logger.error(f"Could not calculate position for {contract.symbol}")
                            return None
                 
            logger.info(f"Timestamp ID for {contract.symbol}: {self.timestamp_id[symbol]}")

            if current_price is None:
                logger.error(f"Current price is None for {contract.symbol} after checking entry prices")
                return None
               

            if current_price is not None:
               

                logger.info(
                    f"Entry price for {contract.symbol}: {entry_price} current price: {current_price} with stopType {stopType}"
                )
                barSizeSetting= self.timestamp_id[symbol]["barSizeSetting"]
                rewardRiskRatio=self.timestamp_id[symbol]["rewardRiskRatio"]
                riskPercentage=self.timestamp_id[symbol]["riskPercentage"]
                accountBalance=self.timestamp_id[symbol]["accountBalance"]
                atrFactor=self.timestamp_id[symbol]["atrFactor"]

                # Calculate position details
             
                quantity, take_profit_price, brokerComish, perShareRisk, toleratedRisk= await compute_position_size(
                    entry_price,
                    stop_loss_price,
                    self.timestamp_id[symbol]["accountBalance"],
                    self.timestamp_id[symbol]["riskPercentage"],
                    self.timestamp_id[symbol]["rewardRiskRatio"],
                    action,
                )
                

                

                

                logger.info(f"jengo here Calculated position for {contract.symbol}: quantity={quantity}, take_profit_price={take_profit_price}, stop_loss_price={stop_loss_price}, brokerComish={brokerComish}, perShareRisk={perShareRisk}, toleratedRisk={toleratedRisk}")
               

                # Place the bracket order
                p_ord_id = order.orderId
                reverseAction = "BUY" if order.action == "SELL" else "SELL"

                # Create the take profit (limit) and stop loss orders
                take_profit_order = LimitOrder(
                    reverseAction, order.totalQuantity, take_profit_price
                )
                stop_loss_order = StopOrder(reverseAction, order.totalQuantity, stop_loss_price)

                if take_profit_price is None:
                    logger.error(f"Take profit price is None for {contract.symbol}")
                    raise ValueError("Take profit price is None")
                if stop_loss_price is None:
                    logger.error(f"Stop loss price is None for {contract.symbol}")
                    raise ValueError("Stop loss price is None")

                logger.info(
                    f"Creating bracket orders for {contract.symbol}: entry={entry_price}, stop_loss_order={stop_loss_price}, take_profit_order={take_profit_price}, quantity={order.totalQuantity}"
                )

                orders = [take_profit_order, stop_loss_order]
                # Group them in an OCA group using oneCancelsAll
                ocaGroup = self.ocaGroup[symbol]
                

                # Create the OCA bracket orders
                try:
                    if take_profit_price is None:
                        logger.error(f"Take profit price is None for {symbol}")
                        raise ValueError("Take profit price is None")
                
                    if stop_loss_price <= 0.0:
                        logger.error(f"Stop loss price is None or invalid for {symbol}")
                        raise ValueError("Stop loss price is invalid")
                
                    if quantity <= 0.0:
                        logger.error(f"Quantity is None or invalid for {symbol}")
                        raise ValueError("Quantity is invalid")
                    bracket_orders = self.ib.oneCancelsAll(orders, ocaGroup, 1)
                    logger.info(
                        f"Created OCA bracket orders for {contract.symbol} with group {ocaGroup}"
                    )
                except Exception as e:
                    logger.error(f"Failed to create OCA bracket orders: {e}")
                    return None

            # Place each order
            if bracket_orders is not None:
                for bracket_order in bracket_orders:
                    logger.info(
                        f"Placing order: {contract.symbol} {bracket_order.action} {bracket_order.totalQuantity} @ {bracket_order.lmtPrice if hasattr(bracket_order, 'lmtPrice') else bracket_order.auxPrice}"
                    )
                   
                    
                    trade = self.ib.placeOrder(contract, bracket_order)
                    if trade:
                        order_details = {
                            "ticker": symbol,
                            "orderAction":action,
                            "orderType":trade.order.orderType,
                            "entry_price": entry_price,
                            "stop_loss_price": stop_loss_price,
                            "take_profit_price": take_profit_price,
                            "quantity": quantity,
                            "rewardRiskRatio":  rewardRiskRatio,
                            "riskPercentage":  riskPercentage,
                            "accountBalance":  accountBalance,
                            "atrFactor": atrFactor,
                            "stopType": stopType,

                        }
                        
                        logger.info(format_order_details_table(order_details))
                        
                        # Remove the timestamp ID data after successful order placement
                        if symbol in self.timestamp_id:
                            del self.timestamp_id[symbol]

                        logger.info(f"Order placed for {contract.symbol}: {bracket_order} priceType = {priceType}")
                        trade.fillEvent += self.parent_filled
                    else:
                        logger.error(
                            f"Order placement failed for {contract.symbol}: {bracket_order}"
                        )

                return bracket_orders
            else:
                logger.error(f"No bracket orders created for {contract.symbol}")
                return None

        except Exception as e:
            logger.error(f"Error in add oca bracket: {e}")
            return None 
    async def cancel_order(self, trade: Trade):
        try:
            permId = trade.order.permId
            logger.info(f"Cancelling order: {trade}")

            if permId in self.triggered_bracket_orders:
                del self.triggered_bracket_orders[permId]
                logger.info(f"Removed permId {permId} from triggered_bracket_orders")

            #logger.info(f"Cancelling order: {trade.contract.symbol} with permId{permId}. All self.triggered_bracket_orders[permId] {self.triggered_bracket_orders[permId]}")
        except Exception as e:

            logger.error(f"Error cancelling order : {e}")
            return False