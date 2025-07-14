from log_config import log_config, logger
log_config.setup()
async def trade_to_dict(trade) -> dict:
    try:
        logger.info(f"Converting trade {trade} to dict")
        return {
            "contract": {
                "secType": trade.contract.secType,
                "conId": trade.contract.conId,
                "symbol": trade.contract.symbol,
                "exchange": trade.contract.exchange,
                "primaryExchange": trade.contract.primaryExchange,
                "currency": trade.contract.currency,
                "localSymbol": trade.contract.localSymbol,
                "tradingClass": trade.contract.tradingClass,
            },
            "order": {
                "orderId": trade.order.orderId,
                "clientId": trade.order.clientId,
                "permId": trade.order.permId,
                "action": trade.order.action,
                "totalQuantity": trade.order.totalQuantity,
                "lmtPrice": trade.order.lmtPrice,
                "auxPrice": trade.order.auxPrice,
                "tif": trade.order.tif,
                "outsideRth": trade.order.outsideRth,
            },
            "orderStatus": {
                "status": trade.orderStatus.status,
                "filled": trade.orderStatus.filled,
                "remaining": trade.orderStatus.remaining,
                "avgFillPrice": trade.orderStatus.avgFillPrice,
                "permId": trade.orderStatus.permId,
                "parentId": trade.orderStatus.parentId,
                "lastFillPrice": trade.orderStatus.lastFillPrice,
                "clientId": trade.orderStatus.clientId,
                "whyHeld": trade.orderStatus.whyHeld,
                "mktCapPrice": trade.orderStatus.mktCapPrice,
            },
            "fills": [str(fill) for fill in trade.fills],
            "log": [
                {
                    "time": log_entry.time.isoformat() if log_entry.time else None,
                    "status": log_entry.status,
                    "message": log_entry.message,
                    "errorCode": log_entry.errorCode,
                }
                for log_entry in trade.log
            ],
            "advancedError": trade.advancedError,
        }
    except Exception as e:
        logger.error(f"Error converting trade to dict: {e}")
        return {}
