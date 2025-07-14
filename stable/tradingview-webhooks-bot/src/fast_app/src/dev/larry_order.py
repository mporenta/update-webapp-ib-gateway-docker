from ib_async import *
import logging
import pandas as pd
logger = logging.getLogger(__name__)
def format_what_if_details_table(order_details):
    """
    Format order details as a nice pandas DataFrame table for terminal display.

    Args:
        order_details: Dictionary containing order details

    Returns:
        Formatted string representation of the table
    """
    # Convert dictionary to a DataFrame with 'Parameter' and 'Value' columns
    df = pd.DataFrame(
        {"Parameter": order_details.keys(), "Value": order_details.values()}
    )

    # Format numerical values
    for idx, param in enumerate(df["Parameter"]):
        value = df.loc[idx, "Value"]
        if isinstance(value, (int, float)):
            if "price" in param or "Balance" in param:
                df.loc[idx, "Value"] = f"${value:.2f}"
            elif "percentage" in param or "Ratio" in param:
                df.loc[idx, "Value"] = f"{value:.2f}"

    # Return formatted table string
    return f"\n{'='*50}\nORDER DETAILS\n{'='*50}\n{df.to_string(index=False)}\n{'='*50}"

ib = IB()
ib.connect('ib-gateway', 4002, clientId=1)

stock = Stock('NVDA', 'SMART', 'USD')
action = "BUY"
quantity = 10
limit_price =113.25
order = LimitOrder(
            action,
            totalQuantity=quantity,
            lmtPrice=round(limit_price, 2),
            tif="GTC",
            outsideRth=True,
            transmit=True,
            
            orderRef=f"Parent Limit",
        )
trade = ib.whatIfOrder(stock, order)
ib.sleep(3)

order_state_json = {
                "status":              trade.status,
                "initMarginBefore":    trade.initMarginBefore,
                "maintMarginBefore":   trade.maintMarginBefore,
                "equityWithLoanBefore":trade.equityWithLoanBefore,
                "initMarginChange":    trade.initMarginChange,
                "maintMarginChange":   trade.maintMarginChange,
                "equityWithLoanChange":trade.equityWithLoanChange,
                "initMarginAfter":     trade.initMarginAfter,
                "maintMarginAfter":    trade.maintMarginAfter,
                "equityWithLoanAfter": trade.equityWithLoanAfter,
                "commission":          round(trade.commission, 2),
                "minCommission":       round(trade.minCommission, 2),
                "maxCommission":       round(trade.maxCommission, 2),
                "commissionCurrency":  trade.commissionCurrency,
                "warningText":         trade.warningText,
                "completedTime":       trade.completedTime,
                "completedStatus":     trade.completedStatus,
            }
table=(format_what_if_details_table(order_state_json))

#print(order_state_json)

print(table)



ib.run()