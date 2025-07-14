from log_config import log_config, logger
log_config.setup()
import pandas_ta as ta
import pandas as pd
def supertrend(df, atr_multiplier=1.5):
    # Calculate the Upper Band(UB) and the Lower Band(LB)
    # Formular: Supertrend =(High+Low)/2 + (Multiplier)∗(ATR)
    logger.debug("atr_multiplier",atr_multiplier)
    current_average_high_low = (df['high']+df['low'])/2
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], period=20)
    df.dropna(inplace=True)
    df['basicUpperband'] = current_average_high_low + (atr_multiplier * df['atr'])
    df['basicLowerband'] = current_average_high_low - (atr_multiplier * df['atr'])
    first_upperBand_value = df['basicUpperband'].iloc[0]
    first_lowerBand_value = df['basicLowerband'].iloc[0]
    upperBand = [first_upperBand_value]
    lowerBand = [first_lowerBand_value]

    for i in range(1, len(df)):
        if df['basicUpperband'].iloc[i] < upperBand[i-1] or df['close'].iloc[i-1] > upperBand[i-1]:
            upperBand.append(df['basicUpperband'].iloc[i])
        else:
            upperBand.append(upperBand[i-1])

        if df['basicLowerband'].iloc[i] > lowerBand[i-1] or df['close'].iloc[i-1] < lowerBand[i-1]:
            lowerBand.append(df['basicLowerband'].iloc[i])
        else:
            lowerBand.append(lowerBand[i-1])

    df['upperband'] = upperBand
    df['lowerband'] = lowerBand
    df.drop(['basicUpperband', 'basicLowerband',], axis=1, inplace=True)
    return df




def supertrend_last(df: pd.DataFrame, atr_period: int = 20, atr_multiplier: float = 1.5):
    """
    Calculate the SuperTrend indicator and return the latest value plus a validity flag.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain 'high', 'low', 'close'.
    atr_period : int
        Period for the ATR calculation.
    atr_multiplier : float
        Multiplier for the ATR to set band widths.

    Returns
    -------
    last_st : float
        The most recent SuperTrend value.
    is_valid : bool
        True if last_st is not NaN.
    """
    df = df.copy()
    logger.debug("atr_period",atr_period)
    logger.debug("atr_multiplier",atr_multiplier)

    # 1) ATR
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=atr_period)

    # drop initial NaNs from ATR
    df.dropna(subset=['atr'], inplace=True)

    # 2) Basic upper/lower bands
    hl2 = (df['high'] + df['low']) / 2
    df['basic_ub'] = hl2 + atr_multiplier * df['atr']
    df['basic_lb'] = hl2 - atr_multiplier * df['atr']

    # 3) Finalized bands
    ub = [df['basic_ub'].iat[0]]
    lb = [df['basic_lb'].iat[0]]
    for i in range(1, len(df)):
        curr_ub = df['basic_ub'].iat[i]
        curr_lb = df['basic_lb'].iat[i]
        prev_ub = ub[i-1]
        prev_lb = lb[i-1]
        prev_close = df['close'].iat[i-1]

        # shrink upper band
        if (curr_ub < prev_ub) or (prev_close > prev_ub):
            ub.append(curr_ub)
        else:
            ub.append(prev_ub)

        # expand lower band
        if (curr_lb > prev_lb) or (prev_close < prev_lb):
            lb.append(curr_lb)
        else:
            lb.append(prev_lb)

    df['ub'] = ub
    df['lb'] = lb

    # 4) SuperTrend line and direction
    st = [df['ub'].iat[0]]      # start with upper band
    for i in range(1, len(df)):
        prev_st = st[i-1]
        curr_close = df['close'].iat[i]
        curr_ub = df['ub'].iat[i]
        curr_lb = df['lb'].iat[i]

        if prev_st == df['ub'].iat[i-1]:
            # previously in down‑trend
            if curr_close <= curr_ub:
                st.append(curr_ub)
            else:
                st.append(curr_lb)
        else:
            # previously in up‑trend
            if curr_close >= curr_lb:
                st.append(curr_lb)
            else:
                st.append(curr_ub)

    df['supertrend'] = st

    # 5) grab the last value
    last_st = df['supertrend'].iat[-1]
    is_valid = pd.notna(last_st)

    return last_st, is_valid
