"""
Breakout Trend Follower Strategy (Python port of Pine Breakout_Trend_follower.txt)

Identifies swing breakouts and uses MA filter for trend confirmation.
- BUY: price breaks above swing high AND close > MA
- SELL: price breaks below swing low (trailing stop)
"""

import pandas as pd

# Configuration
BTF_PVT_LEN = 3         # pivot look-back / look-forward periods
BTF_MA_LEN = 50         # MA period for trend filter
BTF_MA_TYPE = "SMA"     # "SMA" or "EMA"


def pivot_high(highs, pvt_len: int = BTF_PVT_LEN):
    """Return most recent confirmed swing high value."""
    if len(highs) < 2 * pvt_len + 1:
        return None
    for i in range(len(highs) - pvt_len - 1, pvt_len - 1, -1):
        if all(highs[i] >= highs[i - j] for j in range(1, pvt_len + 1)) and \
           all(highs[i] >= highs[i + j] for j in range(1, pvt_len + 1)):
            return highs[i]
    return None


def pivot_low(lows, pvt_len: int = BTF_PVT_LEN):
    """Return most recent confirmed swing low value."""
    if len(lows) < 2 * pvt_len + 1:
        return None
    for i in range(len(lows) - pvt_len - 1, pvt_len - 1, -1):
        if all(lows[i] <= lows[i - j] for j in range(1, pvt_len + 1)) and \
           all(lows[i] <= lows[i + j] for j in range(1, pvt_len + 1)):
            return lows[i]
    return None


def btf_signal(df: pd.DataFrame) -> tuple:
    """
    Breakout Trend Follower signal.
    Returns (signal, buy_level, stop_level) where signal is 'BUY'/'SELL'/None.
    Logic:
      - BUY  when high > swing_high AND close > MA filter
      - SELL when low  < swing_low  (trailing stop)
    """
    if len(df) < BTF_MA_LEN + 2 * BTF_PVT_LEN + 2:
        return None, None, None

    closes = df["close"].tolist()
    highs = df["high"].tolist()
    lows = df["low"].tolist()

    # MA filter
    if BTF_MA_TYPE == "EMA":
        ma_val = df["close"].ewm(span=BTF_MA_LEN, adjust=False).mean().iloc[-1]
    else:
        ma_val = df["close"].rolling(BTF_MA_LEN).mean().iloc[-1]

    buy_level = pivot_high(highs)
    stop_level = pivot_low(lows)

    if buy_level is None or stop_level is None:
        return None, buy_level, stop_level

    last_high = highs[-1]
    last_low = lows[-1]
    last_close = closes[-1]

    if last_high > buy_level and last_close > ma_val:
        return "BUY", buy_level, stop_level
    if last_low < stop_level:
        return "SELL", buy_level, stop_level
    return None, buy_level, stop_level


def breakout_trend_signal(df: pd.DataFrame, index: str, ltp: float):
    """
    Evaluate Breakout Trend Follower signal.
    Returns (signal, buy_level, stop_level).
    """
    btf_sig, buy_lvl, stop_lvl = btf_signal(df)

    if btf_sig == "BUY":
        print(f"[BTF] {index}: Breakout above {buy_lvl:.1f}  → BUY vote")
    elif btf_sig == "SELL":
        print(f"[BTF] {index}: Breakdown below {stop_lvl:.1f}  → SELL vote")

    return btf_sig, buy_lvl, stop_lvl
