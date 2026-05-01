"""
EMA/RSI Momentum Strategy

Combines Exponential Moving Average (EMA) crossover with Relative Strength Index (RSI).
- BUY: EMA-20 > EMA-50 AND RSI > 55
- SELL: EMA-20 < EMA-50 AND RSI < 45
"""

import pandas as pd
import pandas_ta as ta


def ema_rsi_signal(df: pd.DataFrame):
    """
    EMA/RSI momentum confirmation signal.
    Returns 'BUY', 'SELL', or None.
    """
    d = df.copy()
    d["ema20"] = ta.ema(d["close"], length=20)
    d["ema50"] = ta.ema(d["close"], length=50)
    d["rsi"] = ta.rsi(d["close"], length=14)
    d.dropna(inplace=True)
    if d.empty:
        return None
    last = d.iloc[-1]
    if last["ema20"] > last["ema50"] and last["rsi"] > 55:
        return "BUY"
    if last["ema20"] < last["ema50"] and last["rsi"] < 45:
        return "SELL"
    return None


def ema_rsi_confirmation(df: pd.DataFrame, index: str, ltp: float):
    """
    Evaluate EMA/RSI confirmation signal.
    Returns signal ('BUY', 'SELL', or None).
    """
    er_sig = ema_rsi_signal(df)

    if er_sig == "BUY":
        print(f"[EMA/RSI] {index}  → BUY vote")
    elif er_sig == "SELL":
        print(f"[EMA/RSI] {index}  → SELL vote")

    return er_sig
