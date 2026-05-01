"""
Order Block Detection Strategy (Python port of Pine OrderBlock.txt)

Detects institutional support and resistance zones.
- Bullish OB: last red candle before consecutive green candles
- Bearish OB: last green candle before consecutive red candles
"""

import pandas as pd

# Configuration
OB_PERIODS = 5          # periods for Order Block detection
OB_THRESHOLD = 0.0      # % minimum move to validate OB


def detect_order_blocks(df: pd.DataFrame, periods: int = OB_PERIODS,
                         threshold: float = OB_THRESHOLD):
    """
    Returns (bull_ob, bear_ob) — latest identified Order Blocks as dicts,
    or None if not found.
    Logic:
      Bullish OB : last red candle before `periods` consecutive green candles
                   with abs % move >= threshold.
      Bearish OB : last green candle before `periods` consecutive red candles
                   with abs % move >= threshold.
    """
    if len(df) < periods + 2:
        return None, None

    ob_idx = -(periods + 1)           # candle at ob_period position
    ob_candle = df.iloc[ob_idx]

    # % move from OB close to last candle close
    abs_move = abs((df.iloc[-1]["close"] - ob_candle["close"]) /
                   ob_candle["close"]) * 100
    rel_move = abs_move >= threshold

    tail = df.iloc[-(periods):]       # last `periods` candles

    # Bullish OB: OB candle is red, subsequent all green
    bull_ob = None
    if ob_candle["close"] < ob_candle["open"] and rel_move:
        up_candles = (tail["close"] > tail["open"]).sum()
        if up_candles == periods:
            bull_ob = {
                "high" : ob_candle["open"],      # open is upper for bullish OB
                "low"  : ob_candle["low"],
                "avg"  : (ob_candle["open"] + ob_candle["low"]) / 2,
            }

    # Bearish OB: OB candle is green, subsequent all red
    bear_ob = None
    if ob_candle["close"] > ob_candle["open"] and rel_move:
        down_candles = (tail["close"] < tail["open"]).sum()
        if down_candles == periods:
            bear_ob = {
                "high": ob_candle["high"],
                "low" : ob_candle["open"],       # open is lower for bearish OB
                "avg" : (ob_candle["high"] + ob_candle["open"]) / 2,
            }

    return bull_ob, bear_ob


def order_block_signal(df: pd.DataFrame, index: str, ltp: float):
    """
    Evaluate Order Block signal.
    Returns (signal, bull_ob, bear_ob) where signal is 'BUY', 'SELL', or None.
    """
    bull_ob, bear_ob = detect_order_blocks(df)
    signal = None

    if bull_ob:
        # Price is near / inside bullish OB zone → bullish signal
        if bull_ob["low"] <= ltp <= bull_ob["high"] * 1.005:
            signal = "BUY"
            print(f"[OB] {index}: Bullish OB zone {bull_ob['low']:.1f}–{bull_ob['high']:.1f}  → BUY vote")

    if bear_ob:
        if bear_ob["low"] * 0.995 <= ltp <= bear_ob["high"]:
            signal = "SELL"
            print(f"[OB] {index}: Bearish OB zone {bear_ob['low']:.1f}–{bear_ob['high']:.1f}  → SELL vote")

    return signal, bull_ob, bear_ob
