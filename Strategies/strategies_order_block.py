"""
Order Block Detection Strategy (Python port of Pine OrderBlock.txt)

Detects institutional support and resistance zones.
- Bullish OB: last red candle before consecutive green candles
- Bearish OB: last green candle before consecutive red candles
"""

import pandas as pd

# Configuration
OB_PERIODS = 5  # periods for Order Block detection
OB_THRESHOLD = 0.0  # % minimum move to validate OB


# ── ENGULFING PATTERN DETECTORS ──────────────────────────────


def bullish_engulfing(df: pd.DataFrame) -> bool:
    """
    Detect Bullish Engulfing on the last two completed candles.

    Conditions:
      - Previous candle is bearish (close < open)
      - Current  candle is bullish (close > open)
      - Current open  <= previous close  (opens inside or below prev body)
      - Current close >= previous open   (closes above prev body top)
    """
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    return (
        prev["close"] < prev["open"]  # prev is red
        and curr["close"] > curr["open"]  # curr is green
        and curr["open"] <= prev["close"]  # opens inside/below prev body
        and curr["close"] >= prev["open"]  # closes above prev body top
    )


def bearish_engulfing(df: pd.DataFrame) -> bool:
    """
    Detect Bearish Engulfing on the last two completed candles.

    Conditions:
      - Previous candle is bullish (close > open)
      - Current  candle is bearish (close < open)
      - Current open  >= previous close  (opens inside or above prev body)
      - Current close <= previous open   (closes below prev body bottom)
    """
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    return (
        prev["close"] > prev["open"]  # prev is green
        and curr["close"] < curr["open"]  # curr is red
        and curr["open"] >= prev["close"]  # opens inside/above prev body
        and curr["close"] <= prev["open"]  # closes below prev body bottom
    )


def detect_order_blocks(
    df: pd.DataFrame, periods: int = OB_PERIODS, threshold: float = OB_THRESHOLD
):
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

    ob_idx = -(periods + 1)  # candle at ob_period position
    ob_candle = df.iloc[ob_idx]

    # % move from OB close to last candle close
    abs_move = (
        abs((df.iloc[-1]["close"] - ob_candle["close"]) / ob_candle["close"]) * 100
    )
    rel_move = abs_move >= threshold

    tail = df.iloc[-(periods):]  # last `periods` candles

    # Bullish OB: OB candle is red, subsequent all green
    bull_ob = None
    if ob_candle["close"] < ob_candle["open"] and rel_move:
        up_candles = (tail["close"] > tail["open"]).sum()
        if up_candles == periods:
            bull_ob = {
                "high": ob_candle["open"],  # open is upper for bullish OB
                "low": ob_candle["low"],
                "avg": (ob_candle["open"] + ob_candle["low"]) / 2,
            }

    # Bearish OB: OB candle is green, subsequent all red
    bear_ob = None
    if ob_candle["close"] > ob_candle["open"] and rel_move:
        down_candles = (tail["close"] < tail["open"]).sum()
        if down_candles == periods:
            bear_ob = {
                "high": ob_candle["high"],
                "low": ob_candle["open"],  # open is lower for bearish OB
                "avg": (ob_candle["high"] + ob_candle["open"]) / 2,
            }

    return bull_ob, bear_ob


def order_block_signal(df: pd.DataFrame, index: str, ltp: float):
    """
    Evaluate Order Block signal.

    Returns (signal, bull_ob, bear_ob) where signal is:
      'BUY'  — price inside / touching bullish OB zone only
      'SELL' — price inside / touching bearish OB zone only
      'BOTH' — price simultaneously inside BOTH OB zones
               (range-bound; caller places parallel CE + PE)
      None   — price not near any OB zone (hard gate: no trade)

    FIX [Item v]: previously `signal` was set to 'BUY' then
    overwritten to 'SELL' when both zones were active, so the
    parallel 'BOTH' signal never fired. Now uses near_bull /
    near_bear flags before assigning final signal.
    """
    bull_ob, bear_ob = detect_order_blocks(df)

    # Check proximity for each OB zone independently
    near_bull = bull_ob is not None and bull_ob["low"] <= ltp <= bull_ob["high"] * 1.005
    near_bear = bear_ob is not None and bear_ob["low"] * 0.995 <= ltp <= bear_ob["high"]

    if near_bull:
        print(
            f"[OB] {index}: Bullish OB {bull_ob['low']:.1f}–{bull_ob['high']:.1f}"
            f" ltp={ltp:.1f} → BUY zone"
        )
    if near_bear:
        print(
            f"[OB] {index}: Bearish OB {bear_ob['low']:.1f}–{bear_ob['high']:.1f}"
            f" ltp={ltp:.1f} → SELL zone"
        )

    # Determine signal — BOTH fires when price is in both zones simultaneously
    if near_bull and near_bear:
        signal = "BOTH"
    elif near_bull:
        signal = "BUY"
    elif near_bear:
        signal = "SELL"
    else:
        signal = None

    return signal, bull_ob, bear_ob
