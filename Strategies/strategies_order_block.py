"""
==============================================================
  DhanBot / strategies_order_block.py
  [Item iii] BULLISH/BEARISH ENGULFING added
  [Item v]   OB PROXIMITY = HARD GATE (not just a vote)
             Parallel BUY+SELL detection added

  CHANGES:
    [Item iii] bullish_engulfing() and bearish_engulfing() detectors
               added. Both are evaluated on 5-min candles.
               order_block_signal() returns signal only when BOTH
               an OB zone AND an engulfing candle confirm direction.

    [Item v]   Price must be inside/touching an OB zone for any trade.
               If both bull_ob and bear_ob exist simultaneously and
               price is near both, returns ("BOTH", bull_ob, bear_ob)
               so the caller can decide to place parallel signals.
               order_block_signal() is now the sole entry gate —
               no trade fires unless price is near an OB.

  Order Block Detection:
    Bullish OB : last red candle before N consecutive green candles
    Bearish OB : last green candle before N consecutive red candles

  Engulfing (5-min candle confirmation):
    Bullish Engulfing : current green candle body fully wraps prev red body
    Bearish Engulfing : current red candle body fully wraps prev green body
==============================================================
"""

import pandas as pd

# ── CONFIGURATION ─────────────────────────────────────────────
OB_PERIODS    = 5     # consecutive candles needed to validate OB
OB_THRESHOLD  = 0.0   # % minimum move to validate OB (0 = any move)
OB_PROXIMITY  = 0.005 # [Item v] price must be within 0.5% of OB zone


# ── ENGULFING PATTERN DETECTORS ──────────────────────────────

def bullish_engulfing(df: pd.DataFrame) -> bool:
    """
    [Item iii] Detect Bullish Engulfing on the last two completed candles.

    Conditions:
      - Previous candle is bearish (close < open)
      - Current  candle is bullish (close > open)
      - Current open  <= previous close  (opens inside or below prev body)
      - Current close >= previous open   (closes above prev body top)

    Uses [-2] (previous) and [-1] (current) candles.
    Returns True if pattern is confirmed, else False.
    """
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]

    prev_bearish = prev["close"] < prev["open"]
    curr_bullish = curr["close"] > curr["open"]

    if not (prev_bearish and curr_bullish):
        return False

    # Current body must fully engulf previous body
    engulfs = (curr["open"] <= prev["close"] and
               curr["close"] >= prev["open"])
    return engulfs


def bearish_engulfing(df: pd.DataFrame) -> bool:
    """
    [Item iii] Detect Bearish Engulfing on the last two completed candles.

    Conditions:
      - Previous candle is bullish (close > open)
      - Current  candle is bearish (close < open)
      - Current open  >= previous close  (opens inside or above prev body)
      - Current close <= previous open   (closes below prev body bottom)

    Returns True if pattern is confirmed, else False.
    """
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]

    prev_bullish = prev["close"] > prev["open"]
    curr_bearish = curr["close"] < curr["open"]

    if not (prev_bullish and curr_bearish):
        return False

    # Current body must fully engulf previous body
    engulfs = (curr["open"] >= prev["close"] and
               curr["close"] <= prev["open"])
    return engulfs


# ── ORDER BLOCK DETECTOR ──────────────────────────────────────

def detect_order_blocks(df: pd.DataFrame, periods: int = OB_PERIODS,
                        threshold: float = OB_THRESHOLD):
    """
    Detect latest Bullish and Bearish Order Blocks.

    Bullish OB : last red candle before `periods` consecutive green candles
                 with abs % move >= threshold.
    Bearish OB : last green candle before `periods` consecutive red candles
                 with abs % move >= threshold.

    Returns:
        (bull_ob, bear_ob) — dicts with keys high/low/avg, or None if absent.
    """
    if len(df) < periods + 2:
        return None, None

    ob_idx    = -(periods + 1)
    ob_candle = df.iloc[ob_idx]

    abs_move = abs((df.iloc[-1]["close"] - ob_candle["close"]) /
                   ob_candle["close"]) * 100
    rel_move = abs_move >= threshold

    tail = df.iloc[-(periods):]

    # Bullish OB: OB candle red, subsequent all green
    bull_ob = None
    if ob_candle["close"] < ob_candle["open"] and rel_move:
        if (tail["close"] > tail["open"]).sum() == periods:
            bull_ob = {
                "high": ob_candle["open"],
                "low" : ob_candle["low"],
                "avg" : (ob_candle["open"] + ob_candle["low"]) / 2,
            }

    # Bearish OB: OB candle green, subsequent all red
    bear_ob = None
    if ob_candle["close"] > ob_candle["open"] and rel_move:
        if (tail["close"] < tail["open"]).sum() == periods:
            bear_ob = {
                "high": ob_candle["high"],
                "low" : ob_candle["open"],
                "avg" : (ob_candle["high"] + ob_candle["open"]) / 2,
            }

    return bull_ob, bear_ob


# ── PROXIMITY CHECK ───────────────────────────────────────────

def _near_ob(ltp: float, ob: dict, proximity: float = OB_PROXIMITY) -> bool:
    """
    [Item v] True if ltp is inside or within `proximity` % of the OB zone.
    Expands zone slightly on each side so entries trigger before exact touch.
    """
    low_band  = ob["low"]  * (1 - proximity)
    high_band = ob["high"] * (1 + proximity)
    return low_band <= ltp <= high_band


# ── MAIN SIGNAL FUNCTION ──────────────────────────────────────

def order_block_signal(df: pd.DataFrame, index: str, ltp: float):
    """
    [Item v]  OB proximity is a HARD GATE — no signal fires unless
              price is inside or touching an OB zone.
    [Item iii] Signal requires BOTH OB zone proximity AND a confirmed
              engulfing candle pattern on 5-min timeframe.

    Returns:
        (signal, bull_ob, bear_ob)
        signal = "BUY"  | "SELL"  | "BOTH" | None
        "BOTH" means price is near bull OB AND bear OB simultaneously
        (range-bound — caller may place parallel CE+PE or skip).

    [Item v] Parallel signal: if BOTH OBs are active and engulfing
    patterns confirm both directions, returns "BOTH" so dhan_api /
    dhan_live can optionally trade both legs simultaneously.
    """
    bull_ob, bear_ob = detect_order_blocks(df)

    near_bull = bull_ob is not None and _near_ob(ltp, bull_ob)
    near_bear = bear_ob is not None and _near_ob(ltp, bear_ob)

    bull_engulf = bullish_engulfing(df)
    bear_engulf = bearish_engulfing(df)

    # ── Parallel signal [Item v] ──────────────────────────────
    # Price near BOTH zones AND both engulfing patterns confirm
    if near_bull and near_bear and bull_engulf and bear_engulf:
        print(f"[OB] {index}: PARALLEL — near both OBs with dual engulf "
              f"ltp={ltp:.1f}  -> BOTH")
        return "BOTH", bull_ob, bear_ob

    signal = None

    # ── Bullish signal ────────────────────────────────────────
    if near_bull and bull_engulf:
        signal = "BUY"
        print(f"[OB] {index}: Bullish OB {bull_ob['low']:.1f}"
              f"–{bull_ob['high']:.1f} + Bullish Engulfing -> BUY")

    # ── Bearish signal ────────────────────────────────────────
    elif near_bear and bear_engulf:
        signal = "SELL"
        print(f"[OB] {index}: Bearish OB {bear_ob['low']:.1f}"
              f"–{bear_ob['high']:.1f} + Bearish Engulfing -> SELL")

    # ── OB zone present but no engulfing confirmation ─────────
    elif near_bull and not bull_engulf:
        print(f"[OB] {index}: Near Bullish OB but NO engulfing "
              f"— waiting for confirmation (ltp={ltp:.1f})")

    elif near_bear and not bear_engulf:
        print(f"[OB] {index}: Near Bearish OB but NO engulfing "
              f"— waiting for confirmation (ltp={ltp:.1f})")

    # ── [Item v] HARD GATE: price not near any OB — no trade ──
    else:
        if bull_ob or bear_ob:
            print(f"[OB] {index}: OB exists but price NOT near zone "
                  f"(ltp={ltp:.1f}) — no trade")

    return signal, bull_ob, bear_ob