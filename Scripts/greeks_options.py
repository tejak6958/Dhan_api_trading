"""
==============================================================
  DhanBot / Scripts/greeks_options.py
  BLACK-SCHOLES GREEKS + ATM OPTION SELECTOR + COMBINED SIGNAL

  This module was missing from the project — its absence caused:
    ImportError: cannot import name 'combined_signal' from Scripts.greeks_options
  and when monkey-patched with a 2-arg lambda, caused:
    "<lambda>() takes 2 positional arguments but 3 were given"

  EXPORTS used by Dhan_api.py, dhan_live.py, websocket_feed.py:
    combined_signal(df, index, ltp)  -> (signal, label)
    select_option(df_master, index, ltp, signal) -> (sid, name, expiry, delta, gamma)
    MIN_DELTA  : float  = 0.30
    MAX_GAMMA  : float  = 0.005

  combined_signal:
    [Item vii] EMA/RSI strategy REMOVED — only OrderBlock + Engulfing signal.
    [Item v]   Returns "BOTH" when bull and bear OB both trigger simultaneously.
    [Item iii] Expects 5-min candle DataFrame (from dhan_historical.fetch_candles).

  select_option:
    Selects nearest ATM CE (for BUY signal) or PE (for SELL signal) from
    scrip master. Returns Black-Scholes delta and gamma for the selected strike.
==============================================================
"""

import logging
import math
from datetime import datetime

import pandas as pd

from Strategies.strategies_breakout_trend import breakout_trend_signal
from Strategies.strategies_order_block import detect_order_blocks, order_block_signal

logger = logging.getLogger("DhanBot")

# ── GREEKS CONFIG ─────────────────────────────────────────────
MIN_DELTA = 0.30  # skip OTM options with |delta| < this
MAX_GAMMA = 0.005  # skip options near expiry with gamma > this

# Risk-free rate and assumed IV for Black-Scholes in sandbox
_RISK_FREE_RATE = 0.065  # 6.5% Indian repo rate
_DEFAULT_IV = 0.15  # 15% assumed IV (ATM NIFTY typical)


# ── BLACK-SCHOLES GREEKS ──────────────────────────────────────


def _norm_cdf(x: float) -> float:
    """Approximation of standard normal CDF."""
    return (1.0 + math.erf(x / math.sqrt(2))) / 2.0


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def bs_greeks(
    S: float,
    K: float,
    T: float,
    r: float = _RISK_FREE_RATE,
    sigma: float = _DEFAULT_IV,
    option_type: str = "CE",
) -> tuple[float, float]:
    """
    Black-Scholes Delta and Gamma for a European option.

    Args:
        S           : underlying spot price
        K           : option strike price
        T           : time to expiry in years (e.g. 7/365 for 7 days)
        r           : risk-free rate (default 6.5%)
        sigma       : implied volatility (default 15%)
        option_type : "CE" or "PE"

    Returns:
        (delta, gamma) — both floats; delta is signed (CE > 0, PE < 0)
    """
    if T <= 0 or S <= 0 or K <= 0 or sigma <= 0:
        return (0.0, 0.0)
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        gamma = _norm_pdf(d1) / (S * sigma * math.sqrt(T))
        if option_type == "CE":
            delta = _norm_cdf(d1)
        else:
            delta = _norm_cdf(d1) - 1.0  # PE delta is negative
        return (round(delta, 4), round(gamma, 6))
    except Exception:
        return (0.0, 0.0)


# ── ATM OPTION SELECTOR ───────────────────────────────────────


def select_option(
    df_master: pd.DataFrame, index: str, ltp: float, signal: str
) -> tuple:
    """
    Select the nearest ATM option for the given index and signal direction.

    [Item i/vi] Uses the live LTP passed in (not historical candle close),
    so the selected strike reflects the PRESENT market price.

    Args:
        df_master : filtered scrip master DataFrame (NSE FnO options only)
        index     : "NIFTY" or "BANKNIFTY"
        ltp       : current underlying LTP (live, not stale candle)
        signal    : "BUY" -> CE, "SELL" -> PE

    Returns:
        (security_id, symbol_name, expiry_str, delta, gamma)
        or (None, None, None, 0.0, 0.0) if no strike found
    """
    opt_type = "CE" if signal == "BUY" else "PE"

    # Filter for this index and option type
    mask = df_master["SEM_TRADING_SYMBOL"].str.startswith(index) & df_master[
        "SEM_OPTION_TYPE"
    ].str.upper().eq(opt_type)
    candidates = df_master[mask].copy()

    if candidates.empty:
        logger.error(f"[SELECT OPT] No {index} {opt_type} rows in master")
        return (None, None, None, 0.0, 0.0)

    # Parse strike prices — Dhan uses SEM_STRIKE_PRICE column
    strike_col = "SEM_STRIKE_PRICE"
    if strike_col not in candidates.columns:
        # Fallback: extract from symbol e.g. NIFTY25MAY24800CE
        candidates["_strike"] = (
            candidates["SEM_TRADING_SYMBOL"]
            .str.extract(r"(\d{4,6})(?:CE|PE)$")[0]
            .astype(float)
        )
    else:
        candidates["_strike"] = pd.to_numeric(candidates[strike_col], errors="coerce")

    candidates = candidates.dropna(subset=["_strike"])

    if candidates.empty:
        logger.error(f"[SELECT OPT] Strike parse failed for {index} {opt_type}")
        return (None, None, None, 0.0, 0.0)

    # Nearest ATM strike
    candidates["_dist"] = (candidates["_strike"] - ltp).abs()
    row = candidates.loc[candidates["_dist"].idxmin()]
    strike = float(row["_strike"])
    sid = str(row["SEM_SMST_SECURITY_ID"])
    name = str(row["SEM_TRADING_SYMBOL"])
    expiry = str(row.get("SEM_EXPIRY_DATE", "UNKNOWN"))

    # Approximate days to expiry for Greeks
    try:
        exp_dt = datetime.strptime(expiry[:10], "%Y-%m-%d")
        T = max((exp_dt - datetime.now()).days / 365, 1 / 365)
    except Exception:
        T = 7 / 365  # fallback: 7 days

    delta, gamma = bs_greeks(ltp, strike, T, option_type=opt_type)

    logger.info(
        f"[SELECT OPT] {index} {opt_type} strike={strike:.0f} "
        f"sid={sid} delta={delta:.3f} gamma={gamma:.5f} T={T * 365:.0f}d"
    )
    return (sid, name, expiry, delta, gamma)


# ── ENGULFING DETECTORS (Item iii) ────────────────────────────


def _bullish_engulfing(df: pd.DataFrame) -> bool:
    """
    Bullish engulfing on the two most recent completed candles.
    Conditions:
      - Previous candle: bearish (close < open)
      - Current candle:  bullish (close > open)
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


def _bearish_engulfing(df: pd.DataFrame) -> bool:
    """
    Bearish engulfing on the two most recent completed candles.
    Conditions:
      - Previous candle: bullish (close > open)
      - Current candle:  bearish (close < open)
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


# ── COMBINED SIGNAL ───────────────────────────────────────────


def combined_signal(df: pd.DataFrame, index: str, ltp: float) -> tuple[str | None, str]:
    """
    Evaluate all active strategies and return a combined signal.

    [Item vii] EMA/RSI strategy is REMOVED. Only Order Block + Engulfing.
    [Item iii] Expects 5-min candle DataFrame.
    [Item v]   Returns "BOTH" when bull and bear OB zones both trigger at once
               (price range-bound between supply and demand levels).
    [Item v]   OB proximity is a HARD GATE: if price is not near any OB zone,
               no signal is returned regardless of engulfing pattern.

    Args:
        df    : 5-min OHLC DataFrame (at least 10 bars recommended)
        index : "NIFTY" or "BANKNIFTY"  (for logging)
        ltp   : current live underlying LTP

    Returns:
        (signal, label) where signal is "BUY", "SELL", "BOTH", or None
    """
    if df.empty or len(df) < 5:
        logger.info(f"[SIGNAL] {index}: insufficient bars ({len(df)})")
        return (None, "no_data")

    # ── Step 1: Order Block proximity gate (Item v) ──────────
    ob_signal, bull_ob, bear_ob = order_block_signal(df, index, ltp)

    if ob_signal is None and bull_ob is None and bear_ob is None:
        logger.info(f"[SIGNAL] {index}: no OB zone detected — no trade")
        return (None, "no_ob_zone")

    if ob_signal is None:
        # OBs detected but price not near either — hard gate
        logger.info(f"[SIGNAL] {index}: OBs found but price not in zone — skip")
        return (None, "outside_ob_zone")

    # ── Step 2: Engulfing confirmation (Item iii) ─────────────
    bull_eng = _bullish_engulfing(df)
    bear_eng = _bearish_engulfing(df)

    # ── Step 3: Breakout Trend confirmation (BTF vote) ────────
    btf_sig, _buy_lvl, _stop_lvl = breakout_trend_signal(df, index, ltp)

    logger.info(
        f"[SIGNAL] {index}: OB={ob_signal} "
        f"bull_eng={bull_eng} bear_eng={bear_eng} "
        f"btf={btf_sig} ltp={ltp:.2f}"
    )

    # ── Step 4: Combine + parallel logic (Item v) ─────────────
    # Case: price in BOTH OB zones simultaneously (range-bound)
    if ob_signal == "BOTH":
        if bull_eng and bear_eng:
            label = "OB+Engulfing|BOTH"
            if btf_sig == "BUY":
                label = "OB+BullEng+BTF|BOTH"
            return ("BOTH", label)
        elif bull_eng:
            label = "OB+BullEng+BTF" if btf_sig == "BUY" else "OB+BullEng"
            return ("BUY", label)
        elif bear_eng:
            label = "OB+BearEng+BTF" if btf_sig == "SELL" else "OB+BearEng"
            return ("SELL", label)
        else:
            logger.info(f"[SIGNAL] {index}: BOTH OB zones active, no engulfing — wait")
            return (None, "ob_both_no_eng")

    # Case: single OB direction with matching engulfing
    if ob_signal == "BUY" and bull_eng:
        label = "OB+BullEng+BTF" if btf_sig == "BUY" else "OB+BullEng"
        return ("BUY", label)

    if ob_signal == "SELL" and bear_eng:
        label = "OB+BearEng+BTF" if btf_sig == "SELL" else "OB+BearEng"
        return ("SELL", label)

    # OB signal present but engulfing doesn't confirm → wait
    logger.info(f"[SIGNAL] {index}: OB={ob_signal} but no matching engulfing — wait")
    return (None, f"ob_{ob_signal.lower()}_no_eng")
