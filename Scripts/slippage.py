"""
==============================================================
  DhanBot / slippage.py
  [Item ii] SLIPPAGE CONFIG + OPTION PREMIUM FETCHER
            + PAPER FILL SIMULATOR

  Used by Dhan_api.py (sandbox) for paper trade simulation.
  dhan_live.py has its own real-fill fetcher (_fetch_fill_price)
  and uses SLIPPAGE_BUFFER for limit order pricing instead.
==============================================================
"""

import requests
import logging

logger = logging.getLogger("DhanBot")

# ── SLIPPAGE CONFIG ───────────────────────────────────────────
#
# SANDBOX_SLIPPAGE_PCT:
#   Applied to option PREMIUM price (not underlying index LTP).
#   BUY  -> fill = premium * (1 + SANDBOX_SLIPPAGE_PCT)  [pays more]
#   SELL -> fill = premium * (1 - SANDBOX_SLIPPAGE_PCT)  [receives less]
#
#   0.005 = 0.5%  <- old default
#   0.010 = 1.0%  <- current [Item ii]: conservative / worst-case testing
#
# WHY APPLY TO OPTION PREMIUM, NOT UNDERLYING:
#   Wrong: underlying LTP = Rs.48,040 -> 0.5% slip = Rs.240
#          (absurd; option itself costs Rs.200 total)
#   Right: option premium = Rs.200    -> 0.5% slip = Rs.1.00
#          (realistic bid-ask spread on ATM option)
#
SANDBOX_SLIPPAGE_PCT = 0.010   # [Item ii] raised from 0.005 (0.5%) → 0.010 (1.0%)

# Fallback option premium when sandbox API is unavailable
_FALLBACK_PREMIUM = {
    "NIFTY"    : 150.0,
    "BANKNIFTY": 200.0,
}


# ── OPTION PREMIUM FETCHER ────────────────────────────────────

def fetch_option_premium(opt_sid: str, index: str,
                         sandbox_base_url: str,
                         access_token: str,
                         client_id: str) -> float:
    """
    Fetch the OPTION's own market price (LTP) from Dhan quote API.
    Used so slippage is applied to the option premium, not the index.

    Sandbox often fails on this endpoint; falls back to a hardcoded
    realistic estimate so simulation stays meaningful.

    Args:
        opt_sid          : option security ID string
        index            : "NIFTY" or "BANKNIFTY" (for fallback lookup)
        sandbox_base_url : e.g. https://sandbox.dhan.co/v2
        access_token     : from .env
        client_id        : from .env

    Returns:
        float option LTP, or fallback premium if fetch fails
    """
    try:
        url     = f"{sandbox_base_url}/marketfeed/ltp"
        headers = {
            "access-token": access_token,
            "client-id"   : client_id,
            "Content-Type": "application/json",
        }
        r = requests.post(url, json={"NSE_FNO": [int(opt_sid)]},
                          headers=headers, timeout=5)
        r.raise_for_status()
        data    = r.json()
        ltp_val = (data
                   .get("data", {})
                   .get("NSE_FNO", {})
                   .get(str(opt_sid), {})
                   .get("last_price", 0))
        if ltp_val and float(ltp_val) > 0:
            logger.info(f"[OPT PREMIUM] sid={opt_sid} LTP=Rs.{ltp_val:.2f}")
            return float(ltp_val)
    except Exception as e:
        logger.info(f"[OPT PREMIUM] fetch failed sid={opt_sid}: {e}")

    fallback = _FALLBACK_PREMIUM.get(index, 200.0)
    logger.info(f"[OPT PREMIUM] Using fallback Rs.{fallback:.0f} "
                f"(sid={opt_sid})")
    return fallback


# ── PAPER FILL SIMULATOR ──────────────────────────────────────

def simulate_fill(ltp: float, side: str,
                  slippage_pct: float = SANDBOX_SLIPPAGE_PCT) -> float:
    """
    Simulate a realistic fill price for sandbox paper trades.

    WHY: Real MARKET orders fill worse than signal LTP due to
    bid-ask spread. Without this, sandbox PnL is overstated and
    strategies will look more profitable than they are in live trading.

    BUY  -> fill = ltp * (1 + slippage_pct)   [pays more — pessimistic]
    SELL -> fill = ltp * (1 - slippage_pct)   [receives less — pessimistic]

    Args:
        ltp          : option premium at signal time (NOT underlying LTP)
        side         : "BUY" or "SELL"
        slippage_pct : override default if needed (default SANDBOX_SLIPPAGE_PCT)

    Returns:
        float simulated fill price
    """
    if side == "BUY":
        fill = round(ltp * (1 + slippage_pct), 2)
    else:
        fill = round(ltp * (1 - slippage_pct), 2)

    slip = round(fill - ltp, 2)
    logger.info(
        f"[SLIPPAGE SIM] {side} | signal_ltp={ltp:.2f} "
        f"fill={fill:.2f} slippage={slip:+.2f} "
        f"({slippage_pct * 100:.1f}%)"
    )
    return fill