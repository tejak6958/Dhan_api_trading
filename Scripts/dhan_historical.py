"""
=============================================================
  DhanBot / dhan_historical.py
  [Item iii] 5-MIN CANDLES  [Item vi] PRESENT DATA FIRST

  Provides fetch_candles() for Dhan_api.py (sandbox).
  dhan_live.py does NOT use this — it uses WebSocket ticks.

  CHANGES:
    [Item iii] interval changed from 1-min to 5-min throughout.
               Synthetic fallback resampled to 5-min OHLC bars.
    [Item vi]  fetch_live_ltp() added — called by Dhan_api.py
               to get real-time index price for option selection,
               so orders use the PRESENT strike, not stale hist data.

  Attempt order:
    1. dhanhq SDK  -> intraday_minute_data() interval=5 with IDX_I
    2. Raw POST    -> /charts/intraday  interval=5
    3. Synthetic   -> random-walk 1-min bars resampled to 5-min
==============================================================
"""

import logging
import random
from datetime import datetime, timedelta

import pandas as pd
import requests

logger = logging.getLogger("DhanBot")


def _parse_candle_response(data: dict) -> pd.DataFrame:
    """Parse DhanHQ candle API response dict to clean DataFrame."""
    if not data or "open" not in data:
        return pd.DataFrame()
    opens = data.get("open", [])
    if not opens:
        return pd.DataFrame()
    df = pd.DataFrame(
        {
            "open": data["open"],
            "high": data["high"],
            "low": data["low"],
            "close": data["close"],
            "volume": data.get("volume", [0] * len(opens)),
            "timestamp": data.get("timestamp", list(range(len(opens)))),
        }
    )
    return df.dropna().reset_index(drop=True)


def _resample_to_5min(df_1min: pd.DataFrame) -> pd.DataFrame:
    """
    [Item iii] Resample 1-min synthetic bars into 5-min OHLC candles.

    Groups every 5 consecutive 1-min bars:
      open  = first bar open
      high  = max of all highs
      low   = min of all lows
      close = last bar close
      volume= sum of volumes
    Returns a clean DataFrame with sequential timestamp index.
    """
    if df_1min.empty:
        return df_1min
    rows = []
    chunk = 5
    for i in range(0, len(df_1min) - chunk + 1, chunk):
        grp = df_1min.iloc[i : i + chunk]
        rows.append(
            {
                "open": grp.iloc[0]["open"],
                "high": grp["high"].max(),
                "low": grp["low"].min(),
                "close": grp.iloc[-1]["close"],
                "volume": grp["volume"].sum(),
                "timestamp": grp.iloc[-1]["timestamp"],
            }
        )
    result = pd.DataFrame(rows).reset_index(drop=True)
    logger.info(f"[RESAMPLE] {len(df_1min)} x 1-min -> {len(result)} x 5-min bars")
    return result


def _synthetic_candles(n: int = 120, base: float = 22000.0) -> pd.DataFrame:
    """
    [Item iii] Generate synthetic 1-min OHLC bars then resample to 5-min.

    WHY: Sandbox returns HTTP 500 for IDX_I endpoints — known limitation.
    Synthetic bars allow full end-to-end testing of strategy, Greeks,
    and paper order logic without a live market connection.
    n=120 1-min bars -> 24 x 5-min candles (enough for OB detection).
    """
    prices = [base]
    for _ in range(n - 1):
        prices.append(max(prices[-1] + random.gauss(0, base * 0.0005), base * 0.80))
    rows = []
    for p in prices:
        o = round(p, 2)
        h = round(p + abs(random.gauss(0, p * 0.0003)), 2)
        l = round(p - abs(random.gauss(0, p * 0.0003)), 2)
        c = round(p + random.gauss(0, p * 0.0002), 2)
        rows.append(
            {
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": random.randint(1000, 9000),
            }
        )
    df_1min = pd.DataFrame(rows)
    df_1min["timestamp"] = list(range(len(df_1min)))
    logger.info(f"[SYNTHETIC] {n} x 1-min bars generated (base={base:.0f})")
    # [Item iii] Resample to 5-min before returning
    return _resample_to_5min(df_1min)


# Yahoo Finance ticker map for NSE indices (public, no auth required)
_YAHOO_TICKER = {
    "13": "%5ENSEI",  # NIFTY 50
    "25": "%5ENSEBANK",  # Bank NIFTY
}


def _fetch_ltp_yahoo(security_id: str, index: str) -> float | None:
    """
    [Item i / vi] Fallback: fetch real-time index LTP from Yahoo Finance.

    Used when Dhan sandbox /marketfeed/ltp returns 404 (known limitation).
    Yahoo Finance public API requires no credentials and returns current
    market price, so option strike selection always uses PRESENT data.

    Returns float LTP or None on failure.
    """
    ticker = _YAHOO_TICKER.get(security_id)
    if not ticker:
        return None
    try:
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?interval=1m&range=1d"
        )
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json()
        price = (
            data.get("chart", {})
            .get("result", [{}])[0]
            .get("meta", {})
            .get("regularMarketPrice", 0)
        )
        if price and float(price) > 0:
            logger.info(f"[LTP YAHOO] {index} ({ticker}) Rs.{float(price):.2f}")
            return float(price)
        logger.info(f"[LTP YAHOO] {index}: empty price in response")
    except Exception as e:
        logger.info(f"[LTP YAHOO] {index} failed: {e}")
    return None


def fetch_live_ltp(
    security_id: str,
    index: str,
    sandbox_base_url: str,
    access_token: str,
    client_id: str,
) -> float | None:
    """
    [Item i / Item vi] Fetch REAL-TIME index LTP for PRESENT strike selection.

    Attempt order (sandbox):
      1. Dhan sandbox /marketfeed/ltp   — often returns 404 (known limit)
      2. Yahoo Finance public API       — real current market price, no auth
      3. Returns None — caller falls back to synthetic candle close

    This ensures option strike selection uses the PRESENT market price,
    not stale synthetic data, so the ATM strike is always current.

    Args:
        security_id      : "13" NIFTY, "25" BANKNIFTY
        index            : "NIFTY" or "BANKNIFTY" (for logging)
        sandbox_base_url : e.g. https://sandbox.dhan.co/v2
        access_token     : from .env
        client_id        : from .env

    Returns:
        float live LTP (present market price), or None if both attempts fail
    """
    # ── Attempt 1: Dhan sandbox /marketfeed/ltp ─────────────────
    try:
        url = f"{sandbox_base_url}/marketfeed/ltp"
        headers = {
            "access-token": access_token,
            "client-id": client_id,
            "Content-Type": "application/json",
        }
        r = requests.post(
            url, json={"IDX_I": [int(security_id)]}, headers=headers, timeout=5
        )
        r.raise_for_status()
        data = r.json()
        price = (
            data.get("data", {})
            .get("IDX_I", {})
            .get(str(security_id), {})
            .get("last_price", 0)
        )
        if price and float(price) > 0:
            logger.info(f"[LTP DHAN] {index} sid={security_id} Rs.{float(price):.2f}")
            return float(price)
        logger.info(f"[LTP DHAN] {index}: empty response")
    except Exception as e:
        logger.info(f"[LTP DHAN] {index} failed ({e}) — trying Yahoo Finance fallback")

    # ── Attempt 2: Yahoo Finance (present market price, no auth) ──
    yf_price = _fetch_ltp_yahoo(security_id, index)
    if yf_price:
        return yf_price

    # ── Attempt 3: give up — caller uses synthetic candle close ──
    logger.info(
        f"[LTP] {index}: all LTP attempts failed — using candle close as fallback"
    )
    return None


def fetch_candles(
    dhan,
    security_id: str,
    sandbox_base_url: str,
    access_token: str,
    client_id: str,
    is_market_open_fn,
    market_status_reason_fn,
) -> pd.DataFrame:
    """
    [Item iii] Fetch 5-min intraday candles for the UNDERLYING INDEX.
    [Item vi]  Always attempts real API before synthetic fallback.

    Attempt order:
      1. SDK  intraday_minute_data() with interval=5, segment=IDX_I
      2. Raw POST /charts/intraday  with interval=5
      3. Synthetic -> 120 x 1-min bars resampled to 24 x 5-min bars

    Args:
        dhan                  : dhanhq client instance
        security_id           : "13" NIFTY, "25" BANKNIFTY
        sandbox_base_url      : e.g. https://sandbox.dhan.co/v2
        access_token          : from .env
        client_id             : from .env
        is_market_open_fn     : callable -> bool
        market_status_reason_fn: callable -> str

    Returns:
        pd.DataFrame columns open/high/low/close/volume/timestamp (5-min bars)
        or empty DataFrame if market is closed.
    """
    if not is_market_open_fn():
        logger.info(f"fetch_candles skipped: {market_status_reason_fn()}")
        return pd.DataFrame()

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # ── Attempt 1: SDK ────────────────────────────────────────
    try:
        resp = dhan.intraday_minute_data(
            security_id=security_id,
            exchange_segment="IDX_I",
            instrument_type="INDEX",
            interval=5,  # [Item iii] 5-min candles
            from_date=yesterday,
            to_date=today,
        )
        df = _parse_candle_response(resp)
        if not df.empty:
            logger.info(f"[SDK] {len(df)} x 5-min bars  sid={security_id}")
            return df
        logger.info(f"[SDK] empty response  sid={security_id}")
    except Exception as e:
        logger.info(f"[SDK] failed sid={security_id}: {e}")

    # ── Attempt 2: Raw POST ───────────────────────────────────
    try:
        url = f"{sandbox_base_url}/charts/intraday"
        payload = {
            "securityId": security_id,
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX",
            "interval": 5,  # [Item iii] was 1, now 5-min
            "oi": False,
            "fromDate": yesterday,
            "toDate": today,
        }
        headers = {
            "access-token": access_token,
            "client-id": client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        r = requests.post(url, json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        df = _parse_candle_response(r.json())
        if not df.empty:
            logger.info(f"[RAW POST] {len(df)} x 5-min bars  sid={security_id}")
            return df
        logger.info(f"[RAW POST] empty response  sid={security_id}")
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        logger.error(f"[RAW POST] HTTP {code}  sid={security_id}: {e}")
        if code == 500:
            logger.info(
                "[RAW POST] HTTP 500 = known sandbox IDX_I limit "
                "-> switching to synthetic 5-min candles"
            )
    except Exception as e:
        logger.error(f"[RAW POST] error  sid={security_id}: {e}")

    # ── Attempt 3: Synthetic 5-min fallback ──────────────────
    # Updated base prices — refresh periodically to match current market levels
    # NIFTY ~24500  |  BANKNIFTY ~54000  (approximate May-2026 levels)
    base = 24500.0 if security_id == "13" else 54000.0
    # [Item iii] _synthetic_candles now returns 5-min resampled bars
    return _synthetic_candles(n=120, base=base)
