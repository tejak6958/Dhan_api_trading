"""
==============================================================
  DHAN SANDBOX BOT  —  dhan_sandbox.py
  Mode : SANDBOX / Paper Trading
  Feed : Historical candle API (polling every 60s)
         ⚠️  WebSocket is NOT supported in Dhan sandbox.
             We poll intraday candle data every minute instead.
  Strategies:
    1. Order Block  (Bullish / Bearish)
    2. Breakout Trend Follower  (Swing-High breakout + MA filter)
    3. EMA-20/50 crossover + RSI-14  (confirmation layer)
  Options : NSE FnO — NIFTY & BANKNIFTY ATM CE/PE
  Greeks  : Black-Scholes Delta & Gamma filter
  Orders  : Paper (logged only, no real capital at risk)
  Reports : Telegram alerts + EOD backtest summary
==============================================================
"""

import os, time, threading, io
import logging
import numpy as np
import pandas as pd
import pandas_ta as ta
import requests
from datetime import datetime, timedelta
from scipy.stats import norm
from dhanhq import dhanhq, DhanContext
from dotenv import load_dotenv

# ── ENV ──────────────────────────────────────────────────────
load_dotenv()
CLIENT_ID    = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BOT_TOKEN    = os.getenv("BOT_TOKEN")
CHAT_ID      = os.getenv("CHAT_ID")

# ── LOGGING ───────────────────────────────────────────────────
LOG_FILE = "dhan_bot.log"

logger = logging.getLogger("DhanBot")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(file_handler)

def log_info(message: str):
    logger.info(message)


def log_error(message: str):
    logger.error(message)

# ── MARKET HOURS CHECK ───────────────────────────────────────
def is_market_open() -> bool:
    """Check if current time is within market hours (09:15 - 15:30 IST)."""
    now = datetime.now()
    # NSE market hours: 09:15 - 15:30 IST (Monday-Friday)
    # Note: This is simplified; excludes weekends and holidays
    market_open = now.hour > 9 or (now.hour == 9 and now.minute >= 15)
    market_close = now.hour < 15 or (now.hour == 15 and now.minute < 30)
    return market_open and market_close

# ── SANDBOX BASE URL ─────────────────────────────────────────
SANDBOX_BASE_URL = "https://sandbox.dhan.co/v2"

# ── TRADING CONFIG ───────────────────────────────────────────
LOT_SIZES   = {"NIFTY": 75, "BANKNIFTY": 30}
RISK_FREE   = 0.068        # ~10yr G-sec yield
IV_ASSUMED  = 0.15         # fallback IV (15 %)

# Greeks filter
MIN_DELTA   = 0.30         # |delta| must be ≥ this
MAX_GAMMA   = 0.05         # gamma must be ≤ this

# Candle interval for polling (minutes)
CANDLE_INTERVAL = 1        # 1-min candles
POLL_SLEEP      = 60       # seconds between API polls

# Order Block settings (mirrors Pine: periods=5, threshold=0.0)
OB_PERIODS   = 5
OB_THRESHOLD = 0.0         # % minimum move to validate OB

# Breakout Trend Follower settings
BTF_PVT_LEN = 3            # pivot look-back / look-forward
BTF_MA_LEN  = 50           # MA period for trend filter
BTF_MA_TYPE = "SMA"        # "SMA" or "EMA"

# ── GLOBALS ──────────────────────────────────────────────────
TOTAL_PNL       = 0.0
TRADE_COUNT     = 0
WIN_COUNT       = 0
BACKTEST_TRADES = []        # paper trades accumulator
ORDER_LOCK      = threading.Lock()

# ── TELEGRAM ─────────────────────────────────────────────────

def send_alert(msg: str) -> bool:
    """Send message to Telegram bot; return True if delivered."""
    try:
        url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        result = resp.json()
        if result.get("ok"):
            print(f"[TELEGRAM ✓] {msg[:60]}…")
            log_info(f"Telegram delivered: {msg[:60]}…")
            return True
        print(f"[TELEGRAM ✗] {result}")
        log_error(f"Telegram failed: {result}")
        return False
    except Exception as e:
        print(f"[TELEGRAM EXCEPTION] {e}")
        log_error(f"Telegram exception: {e}")
        return False

# ── DHAN CLIENT ──────────────────────────────────────────────

dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan = dhanhq(dhan_context)
dhan.dhan_http.base_url = SANDBOX_BASE_URL   # ← point to sandbox

def check_login() -> bool:
    try:
        res = dhan.get_fund_limits()
        if res and res.get("status") == "success":
            bal = res.get("data", {}).get("available_balance", "N/A")
            msg = (f"✅ <b>SANDBOX LOGIN OK</b>\n"
                   f"Mode    : SANDBOX\n"
                   f"Base URL: {SANDBOX_BASE_URL}\n"
                   f"Balance : {bal}\n"
                   f"Note    : WebSocket not supported in sandbox.\n"
                   f"          Using historical candle polling instead.")
            print(msg)
            ok = send_alert(msg)
            if not ok:
                print("⚠️  Telegram delivery failed — check BOT_TOKEN / CHAT_ID")
            return True
        print("❌ LOGIN FAILED")
        log_error("Sandbox login failed")
        return False
    except Exception as e:
        print(f"❌ LOGIN ERROR: {e}")
        log_error(f"Sandbox login error: {e}")
        return False

if not check_login():
    print("Stopping bot — login failed.")
    log_error("Stopping bot because login failed")
    exit()

# ── SCRIP MASTER ─────────────────────────────────────────────

MASTER_CSV   = "scrip_master.csv"
MASTER_URL   = "https://images.dhan.co/api-data/api-scrip-master.csv"
REFRESH_DAYS = 7

def _cache_fresh(fp: str, days: int) -> bool:
    if not os.path.exists(fp):
        return False
    age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(fp))
    return age < timedelta(days=days)

def load_scrip_master() -> pd.DataFrame:
    if _cache_fresh(MASTER_CSV, REFRESH_DAYS):
        send_alert("📂 Scrip master: loaded from local cache")
        return pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)
    send_alert("⏳ Downloading scrip master CSV (weekly refresh)…")
    resp = requests.get(MASTER_URL, timeout=120, stream=True)
    resp.raise_for_status()
    with open(MASTER_CSV, "wb") as f:
        for chunk in resp.iter_content(1024 * 256):
            f.write(chunk)
    df = pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)
    send_alert(f"✅ Scrip master downloaded: {len(df):,} rows")
    return df

df_master = load_scrip_master()
df_master = df_master[
    df_master["SEM_TRADING_SYMBOL"].str.contains("NIFTY|BANKNIFTY", na=False) &
    df_master["SEM_INSTRUMENT_NAME"].str.contains("OPT", na=False)
].reset_index(drop=True)
print(f"✅ Filtered master: {len(df_master)} option rows")

# ── DHAN HISTORICAL DATA ──────────────────────────────────────
# Dhan intraday candle endpoint (no WebSocket needed in sandbox)

def fetch_candles(security_id: str, exchange: str = "NSE_EQ",
                  instrument: str = "INDEX", interval: str = "1") -> pd.DataFrame:
    """
    Fetch today's intraday candles from Dhan historical API.
    Returns DataFrame with columns: open, high, low, close, volume, timestamp.
    Returns empty DataFrame if market is not open or API fails.
    """
    # Skip API call if market is not open
    if not is_market_open():
        return pd.DataFrame()
    
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        # Dhan intraday candle API
        url = f"{SANDBOX_BASE_URL}/charts/intraday"
        payload = {
            "securityId" : security_id,
            "exchangeSegment": exchange,
            "instrument"  : instrument,
            "interval"    : interval,
            "fromDate"    : today,
            "toDate"      : today,
        }
        headers = {
            "access-token": ACCESS_TOKEN,
            "client-id"   : CLIENT_ID,
            "Content-Type": "application/json",
        }
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # Dhan returns lists: open, high, low, close, volume, timestamp
        if not data or "open" not in data:
            return pd.DataFrame()

        df = pd.DataFrame({
            "open"     : data["open"],
            "high"     : data["high"],
            "low"      : data["low"],
            "close"    : data["close"],
            "volume"   : data.get("volume", [0] * len(data["open"])),
            "timestamp": data.get("timestamp", list(range(len(data["open"])))),
        })
        df = df.dropna().reset_index(drop=True)
        return df
    except Exception as e:
        error_msg = str(e)
        if "500" in error_msg or "502" in error_msg or "503" in error_msg:
            # Server error likely due to market not open or API issue
            log_info(f"Candle fetch for security_id={security_id}: API returned server error (likely market closed): {error_msg}")
        else:
            print(f"[CANDLE FETCH ERROR] {e}")
            log_error(f"Candle fetch error for security_id={security_id}: {e}")
        return pd.DataFrame()

# ── GREEKS ────────────────────────────────────────────────────

def bs_greeks(S, K, T, r, sigma, option_type="CE"):
    if T <= 0 or sigma <= 0:
        return 0.0, 0.0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    delta = norm.cdf(d1) if option_type == "CE" else norm.cdf(d1) - 1
    return round(delta, 4), round(gamma, 6)

def days_to_expiry(expiry_str: str) -> float:
    try:
        exp = datetime.strptime(expiry_str, "%Y-%m-%d")
        d   = (exp - datetime.now()).days
        return max(d / 365, 1 / 365)
    except Exception:
        return 7 / 365

# ── OPTION SELECTOR ───────────────────────────────────────────

def get_atm(price: float, step: int) -> int:
    return int(round(price / step) * step)

def select_option(index: str, ltp: float, signal: str):
    df = df_master[
        df_master["SEM_TRADING_SYMBOL"].str.contains(index, na=False)
    ].copy()
    df["EXPIRY"] = pd.to_datetime(df["SEM_EXPIRY_DATE"], errors="coerce")
    df = df[df["EXPIRY"] == df["EXPIRY"].min()]

    step     = 50 if index == "NIFTY" else 100
    strike   = get_atm(ltp, step)
    opt_type = "CE" if signal == "BUY" else "PE"

    df = df[
        (df["SEM_OPTION_TYPE"] == opt_type) &
        (df["SEM_STRIKE_PRICE"].astype(float).astype(int) == strike)
    ]
    if df.empty:
        return None, None, None, None, None

    row        = df.iloc[0]
    expiry_str = str(row["SEM_EXPIRY_DATE"])[:10]
    T          = days_to_expiry(expiry_str)
    delta, gamma = bs_greeks(ltp, strike, T, RISK_FREE, IV_ASSUMED, opt_type)
    return str(int(row["SEM_SM_ID"])), row["SEM_TRADING_SYMBOL"], expiry_str, delta, gamma

# ── ORDER BLOCK DETECTOR (Python port of Pine) ────────────────

def detect_order_blocks(df: pd.DataFrame, periods: int = OB_PERIODS,
                         threshold: float = OB_THRESHOLD):
    """
    Returns (bull_ob, bear_ob) — latest identified Order Blocks as dicts,
    or None if not found.
    Logic matches the Pine script in OrderBlock.txt:
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

# ── BREAKOUT TREND FOLLOWER (Python port of Pine) ─────────────

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
    Matches Pine strategy in Breakout_Trend_follower.txt:
      - BUY  when high > swing_high AND close > MA filter
      - SELL when low  < swing_low  (trailing stop)
    """
    if len(df) < BTF_MA_LEN + 2 * BTF_PVT_LEN + 2:
        return None, None, None

    closes = df["close"].tolist()
    highs  = df["high"].tolist()
    lows   = df["low"].tolist()

    # MA filter
    if BTF_MA_TYPE == "EMA":
        ma_val = df["close"].ewm(span=BTF_MA_LEN, adjust=False).mean().iloc[-1]
    else:
        ma_val = df["close"].rolling(BTF_MA_LEN).mean().iloc[-1]

    buy_level  = pivot_high(highs)
    stop_level = pivot_low(lows)

    if buy_level is None or stop_level is None:
        return None, buy_level, stop_level

    last_high  = highs[-1]
    last_low   = lows[-1]
    last_close = closes[-1]

    if last_high > buy_level and last_close > ma_val:
        return "BUY", buy_level, stop_level
    if last_low < stop_level:
        return "SELL", buy_level, stop_level
    return None, buy_level, stop_level

# ── EMA + RSI SIGNAL (original layer) ─────────────────────────

def ema_rsi_signal(df: pd.DataFrame):
    d = df.copy()
    d["ema20"] = ta.ema(d["close"], length=20)
    d["ema50"] = ta.ema(d["close"], length=50)
    d["rsi"]   = ta.rsi(d["close"], length=14)
    d.dropna(inplace=True)
    if d.empty:
        return None
    last = d.iloc[-1]
    if last["ema20"] > last["ema50"] and last["rsi"] > 55:
        return "BUY"
    if last["ema20"] < last["ema50"] and last["rsi"] < 45:
        return "SELL"
    return None

# ── COMBINED SIGNAL ───────────────────────────────────────────

def combined_signal(df: pd.DataFrame, index: str, ltp: float):
    """
    Combine all three strategies:
      • Order Block (institutional support / resistance zone)
      • Breakout Trend Follower (swing breakout + MA filter)
      • EMA/RSI (momentum confirmation)
    Signal is only fired when at least 2 of 3 agree.
    Returns 'BUY', 'SELL', or None.
    """
    votes_buy  = 0
    votes_sell = 0

    # 1 ── Order Block
    bull_ob, bear_ob = detect_order_blocks(df)
    if bull_ob:
        # Price is near / inside bullish OB zone → bullish signal
        if bull_ob["low"] <= ltp <= bull_ob["high"] * 1.005:
            votes_buy += 1
            print(f"[OB] {index}: Bullish OB zone {bull_ob['low']:.1f}–{bull_ob['high']:.1f}  → BUY vote")
    if bear_ob:
        if bear_ob["low"] * 0.995 <= ltp <= bear_ob["high"]:
            votes_sell += 1
            print(f"[OB] {index}: Bearish OB zone {bear_ob['low']:.1f}–{bear_ob['high']:.1f}  → SELL vote")

    # 2 ── Breakout Trend Follower
    btf_sig, buy_lvl, stop_lvl = btf_signal(df)
    if btf_sig == "BUY":
        votes_buy  += 1
        print(f"[BTF] {index}: Breakout above {buy_lvl:.1f}  → BUY vote")
    elif btf_sig == "SELL":
        votes_sell += 1
        print(f"[BTF] {index}: Breakdown below {stop_lvl:.1f}  → SELL vote")

    # 3 ── EMA / RSI
    er_sig = ema_rsi_signal(df)
    if er_sig == "BUY":
        votes_buy  += 1
        print(f"[EMA/RSI] {index}  → BUY vote")
    elif er_sig == "SELL":
        votes_sell += 1
        print(f"[EMA/RSI] {index}  → SELL vote")

    if votes_buy >= 2:
        return "BUY"
    if votes_sell >= 2:
        return "SELL"
    return None

# ── PAPER ORDER ───────────────────────────────────────────────

def paper_order(sid, qty, side, name):
    """Simulate order in sandbox — no actual API call to place_order."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[PAPER ORDER] {ts} | {side} | {name} | qty={qty} | sid={sid}")
    return {"status": "success", "orderId": f"PAPER_{ts}"}

# ── BACKTEST REPORT ───────────────────────────────────────────

def run_backtest_report():
    if not BACKTEST_TRADES:
        send_alert("📊 Backtest: No trades captured today.")
        return

    df = pd.DataFrame(BACKTEST_TRADES, columns=[
        "time", "symbol", "strategy", "reason",
        "entry", "exit", "pnl", "delta", "gamma"
    ])

    total_pnl = df["pnl"].sum()
    wins      = (df["pnl"] > 0).sum()
    losses    = (df["pnl"] <= 0).sum()
    win_rate  = wins / len(df) * 100 if len(df) else 0
    max_dd    = df["pnl"].cumsum().min() if len(df) else 0

    strat_pnl = df.groupby("strategy")["pnl"].sum().to_string()

    report = (
        f"📊 <b>SANDBOX BACKTEST REPORT</b>\n"
        f"{'─'*32}\n"
        f"Date         : {datetime.now().strftime('%Y-%m-%d')}\n"
        f"Mode         : SANDBOX (paper)\n"
        f"Total Trades : {len(df)}\n"
        f"Wins         : {wins}   Losses: {losses}\n"
        f"Win Rate     : {win_rate:.1f}%\n"
        f"Net PnL      : ₹{total_pnl:.2f}\n"
        f"Max Drawdown : ₹{max_dd:.2f}\n"
        f"Avg Delta    : {df['delta'].mean():.3f}\n"
        f"Avg Gamma    : {df['gamma'].mean():.5f}\n"
        f"\nPnL by Strategy:\n{strat_pnl}\n"
        f"{'─'*32}"
    )

    with open("sandbox_backtest_report.txt", "w") as f:
        f.write(report)
    send_alert(report)
    print(report)

def log_trade(row: list):
    with open("sandbox_trades.csv", "a") as f:
        f.write(",".join(map(str, row)) + "\n")

def show_dashboard():
    print(f"\n{'='*40}")
    print(f"  SANDBOX DASHBOARD  {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Trades: {TRADE_COUNT}  Wins: {WIN_COUNT}  PnL: ₹{TOTAL_PNL:.2f}")
    print(f"{'='*40}\n")

# ── INDEX FEED IDs ────────────────────────────────────────────
# Dhan index security IDs (NSE segment)
INDEX_IDS = {
    "NIFTY"    : "13",
    "BANKNIFTY": "25",
}

# Per-index state
indices = {
    idx: {
        "in_trade"    : False,
        "order_placed": False,
        "entry"       : 0.0,
        "sl"          : 0.0,
        "target"      : 0.0,
        "opt_sid"     : None,
        "qty"         : 0,
        "name"        : "",
        "delta"       : 0.0,
        "gamma"       : 0.0,
        "signal"      : "",
        "strategy"    : "",
    }
    for idx in INDEX_IDS
}

# ── MAIN POLLING LOOP ─────────────────────────────────────────

def process_index(index: str, sid: str):
    """
    Fetch latest candles for one index, evaluate all signals,
    and manage paper entry / exit.
    """
    global TOTAL_PNL, TRADE_COUNT, WIN_COUNT

    df = fetch_candles(sid)
    if df.empty or len(df) < 60:
        if df.empty:
            # No data means either market not open or API issue
            pass  # Suppress message; logged in fetch_candles if error
        else:
            log_info(f"[{index}] Insufficient candle data: {len(df)} bars (need 60)")
        return

    ltp = float(df.iloc[-1]["close"])
    print(f"[{index}] LTP={ltp:.2f}  bars={len(df)}")
    log_info(f"[{index}] Processing: LTP={ltp:.2f}, bars={len(df)}")

    data = indices[index]

    # ── EXIT CHECK (always runs first) ──────────────────────
    if data["in_trade"]:
        exit_flag = False
        reason    = ""
        if ltp <= data["sl"]:
            exit_flag, reason = True, "SL HIT"
        elif ltp >= data["target"]:
            exit_flag, reason = True, "TARGET HIT"

        if exit_flag:
            with ORDER_LOCK:
                if not data["in_trade"]:
                    return
                paper_order(data["opt_sid"], data["qty"], "SELL", data["name"])

                pnl = (ltp - data["entry"]) * data["qty"]
                TOTAL_PNL   += pnl
                TRADE_COUNT += 1
                if pnl > 0:
                    WIN_COUNT += 1

                BACKTEST_TRADES.append([
                    datetime.now(), data["name"], data["strategy"], reason,
                    data["entry"], ltp, pnl, data["delta"], data["gamma"]
                ])
                log_trade([
                    datetime.now(), data["name"], data["strategy"], reason,
                    data["entry"], ltp, round(pnl, 2), data["delta"], data["gamma"]
                ])

                send_alert(
                    f"🔴 <b>EXIT | {index}</b>\n"
                    f"Strategy : {data['strategy']}\n"
                    f"Reason   : {reason}\n"
                    f"Option   : {data['name']}\n"
                    f"Entry    : {data['entry']}  Exit: {ltp}\n"
                    f"PnL      : ₹{pnl:.2f}\n"
                    f"Total PnL: ₹{TOTAL_PNL:.2f}\n"
                    f"Mode     : SANDBOX"
                )
                data.update({"in_trade": False, "order_placed": False})
                show_dashboard()
        return   # don't look for new entry while in trade

    # ── ENTRY CHECK ─────────────────────────────────────────
    signal = combined_signal(df, index, ltp)

    if signal and not data["order_placed"]:
        with ORDER_LOCK:
            if data["in_trade"] or data["order_placed"]:
                return

            opt_sid, name, expiry_str, delta, gamma = select_option(
                index, ltp, signal)

            if not opt_sid:
                send_alert(f"⚠️ {index}: No option found near strike {ltp:.0f}")
                return

            abs_delta = abs(delta)
            if abs_delta < MIN_DELTA:
                send_alert(f"⚠️ {index} {name}: Delta {abs_delta:.2f} < {MIN_DELTA} — skip")
                return
            if gamma > MAX_GAMMA:
                send_alert(f"⚠️ {index} {name}: Gamma {gamma:.5f} > {MAX_GAMMA} — skip")
                return

            qty = LOT_SIZES[index]
            data["order_placed"] = True

            # Detect which strategy fired and inform Telegram
            bull_ob, bear_ob = detect_order_blocks(df)
            btf_sig, buy_lvl, stop_lvl = btf_signal(df)
            er_sig  = ema_rsi_signal(df)

            strats_active = []
            if (bull_ob and signal == "BUY") or (bear_ob and signal == "SELL"):
                strats_active.append("Order Block")
            if btf_sig == signal:
                strats_active.append("Breakout Trend Follower")
            if er_sig == signal:
                strats_active.append("EMA/RSI")
            strat_label = " + ".join(strats_active) if strats_active else "Combined"

            resp = paper_order(opt_sid, qty, "BUY", name)

            data.update({
                "in_trade"    : True,
                "entry"       : ltp,
                "sl"          : ltp * 0.75,
                "target"      : ltp * 1.50,
                "opt_sid"     : opt_sid,
                "qty"         : qty,
                "name"        : name,
                "delta"       : delta,
                "gamma"       : gamma,
                "signal"      : signal,
                "strategy"    : strat_label,
            })

            send_alert(
                f"🟢 <b>PAPER ENTRY | {index}</b>\n"
                f"Strategy : {strat_label}\n"
                f"Signal   : {signal}\n"
                f"Option   : {name}\n"
                f"LTP      : {ltp}\n"
                f"Expiry   : {expiry_str}\n"
                f"Delta    : {delta:.3f}   Gamma: {gamma:.5f}\n"
                f"SL       : {data['sl']:.2f}   Target: {data['target']:.2f}\n"
                f"Mode     : SANDBOX (paper trade)"
            )

# ── STARTUP BANNER ────────────────────────────────────────────

send_alert(
    "🚀 <b>SANDBOX BOT STARTED</b>\n"
    "Indices  : NIFTY &amp; BANKNIFTY\n"
    "Feed     : Historical candle polling (1-min)\n"
    "Strategies:\n"
    "  • Order Block\n"
    "  • Breakout Trend Follower\n"
    "  • EMA-20/50 + RSI-14\n"
    "Greeks   : Delta≥0.30, Gamma≤0.05\n"
    "Orders   : PAPER (no real money)\n"
    f"Poll     : every {POLL_SLEEP}s"
)

# ── RUN ───────────────────────────────────────────────────────

print("\n🔁 Starting sandbox polling loop…")

while True:
    now = datetime.now()

    # Market hours: 09:15 – 15:30 IST
    if now.hour < 9 or (now.hour == 9 and now.minute < 15):
        msg = f"[{now.strftime('%H:%M')}] Market not open — waiting for market open at 09:15 IST"
        print(msg)
        log_info(f"Pre-market at {now.strftime('%H:%M')} IST")
        time.sleep(60)
        continue

    if now.hour == 15 and now.minute >= 30:
        send_alert("📅 Market closed. Generating sandbox backtest report…")
        log_info(f"Market closed at {now.strftime('%H:%M')} IST — generating report")
        run_backtest_report()
        print("✅ Bot finished for today.")
        break

    for idx, sid in INDEX_IDS.items():
        try:
            process_index(idx, sid)
        except Exception as e:
            print(f"[ERROR] {idx}: {e}")
            log_error(f"{idx} processing error: {e}")
            send_alert(f"⚠️ {idx} processing error: {e}")

    time.sleep(POLL_SLEEP)