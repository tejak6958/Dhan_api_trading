"""
==============================================================
  DHAN SANDBOX BOT  —  Dhan_api.py
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

  FIXES vs original:
    1. Removed calls to undefined btf_signal() / ema_rsi_signal()
       — now uses the correctly imported breakout_trend_signal()
         and ema_rsi_confirmation() throughout
    2. Added weekend + holiday check in is_market_open()
    3. fetch_candles() exchangeSegment changed to "IDX_I" for indices
    4. Added .env validation at startup with clear error messages
    5. Added safe base_url override with fallback
    6. process_index() now prints status even when market is closed
       so you can see the bot is alive in VSCode terminal
    7. Startup Telegram + print always fires regardless of market hours
==============================================================
"""

import os
import sys
import time
import threading
import logging
import numpy as np
import pandas as pd
import pandas_ta as ta
import requests
from datetime import datetime, timedelta
from scipy.stats import norm
from dhanhq import dhanhq, DhanContext
from dotenv import load_dotenv

# ── STRATEGY MODULES ──────────────────────────────────────────
from strategies_order_block import detect_order_blocks, order_block_signal
from strategies_breakout_trend import breakout_trend_signal
from strategies_ema_rsi import ema_rsi_confirmation

# ── ENV ──────────────────────────────────────────────────────
load_dotenv()
CLIENT_ID    = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BOT_TOKEN    = os.getenv("BOT_TOKEN")
CHAT_ID      = os.getenv("CHAT_ID")

# ── VALIDATE ENV VARIABLES ───────────────────────────────────
def validate_env():
    missing = []
    for name, val in [
        ("CLIENT_ID",    CLIENT_ID),
        ("ACCESS_TOKEN", ACCESS_TOKEN),
        ("BOT_TOKEN",    BOT_TOKEN),
        ("CHAT_ID",      CHAT_ID),
    ]:
        if not val or val.strip() == "":
            missing.append(name)
    if missing:
        print(f"❌ MISSING ENV VARIABLES: {', '.join(missing)}")
        print("   → Check your .env file in the project folder.")
        sys.exit(1)
    print("✅ ENV variables loaded OK")

validate_env()

# ── LOGGING ───────────────────────────────────────────────────
LOG_FILE = "dhan_bot.log"

logger = logging.getLogger("DhanBot")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                      datefmt="%Y-%m-%d %H:%M:%S")
)
logger.addHandler(file_handler)

# Also log to console so VSCode terminal shows activity
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                      datefmt="%H:%M:%S")
)
logger.addHandler(console_handler)

def log_info(msg: str):  logger.info(msg)
def log_error(msg: str): logger.error(msg)

# ── MARKET HOURS CHECK ───────────────────────────────────────
# NSE holidays 2025 (add/remove as needed)
NSE_HOLIDAYS_2025 = {
    "2025-01-26",  # Republic Day
    "2025-03-14",  # Holi
    "2025-04-14",  # Dr. Ambedkar Jayanti
    "2025-04-18",  # Good Friday
    "2025-05-01",  # Maharashtra Day
    "2025-08-15",  # Independence Day
    "2025-10-02",  # Gandhi Jayanti
    "2025-10-24",  # Dussehra
    "2025-11-05",  # Diwali Laxmi Puja
    "2025-11-15",  # Gurunanak Jayanti
    "2025-12-25",  # Christmas
}

def is_market_open() -> bool:
    """
    Returns True only if:
      - Weekday (Mon–Fri)
      - Not an NSE holiday
      - Time is between 09:15 and 15:30 IST
    """
    now = datetime.now()

    # Weekend check
    if now.weekday() >= 5:   # 5=Saturday, 6=Sunday
        return False

    # Holiday check
    today_str = now.strftime("%Y-%m-%d")
    if today_str in NSE_HOLIDAYS_2025:
        return False

    # Time check: 09:15 to 15:30
    start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= now <= end

def market_status_reason() -> str:
    """Human-readable reason why market is closed (for logging)."""
    now = datetime.now()
    if now.weekday() >= 5:
        return f"Weekend ({now.strftime('%A')})"
    today_str = now.strftime("%Y-%m-%d")
    if today_str in NSE_HOLIDAYS_2025:
        return f"NSE Holiday ({today_str})"
    start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now < start:
        return f"Pre-market (opens at 09:15, now {now.strftime('%H:%M')})"
    if now > end:
        return f"Post-market (closed at 15:30, now {now.strftime('%H:%M')})"
    return "Open"

# ── SANDBOX BASE URL ─────────────────────────────────────────
SANDBOX_BASE_URL = "https://sandbox.dhan.co/v2"

# ── TRADING CONFIG ───────────────────────────────────────────
LOT_SIZES   = {"NIFTY": 75, "BANKNIFTY": 30}
RISK_FREE   = 0.068        # ~10yr G-sec yield
IV_ASSUMED  = 0.15         # fallback IV (15%)

MIN_DELTA   = 0.30
MAX_GAMMA   = 0.05

CANDLE_INTERVAL = 1        # 1-min candles
POLL_SLEEP      = 60       # seconds between polls

OB_PERIODS   = 5
OB_THRESHOLD = 0.0

BTF_PVT_LEN = 3
BTF_MA_LEN  = 50
BTF_MA_TYPE = "SMA"

# ── GLOBALS ──────────────────────────────────────────────────
TOTAL_PNL       = 0.0
TRADE_COUNT     = 0
WIN_COUNT       = 0
BACKTEST_TRADES = []
ORDER_LOCK      = threading.Lock()

# ── TELEGRAM ─────────────────────────────────────────────────

def send_alert(msg: str) -> bool:
    try:
        url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
        result = resp.json()
        if result.get("ok"):
            log_info(f"Telegram ✓: {msg[:80].strip()}")
            return True
        log_error(f"Telegram ✗: {result}")
        print(f"[TELEGRAM ✗] {result}")
        return False
    except Exception as e:
        log_error(f"Telegram exception: {e}")
        print(f"[TELEGRAM EXCEPTION] {e}")
        return False

# ── DHAN CLIENT ──────────────────────────────────────────────

dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan = dhanhq(dhan_context)

# Safe base URL override — works across dhanhq library versions
try:
    dhan.dhan_http.base_url = SANDBOX_BASE_URL
    log_info(f"Base URL set via dhan_http: {SANDBOX_BASE_URL}")
except AttributeError:
    try:
        dhan.base_url = SANDBOX_BASE_URL
        log_info(f"Base URL set via dhan.base_url: {SANDBOX_BASE_URL}")
    except AttributeError:
        log_error("⚠️ Could not override base URL — check dhanhq library version")
        print("⚠️  Could not set sandbox base URL. Dhan API calls may go to live endpoint.")

def check_login() -> bool:
    try:
        res = dhan.get_fund_limits()
        if res and res.get("status") == "success":
            bal = res.get("data", {}).get("available_balance", "N/A")
            msg = (
                f"✅ <b>SANDBOX LOGIN OK</b>\n"
                f"Mode    : SANDBOX\n"
                f"Base URL: {SANDBOX_BASE_URL}\n"
                f"Balance : {bal}\n"
                f"Market  : {market_status_reason()}\n"
                f"Note    : WebSocket not supported in sandbox.\n"
                f"          Using historical candle polling instead."
            )
            print(msg)
            ok = send_alert(msg)
            if not ok:
                print("⚠️  Telegram delivery failed — check BOT_TOKEN / CHAT_ID")
            return True
        print("❌ LOGIN FAILED — response:", res)
        log_error(f"Sandbox login failed: {res}")
        return False
    except Exception as e:
        print(f"❌ LOGIN ERROR: {e}")
        log_error(f"Sandbox login error: {e}")
        return False

if not check_login():
    print("Stopping bot — login failed.")
    log_error("Stopping bot because login failed")
    sys.exit(1)

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
        log_info("Scrip master: loaded from local cache")
        send_alert("📂 Scrip master: loaded from local cache")
        return pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)
    log_info("Downloading scrip master CSV…")
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

def fetch_candles(
    security_id: str,
    exchange: str = "IDX_I",      # ← FIX: was "NSE_EQ" — indices must use IDX_I
    instrument: str = "INDEX",
    interval: str = "1",
) -> pd.DataFrame:
    """
    Fetch today's intraday 1-min candles from Dhan sandbox API.
    Returns empty DataFrame if market is closed or API fails.
    Logs reason clearly so VSCode terminal shows what's happening.
    """
    if not is_market_open():
        reason = market_status_reason()
        log_info(f"fetch_candles skipped (market closed): {reason}")
        return pd.DataFrame()

    today = datetime.now().strftime("%Y-%m-%d")
    try:
        url = f"{SANDBOX_BASE_URL}/charts/intraday"
        payload = {
            "securityId"     : security_id,
            "exchangeSegment": exchange,
            "instrument"     : instrument,
            "interval"       : interval,
            "fromDate"       : today,
            "toDate"         : today,
        }
        headers = {
            "access-token": ACCESS_TOKEN,
            "client-id"   : CLIENT_ID,
            "Content-Type": "application/json",
        }
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if not data or "open" not in data:
            log_info(f"fetch_candles: empty response for sid={security_id}")
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
        log_info(f"fetch_candles: got {len(df)} bars for sid={security_id}")
        return df

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        log_error(f"fetch_candles HTTP {status} for sid={security_id}: {e}")
        return pd.DataFrame()
    except Exception as e:
        log_error(f"fetch_candles error for sid={security_id}: {e}")
        return pd.DataFrame()

# ── GREEKS ────────────────────────────────────────────────────

def bs_greeks(S, K, T, r, sigma, option_type="CE"):
    if T <= 0 or sigma <= 0:
        return 0.0, 0.0
    d1    = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
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

# ── COMBINED SIGNAL ───────────────────────────────────────────

def combined_signal(df: pd.DataFrame, index: str, ltp: float):
    """
    Fires only when ≥2 of 3 strategies agree.
    Uses the correctly imported function names throughout.
    Returns 'BUY', 'SELL', or None.
    """
    votes_buy  = 0
    votes_sell = 0
    labels_buy  = []
    labels_sell = []

    # 1 ── Order Block (from strategies_order_block.py)
    ob_sig, bull_ob, bear_ob = order_block_signal(df, index, ltp)
    if ob_sig == "BUY":
        votes_buy  += 1;  labels_buy.append("Order Block")
    elif ob_sig == "SELL":
        votes_sell += 1;  labels_sell.append("Order Block")

    # 2 ── Breakout Trend Follower (from strategies_breakout_trend.py)
    #      FIX: was incorrectly calling undefined btf_signal()
    btf_sig, buy_lvl, stop_lvl = breakout_trend_signal(df, index, ltp)
    if btf_sig == "BUY":
        votes_buy  += 1;  labels_buy.append("BTF")
    elif btf_sig == "SELL":
        votes_sell += 1;  labels_sell.append("BTF")

    # 3 ── EMA / RSI (from strategies_ema_rsi.py)
    #      FIX: was incorrectly calling undefined ema_rsi_signal()
    er_sig = ema_rsi_confirmation(df, index, ltp)
    if er_sig == "BUY":
        votes_buy  += 1;  labels_buy.append("EMA/RSI")
    elif er_sig == "SELL":
        votes_sell += 1;  labels_sell.append("EMA/RSI")

    if votes_buy >= 2:
        return "BUY", " + ".join(labels_buy)
    if votes_sell >= 2:
        return "SELL", " + ".join(labels_sell)
    return None, ""

# ── PAPER ORDER ───────────────────────────────────────────────

def paper_order(sid, qty, side, name):
    ts = datetime.now().strftime("%H:%M:%S")
    msg = f"[PAPER ORDER] {ts} | {side} | {name} | qty={qty} | sid={sid}"
    print(msg)
    log_info(msg)
    return {"status": "success", "orderId": f"PAPER_{ts}"}

# ── BACKTEST REPORT ───────────────────────────────────────────

def run_backtest_report():
    if not BACKTEST_TRADES:
        send_alert("📊 Backtest: No trades captured today.")
        log_info("Backtest report: no trades today")
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
    msg = (f"\n{'='*40}\n"
           f"  SANDBOX DASHBOARD  {datetime.now().strftime('%H:%M:%S')}\n"
           f"  Trades: {TRADE_COUNT}  Wins: {WIN_COUNT}  PnL: ₹{TOTAL_PNL:.2f}\n"
           f"{'='*40}\n")
    print(msg)

# ── INDEX FEED IDs ────────────────────────────────────────────
INDEX_IDS = {
    "NIFTY"    : "13",
    "BANKNIFTY": "25",
}

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
    global TOTAL_PNL, TRADE_COUNT, WIN_COUNT

    df = fetch_candles(sid)

    if df.empty:
        # Market closed or API returned nothing — already logged in fetch_candles
        return

    if len(df) < 60:
        log_info(f"[{index}] Only {len(df)} bars — need 60 to evaluate signals, waiting…")
        return

    ltp = float(df.iloc[-1]["close"])
    log_info(f"[{index}] LTP={ltp:.2f}  bars={len(df)}")

    data = indices[index]

    # ── EXIT CHECK ──────────────────────────────────────────
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
        return   # never open a new trade in the same tick as exit check

    # ── ENTRY CHECK ─────────────────────────────────────────
    signal, strat_label = combined_signal(df, index, ltp)

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
                send_alert(
                    f"⚠️ {index} {name}: Delta {abs_delta:.2f} < {MIN_DELTA} — skip")
                return
            if gamma > MAX_GAMMA:
                send_alert(
                    f"⚠️ {index} {name}: Gamma {gamma:.5f} > {MAX_GAMMA} — skip")
                return

            qty = LOT_SIZES[index]
            data["order_placed"] = True

            paper_order(opt_sid, qty, "BUY", name)

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

startup_msg = (
    "🚀 <b>SANDBOX BOT STARTED</b>\n"
    "Indices  : NIFTY &amp; BANKNIFTY\n"
    "Feed     : Historical candle polling (1-min)\n"
    "Strategies:\n"
    "  • Order Block\n"
    "  • Breakout Trend Follower\n"
    "  • EMA-20/50 + RSI-14\n"
    "Greeks   : Delta≥0.30, Gamma≤0.05\n"
    "Orders   : PAPER (no real money)\n"
    f"Poll     : every {POLL_SLEEP}s\n"
    f"Market   : {market_status_reason()}"
)
print(startup_msg)
send_alert(startup_msg)

# ── RUN ───────────────────────────────────────────────────────

print("\n🔁 Starting sandbox polling loop…")

while True:
    now = datetime.now()

    # ── Weekend / Holiday — skip the whole day ───────────────
    if now.weekday() >= 5:
        reason = "Saturday" if now.weekday() == 5 else "Sunday"
        print(f"[{now.strftime('%H:%M')}] {reason} — bot is idle. "
              f"Sleeping 1 hour.")
        log_info(f"Weekend ({reason}) — sleeping 1h")
        time.sleep(3600)
        continue

    today_str = now.strftime("%Y-%m-%d")
    if today_str in NSE_HOLIDAYS_2025:
        print(f"[{now.strftime('%H:%M')}] NSE Holiday ({today_str}) — sleeping 1 hour.")
        log_info(f"NSE Holiday {today_str} — sleeping 1h")
        time.sleep(3600)
        continue

    # ── Pre-market ───────────────────────────────────────────
    if now.hour < 9 or (now.hour == 9 and now.minute < 15):
        wait_secs = (
            now.replace(hour=9, minute=15, second=0, microsecond=0) - now
        ).seconds
        print(f"[{now.strftime('%H:%M')}] Pre-market — market opens in "
              f"{wait_secs // 60}m {wait_secs % 60}s")
        time.sleep(min(60, wait_secs))
        continue

    # ── Market closed for today ──────────────────────────────
    if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
        send_alert("📅 Market closed. Generating sandbox backtest report…")
        log_info(f"Market closed at {now.strftime('%H:%M')} IST")
        run_backtest_report()
        print("✅ Bot finished for today.")
        break

    # ── Active market hours — poll each index ────────────────
    for idx, sid in INDEX_IDS.items():
        try:
            process_index(idx, sid)
        except Exception as e:
            print(f"[ERROR] {idx}: {e}")
            log_error(f"{idx} processing error: {e}")
            send_alert(f"⚠️ {idx} processing error: {e}")

    time.sleep(POLL_SLEEP)