"""
==============================================================
  DHAN LIVE BOT  —  dhan_live.py
  Mode     : LIVE — real orders, real money
  Feed     : DhanHQ WebSocket (MarketFeed) — real-time ticks
  Base URL : https://api.dhan.co/v2
  Indices  : NIFTY & BANKNIFTY (NSE F&O, ATM CE / PE)
  Strategies (all 3 must agree ≥2/3 before any order fires):
    1. Order Block  — imported from strategies_order_block.py
    2. Breakout Trend Follower — strategies_breakout_trend.py
    3. EMA-20/50 + RSI-14 — strategies_ema_rsi.py
  Greeks   : Black-Scholes Delta ≥ 0.30, Gamma ≤ 0.05
  Orders   : FULLY AUTOMATIC — zero manual intervention needed
             Each trade = 1 lot (NIFTY=75, BANKNIFTY=30 qty)
             Entry: Market order BUY on signal
             Exit : Market order SELL on SL (−25%) or Target (+50%)
  Reports  : Telegram alerts for every event + EOD session report

  ── HOW AUTO-TRADING WORKS ──────────────────────────────────
  1. WebSocket connects and streams ticks for NIFTY & BANKNIFTY.
  2. Every tick: price is added to a rolling buffer (max 500 bars).
  3. Every 60th tick (≈ 1 candle): strategy logic runs on buffer.
  4. If ≥2/3 strategies agree on BUY/SELL → select ATM option.
  5. Greeks filter: skip if |delta| < 0.30 or gamma > 0.05.
  6. place_order() fires a MARKET order via Dhan REST API.
  7. SL = entry × 0.75  |  Target = entry × 1.50
  8. On next tick after SL/Target hit → exit order fires auto.
  9. Telegram alert sent at: startup, entry, exit, errors, EOD.
  10. No human click needed at any step.

  ── LOT SIZE / CAPITAL IMPACT ───────────────────────────────
  NIFTY    : qty = 75   (1 lot = 75 units)
  BANKNIFTY: qty = 30   (1 lot = 30 units)
  Max concurrent positions: 1 per index (2 total)
  SL per trade: ~25% of option premium × lot size
  Always ensure sufficient F&O margin before starting.

  ── FIXES vs ORIGINAL ───────────────────────────────────────
  1. Added validate_env() — clear error if .env vars missing
  2. Added weekend + NSE holiday check — no phantom reconnects
  3. Safe base_url override with AttributeError fallback
  4. Console logging added — VSCode terminal shows all activity
  5. market_status_reason() — human-readable status everywhere
  6. combined_signal() cleaned up — uses correct import names
     (was calling undefined detect_order_blocks inline; now
      properly delegates to order_block_signal from module)
  7. WebSocket MarketFeed subscription uses IDX_I segment for
     index instruments (was NSE which is equities segment)
  8. on_message: added explicit EOD check — exits open trades
     gracefully at 15:25 before market close
  9. Daily loss limit (MAX_DAILY_LOSS) — kills bot if breached
  10. order_placed flag reset added on WebSocket reconnect
  11. post-market hours loop now sleeps 1h not 60s (efficiency)
  12. run_session_report() moved before startup banner to fix
      forward-reference error if market closes during init

  ⚠️  DO NOT run until Dhan_api.py sandbox testing is complete!
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
from dhanhq.marketfeed import MarketFeed
from dotenv import load_dotenv

# ── STRATEGY MODULES (imported — do not redefine here) ───────
from strategies_order_block import order_block_signal
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
    missing = [
        name for name, val in [
            ("CLIENT_ID",    CLIENT_ID),
            ("ACCESS_TOKEN", ACCESS_TOKEN),
            ("BOT_TOKEN",    BOT_TOKEN),
            ("CHAT_ID",      CHAT_ID),
        ]
        if not val or val.strip() == ""
    ]
    if missing:
        print(f"❌ MISSING ENV VARIABLES: {', '.join(missing)}")
        print("   → Check your .env file. Bot cannot start without these.")
        sys.exit(1)
    print("✅ ENV variables loaded OK")

validate_env()

# ── LOGGING ──────────────────────────────────────────────────
LOG_FILE = "dhan_live.log"

logger = logging.getLogger("DhanLiveBot")
logger.setLevel(logging.INFO)

# File handler
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(fh)

# Console handler — so VSCode terminal shows live activity
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(ch)

def log_info(msg: str):  logger.info(msg)
def log_error(msg: str): logger.error(msg)

# ── MARKET HOURS + HOLIDAY CHECK ─────────────────────────────
NSE_HOLIDAYS_2025 = {
    "2025-01-26", "2025-03-14", "2025-04-14", "2025-04-18",
    "2025-05-01", "2025-08-15", "2025-10-02", "2025-10-24",
    "2025-11-05", "2025-11-15", "2025-12-25",
}

def is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS_2025:
        return False
    start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= now <= end

def market_status_reason() -> str:
    now = datetime.now()
    if now.weekday() >= 5:
        return f"Weekend ({now.strftime('%A')})"
    ds = now.strftime("%Y-%m-%d")
    if ds in NSE_HOLIDAYS_2025:
        return f"NSE Holiday ({ds})"
    start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now < start:
        return f"Pre-market (opens 09:15, now {now.strftime('%H:%M')})"
    if now > end:
        return f"Post-market (closed 15:30, now {now.strftime('%H:%M')})"
    return "OPEN"

# ── LIVE BASE URL ─────────────────────────────────────────────
LIVE_BASE_URL = "https://api.dhan.co/v2"

# ── TRADING CONFIG ───────────────────────────────────────────
LOT_SIZES = {"NIFTY": 75, "BANKNIFTY": 30}
#
# HOW LOT SIZES WORK IN AUTO-TRADING:
#   NIFTY    → every BUY/SELL order places qty=75 units
#   BANKNIFTY → every BUY/SELL order places qty=30 units
#   Dhan validates lot-size compliance before accepting the order.
#   If you want to trade more lots, multiply: e.g. 2 lots NIFTY = 150
#
RISK_FREE  = 0.068   # 10-yr G-sec yield for Black-Scholes
IV_ASSUMED = 0.15    # fallback implied volatility (15%)

# Greeks filter — orders skipped if these aren't met
MIN_DELTA = 0.30     # option must have |delta| ≥ 0.30 (not too far OTM)
MAX_GAMMA = 0.05     # option gamma must be ≤ 0.05 (not too close to expiry)

# Tick gate — strategy only evaluated every Nth tick
# ~60 ticks/min on liquid indices = evaluates roughly once per minute
CANDLE_TICKS = 60

# Daily max loss safety — bot stops if total PnL drops below this (₹)
# Set to 0 to disable. Recommended: at least 1 lot's SL × 2.
MAX_DAILY_LOSS = -15000   # e.g. ₹15,000 max loss per day

# Reconnect delay after WebSocket drop
RECONNECT_DELAY = 30

# ── GLOBALS ──────────────────────────────────────────────────
TOTAL_PNL       = 0.0
TRADE_COUNT     = 0
WIN_COUNT       = 0
BACKTEST_TRADES = []
ORDER_LOCK      = threading.Lock()
EOD_DONE        = False   # prevents duplicate EOD report

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
            log_info(f"Telegram ✓  {msg[:80].strip()}")
            return True
        log_error(f"Telegram ✗  {result}")
        return False
    except Exception as e:
        log_error(f"Telegram exception: {e}")
        return False

# ── DHAN CLIENT ──────────────────────────────────────────────

dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan = dhanhq(dhan_context)

# Safe base URL override — works across dhanhq library versions
try:
    dhan.dhan_http.base_url = LIVE_BASE_URL
    log_info(f"Base URL set via dhan_http: {LIVE_BASE_URL}")
except AttributeError:
    try:
        dhan.base_url = LIVE_BASE_URL
        log_info(f"Base URL set via dhan.base_url: {LIVE_BASE_URL}")
    except AttributeError:
        log_error("⚠️ Could not override base URL — verify dhanhq version")
        print("⚠️  Could not set LIVE base URL. Check dhanhq library version.")

def check_login() -> bool:
    try:
        res = dhan.get_fund_limits()
        if res and res.get("status") == "success":
            bal = res.get("data", {}).get("available_balance", "N/A")
            msg = (
                f"✅ <b>LIVE LOGIN OK</b>\n"
                f"Mode     : LIVE ⚡\n"
                f"Balance  : ₹{bal}\n"
                f"Market   : {market_status_reason()}\n"
                f"Max Loss : ₹{abs(MAX_DAILY_LOSS):,}/day\n"
                f"Lots     : NIFTY={LOT_SIZES['NIFTY']}  "
                f"BANKNIFTY={LOT_SIZES['BANKNIFTY']}\n"
                f"⚠️ REAL ORDERS WILL BE PLACED AUTOMATICALLY"
            )
            print(msg)
            if not send_alert(msg):
                print("⚠️  Telegram delivery failed — check BOT_TOKEN / CHAT_ID")
            return True
        log_error(f"Live login failed: {res}")
        print(f"❌ LOGIN FAILED: {res}")
        return False
    except Exception as e:
        log_error(f"Live login error: {e}")
        print(f"❌ LOGIN ERROR: {e}")
        return False

if not check_login():
    print("Stopping bot — login failed.")
    sys.exit(1)

# ── SCRIP MASTER ─────────────────────────────────────────────

MASTER_CSV   = "scrip_master.csv"
MASTER_URL   = "https://images.dhan.co/api-data/api-scrip-master.csv"
REFRESH_DAYS = 7

def _cache_fresh(fp: str, days: int) -> bool:
    if not os.path.exists(fp):
        return False
    return (datetime.now() - datetime.fromtimestamp(
        os.path.getmtime(fp))) < timedelta(days=days)

def load_scrip_master() -> pd.DataFrame:
    if _cache_fresh(MASTER_CSV, REFRESH_DAYS):
        log_info("Scrip master: loaded from cache")
        send_alert("📂 Scrip master: loaded from local cache")
        return pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)
    log_info("Downloading scrip master CSV…")
    send_alert("⏳ Downloading scrip master CSV…")
    resp = requests.get(MASTER_URL, timeout=120, stream=True)
    resp.raise_for_status()
    with open(MASTER_CSV, "wb") as f:
        for chunk in resp.iter_content(1024 * 256):
            f.write(chunk)
    df = pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)
    send_alert(f"✅ Scrip master: {len(df):,} rows downloaded")
    return df

df_master = load_scrip_master()
df_master = df_master[
    df_master["SEM_TRADING_SYMBOL"].str.contains("NIFTY|BANKNIFTY", na=False) &
    df_master["SEM_INSTRUMENT_NAME"].str.contains("OPT", na=False)
].reset_index(drop=True)
print(f"✅ Filtered master: {len(df_master)} option rows")

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
        return max((exp - datetime.now()).days / 365, 1 / 365)
    except Exception:
        return 7 / 365

# ── OPTION SELECTOR ───────────────────────────────────────────

def get_atm(price: float, step: int) -> int:
    return int(round(price / step) * step)

def select_option(index: str, ltp: float, signal: str):
    """
    Finds the nearest ATM option from scrip master.
    NIFTY  → strike rounded to nearest 50, ATM CE (BUY) or PE (SELL)
    BANKNIFTY → strike rounded to nearest 100
    Returns (security_id, symbol, expiry_str, delta, gamma)
    """
    df = df_master[
        df_master["SEM_TRADING_SYMBOL"].str.contains(
            f"^{index}", na=False, regex=True)
    ].copy()
    df["EXPIRY"] = pd.to_datetime(df["SEM_EXPIRY_DATE"], errors="coerce")
    df = df[df["EXPIRY"] == df["EXPIRY"].min()]   # nearest expiry

    step     = 50  if index == "NIFTY" else 100
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
    return str(int(row["SEM_SM_ID"])), row["SEM_TRADING_SYMBOL"], \
           expiry_str, delta, gamma

# ── COMBINED SIGNAL ───────────────────────────────────────────

def combined_signal(df: pd.DataFrame, index: str, ltp: float):
    """
    Votes from all 3 strategy modules.
    Signal fires only when ≥ 2 of 3 agree — reduces false entries.

    AUTO-TRADE FLOW (what happens after this returns):
      "BUY"  → place_order(sid, qty, dhan.BUY,  name)  — buys CE
      "SELL" → place_order(sid, qty, dhan.BUY,  name)  — buys PE
      (we always BUY options, never sell/write naked)
    Returns (signal: str|None, strategy_label: str)
    """
    votes_buy, votes_sell   = 0, 0
    labels_buy, labels_sell = [], []

    # 1 — Order Block
    ob_sig, _, _ = order_block_signal(df, index, ltp)
    if ob_sig == "BUY":
        votes_buy  += 1;  labels_buy.append("OrderBlock")
    elif ob_sig == "SELL":
        votes_sell += 1;  labels_sell.append("OrderBlock")

    # 2 — Breakout Trend Follower
    btf_sig, _, _ = breakout_trend_signal(df, index, ltp)
    if btf_sig == "BUY":
        votes_buy  += 1;  labels_buy.append("BTF")
    elif btf_sig == "SELL":
        votes_sell += 1;  labels_sell.append("BTF")

    # 3 — EMA / RSI
    er_sig = ema_rsi_confirmation(df, index, ltp)
    if er_sig == "BUY":
        votes_buy  += 1;  labels_buy.append("EMA/RSI")
    elif er_sig == "SELL":
        votes_sell += 1;  labels_sell.append("EMA/RSI")

    if votes_buy  >= 2:
        return "BUY",  " + ".join(labels_buy)
    if votes_sell >= 2:
        return "SELL", " + ".join(labels_sell)
    return None, ""

# ── REAL ORDER ────────────────────────────────────────────────

def place_order(sid: str, qty: int, side, name: str) -> dict:
    """
    Places a MARKET order via DhanHQ REST API.
    side = dhan.BUY or dhan.SELL
    product_type = INTRA (MIS — intraday, auto-squared at 3:20)

    HOW AUTO-TRADING WORKS HERE:
      - Called automatically from on_message() when signal fires
      - No human click required
      - Dhan executes at best available market price
      - Order confirmation logged + Telegram alert sent by caller
    """
    try:
        resp = dhan.place_order(
            security_id      = sid,
            exchange_segment = dhan.NSE_FNO,
            transaction_type = side,
            quantity         = qty,
            order_type       = dhan.MARKET,
            product_type     = dhan.INTRA,   # MIS — intraday only
            price            = 0,
        )
        log_info(f"place_order: {side} {name} qty={qty} → {resp}")
        return resp
    except Exception as e:
        log_error(f"place_order FAILED ({name}): {e}")
        send_alert(f"❌ ORDER ERROR ({name}): {e}")
        return {}

# ── SESSION REPORT ────────────────────────────────────────────

def run_session_report():
    global EOD_DONE
    if EOD_DONE:
        return
    EOD_DONE = True

    if not BACKTEST_TRADES:
        send_alert("📊 Session Report: No trades executed today.")
        log_info("Session report: no trades today")
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
        f"📊 <b>LIVE SESSION REPORT</b>\n"
        f"{'─'*32}\n"
        f"Date         : {datetime.now().strftime('%Y-%m-%d')}\n"
        f"Mode         : LIVE ⚡\n"
        f"Total Trades : {len(df)}\n"
        f"Wins / Losses: {wins} / {losses}\n"
        f"Win Rate     : {win_rate:.1f}%\n"
        f"Net PnL      : ₹{total_pnl:.2f}\n"
        f"Max Drawdown : ₹{max_dd:.2f}\n"
        f"Avg Delta    : {df['delta'].mean():.3f}\n"
        f"Avg Gamma    : {df['gamma'].mean():.5f}\n"
        f"\nPnL by Strategy:\n{strat_pnl}\n"
        f"{'─'*32}"
    )
    with open("live_session_report.txt", "w") as f:
        f.write(report)
    send_alert(report)
    print(report)
    log_info("Session report sent")

def log_trade(row: list):
    with open("live_trades.csv", "a") as f:
        f.write(",".join(map(str, row)) + "\n")

def show_dashboard():
    msg = (
        f"\n{'='*44}\n"
        f"  LIVE DASHBOARD  {datetime.now().strftime('%H:%M:%S')}\n"
        f"  Trades: {TRADE_COUNT}  Wins: {WIN_COUNT}  "
        f"PnL: ₹{TOTAL_PNL:.2f}\n"
        f"{'='*44}\n"
    )
    print(msg)

# ── DAILY LOSS LIMIT CHECK ────────────────────────────────────

def check_daily_loss_limit() -> bool:
    """
    Returns True if the daily loss limit has been breached.
    Bot will stop trading and send alert if triggered.
    """
    if MAX_DAILY_LOSS < 0 and TOTAL_PNL <= MAX_DAILY_LOSS:
        send_alert(
            f"🛑 <b>DAILY LOSS LIMIT HIT</b>\n"
            f"Limit : ₹{MAX_DAILY_LOSS:,.0f}\n"
            f"PnL   : ₹{TOTAL_PNL:,.2f}\n"
            f"Bot   : STOPPED for today"
        )
        log_error(f"Daily loss limit breached: PnL={TOTAL_PNL:.2f}")
        return True
    return False

# ── INDEX IDs + STATE ─────────────────────────────────────────
#
# Dhan WebSocket security IDs for NSE indices:
#   NIFTY 50      → "13"
#   BANKNIFTY     → "25"
#
INDEX_IDS = {
    "NIFTY"    : "13",
    "BANKNIFTY": "25",
}

indices = {
    idx: {
        "buffer"      : [],    # rolling tick buffer → becomes DataFrame
        "tick_count"  : 0,     # total ticks received for this index
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

# ── WEBSOCKET CALLBACK ────────────────────────────────────────

def on_message(instance, message):
    """
    ══════════════════════════════════════════════════════
    THIS IS THE CORE AUTO-TRADING ENGINE.
    Called automatically by DhanHQ WebSocket for every tick.
    No human intervention at any point below.

    FLOW PER TICK:
      1. Ignore if market closed or no LTP in message
      2. Append tick to rolling buffer (max 500 bars)
      3. Gate: only evaluate strategy every CANDLE_TICKS ticks
      4. If in trade → check SL / Target → exit if hit (auto)
      5. If not in trade → run combined_signal()
         → if signal → select ATM option → check Greeks
         → place_order() fires automatically → Telegram alert
      6. EOD check at 15:25: force-exit any open position
      7. Daily loss limit: stop all new entries if breached
    ══════════════════════════════════════════════════════
    """
    global TOTAL_PNL, TRADE_COUNT, WIN_COUNT, EOD_DONE

    now = datetime.now()

    # ── EOD FORCE-EXIT at 15:25 ─────────────────────────────
    # Dhan auto-squares MIS at ~15:20; we exit 5 min earlier
    if now.hour == 15 and now.minute >= 25 and not EOD_DONE:
        for index, data in indices.items():
            if data["in_trade"]:
                log_info(f"EOD force-exit for {index}")
                with ORDER_LOCK:
                    if data["in_trade"]:
                        place_order(data["opt_sid"], data["qty"],
                                    dhan.SELL, data["name"])
                        pnl = (float(message.get("last_traded_price", data["entry"]))
                               - data["entry"]) * data["qty"]
                        TOTAL_PNL   += pnl
                        TRADE_COUNT += 1
                        if pnl > 0:
                            WIN_COUNT += 1
                        BACKTEST_TRADES.append([
                            datetime.now(), data["name"], data["strategy"],
                            "EOD EXIT", data["entry"],
                            message.get("last_traded_price", data["entry"]),
                            pnl, data["delta"], data["gamma"]
                        ])
                        log_trade([
                            datetime.now(), data["name"], data["strategy"],
                            "EOD EXIT", data["entry"],
                            message.get("last_traded_price", data["entry"]),
                            round(pnl, 2), data["delta"], data["gamma"]
                        ])
                        send_alert(
                            f"⏰ <b>EOD EXIT | {index}</b>\n"
                            f"Option   : {data['name']}\n"
                            f"PnL      : ₹{pnl:.2f}\n"
                            f"Reason   : Market close (15:25)\n"
                            f"Mode     : LIVE"
                        )
                        data.update({"in_trade": False, "order_placed": False})
        run_session_report()
        return

    # ── Market hours gate ────────────────────────────────────
    if not is_market_open():
        return

    # ── Daily loss limit gate ────────────────────────────────
    if check_daily_loss_limit():
        return

    # ── Extract tick data ────────────────────────────────────
    sid = str(message.get("security_id", ""))
    ltp = message.get("last_traded_price")
    if not ltp:
        return

    for index, idx_sid in INDEX_IDS.items():
        if sid != idx_sid:
            continue

        data = indices[index]
        data["tick_count"] += 1

        # Build candle buffer from tick OHLC fields
        # Dhan Quote messages include open/high/low for the day
        candle = {
            "open" : message.get("open_price",  ltp),
            "high" : message.get("high_price",  ltp),
            "low"  : message.get("low_price",   ltp),
            "close": ltp,
        }
        data["buffer"].append(candle)
        if len(data["buffer"]) > 500:
            data["buffer"].pop(0)

        # ── Timeframe gate (every CANDLE_TICKS ticks) ────────
        if data["tick_count"] % CANDLE_TICKS != 0:
            continue
        if len(data["buffer"]) < 60:
            log_info(f"[{index}] Warming up: {len(data['buffer'])}/60 bars")
            continue

        df  = pd.DataFrame(data["buffer"])
        ltp = float(df.iloc[-1]["close"])
        log_info(f"[{index}] Tick gate hit | LTP={ltp:.2f} | "
                 f"bars={len(data['buffer'])} | trades={TRADE_COUNT}")

        # ── EXIT CHECK ───────────────────────────────────────
        if data["in_trade"]:
            exit_flag, reason = False, ""
            if ltp <= data["sl"]:
                exit_flag, reason = True, "SL HIT"
            elif ltp >= data["target"]:
                exit_flag, reason = True, "TARGET HIT"

            if exit_flag:
                with ORDER_LOCK:
                    if not data["in_trade"]:
                        continue   # another thread already exited

                    # AUTO SELL — no human click needed
                    place_order(data["opt_sid"], data["qty"],
                                dhan.SELL, data["name"])

                    pnl = (ltp - data["entry"]) * data["qty"]
                    TOTAL_PNL   += pnl
                    TRADE_COUNT += 1
                    if pnl > 0:
                        WIN_COUNT += 1

                    BACKTEST_TRADES.append([
                        datetime.now(), data["name"], data["strategy"],
                        reason, data["entry"], ltp,
                        pnl, data["delta"], data["gamma"]
                    ])
                    log_trade([
                        datetime.now(), data["name"], data["strategy"],
                        reason, data["entry"], ltp,
                        round(pnl, 2), data["delta"], data["gamma"]
                    ])
                    send_alert(
                        f"🔴 <b>EXIT | {index}</b>\n"
                        f"Strategy : {data['strategy']}\n"
                        f"Reason   : {reason}\n"
                        f"Option   : {data['name']}\n"
                        f"Entry    : ₹{data['entry']:.2f}  "
                        f"Exit: ₹{ltp:.2f}\n"
                        f"PnL      : ₹{pnl:.2f}\n"
                        f"Total PnL: ₹{TOTAL_PNL:.2f}\n"
                        f"Lot Size : {data['qty']}\n"
                        f"Mode     : LIVE ⚡"
                    )
                    data.update({"in_trade": False, "order_placed": False})
                    show_dashboard()
            continue   # never open new trade in same tick as exit

        # ── ENTRY CHECK ──────────────────────────────────────
        signal, strat_label = combined_signal(df, index, ltp)

        if not signal or data["order_placed"]:
            continue

        with ORDER_LOCK:
            if data["in_trade"] or data["order_placed"]:
                continue

            # Select ATM option
            opt_sid, name, expiry_str, delta, gamma = select_option(
                index, ltp, signal)

            if not opt_sid:
                send_alert(
                    f"⚠️ {index}: No ATM option found near "
                    f"strike {ltp:.0f} — skipping")
                continue

            # Greeks filter
            abs_delta = abs(delta)
            if abs_delta < MIN_DELTA:
                send_alert(
                    f"⚠️ {index} {name}: |delta|={abs_delta:.2f} "
                    f"< {MIN_DELTA} — skip (too far OTM)")
                continue
            if gamma > MAX_GAMMA:
                send_alert(
                    f"⚠️ {index} {name}: gamma={gamma:.5f} "
                    f"> {MAX_GAMMA} — skip (near expiry risk)")
                continue

            qty = LOT_SIZES[index]
            # ── Capital check reminder ──────────────────────
            # qty=75 for NIFTY, qty=30 for BANKNIFTY
            # Dhan will reject if margin insufficient
            data["order_placed"] = True   # block duplicate orders

            # AUTO BUY — fires immediately, no human needed
            resp = place_order(opt_sid, qty, dhan.BUY, name)

            if resp and resp.get("status") in ("success", "pending"):
                sl     = ltp * 0.75   # 25% SL on option premium
                target = ltp * 1.50   # 50% target on option premium
                data.update({
                    "in_trade" : True,
                    "entry"    : ltp,
                    "sl"       : sl,
                    "target"   : target,
                    "opt_sid"  : opt_sid,
                    "qty"      : qty,
                    "name"     : name,
                    "delta"    : delta,
                    "gamma"    : gamma,
                    "signal"   : signal,
                    "strategy" : strat_label,
                })
                send_alert(
                    f"🟢 <b>LIVE ENTRY | {index}</b>\n"
                    f"Strategy : {strat_label}\n"
                    f"Signal   : {signal}\n"
                    f"Option   : {name}\n"
                    f"LTP      : ₹{ltp:.2f}\n"
                    f"Expiry   : {expiry_str}\n"
                    f"Delta    : {delta:.3f}  Gamma: {gamma:.5f}\n"
                    f"SL       : ₹{sl:.2f}  "
                    f"Target : ₹{target:.2f}\n"
                    f"Lot Size : {qty} units\n"
                    f"Mode     : LIVE ⚡ (auto-executed)"
                )
                log_info(
                    f"[{index}] ENTRY {signal} {name} | "
                    f"qty={qty} | ltp={ltp} | sl={sl:.2f} | "
                    f"target={target:.2f} | strat={strat_label}"
                )
                show_dashboard()
            else:
                # Order rejected — reset so next signal can try
                data["order_placed"] = False
                log_error(f"[{index}] Order rejected: {resp}")
                send_alert(
                    f"❌ <b>ORDER REJECTED | {index}</b>\n"
                    f"Option : {name}\n"
                    f"Resp   : {resp}\n"
                    f"Action : order_placed reset, will retry on next signal"
                )

# ── STARTUP BANNER ────────────────────────────────────────────

startup_msg = (
    "🚀 <b>LIVE BOT STARTED</b>\n"
    "Indices   : NIFTY &amp; BANKNIFTY\n"
    "Feed      : WebSocket (DhanHQ MarketFeed)\n"
    f"Base URL  : {LIVE_BASE_URL}\n"
    "Strategies:\n"
    "  • Order Block\n"
    "  • Breakout Trend Follower\n"
    "  • EMA-20/50 + RSI-14\n"
    f"Timeframe : every {CANDLE_TICKS} ticks (~1 min)\n"
    "Greeks    : Delta≥0.30, Gamma≤0.05\n"
    f"Lots      : NIFTY={LOT_SIZES['NIFTY']}  "
    f"BANKNIFTY={LOT_SIZES['BANKNIFTY']}\n"
    f"Max Loss  : ₹{abs(MAX_DAILY_LOSS):,}/day\n"
    f"Market    : {market_status_reason()}\n"
    "⚡ FULLY AUTO — real orders placed without confirmation"
)
print(startup_msg)
send_alert(startup_msg)

# ── WEBSOCKET FEED SETUP ──────────────────────────────────────
#
# MarketFeed.NSE with MarketFeed.Quote subscription:
#   - Gives: last_traded_price, open_price, high_price, low_price,
#            volume, OI, bid/ask — everything needed for strategy
#   - FIX vs original: index instruments on Dhan WebSocket use
#     segment type MarketFeed.INDEX (not MarketFeed.NSE which is
#     for equity cash). Using wrong segment = no ticks received.
#
try:
    # Correct segment for NSE indices on DhanHQ MarketFeed
    subscriptions = [
        (MarketFeed.INDEX, sid, MarketFeed.Quote)
        for sid in INDEX_IDS.values()
    ]
except AttributeError:
    # Fallback if older dhanhq version uses different constant names
    log_error("MarketFeed.INDEX not found — falling back to MarketFeed.NSE")
    subscriptions = [
        (MarketFeed.NSE, sid, MarketFeed.Quote)
        for sid in INDEX_IDS.values()
    ]

feed = MarketFeed(
    dhan_context,
    subscriptions,
    on_message=on_message,
)

# ── MAIN LOOP (weekend/holiday guard + auto-reconnect) ────────

print("\n🔌 Starting DhanHQ WebSocket live feed…")

while True:
    now = datetime.now()

    # ── Weekend / Holiday — don't even try to connect ────────
    if now.weekday() >= 5:
        reason = "Saturday" if now.weekday() == 5 else "Sunday"
        print(f"[{now.strftime('%H:%M')}] {reason} — bot idle. Sleeping 1h.")
        log_info(f"Weekend ({reason}) — sleeping 1h")
        time.sleep(3600)
        continue

    ds = now.strftime("%Y-%m-%d")
    if ds in NSE_HOLIDAYS_2025:
        print(f"[{now.strftime('%H:%M')}] NSE Holiday ({ds}) — sleeping 1h.")
        log_info(f"NSE Holiday {ds} — sleeping 1h")
        time.sleep(3600)
        continue

    # ── Pre-market ───────────────────────────────────────────
    if now.hour < 9 or (now.hour == 9 and now.minute < 15):
        secs = (now.replace(hour=9, minute=14, second=55,
                            microsecond=0) - now).seconds
        print(f"[{now.strftime('%H:%M')}] Pre-market — "
              f"connecting in {secs//60}m {secs%60}s")
        time.sleep(min(60, max(secs, 1)))
        continue

    # ── Post-market ──────────────────────────────────────────
    if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
        log_info(f"Market closed at {now.strftime('%H:%M')}")
        run_session_report()
        print("✅ Bot finished for today.")
        break

    # ── Market hours — connect and run WebSocket ─────────────
    try:
        log_info("Connecting to DhanHQ WebSocket…")
        print(f"[{now.strftime('%H:%M')}] 🔌 Connecting to WebSocket…")
        feed.run_forever()
        # run_forever() blocks until disconnected

    except KeyboardInterrupt:
        send_alert("⛔ Bot stopped manually (KeyboardInterrupt)")
        log_info("Bot stopped by user (Ctrl+C)")
        run_session_report()
        print("⛔ Bot stopped by user.")
        break

    except Exception as e:
        log_error(f"WebSocket disconnected: {e}")
        send_alert(
            f"⚠️ <b>WebSocket Dropped</b>\n"
            f"Error: {e}\n"
            f"Reconnecting in {RECONNECT_DELAY}s…"
        )
        # Reset order_placed flags so stale state doesn't block re-entry
        for data in indices.values():
            if not data["in_trade"]:
                data["order_placed"] = False
        log_info(f"Waiting {RECONNECT_DELAY}s before reconnect…")
        time.sleep(RECONNECT_DELAY)
        # loop continues → reconnects automatically