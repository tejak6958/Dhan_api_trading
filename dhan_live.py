"""
==============================================================
  DHAN LIVE BOT  —  dhan_live.py
  Mode : LIVE trading on DhanHQ
  Feed : WebSocket (DhanHQ MarketFeed — real-time tick stream)
  Base : https://api.dhan.co/v2   (official live endpoint)
  Strategies:
    1. Order Block  (Bullish / Bearish)
    2. Breakout Trend Follower  (Swing-High breakout + MA filter)
    3. EMA-20/50 crossover + RSI-14  (momentum confirmation)
  Options : NSE FnO — NIFTY & BANKNIFTY ATM CE/PE
  Greeks  : Black-Scholes Delta & Gamma filter
  Orders  : REAL orders via DhanHQ API
  Reports : Telegram alerts + EOD backtest/session summary
  ⚠️  DO NOT run this file until sandbox testing is complete!
==============================================================
"""

import os, time, threading
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

# ── LOGGING ──────────────────────────────────────────────────────
LOG_FILE = "dhan_live.log"

logger = logging.getLogger("DhanLiveBot")
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
    market_open = now.hour > 9 or (now.hour == 9 and now.minute >= 15)
    market_close = now.hour < 15 or (now.hour == 15 and now.minute < 30)
    return market_open and market_close

# ── LIVE BASE URL ─────────────────────────────────────────────
LIVE_BASE_URL = "https://api.dhan.co/v2"

# ── TRADING CONFIG ───────────────────────────────────────────
LOT_SIZES   = {"NIFTY": 75, "BANKNIFTY": 30}
RISK_FREE   = 0.068        # ~10yr G-sec yield
IV_ASSUMED  = 0.15         # fallback IV (15%)

# Greeks filter
MIN_DELTA   = 0.30
MAX_GAMMA   = 0.05

# Timeframe gate: only evaluate signal every N WebSocket ticks
CANDLE_TICKS = 60          # ~60 ticks per "candle" bar

# Order Block settings
OB_PERIODS   = 5
OB_THRESHOLD = 0.0

# Breakout Trend Follower settings
BTF_PVT_LEN = 3
BTF_MA_LEN  = 50
BTF_MA_TYPE = "SMA"

# ── GLOBALS ──────────────────────────────────────────────────
TOTAL_PNL       = 0.0
TRADE_COUNT     = 0
WIN_COUNT       = 0
BACKTEST_TRADES = []        # session trade log for EOD report
ORDER_LOCK      = threading.Lock()

# ── TELEGRAM ─────────────────────────────────────────────────

def send_alert(msg: str) -> bool:
    """Send message to Telegram; return True if delivered."""
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
# ← Explicitly set LIVE base URL (never point to sandbox in this file)
dhan.dhan_http.base_url = LIVE_BASE_URL

def check_login() -> bool:
    try:
        res = dhan.get_fund_limits()
        if res and res.get("status") == "success":
            bal = res.get("data", {}).get("available_balance", "N/A")
            msg = (f"✅ <b>LIVE LOGIN OK</b>\n"
                   f"Mode    : LIVE\n"
                   f"Base URL: {LIVE_BASE_URL}\n"
                   f"Balance : ₹{bal}\n"
                   f"⚠️ Real orders will be placed!")
            print(msg)
            ok = send_alert(msg)
            if not ok:
                print("⚠️  Telegram delivery failed — check BOT_TOKEN / CHAT_ID")
            return True
        print("❌ LOGIN FAILED")
        log_error("Live login failed")
        return False
    except Exception as e:
        print(f"❌ LOGIN ERROR: {e}")
        log_error(f"Live login error: {e}")
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

# ── ORDER BLOCK DETECTOR ──────────────────────────────────────

def detect_order_blocks(df: pd.DataFrame, periods: int = OB_PERIODS,
                         threshold: float = OB_THRESHOLD):
    """
    Python translation of Pine Order Block Finder (OrderBlock.txt).
    Bullish OB : last red candle before `periods` consecutive green candles.
    Bearish OB : last green candle before `periods` consecutive red candles.
    Returns (bull_ob_dict | None, bear_ob_dict | None).
    """
    if len(df) < periods + 2:
        return None, None

    ob_candle = df.iloc[-(periods + 1)]
    abs_move  = abs((df.iloc[-1]["close"] - ob_candle["close"]) /
                    ob_candle["close"]) * 100
    rel_move  = abs_move >= threshold
    tail      = df.iloc[-periods:]

    bull_ob = None
    if ob_candle["close"] < ob_candle["open"] and rel_move:
        if (tail["close"] > tail["open"]).sum() == periods:
            bull_ob = {
                "high": ob_candle["open"],
                "low" : ob_candle["low"],
                "avg" : (ob_candle["open"] + ob_candle["low"]) / 2,
            }

    bear_ob = None
    if ob_candle["close"] > ob_candle["open"] and rel_move:
        if (tail["close"] < tail["open"]).sum() == periods:
            bear_ob = {
                "high": ob_candle["high"],
                "low" : ob_candle["open"],
                "avg" : (ob_candle["high"] + ob_candle["open"]) / 2,
            }

    return bull_ob, bear_ob

# ── BREAKOUT TREND FOLLOWER ───────────────────────────────────

def pivot_high(highs, pvt_len=BTF_PVT_LEN):
    if len(highs) < 2 * pvt_len + 1:
        return None
    for i in range(len(highs) - pvt_len - 1, pvt_len - 1, -1):
        if all(highs[i] >= highs[i - j] for j in range(1, pvt_len + 1)) and \
           all(highs[i] >= highs[i + j] for j in range(1, pvt_len + 1)):
            return highs[i]
    return None

def pivot_low(lows, pvt_len=BTF_PVT_LEN):
    if len(lows) < 2 * pvt_len + 1:
        return None
    for i in range(len(lows) - pvt_len - 1, pvt_len - 1, -1):
        if all(lows[i] <= lows[i - j] for j in range(1, pvt_len + 1)) and \
           all(lows[i] <= lows[i + j] for j in range(1, pvt_len + 1)):
            return lows[i]
    return None

def btf_signal(df: pd.DataFrame) -> tuple:
    """
    Breakout Trend Follower signal (Python port of Breakout_Trend_follower.txt).
    Returns (signal, buy_level, stop_level).
    """
    if len(df) < BTF_MA_LEN + 2 * BTF_PVT_LEN + 2:
        return None, None, None

    highs  = df["high"].tolist()
    lows   = df["low"].tolist()

    if BTF_MA_TYPE == "EMA":
        ma_val = df["close"].ewm(span=BTF_MA_LEN, adjust=False).mean().iloc[-1]
    else:
        ma_val = df["close"].rolling(BTF_MA_LEN).mean().iloc[-1]

    buy_level  = pivot_high(highs)
    stop_level = pivot_low(lows)

    if buy_level is None or stop_level is None:
        return None, None, None

    if highs[-1] > buy_level and df["close"].iloc[-1] > ma_val:
        return "BUY", buy_level, stop_level
    if lows[-1] < stop_level:
        return "SELL", buy_level, stop_level
    return None, buy_level, stop_level

# ── EMA + RSI ─────────────────────────────────────────────────

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
    Fire only when ≥2 of 3 strategies agree.
    Uses imported strategy modules for cleaner code organization.
    Returns ('BUY'|'SELL'|None, strategy_label).
    """
    votes_buy, votes_sell = 0, 0
    labels_buy, labels_sell = [], []

    # 1 ── Order Block
    ob_sig, bull_ob, bear_ob = order_block_signal(df, index, ltp)
    if ob_sig == "BUY":
        votes_buy += 1
        labels_buy.append("Order Block")
    elif ob_sig == "SELL":
        votes_sell += 1
        labels_sell.append("Order Block")

    # 2 ── Breakout Trend Follower
    btf_sig, buy_lvl, stop_lvl = breakout_trend_signal(df, index, ltp)
    if btf_sig == "BUY":
        votes_buy += 1
        labels_buy.append("BTF")
    elif btf_sig == "SELL":
        votes_sell += 1
        labels_sell.append("BTF")

    # 3 ── EMA/RSI
    er_sig = ema_rsi_confirmation(df, index, ltp)
    if er_sig == "BUY":
        votes_buy += 1
        labels_buy.append("EMA/RSI")
    elif er_sig == "SELL":
        votes_sell += 1
        labels_sell.append("EMA/RSI")

    if votes_buy >= 2:
        return "BUY", " + ".join(labels_buy)
    if votes_sell >= 2:
        return "SELL", " + ".join(labels_sell)
    return None, ""

# ── REAL ORDER ────────────────────────────────────────────────

def place_order(sid: str, qty: int, side, name: str):
    try:
        resp = dhan.place_order(
            security_id      = sid,
            exchange_segment = dhan.NSE_FNO,
            transaction_type = side,
            quantity         = qty,
            order_type       = dhan.MARKET,
            product_type     = dhan.INTRA,
            price            = 0
        )
        return resp
    except Exception as e:
        send_alert(f"❌ ORDER ERROR ({name}): {e}")
        return None

# ── LOGGING + DASHBOARD ───────────────────────────────────────

def log_trade(row: list):
    with open("live_trades.csv", "a") as f:
        f.write(",".join(map(str, row)) + "\n")

def show_dashboard():
    print(f"\n{'='*42}")
    print(f"  LIVE DASHBOARD  {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Trades: {TRADE_COUNT}  Wins: {WIN_COUNT}  PnL: ₹{TOTAL_PNL:.2f}")
    print(f"{'='*42}\n")

# ── SESSION / BACKTEST REPORT ─────────────────────────────────

def run_session_report():
    if not BACKTEST_TRADES:
        send_alert("📊 Session Report: No trades today.")
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
        f"Mode         : LIVE\n"
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
    with open("live_session_report.txt", "w") as f:
        f.write(report)
    send_alert(report)
    print(report)

# ── INDEX IDs + STATE ─────────────────────────────────────────
# Correct Dhan security IDs for index WebSocket feed
INDEX_IDS = {
    "NIFTY"    : "13",
    "BANKNIFTY": "25",
}

indices = {
    idx: {
        "buffer"      : [],      # rolling tick buffer
        "tick_count"  : 0,
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
    Called for every live tick from DhanHQ WebSocket MarketFeed.
    Thread-safe: ORDER_LOCK prevents simultaneous orders.
    Timeframe gate: signal evaluated only every CANDLE_TICKS ticks.
    Respects market hours: no orders placed outside 09:15-15:30 IST.
    """
    global TOTAL_PNL, TRADE_COUNT, WIN_COUNT
    
    # Skip processing if market is not open
    if not is_market_open():
        return

    sid = str(message.get("security_id", ""))
    ltp = message.get("last_traded_price")
    if not ltp:
        return

    for index, idx_sid in INDEX_IDS.items():
        if sid != idx_sid:
            continue

        data = indices[index]
        data["tick_count"] += 1

        # Build rolling candle buffer from tick fields
        candle = {
            "open" : message.get("open_price",  ltp),
            "high" : message.get("high_price",  ltp),
            "low"  : message.get("low_price",   ltp),
            "close": ltp,
        }
        data["buffer"].append(candle)
        if len(data["buffer"]) > 500:
            data["buffer"].pop(0)

        # ── TIMEFRAME GATE ───────────────────────────────────
        # Only run strategy logic every CANDLE_TICKS ticks to
        # simulate a candle close — prevents over-trading on noise.
        if data["tick_count"] % CANDLE_TICKS != 0:
            continue
        if len(data["buffer"]) < 60:
            continue

        df = pd.DataFrame(data["buffer"])

        # ── EXIT CHECK ───────────────────────────────────────
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
                        continue
                    place_order(data["opt_sid"], data["qty"], dhan.SELL, data["name"])

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
                        f"Mode     : LIVE"
                    )
                    data.update({"in_trade": False, "order_placed": False})
                    show_dashboard()
            continue   # don't enter new trade while managing exit

        # ── ENTRY CHECK ──────────────────────────────────────
        signal, strat_label = combined_signal(df, index, ltp)

        if signal and not data["order_placed"]:
            with ORDER_LOCK:
                if data["in_trade"] or data["order_placed"]:
                    continue

                opt_sid, name, expiry_str, delta, gamma = select_option(
                    index, ltp, signal)

                if not opt_sid:
                    send_alert(f"⚠️ {index}: No option found near {ltp:.0f}")
                    continue

                abs_delta = abs(delta)
                if abs_delta < MIN_DELTA:
                    send_alert(f"⚠️ {index} {name}: Delta {abs_delta:.2f} < {MIN_DELTA} — skip")
                    continue
                if gamma > MAX_GAMMA:
                    send_alert(f"⚠️ {index} {name}: Gamma {gamma:.5f} > {MAX_GAMMA} — skip")
                    continue

                qty = LOT_SIZES[index]
                data["order_placed"] = True    # ← block new entries immediately

                resp = place_order(opt_sid, qty, dhan.BUY, name)

                if resp and resp.get("status") in ("success", "pending"):
                    data.update({
                        "in_trade" : True,
                        "entry"    : ltp,
                        "sl"       : ltp * 0.75,
                        "target"   : ltp * 1.50,
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
                        f"LTP      : {ltp}\n"
                        f"Expiry   : {expiry_str}\n"
                        f"Delta    : {delta:.3f}   Gamma: {gamma:.5f}\n"
                        f"SL       : {data['sl']:.2f}   Target: {data['target']:.2f}\n"
                        f"Lot Size : {qty}\n"
                        f"Mode     : ⚡ LIVE"
                    )
                else:
                    data["order_placed"] = False   # reset on failure
                    send_alert(f"❌ {index}: Order failed — {resp}")

# ── STARTUP BANNER ────────────────────────────────────────────

send_alert(
    "🚀 <b>LIVE BOT STARTED</b>\n"
    "Indices   : NIFTY &amp; BANKNIFTY\n"
    "Feed      : WebSocket (DhanHQ MarketFeed)\n"
    "Base URL  : " + LIVE_BASE_URL + "\n"
    "Strategies:\n"
    "  • Order Block\n"
    "  • Breakout Trend Follower\n"
    "  • EMA-20/50 + RSI-14\n"
    "Timeframe : every 60 ticks\n"
    "Greeks    : Delta≥0.30, Gamma≤0.05\n"
    "⚠️ REAL MONEY — LIVE ORDERS ACTIVE"
)

# ── WEBSOCKET FEED ────────────────────────────────────────────

feed = MarketFeed(
    dhan_context,
    [(MarketFeed.NSE, sid, MarketFeed.Quote) for sid in INDEX_IDS.values()],
    on_message=on_message
)

# ── MAIN LOOP (with auto-reconnect) ──────────────────────────

RECONNECT_DELAY = 30

while True:
    # Check if market is open before attempting to connect
    now = datetime.now()
    if now.hour < 9 or (now.hour == 9 and now.minute < 15):
        msg = f"[{now.strftime('%H:%M')}] Market not open — waiting for 09:15 IST"
        print(msg)
        log_info(msg)
        time.sleep(60)
        continue
    
    if now.hour == 15 and now.minute >= 30:
        send_alert("📅 Market closed — generating session report…")
        log_info("Market closed at 15:30 IST — generating session report")
        run_session_report()
        print("✅ Bot finished for today.")
        break
    
    try:
        print("🔌 Connecting to DhanHQ WebSocket…")
        log_info("Connecting to WebSocket feed")
        feed.run_forever()
    except Exception as e:
        error_msg = str(e)
        print(f"⚠️ WebSocket disconnected: {e}")
        log_error(f"WebSocket disconnected: {error_msg}")
        send_alert(f"⚠️ WebSocket dropped — reconnecting in {RECONNECT_DELAY}s…\n{error_msg}")
        time.sleep(RECONNECT_DELAY)
