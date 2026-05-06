"""
==============================================================
  DhanBot / dhan_live.py
  Mode  : LIVE -- real orders, real money
  Feed  : DhanHQ WebSocket (MarketFeed) -- real-time ticks

  CHANGES (from Dhan_query.txt):
    [Item ii]  SLIPPAGE_BUFFER raised from Rs.1.00 to Rs.2.00.
    [Item iii] CANDLE_TICKS = 300 (5-min gate, ~1 tick/s).
    [Item iv]  SL=5%, Target=10% (set in websocket_feed.py).
    [Item v]   "BOTH" parallel CE+PE handled in websocket_feed.py.
    [Item vii] EMA/RSI monkey-patched out of combined_signal.

  DO NOT run until Dhan_api.py sandbox testing is complete.
==============================================================
"""

import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
import requests
from dhanhq import DhanContext, dhanhq
from dotenv import load_dotenv

# [Item vii] Disable EMA/RSI strategy if still present in greeks_options
import Scripts.greeks_options as _go

# ── DHANBOT MODULES ───────────────────────────────────────────
from Scripts.greeks_options import MAX_GAMMA, MIN_DELTA, combined_signal, select_option
from Scripts.tick_recorder import TickRecorder
from Scripts.webhook_trade import execute_signal, start_webhook
from Scripts.websocket_feed import build_feed, make_on_message

if hasattr(_go, "ema_rsi_confirmation"):
    _go.ema_rsi_confirmation = lambda df, index, ltp=None: (None, "disabled")
    print("[Item vii] EMA/RSI strategy DISABLED in combined_signal")

# ── ENV ──────────────────────────────────────────────────────
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TOKEN_TYPE = os.getenv("TOKEN_TYPE", "web").lower()
TOKEN_ISSUED_AT = os.getenv("TOKEN_ISSUED_AT", "")


# ── VALIDATE ENV ─────────────────────────────────────────────
def validate_env():
    missing = [
        n
        for n, v in [
            ("CLIENT_ID", CLIENT_ID),
            ("ACCESS_TOKEN", ACCESS_TOKEN),
            ("BOT_TOKEN", BOT_TOKEN),
            ("CHAT_ID", CHAT_ID),
        ]
        if not v or not v.strip()
    ]
    if missing:
        print(f"MISSING ENV VARIABLES: {', '.join(missing)}")
        sys.exit(1)
    print("ENV variables loaded OK")


validate_env()


# ── TOKEN EXPIRY GUARD ────────────────────────────────────────
def check_token_expiry():
    if TOKEN_TYPE == "api":
        print("TOKEN_TYPE=api -> 30-day token. Safe for live.")
        return
    if not TOKEN_ISSUED_AT:
        print("TOKEN_ISSUED_AT not set. Recommend TOKEN_TYPE=api for live.")
        return
    try:
        issued = datetime.fromisoformat(TOKEN_ISSUED_AT)
        age = datetime.now() - issued
        remaining = timedelta(hours=24) - age
        if age > timedelta(hours=23):
            print(f"TOKEN EXPIRED (age={age}). LIVE BLOCKED. Regenerate token.")
            sys.exit(1)
        else:
            print(f"Token OK: age={age}, expires in ~{remaining}")
            if remaining < timedelta(hours=2):
                os.environ["_TOKEN_EXPIRY_WARN"] = (
                    f"TOKEN EXPIRING SOON -- ~{remaining} remaining.\n"
                    f"Regenerate before next session or switch to TOKEN_TYPE=api"
                )
    except ValueError:
        print(f"TOKEN_ISSUED_AT format invalid: '{TOKEN_ISSUED_AT}'")


check_token_expiry()

# ── LOGGING ──────────────────────────────────────────────────
LOG_FILE = "dhan_live.log"
logger = logging.getLogger("DhanBot")
logger.setLevel(logging.INFO)

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(
    logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
)
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
)
logger.addHandler(ch)


def log_info(msg):
    logger.info(msg)


def log_error(msg):
    logger.error(msg)


# ── MARKET HOURS ─────────────────────────────────────────────
NSE_HOLIDAYS_2026 = {
    "2026-01-15",
    "2026-01-26",
    "2026-03-03",
    "2026-03-26",
    "2026-03-31",
    "2026-04-03",
    "2026-04-14",
    "2026-05-01",
    "2026-05-28",
    "2026-06-26",
    "2026-09-14",
    "2026-10-02",
    "2026-10-20",
    "2026-11-10",
    "2026-11-24",
    "2026-12-25",
}


def is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS_2026:
        return False
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= now <= end


def market_status_reason() -> str:
    now = datetime.now()
    if now.weekday() >= 5:
        return f"Weekend ({now.strftime('%A')})"
    ds = now.strftime("%Y-%m-%d")
    if ds in NSE_HOLIDAYS_2026:
        return f"NSE Holiday ({ds})"
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now < start:
        return f"Pre-market (opens 09:15, now {now.strftime('%H:%M')})"
    if now > end:
        return f"Post-market (closed 15:30, now {now.strftime('%H:%M')})"
    return "OPEN"


# ── CONFIG ────────────────────────────────────────────────────
LIVE_BASE_URL = "https://api.dhan.co/v2"
LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 30}
CANDLE_TICKS = 300  # [Item iii] was 60 (1-min); now 300 (5-min)
MAX_DAILY_LOSS = -15000
RECONNECT_DELAY = 30
WEBHOOK_PORT = 5002

# [Item ii] Raised: Rs.1.00 → Rs.2.00 → Rs.3.00
# Rs.3 buffer ensures LIMIT orders fill on ATM options with wider spreads
# (NIFTY ATM spread typically Rs.2–5; BANKNIFTY Rs.5–10)
SLIPPAGE_BUFFER = 3.00
FILL_POLL_RETRIES = 6
FILL_POLL_DELAY = 0.5

# ── GLOBALS ──────────────────────────────────────────────────
BACKTEST_TRADES = []
ORDER_LOCK = threading.Lock()
EOD_DONE = False
pnl_state = {"total_pnl": 0.0, "trade_count": 0, "win_count": 0}

# ── TICK RECORDER ────────────────────────────────────────────
tick_rec = TickRecorder(mode="live")


# ── TELEGRAM ─────────────────────────────────────────────────
def send_alert(msg: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
        result = resp.json()
        if result.get("ok"):
            log_info(f"Telegram OK: {msg[:80].strip()}")
            return True
        log_error(f"Telegram fail: {result}")
        return False
    except Exception as e:
        log_error(f"Telegram exception: {e}")
        return False


# ── DHAN CLIENT ──────────────────────────────────────────────
dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan = dhanhq(dhan_context)

try:
    dhan.dhan_http.base_url = LIVE_BASE_URL
except AttributeError:
    try:
        dhan.base_url = LIVE_BASE_URL
    except AttributeError:
        log_error("Could not override base URL -- check dhanhq version")


def check_login() -> bool:
    try:
        res = dhan.get_fund_limits()
        if res and res.get("status") == "success":
            bal = res.get("data", {}).get("available_balance", "N/A")
            msg = (
                f"LIVE LOGIN OK\n"
                f"Balance: Rs.{bal} | Token: {TOKEN_TYPE.upper()}\n"
                f"Max Loss: Rs.{abs(MAX_DAILY_LOSS):,}/day\n"
                f"Market: {market_status_reason()}\n"
                f"Candles: 5-min ({CANDLE_TICKS} ticks) [Item iii]\n"
                f"SL: 5%  Target: 10% [Item iv]\n"
                f"Slip Buffer: Rs.{SLIPPAGE_BUFFER} [Item ii]\n"
                f"RSI/EMA: DISABLED [Item vii]\n"
                f"Webhook: port {WEBHOOK_PORT}\n"
                f"Ticks: {tick_rec.csv_path}\n"
                "REAL ORDERS WILL BE PLACED AUTOMATICALLY"
            )
            print(msg)
            if not send_alert(msg):
                print("Telegram delivery failed -- check BOT_TOKEN / CHAT_ID")
            warn = os.environ.pop("_TOKEN_EXPIRY_WARN", None)
            if warn:
                send_alert(warn)
            return True
        log_error(f"Login failed: {res}")
        return False
    except Exception as e:
        log_error(f"Login error: {e}")
        return False


if not check_login():
    print("Stopping -- login failed.")
    sys.exit(1)

# ── SCRIP MASTER ─────────────────────────────────────────────
MASTER_CSV = "scrip_master.csv"
MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
REFRESH_DAYS = 7


def _cache_fresh(fp, days):
    if not os.path.exists(fp):
        return False
    return (datetime.now() - datetime.fromtimestamp(os.path.getmtime(fp))) < timedelta(
        days=days
    )


def load_scrip_master():
    if _cache_fresh(MASTER_CSV, REFRESH_DAYS):
        return pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)
    resp = requests.get(MASTER_URL, timeout=120, stream=True)
    resp.raise_for_status()
    with open(MASTER_CSV, "wb") as f:
        for chunk in resp.iter_content(1024 * 256):
            f.write(chunk)
    return pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)


df_master = load_scrip_master()
df_master = df_master[
    df_master["SEM_TRADING_SYMBOL"].str.contains("NIFTY|BANKNIFTY", na=False)
    & df_master["SEM_INSTRUMENT_NAME"].str.contains("OPT", na=False)
].reset_index(drop=True)
print(f"Filtered master: {len(df_master)} option rows")

# ── INDEX STATE ───────────────────────────────────────────────
INDEX_IDS = {"NIFTY": "13", "BANKNIFTY": "25"}

indices = {
    idx: {
        "buffer": [],
        "tick_count": 0,
        "in_trade": False,
        "order_placed": False,
        "entry": 0.0,
        "sl": 0.0,
        "target": 0.0,
        "opt_sid": None,
        "qty": 0,
        "name": "",
        "delta": 0.0,
        "gamma": 0.0,
        "signal": "",
        "strategy": "",
    }
    for idx in INDEX_IDS
}


# ── REAL ORDER PLACEMENT ──────────────────────────────────────
def _fetch_fill_price(order_resp: dict, fallback_ltp: float) -> float:
    order_id = order_resp.get("data", {}).get("orderId") or order_resp.get("orderId")
    if not order_id:
        return fallback_ltp
    for attempt in range(1, FILL_POLL_RETRIES + 1):
        time.sleep(FILL_POLL_DELAY)
        try:
            status_resp = dhan.get_order_by_id(order_id)
            order_data = status_resp.get("data", {})
            traded_price = float(order_data.get("tradedPrice", 0) or 0)
            order_status = order_data.get("orderStatus", "")
            if traded_price > 0 and order_status in ("TRADED", "PART_TRADED"):
                log_info(
                    f"Fill: orderId={order_id} price={traded_price:.2f} "
                    f"status={order_status} attempt={attempt}"
                )
                return traded_price
        except Exception as e:
            log_error(f"Fill poll error attempt {attempt}: {e}")
    log_error(
        f"Could not get fill after {FILL_POLL_RETRIES} attempts -- "
        f"using fallback {fallback_ltp}"
    )
    return fallback_ltp


def place_order(sid: str, qty: int, side, name: str, ltp: float = 0.0) -> dict:
    """
    Place a LIMIT order with slippage buffer via DhanHQ REST API.
    [Item ii] SLIPPAGE_BUFFER = Rs.2.00 (was Rs.1.00).
    """
    try:
        if ltp > 0:
            if side == dhan.BUY:
                limit_price = round(ltp + SLIPPAGE_BUFFER, 2)
            else:
                limit_price = max(round(ltp - SLIPPAGE_BUFFER, 2), 0.05)
            order_type = dhan.LIMIT
        else:
            limit_price = 0
            order_type = dhan.MARKET
            log_error(f"No LTP for {name} -- falling back to MARKET order")

        resp = dhan.place_order(
            security_id=sid,
            exchange_segment=dhan.NSE_FNO,
            transaction_type=side,
            quantity=qty,
            order_type=order_type,
            product_type=dhan.INTRA,
            price=limit_price,
        )
        log_info(
            f"place_order: {side} {name} qty={qty} "
            f"type={order_type} limit={limit_price} -> {resp}"
        )

        fill_price = _fetch_fill_price(resp, ltp)
        resp["fill_price"] = fill_price
        slippage_pts = round(fill_price - ltp, 2) if ltp > 0 else 0.0
        log_info(
            f"[SLIPPAGE] {name} signal_ltp={ltp:.2f} "
            f"fill={fill_price:.2f} slip={slippage_pts:+.2f}"
        )

        if abs(slippage_pts) > SLIPPAGE_BUFFER * 3:
            send_alert(
                f"HIGH SLIPPAGE | {name}\n"
                f"Signal LTP: Rs.{ltp:.2f}  Fill: Rs.{fill_price:.2f}\n"
                f"Slippage: {slippage_pts:+.2f} pts\n"
                f"Consider increasing SLIPPAGE_BUFFER."
            )
        return resp
    except Exception as e:
        log_error(f"place_order FAILED ({name}): {e}")
        send_alert(f"ORDER ERROR ({name}): {e}")
        return {}


# ── SESSION REPORT ────────────────────────────────────────────
def run_session_report():
    global EOD_DONE
    if EOD_DONE:
        return
    EOD_DONE = True

    if not BACKTEST_TRADES:
        send_alert("Session Report: No trades today.")
        return

    df = pd.DataFrame(
        BACKTEST_TRADES,
        columns=[
            "time",
            "symbol",
            "strategy",
            "reason",
            "entry",
            "exit",
            "pnl",
            "delta",
            "gamma",
        ],
    )
    total_pnl = df["pnl"].sum()
    wins = (df["pnl"] > 0).sum()
    losses = (df["pnl"] <= 0).sum()
    win_rate = wins / len(df) * 100 if len(df) else 0
    max_dd = df["pnl"].cumsum().min() if len(df) else 0
    strat_pnl = df.groupby("strategy")["pnl"].sum().to_string()

    report = (
        f"LIVE SESSION REPORT\n"
        f"{'─' * 32}\n"
        f"Date        : {datetime.now().strftime('%Y-%m-%d')}\n"
        f"Mode        : LIVE\n"
        f"Candles     : 5-min [Item iii]\n"
        f"SL          : 5%  Target: 10% [Item iv]\n"
        f"Slip Buffer : Rs.{SLIPPAGE_BUFFER} [Item ii]\n"
        f"Strategies  : OB+Engulfing, BreakoutTrend [Item vii]\n"
        f"Trades      : {len(df)}\n"
        f"Wins/Losses : {wins}/{losses}\n"
        f"Win Rate    : {win_rate:.1f}%\n"
        f"Net PnL     : Rs.{total_pnl:.2f}\n"
        f"Max Drawdown: Rs.{max_dd:.2f}\n"
        f"Avg Delta   : {df['delta'].mean():.3f}\n"
        f"Avg Gamma   : {df['gamma'].mean():.5f}\n"
        f"\nPnL by Strategy:\n{strat_pnl}\n"
        f"{'─' * 32}"
    )
    with open("live_session_report.txt", "w", encoding="utf-8") as f:
        f.write(report)
    send_alert(report)
    print(report)


def log_trade(row: list):
    with open("live_trades.csv", "a", encoding="utf-8") as f:
        f.write(",".join(map(str, row)) + "\n")


def show_dashboard(trade_count, win_count, total_pnl):
    print(
        f"\n{'=' * 44}\n"
        f"  LIVE  {datetime.now().strftime('%H:%M:%S')}\n"
        f"  Trades:{trade_count}  Wins:{win_count}  "
        f"PnL:Rs.{total_pnl:.2f}\n"
        f"{'=' * 44}\n"
    )


def check_daily_loss(total_pnl: float) -> bool:
    if MAX_DAILY_LOSS < 0 and total_pnl <= MAX_DAILY_LOSS:
        send_alert(
            f"DAILY LOSS LIMIT HIT\n"
            f"Limit: Rs.{MAX_DAILY_LOSS:,.0f}  PnL: Rs.{total_pnl:,.2f}\n"
            f"Bot STOPPED for today."
        )
        log_error(f"Daily loss limit breached: PnL={total_pnl:.2f}")
        return True
    return False


# ── CONTEXT + WEBSOCKET CALLBACK ─────────────────────────────
_select_option_fn = partial(select_option, df_master)

live_context = {
    "indices": indices,
    "order_lock": ORDER_LOCK,
    "select_option_fn": _select_option_fn,
    "place_order_fn": place_order,
    "fetch_premium_fn": None,
    "send_alert_fn": send_alert,
    "log_info_fn": log_info,
    "log_error_fn": log_error,
    "lot_sizes": LOT_SIZES,
    "min_delta": MIN_DELTA,
    "max_gamma": MAX_GAMMA,
    "mode": "live",
    "backtest_trades": BACKTEST_TRADES,
    "pnl_state": pnl_state,
}

# [Item iii] CANDLE_TICKS=300 drives the 5-min evaluation gate
on_message, ws_state = make_on_message(
    indices=indices,
    index_ids=INDEX_IDS,
    candle_ticks=CANDLE_TICKS,  # 300 = 5-min
    combined_signal_fn=combined_signal,
    select_option_fn=_select_option_fn,
    place_order_fn=place_order,
    send_alert_fn=send_alert,
    log_info_fn=log_info,
    log_error_fn=log_error,
    log_trade_fn=log_trade,
    show_dashboard_fn=show_dashboard,
    run_session_report_fn=run_session_report,
    check_daily_loss_fn=check_daily_loss,
    is_market_open_fn=is_market_open,
    lot_sizes=LOT_SIZES,
    min_delta=MIN_DELTA,
    max_gamma=MAX_GAMMA,
    order_lock=ORDER_LOCK,
    backtest_trades=BACKTEST_TRADES,
    dhan=dhan,
    tick_rec=tick_rec,
)

# ── WEBHOOK ───────────────────────────────────────────────────
start_webhook(
    context=live_context,
    index_ids=INDEX_IDS,
    is_market_open_fn=is_market_open,
    market_status_reason_fn=market_status_reason,
    check_daily_loss_fn=check_daily_loss,
    port=WEBHOOK_PORT,
    mode="LIVE",
)

# ── BUILD WEBSOCKET FEED ──────────────────────────────────────
feed = build_feed(dhan_context, INDEX_IDS, on_message)

# ── STARTUP BANNER ────────────────────────────────────────────
startup_msg = (
    "LIVE BOT STARTED\n"
    f"Indices: NIFTY & BANKNIFTY\n"
    f"Feed: WebSocket (DhanHQ MarketFeed) | Ticks: {tick_rec.csv_path}\n"
    f"Timeframe: every {CANDLE_TICKS} ticks (~5 min) [Item iii]\n"
    f"Greeks: Delta>={MIN_DELTA}  Gamma<={MAX_GAMMA}\n"
    f"Lots: NIFTY={LOT_SIZES['NIFTY']}  BANKNIFTY={LOT_SIZES['BANKNIFTY']}\n"
    f"SL: 5%  Target: 10% [Item iv]\n"
    f"Slip Buffer: Rs.{SLIPPAGE_BUFFER} [Item ii]\n"
    f"RSI/EMA: DISABLED [Item vii]\n"
    f"Max Loss: Rs.{abs(MAX_DAILY_LOSS):,}/day | Token: {TOKEN_TYPE.upper()}\n"
    f"Webhook: port {WEBHOOK_PORT} | Market: {market_status_reason()}\n"
    "FULLY AUTO -- real orders placed without confirmation"
)
print(startup_msg)
send_alert(startup_msg)

# ── MAIN LOOP ─────────────────────────────────────────────────
print("Starting DhanHQ WebSocket live feed (5-min candle gate)...")

while True:
    if os.path.exists("STOP"):
        os.remove("STOP")
        send_alert("STOP file detected -- shutting down cleanly")
        run_session_report()
        break

    now = datetime.now()

    if now.weekday() >= 5:
        reason = "Saturday" if now.weekday() == 5 else "Sunday"
        print(f"[{now.strftime('%H:%M')}] {reason} -- sleeping 1h")
        time.sleep(3600)
        continue

    ds = now.strftime("%Y-%m-%d")
    if ds in NSE_HOLIDAYS_2026:
        print(f"[{now.strftime('%H:%M')}] NSE Holiday -- sleeping 1h")
        time.sleep(3600)
        continue

    if now.hour < 9 or (now.hour == 9 and now.minute < 15):
        secs = (now.replace(hour=9, minute=14, second=55, microsecond=0) - now).seconds
        print(
            f"[{now.strftime('%H:%M')}] Pre-market -- "
            f"connecting in {secs // 60}m {secs % 60}s"
        )
        time.sleep(min(60, max(secs, 1)))
        continue

    if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
        log_info(f"Market closed at {now.strftime('%H:%M')}")
        run_session_report()
        print("Bot finished for today.")
        break

    try:
        log_info("Connecting to DhanHQ WebSocket...")
        feed.run_forever()

    except KeyboardInterrupt:
        send_alert("Bot stopped manually (Ctrl+C)")
        run_session_report()
        print("Bot stopped by user.")
        break

    except Exception as e:
        log_error(f"WebSocket dropped: {e}")
        send_alert(f"WebSocket Dropped: {e}\nReconnecting in {RECONNECT_DELAY}s...")
        for data in indices.values():
            if not data["in_trade"]:
                data["order_placed"] = False
        time.sleep(RECONNECT_DELAY)
