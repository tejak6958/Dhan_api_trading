"""
==============================================================
  DhanBot / Dhan_api.py
  Mode : SANDBOX / Paper Trading
  Feed : Historical candle API (polling every 60s)

  CHANGES (from Dhan_query.txt):
    [Item i]   fetch_live_ltp() used before execute_signal() so
               option strike is chosen on PRESENT market price,
               not stale synthetic candle close.
    [Item ii]  SANDBOX_SLIPPAGE_PCT = 1.0% (in slippage.py).
    [Item iii] fetch_candles() now returns 5-min bars (dhan_historical.py).
               POLL_SLEEP reduced to 300s (5 min) to match candle size.
               Engulfing detection logs visible in process_index output.
    [Item iv]  SL = 5% / Target = 10% (in webhook_trade.py).
               Exit check thresholds updated here accordingly.
    [Item v]   "BOTH" parallel signal handled in process_index():
               execute_signal called for both BUY and SELL legs.
    [Item vi]  Live LTP fetch before option selection (same as Item i).
    [Item vii] EMA/RSI strategy removed from combined_signal import.
               Only Order Block + Breakout Trend strategies active.

  Strategies (OB proximity + engulfing gate required):
    1. Order Block + Bullish/Bearish Engulfing (strategies_order_block.py)
    2. Breakout Trend                          (strategies_breakout_trend.py)
    NOTE: EMA-RSI removed per [Item vii].

  Options : NSE FnO -- NIFTY & BANKNIFTY ATM CE/PE
  Greeks  : Black-Scholes Delta & Gamma filter
  Orders  : Paper (logged only, no real capital at risk)
  Reports : Telegram alerts + EOD backtest summary
==============================================================
"""

import os
import sys
import time
import threading
import logging
import pandas as pd
import requests
from datetime import datetime, timedelta
from dhanhq import dhanhq, DhanContext
from dotenv import load_dotenv
from functools import partial

# ── DHANBOT MODULES ───────────────────────────────────────────
from Scripts.greeks_options  import (combined_signal, select_option,
                              MIN_DELTA, MAX_GAMMA)
from Scripts.slippage        import (SANDBOX_SLIPPAGE_PCT, fetch_option_premium,
                              simulate_fill)
from Scripts.dhan_historical import fetch_candles, fetch_live_ltp
from Scripts.tick_recorder   import TickRecorder
from Scripts.webhook_trade   import execute_signal, start_webhook

# [Item vii] Disable EMA/RSI strategy if still present in greeks_options
import Scripts.greeks_options as _go
if hasattr(_go, "ema_rsi_confirmation"):
    _go.ema_rsi_confirmation = lambda df, index: (None, "disabled")
    print("[Item vii] EMA/RSI strategy DISABLED in combined_signal")

# ── ENV ──────────────────────────────────────────────────────
load_dotenv()
CLIENT_ID        = os.getenv("CLIENT_ID")
ACCESS_TOKEN     = os.getenv("ACCESS_TOKEN")
BOT_TOKEN        = os.getenv("BOT_TOKEN")
CHAT_ID          = os.getenv("CHAT_ID")
TOKEN_TYPE       = os.getenv("TOKEN_TYPE", "web").lower()
TOKEN_ISSUED_AT  = os.getenv("TOKEN_ISSUED_AT", "")

# ── VALIDATE ENV ─────────────────────────────────────────────
def validate_env():
    missing = [n for n, v in [("CLIENT_ID", CLIENT_ID),
                               ("ACCESS_TOKEN", ACCESS_TOKEN),
                               ("BOT_TOKEN", BOT_TOKEN),
                               ("CHAT_ID", CHAT_ID)]
               if not v or not v.strip()]
    if missing:
        print(f"MISSING ENV VARIABLES: {', '.join(missing)}")
        print("   -> Check your .env file.")
        sys.exit(1)
    print("ENV variables loaded OK")

validate_env()

# ── TOKEN EXPIRY GUARD ────────────────────────────────────────
def check_token_expiry():
    if TOKEN_TYPE == "api":
        print("TOKEN_TYPE=api -> 30-day token. No 24h expiry concern.")
        return
    if not TOKEN_ISSUED_AT:
        print("TOKEN_ISSUED_AT not set in .env. Cannot verify 24h expiry.")
        return
    try:
        issued    = datetime.fromisoformat(TOKEN_ISSUED_AT)
        age       = datetime.now() - issued
        remaining = timedelta(hours=24) - age
        if age > timedelta(hours=23):
            print(f"TOKEN EXPIRED (age={age}). Regenerate ACCESS_TOKEN.")
        else:
            print(f"Token OK: age={age}, expires in ~{remaining}")
    except ValueError:
        print(f"TOKEN_ISSUED_AT format invalid: '{TOKEN_ISSUED_AT}'.")

check_token_expiry()

# ── LOGGING ───────────────────────────────────────────────────
LOG_FILE = "dhan_bot.log"
logger   = logging.getLogger("DhanBot")
logger.setLevel(logging.INFO)

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(fh)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(ch)

def log_info(msg):  logger.info(msg)
def log_error(msg): logger.error(msg)

# ── MARKET HOURS ─────────────────────────────────────────────
NSE_HOLIDAYS_2026 = {
    "2026-01-15", "2026-01-26", "2026-03-03", "2026-03-26",
    "2026-03-31", "2026-04-03", "2026-04-14", "2026-05-01",
    "2026-05-28", "2026-06-26", "2026-09-14", "2026-10-02",
    "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25"
}

def is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5: return False
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS_2026: return False
    start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= now <= end

def market_status_reason() -> str:
    now = datetime.now()
    if now.weekday() >= 5:
        return f"Weekend ({now.strftime('%A')})"
    ds = now.strftime("%Y-%m-%d")
    if ds in NSE_HOLIDAYS_2026:
        return f"NSE Holiday ({ds})"
    start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now < start:
        return f"Pre-market (opens 09:15, now {now.strftime('%H:%M')})"
    if now > end:
        return f"Post-market (closed 15:30, now {now.strftime('%H:%M')})"
    return "Open"

# ── CONFIG ────────────────────────────────────────────────────
SANDBOX_BASE_URL = "https://sandbox.dhan.co/v2"
LOT_SIZES        = {"NIFTY": 65, "BANKNIFTY": 30}
POLL_SLEEP       = 300   # [Item iii] was 60s; now 300s = 5 min (matches candle)
WEBHOOK_PORT     = 5001

# ── GLOBALS ──────────────────────────────────────────────────
BACKTEST_TRADES = []
ORDER_LOCK      = threading.Lock()
pnl_state       = {"total_pnl": 0.0, "trade_count": 0, "win_count": 0}

# ── TICK RECORDER ────────────────────────────────────────────
tick_rec = TickRecorder(mode="sandbox")
log_info(f"Tick recorder -> {tick_rec.csv_path}")

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
            log_info(f"Telegram OK: {msg[:80].strip()}")
            return True
        log_error(f"Telegram fail: {result}")
        return False
    except Exception as e:
        log_error(f"Telegram exception: {e}")
        return False

# ── DHAN CLIENT ──────────────────────────────────────────────
dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan         = dhanhq(dhan_context)

try:
    dhan.dhan_http.base_url = SANDBOX_BASE_URL
except AttributeError:
    try:
        dhan.base_url = SANDBOX_BASE_URL
    except AttributeError:
        log_error("Could not override base URL -- check dhanhq version")

def check_login() -> bool:
    try:
        res = dhan.get_fund_limits()
        if res and res.get("status") == "success":
            bal = res.get("data", {}).get("available_balance", "N/A")
            msg = (f"SANDBOX LOGIN OK\n"
                   f"Balance: {bal} | Token: {TOKEN_TYPE.upper()}\n"
                   f"Market: {market_status_reason()}\n"
                   f"Webhook: port {WEBHOOK_PORT}\n"
                   f"Ticks: {tick_rec.csv_path}\n"
                   f"Poll: every {POLL_SLEEP}s (5-min candles)\n"
                   f"SL: 5% | Target: 10% | Slippage: "
                   f"{SANDBOX_SLIPPAGE_PCT*100:.1f}%\n"
                   f"Strategies: OrderBlock+Engulfing, BreakoutTrend\n"
                   f"RSI/EMA: DISABLED")
            print(msg)
            send_alert(msg)
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
MASTER_CSV   = "scrip_master.csv"
MASTER_URL   = "https://images.dhan.co/api-data/api-scrip-master.csv"
REFRESH_DAYS = 7

def _cache_fresh(fp, days):
    if not os.path.exists(fp): return False
    return (datetime.now() - datetime.fromtimestamp(
        os.path.getmtime(fp))) < timedelta(days=days)

def load_scrip_master():
    if _cache_fresh(MASTER_CSV, REFRESH_DAYS):
        log_info("Scrip master: from cache")
        return pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)
    log_info("Downloading scrip master...")
    resp = requests.get(MASTER_URL, timeout=120, stream=True)
    resp.raise_for_status()
    with open(MASTER_CSV, "wb") as f:
        for chunk in resp.iter_content(1024 * 256):
            f.write(chunk)
    return pd.read_csv(MASTER_CSV, dtype=str, low_memory=False)

df_master = load_scrip_master()
df_master = df_master[
    df_master["SEM_TRADING_SYMBOL"].str.contains("NIFTY|BANKNIFTY", na=False) &
    df_master["SEM_INSTRUMENT_NAME"].str.contains("OPT", na=False)
].reset_index(drop=True)
print(f"Filtered master: {len(df_master)} option rows")

# ── INDEX STATE ───────────────────────────────────────────────
INDEX_IDS = {"NIFTY": "13", "BANKNIFTY": "25"}

indices = {
    idx: {
        "in_trade": False, "order_placed": False,
        "entry": 0.0, "sl": 0.0, "target": 0.0,
        "opt_sid": None, "qty": 0, "name": "",
        "delta": 0.0, "gamma": 0.0, "signal": "", "strategy": "",
    }
    for idx in INDEX_IDS
}

# ── PAPER ORDER ───────────────────────────────────────────────
def paper_order(sid: str, qty: int, side: str, name: str,
                ltp: float = 0.0) -> dict:
    """
    Simulate a paper order with slippage applied to the option premium.
    fill_price is returned so callers use it for SL/Target/PnL.
    """
    fill_price = simulate_fill(ltp, side) if ltp > 0 else ltp
    slip       = round(fill_price - ltp, 2) if ltp > 0 else 0.0
    ts         = datetime.now().strftime("%H:%M:%S")
    msg = (f"[PAPER] {ts} | {side} | {name} | qty={qty} | "
           f"signal_ltp={ltp:.2f} | fill={fill_price:.2f} | "
           f"slippage={slip:+.2f}")
    print(msg)
    log_info(msg)
    return {"status": "success", "orderId": f"PAPER_{ts}",
            "fill_price": fill_price}

# ── CONTEXT ───────────────────────────────────────────────────
_select_option_fn = partial(select_option, df_master)

_fetch_premium_fn = partial(
    fetch_option_premium,
    sandbox_base_url=SANDBOX_BASE_URL,
    access_token=ACCESS_TOKEN,
    client_id=CLIENT_ID,
)

sandbox_context = {
    "indices"         : indices,
    "order_lock"      : ORDER_LOCK,
    "select_option_fn": _select_option_fn,
    "place_order_fn"  : paper_order,
    "fetch_premium_fn": _fetch_premium_fn,
    "send_alert_fn"   : send_alert,
    "log_info_fn"     : log_info,
    "log_error_fn"    : log_error,
    "lot_sizes"       : LOT_SIZES,
    "min_delta"       : MIN_DELTA,
    "max_gamma"       : MAX_GAMMA,
    "mode"            : "sandbox",
    "backtest_trades" : BACKTEST_TRADES,
    "pnl_state"       : pnl_state,
}

# ── WEBHOOK ───────────────────────────────────────────────────
start_webhook(
    context=sandbox_context,
    index_ids=INDEX_IDS,
    is_market_open_fn=is_market_open,
    market_status_reason_fn=market_status_reason,
    port=WEBHOOK_PORT,
    mode="SANDBOX",
)

# ── BACKTEST REPORT ───────────────────────────────────────────
def run_backtest_report():
    if not BACKTEST_TRADES:
        send_alert("Backtest: No trades captured today.")
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
    avg_slip  = df.apply(
        lambda r: (r["entry"] + r["exit"]) * SANDBOX_SLIPPAGE_PCT, axis=1
    ).mean() if len(df) else 0

    report = (
        f"SANDBOX BACKTEST REPORT\n"
        f"{'─'*32}\n"
        f"Date         : {datetime.now().strftime('%Y-%m-%d')}\n"
        f"Mode         : SANDBOX (paper)\n"
        f"Candles      : 5-min [Item iii]\n"
        f"Slippage     : {SANDBOX_SLIPPAGE_PCT*100:.1f}% per fill [Item ii]\n"
        f"SL           : 5%  Target: 10% [Item iv]\n"
        f"Strategies   : OB+Engulfing, BreakoutTrend [Item vii]\n"
        f"Total Trades : {len(df)}\n"
        f"Wins/Losses  : {wins}/{losses}\n"
        f"Win Rate     : {win_rate:.1f}%\n"
        f"Net PnL      : Rs.{total_pnl:.2f}\n"
        f"Max Drawdown : Rs.{max_dd:.2f}\n"
        f"Avg Slip/Trade: Rs.{avg_slip:.2f}\n"
        f"Avg Delta    : {df['delta'].mean():.3f}\n"
        f"Avg Gamma    : {df['gamma'].mean():.5f}\n"
        f"\nPnL by Strategy:\n{strat_pnl}\n"
        f"{'─'*32}\n"
        f"PnL includes {SANDBOX_SLIPPAGE_PCT*100:.1f}% slippage both legs."
    )

    with open("sandbox_backtest_report.txt", "w", encoding="utf-8") as f:
        f.write(report)
    send_alert(report)
    print(report)

def log_trade(row: list):
    with open("sandbox_trades.csv", "a", encoding="utf-8") as f:
        f.write(",".join(map(str, row)) + "\n")

def show_dashboard():
    print(f"\n{'='*40}\n"
          f"  SANDBOX  {datetime.now().strftime('%H:%M:%S')}\n"
          f"  Trades:{pnl_state['trade_count']}  "
          f"Wins:{pnl_state['win_count']}  "
          f"PnL:Rs.{pnl_state['total_pnl']:.2f}\n"
          f"{'='*40}\n")

# ── PROCESS INDEX ─────────────────────────────────────────────
def process_index(index: str, sid: str):
    """
    [Item iii] Fetches 5-min candles.
    [Item i/vi] Gets present live LTP before option selection.
    [Item v]   Handles "BOTH" parallel signal.
    [Item iv]  SL=5%, Target=10% on exit check (mirrors webhook_trade.py).
    [Item vii] RSI/EMA disabled upstream in combined_signal.
    """
    df = fetch_candles(
        dhan=dhan,
        security_id=sid,
        sandbox_base_url=SANDBOX_BASE_URL,
        access_token=ACCESS_TOKEN,
        client_id=CLIENT_ID,
        is_market_open_fn=is_market_open,
        market_status_reason_fn=market_status_reason,
    )

    if df.empty:
        return

    tick_rec.record_candles(index, df)

    if len(df) < 10:   # need at least 10 x 5-min bars (was 60 x 1-min)
        log_info(f"[{index}] {len(df)} 5-min bars -- need 10, waiting")
        return

    # [Item i / vi] Prefer live LTP for strike selection; fall back to candle
    live_ltp = fetch_live_ltp(
        security_id=sid, index=index,
        sandbox_base_url=SANDBOX_BASE_URL,
        access_token=ACCESS_TOKEN,
        client_id=CLIENT_ID,
    )
    ltp  = live_ltp if live_ltp else float(df.iloc[-1]["close"])
    data = indices[index]
    log_info(f"[{index}] {'LIVE' if live_ltp else 'HIST'} LTP={ltp:.2f} "
             f"bars={len(df)}")

    # ── EXIT CHECK [Item iv] SL=5% / Target=10% ───────────────
    if data["in_trade"]:
        exit_flag, reason = False, ""
        if ltp <= data["sl"]:
            exit_flag, reason = True, "SL HIT"
        elif ltp >= data["target"]:
            exit_flag, reason = True, "TARGET HIT"

        if exit_flag:
            with ORDER_LOCK:
                if not data["in_trade"]:
                    return
                exit_premium  = _fetch_premium_fn(data["opt_sid"], index)
                exit_resp     = paper_order(data["opt_sid"], data["qty"],
                                            "SELL", data["name"],
                                            ltp=exit_premium)
                exit_fill     = exit_resp.get("fill_price", exit_premium)
                exit_slippage = round(exit_fill - exit_premium, 2)
                pnl           = (exit_fill - data["entry"]) * data["qty"]

                pnl_state["total_pnl"]   += pnl
                pnl_state["trade_count"] += 1
                if pnl > 0:
                    pnl_state["win_count"] += 1

                BACKTEST_TRADES.append([
                    datetime.now(), data["name"], data["strategy"],
                    reason, data["entry"], exit_fill,
                    pnl, data["delta"], data["gamma"]
                ])
                log_trade([
                    datetime.now(), data["name"], data["strategy"],
                    reason, data["entry"], exit_fill,
                    round(pnl, 2), data["delta"], data["gamma"]
                ])
                send_alert(
                    f"EXIT | {index} | {data['strategy']}\n"
                    f"Reason: {reason} | Option: {data['name']}\n"
                    f"Entry: Rs.{data['entry']:.2f}  "
                    f"Exit Fill: Rs.{exit_fill:.2f} "
                    f"(slip {exit_slippage:+.2f})\n"
                    f"PnL: Rs.{pnl:.2f}  "
                    f"Total: Rs.{pnl_state['total_pnl']:.2f} | SANDBOX"
                )
                data.update({"in_trade": False, "order_placed": False})
                show_dashboard()
        return

    # ── ENTRY CHECK ──────────────────────────────────────────
    signal, strat_label = combined_signal(df, index, ltp)

    # [Item v] Parallel signal: execute both BUY and SELL legs
    if signal == "BOTH":
        log_info(f"[{index}] BOTH signal — placing parallel CE+PE")
        for direction in ("BUY", "SELL"):
            execute_signal(
                index=index, ltp=ltp, signal=direction,
                strat_label=f"{strat_label}|BOTH",
                context=sandbox_context, source="poll"
            )
        return

    # Standard single direction
    execute_signal(
        index=index, ltp=ltp, signal=signal, strat_label=strat_label,
        context=sandbox_context, source="poll"
    )

# ── STARTUP BANNER ────────────────────────────────────────────
startup_msg = (
    "SANDBOX BOT STARTED\n"
    f"Indices: NIFTY & BANKNIFTY\n"
    f"Feed: Candle poll every {POLL_SLEEP}s (5-min candles) [Item iii]\n"
    f"LTP: Live fetch before each trade [Item i/vi]\n"
    f"SL: 5%  Target: 10% [Item iv]\n"
    f"Slippage: {SANDBOX_SLIPPAGE_PCT*100:.1f}% per fill [Item ii]\n"
    f"Signals: OB+Engulfing near OB zone only [Item iii/v]\n"
    f"RSI/EMA: DISABLED [Item vii]\n"
    f"Webhook: port {WEBHOOK_PORT}\n"
    f"Token: {TOKEN_TYPE.upper()} | Market: {market_status_reason()}\n"
    "Orders: PAPER ONLY -- no real money"
)
print(startup_msg)
send_alert(startup_msg)

# ── MAIN POLL LOOP ────────────────────────────────────────────
print("Starting sandbox polling loop (5-min candle cycle)...")

while True:
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
        wait = (now.replace(hour=9, minute=15, second=0,
                            microsecond=0) - now).seconds
        print(f"[{now.strftime('%H:%M')}] Pre-market -- "
              f"opens in {wait//60}m {wait%60}s")
        time.sleep(min(60, wait))
        continue

    if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
        send_alert("Market closed. Generating sandbox report...")
        run_backtest_report()
        print("Bot finished for today.")
        break

    for idx, sid in INDEX_IDS.items():
        try:
            process_index(idx, sid)
        except Exception as e:
            print(f"[ERROR] {idx}: {e}")
            log_error(f"{idx} error: {e}")
            send_alert(f"ERROR {idx}: {e}")

    time.sleep(POLL_SLEEP)