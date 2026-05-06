<div align="center">

# 🤖 DhanBot — Automated Options Trading Bot

### NSE F&O · NIFTY & BANKNIFTY · ATM CE/PE · DhanHQ API

[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![DhanHQ](https://img.shields.io/badge/DhanHQ-API%20v2-00C853?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PC9zdmc+&logoColor=white)](https://dhanhq.co)
[![WebSocket](https://img.shields.io/badge/WebSocket-Live%20Feed-FF6B35?style=for-the-badge&logo=websocket&logoColor=white)](https://dhanhq.co/docs/latest/marketfeed/)
[![Yahoo Finance](https://img.shields.io/badge/Yahoo%20Finance-LTP%20Fallback-720E9E?style=for-the-badge&logo=yahoo&logoColor=white)](https://finance.yahoo.com)
[![Telegram](https://img.shields.io/badge/Telegram-Alerts-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![Flask](https://img.shields.io/badge/Flask-Webhook-000000?style=for-the-badge&logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![Pandas](https://img.shields.io/badge/Pandas-Data-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

> **Paper-trade tested · Live-ready · Institutional order-block strategy**

</div>

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tools & Technologies](#-tools--technologies)
- [Project Structure](#-project-structure)
- [Strategies](#-strategies)
- [Signal Flow](#-signal-flow)
- [Greeks Filter](#-greeks-filter)
- [Slippage Model](#-slippage-model)
- [Quick Start](#-quick-start)
- [Configuration (.env)](#-configuration-env)
- [Running the Bot](#-running-the-bot)
- [Monitoring & Logs](#-monitoring--logs)
- [Sandbox vs Live](#-sandbox-vs-live-comparison)
- [Important Notes](#-important-notes)

---

## 🔍 Overview

**DhanBot** is a fully automated NSE F&O options trading bot built on the [DhanHQ broker API](https://dhanhq.co). It identifies institutional **Order Block** zones on 5-minute candles, confirms entry with **Bullish/Bearish Engulfing** patterns and **Breakout Trend** signals, then places ATM CE or PE orders on NIFTY and BANKNIFTY.

| Mode | File | Description |
|------|------|-------------|
| **Sandbox** | `Dhan_api.py` | Paper trading — no real money. Candle polling every 5 min. |
| **Live** | `dhan_live.py` | Real orders via DhanHQ WebSocket real-time feed. |

**What it does:**
- 🏦 Detects institutional **Order Block** zones (demand/supply)
- 🕯️ Confirms entry with **5-min Engulfing** candle pattern
- 📊 Filters options by **Black-Scholes Greeks** (Delta ≥ 0.30, Gamma ≤ 0.005)
- ⚡ Places **parallel CE + PE** orders when price is between both OB zones
- 📱 Sends real-time **Telegram alerts** for entries, exits, and EOD report
- 🔔 Accepts **TradingView webhook** alerts via Flask server
- 📈 Fetches **present market LTP** via Yahoo Finance for accurate strike selection
- 💾 Logs every tick and trade to CSV for post-session analysis

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                          │
│                                                             │
│  ┌─────────────────┐    ┌─────────────────────────────┐    │
│  │  DhanHQ API v2  │    │   Yahoo Finance Public API   │    │
│  │  /charts/intra  │    │   ^NSEI  /  ^ENSEBANK        │    │
│  │  day (5-min)    │    │   (Real-time LTP fallback)   │    │
│  └────────┬────────┘    └──────────────┬────────────────┘    │
│           │                            │                     │
│           ▼                            ▼                     │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              dhan_historical.py                       │    │
│  │   fetch_candles()  +  fetch_live_ltp()               │    │
│  │   Attempt 1: Dhan API → Attempt 2: Yahoo Finance     │    │
│  │   Attempt 3: Synthetic 5-min fallback (24 bars)      │    │
│  └────────────────────────┬────────────────────────────┘    │
└───────────────────────────┼────────────────────────────────--┘
                            │
          ┌─────────────────▼──────────────────┐
          │         SIGNAL ENGINE              │
          │                                    │
          │  greeks_options.combined_signal()  │
          │  ┌──────────────────────────────┐  │
          │  │ Step 1: OB Proximity Gate    │  │
          │  │  order_block_signal()        │  │
          │  │  → BUY / SELL / BOTH / None  │  │
          │  ├──────────────────────────────┤  │
          │  │ Step 2: Engulfing Confirm    │  │
          │  │  _bullish_engulfing()        │  │
          │  │  _bearish_engulfing()        │  │
          │  ├──────────────────────────────┤  │
          │  │ Step 3: BTF Vote             │  │
          │  │  breakout_trend_signal()     │  │
          │  └──────────────────────────────┘  │
          └─────────────────┬──────────────────┘
                            │
         ┌──────────────────┴───────────────────┐
         │                                      │
         ▼                                      ▼
┌────────────────────┐              ┌────────────────────────┐
│   SANDBOX MODE     │              │      LIVE MODE          │
│   Dhan_api.py      │              │      dhan_live.py       │
│                    │              │                         │
│ Poll every 300s    │              │ DhanHQ WebSocket        │
│ Paper orders       │              │ Real-time ticks         │
│ Slippage: 1.5%     │              │ LIMIT orders            │
│ Port: 5001         │              │ Slippage buffer: Rs.3   │
│                    │              │ Port: 5002              │
└────────┬───────────┘              └───────────┬────────────┘
         │                                      │
         ▼                                      ▼
┌──────────────────────────────────────────────────────────┐
│                   OUTPUT LAYER                            │
│  Telegram Alerts · CSV Trade Log · EOD Report · Log File  │
└──────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tools & Technologies

| Tool / Library | Version | Purpose |
|---------------|---------|---------|
| ![Python](https://img.shields.io/badge/-Python-3776AB?logo=python&logoColor=white) **Python** | 3.11+ | Core runtime |
| **dhanhq** | latest | DhanHQ broker SDK — order placement, history, OAuth |
| ![WebSocket](https://img.shields.io/badge/-WebSocket-FF6B35) **DhanHQ WebSocket** | MarketFeed | Real-time tick feed for live bot (`dhan_live.py`) |
| ![Yahoo Finance](https://img.shields.io/badge/-Yahoo%20Finance-720E9E?logo=yahoo) **Yahoo Finance API** | Public REST | Present LTP fallback (`^NSEI`, `^ENSEBANK`) when Dhan sandbox returns 404 |
| ![Pandas](https://img.shields.io/badge/-Pandas-150458?logo=pandas) **pandas** | ≥ 2.0 | OHLCV DataFrame manipulation, candle resampling |
| **numpy** | ≥ 1.26 | Numerical operations |
| **scipy** | ≥ 1.12 | Black-Scholes normal CDF (`scipy.special.ndtr`) |
| **pandas-ta** | latest | Technical indicators reference (not in active signal path) |
| ![Flask](https://img.shields.io/badge/-Flask-000000?logo=flask) **Flask** | ≥ 3.0 | TradingView webhook receiver (`/webhook` endpoint) |
| **websockets** | ≥ 12.0 | Underlying WebSocket transport for DhanHQ MarketFeed |
| **requests** | ≥ 2.31 | Telegram Bot API, Yahoo Finance, Dhan REST calls |
| ![Telegram](https://img.shields.io/badge/-Telegram-26A5E4?logo=telegram) **Telegram Bot API** | Bot API v7 | Real-time trade alerts and EOD session report |
| **python-dotenv** | ≥ 1.0 | Load secrets from `.env` file |
| **threading** | stdlib | Webhook server, tick recorder — non-blocking daemon threads |
| **math / ast** | stdlib | Black-Scholes Greeks (`math.erf`, `math.log`) |
| **csv / logging** | stdlib | Trade log CSV and rotating log file |

---

## 📁 Project Structure

```
Dhan_Execution/
│
├── Dhan_api.py                  ← SANDBOX entry point (paper trading)
├── dhan_live.py                 ← LIVE entry point  (real orders)
├── requirements.txt             ← All Python dependencies
├── .env                         ← Secrets (NOT committed — see below)
├── .gitignore
├── README.md
│
├── Scripts/                     ← Helper package
│   ├── __init__.py
│   ├── greeks_options.py        ← Black-Scholes Greeks + combined_signal()
│   ├── slippage.py              ← Slippage config + simulate_fill()
│   ├── dhan_historical.py       ← Candle fetch + Yahoo Finance LTP fallback
│   ├── tick_recorder.py         ← Thread-safe CSV tick logger
│   ├── webhook_trade.py         ← Flask webhook + execute_signal()
│   └── websocket_feed.py        ← DhanHQ MarketFeed WebSocket callback factory
│
├── Strategies/                  ← Strategy package
│   ├── __init__.py
│   ├── strategies_order_block.py    ← Order Block + Engulfing detection (PRIMARY)
│   ├── strategies_breakout_trend.py ← Breakout Trend Follower (BTF vote)
│   └── strategies_ema_rsi.py        ← EMA/RSI (DISABLED — kept for reference)
│
└── (auto-generated, git-ignored)
    ├── dhan_bot.log             ← Sandbox runtime log
    ├── dhan_live.log            ← Live runtime log
    ├── sandbox_trades.csv       ← Paper trade entries/exits
    ├── live_trades.csv          ← Live trade entries/exits
    ├── ticks_sandbox_*.csv      ← Sandbox candle records
    ├── ticks_live_*.csv         ← Live WebSocket tick records
    └── sandbox_backtest_report.txt ← EOD sandbox summary
```

---

## 📊 Strategies

### 1. Order Block Detection (`strategies_order_block.py`) — **PRIMARY HARD GATE**

Identifies institutional supply and demand zones on 5-minute candles.

```
Bullish OB:  Last RED candle before N consecutive GREEN candles
             → Demand zone (support) — look for BUY

Bearish OB:  Last GREEN candle before N consecutive RED candles
             → Supply zone (resistance) — look for SELL
```

**OB Zone dictionary:**
```python
bull_ob = { "high": ob_candle["open"],  # upper edge of demand zone
            "low" : ob_candle["low"],   # lower edge
            "avg" : (high + low) / 2 }  # midpoint
```

**Proximity check:** LTP must be **inside or within 0.5%** of the zone boundary for a signal to fire. Price NOT near any OB → **no trade** (hard gate).

**Parallel signal:** When LTP is simultaneously inside **both** a bullish and a bearish OB zone, `signal = "BOTH"` → places **CE + PE** in parallel.

---

### 2. Engulfing Candle Confirmation (`strategies_order_block.py`)

Applied to the two most recent **5-minute** bars before every entry decision.

| Pattern | Condition |
|---------|-----------|
| **Bullish Engulfing** | Prev candle red · Current candle green · Current body fully wraps prev body |
| **Bearish Engulfing** | Prev candle green · Current candle red · Current body fully wraps prev body |

> ⚠️ No trade fires unless **OB zone proximity AND matching engulfing** both confirm.

---

### 3. Breakout Trend Follower — BTF (`strategies_breakout_trend.py`) — **VOTE**

Swing breakout strategy using pivot highs/lows and a 50-period SMA filter.

```
BUY  signal: price breaks above swing high AND close > SMA-50
SELL signal: price breaks below swing low
```

BTF adds a **confirmation label** to the strategy tag when it agrees with the OB+Engulfing direction (e.g., `OB+BullEng+BTF`). It does **not** block or override the OB gate.

---

### 4. EMA / RSI — **DISABLED**

File `Strategies/strategies_ema_rsi.py` is kept for reference only. It is **never imported** in the active signal path. Monkey-patch in `Dhan_api.py` and `dhan_live.py` ensures it cannot accidentally activate.

---

## 🔄 Signal Flow

```
Every 5 minutes (sandbox poll / live candle gate):

  1.  fetch_candles()          → 5-min OHLCV DataFrame (24+ bars)
  2.  fetch_live_ltp()         → Present market LTP
        Attempt 1: Dhan /marketfeed/ltp
        Attempt 2: Yahoo Finance ^NSEI / ^ENSEBANK   ← real-time
        Attempt 3: candle close fallback

  3.  combined_signal(df, index, ltp):
        a. order_block_signal() → detect OB zones, check proximity
              near_bull AND near_bear  →  "BOTH"
              near_bull only           →  "BUY"
              near_bear only           →  "SELL"
              neither                  →  None  (NO TRADE)
        b. _bullish_engulfing() / _bearish_engulfing()
              Signal must be confirmed by matching engulfing pattern
        c. breakout_trend_signal()
              Adds "+BTF" to label if BTF agrees with direction

  4.  select_option(df_master, index, ltp, signal)
        → finds nearest ATM CE (BUY) or PE (SELL) in scrip master
        → computes Black-Scholes Delta & Gamma for filter

  5.  Greeks filter:
        |Delta| < 0.30  → skip (too OTM)
        Gamma   > 0.005 → skip (too close to expiry)

  6.  execute_signal() / _place_entry()
        → paper_order() [sandbox]  OR  place_order() [live LIMIT]
        → SL = fill * 0.95   (5% stop loss)
        → Target = fill * 1.10  (10% target)

  7.  Telegram alert + trade log CSV entry
```

---

## ⚗️ Greeks Filter

Black-Scholes Delta and Gamma computed in `Scripts/greeks_options.py` using:

- **Risk-free rate:** 6.5% (Indian repo rate)
- **Assumed IV:** 15% (ATM NIFTY typical)
- **Time to expiry (T):** derived from `SEM_EXPIRY_DATE` in scrip master

| Parameter | Threshold | Reason |
|-----------|-----------|--------|
| `\|Delta\|` | ≥ **0.30** | Skip deep OTM options (low sensitivity to underlying) |
| `Gamma` | ≤ **0.005** | Skip options near expiry (gamma risk too high) |

---

## 📉 Slippage Model

### Sandbox (`Scripts/slippage.py`)
```python
SANDBOX_SLIPPAGE_PCT = 0.015   # 1.5% of option premium per fill

BUY  fill = premium × 1.015   # pays more  (pessimistic)
SELL fill = premium × 0.985   # receives less (pessimistic)
```

| Index | ATM Premium | Slippage/fill |
|-------|-------------|--------------|
| NIFTY | ~Rs. 150 | **Rs. 2.25** |
| BANKNIFTY | ~Rs. 200 | **Rs. 3.00** |

### Live (`dhan_live.py`)
```python
SLIPPAGE_BUFFER = 3.00   # Rs. 3 added to LTP for BUY LIMIT orders

BUY  limit price = LTP + Rs.3   (ensures fill in rising market)
SELL limit price = LTP − Rs.3   (floor at Rs.0.05)
```
If actual slippage exceeds `3 × buffer`, a high-slippage Telegram alert fires automatically.

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- DhanHQ account with sandbox + live access
- Telegram Bot Token and Chat ID
- `.env` file with credentials (see below)

### Install Dependencies

```bash
git clone https://github.com/tejak6958/Dhan_api_trading.git
cd Dhan_api_trading
pip install -r requirements.txt
```

### Create `.env` File

```env
CLIENT_ID       = "your_dhan_client_id"
ACCESS_TOKEN    = "your_dhan_access_token"
BOT_TOKEN       = "your_telegram_bot_token"
CHAT_ID         = your_telegram_chat_id
TOKEN_TYPE      = "api"          # "api" = 30-day token | "web" = 24-hour token
TOKEN_ISSUED_AT = "2026-05-07T09:00:00"   # only needed if TOKEN_TYPE=web
```

> ⚠️ Never commit `.env` to GitHub. It is already in `.gitignore`.

---

## ⚙️ Configuration (.env)

| Variable | Description | Example |
|----------|-------------|---------|
| `CLIENT_ID` | DhanHQ client/user ID | `"2508191356"` |
| `ACCESS_TOKEN` | DhanHQ JWT access token | `"eyJhbG..."` |
| `BOT_TOKEN` | Telegram bot token from [@BotFather](https://t.me/BotFather) | `"8422359342:AAF..."` |
| `CHAT_ID` | Telegram chat ID to receive alerts | `1932823870` |
| `TOKEN_TYPE` | `api` (30-day) or `web` (24-hour) | `"api"` |
| `TOKEN_ISSUED_AT` | ISO timestamp of token creation (web only) | `"2026-05-07T09:00:00"` |

**Key constants (edit in source if needed):**

| File | Constant | Default | Description |
|------|----------|---------|-------------|
| `Dhan_api.py` | `POLL_SLEEP` | `300` | Sandbox poll interval in seconds (5 min) |
| `Dhan_api.py` | `WEBHOOK_PORT` | `5001` | Flask webhook port (sandbox) |
| `dhan_live.py` | `CANDLE_TICKS` | `300` | WebSocket ticks per candle gate (5 min) |
| `dhan_live.py` | `MAX_DAILY_LOSS` | `-15000` | Hard stop in Rs. per day |
| `dhan_live.py` | `SLIPPAGE_BUFFER` | `3.00` | LIMIT order Rs. buffer |
| `dhan_live.py` | `WEBHOOK_PORT` | `5002` | Flask webhook port (live) |
| `slippage.py` | `SANDBOX_SLIPPAGE_PCT` | `0.015` | 1.5% paper slippage |
| `strategies_order_block.py` | `OB_PERIODS` | `5` | Candles needed to validate OB |

---

## ▶️ Running the Bot

### Sandbox (Paper Trading — No Real Money)

```bash
python Dhan_api.py
```

- Runs during NSE market hours: **09:15 – 15:30 IST, Mon–Fri**
- Pre-market: waits automatically and prints countdown
- Generates `sandbox_backtest_report.txt` + Telegram EOD summary at 15:30

### Live (Real Orders — Real Money)

> ⚠️ **Complete at least 5–10 sandbox sessions before going live.**

```bash
python dhan_live.py
```

- Connects to DhanHQ WebSocket for real-time ticks
- Places real LIMIT orders on NSE F&O
- Emergency stop: create a file named `STOP` in the project folder
- Daily loss limit: Rs.15,000 (configurable via `MAX_DAILY_LOSS`)

### TradingView Webhook

Send a POST request to `http://your-server:5001/webhook` (sandbox) or `:5002/webhook` (live):

```json
{
  "index":    "NIFTY",
  "signal":   "BUY",
  "ltp":      24500.00,
  "strategy": "TradingView"
}
```

Health check: `GET http://localhost:5001/` · Status: `GET http://localhost:5001/status`

---

## 📡 Monitoring & Logs

| File | Contents | Updated |
|------|----------|---------|
| `dhan_bot.log` | All sandbox events, errors, signals | Every poll cycle |
| `dhan_live.log` | All live events, order fills, errors | Every WebSocket tick gate |
| `sandbox_trades.csv` | Entry/exit/PnL rows (sandbox) | Per trade |
| `live_trades.csv` | Entry/exit/PnL rows (live) | Per trade |
| `ticks_sandbox_YYYYMMDD.csv` | 5-min candle bars recorded | Per poll |
| `ticks_live_YYYYMMDD.csv` | Individual WebSocket ticks | Per tick |

**Key log prefixes to watch:**

```
[LTP DHAN]   → Dhan API LTP fetched successfully
[LTP YAHOO]  → Yahoo Finance LTP used (Dhan failed)
[OB]         → Order Block zone detected / proximity check
[SIGNAL]     → combined_signal() output
[PAPER]      → Paper order placed (sandbox)
[SLIPPAGE]   → Fill vs signal LTP comparison
[WEBHOOK]    → TradingView alert received
[WS FEED]    → WebSocket subscription status (live)
```

---

## 📊 Sandbox vs Live Comparison

| Aspect | Sandbox (`Dhan_api.py`) | Live (`dhan_live.py`) |
|--------|------------------------|----------------------|
| **Data feed** | Candle poll every 300s | DhanHQ WebSocket real-time |
| **Candle data** | Dhan API → Yahoo LTP → Synthetic | Rolling WebSocket tick buffer |
| **LTP for strikes** | Yahoo Finance (real-time) | WebSocket `last_traded_price` |
| **Order execution** | Paper only — no real money | Real LIMIT orders via Dhan API |
| **Slippage** | 1.5% on option premium | Rs.3 buffer on LIMIT price |
| **SL / Target** | 5% / 10% on option fill | 5% / 10% on option fill |
| **Engulfing gate** | `combined_signal()` | `websocket_feed.on_message()` |
| **OB hard gate** | `process_index()` | `websocket_feed.on_message()` |
| **Daily loss limit** | None (paper mode) | Rs.15,000/day hard stop |
| **EOD exit** | `run_backtest_report()` at 15:30 | Force-exit at 15:25 |
| **Webhook port** | 5001 | 5002 |
| **Log file** | `dhan_bot.log` | `dhan_live.log` |
| **Tick CSV** | `ticks_sandbox_YYYYMMDD.csv` | `ticks_live_YYYYMMDD.csv` |
| **Reconnect** | N/A (polling) | Auto-reconnect after 30s |
| **STOP file** | Not supported | `STOP` file triggers clean shutdown |

---

## ⚠️ Important Notes

### Sandbox API Limitations (Dhan)
- `/charts/intraday` returns **HTTP 500** for `IDX_I` (index) instruments — known limitation
- `/marketfeed/ltp` returns **HTTP 404** for index LTP in sandbox
- **Workaround:** Bot automatically falls back to Yahoo Finance for real LTP, and generates realistic 5-min synthetic candles for strategy testing

### Stop Loss Behaviour
- SL = **5% of option fill price** (not underlying index)
- ATM NIFTY ~Rs.150 → SL at Rs.142.50 (~12 NIFTY pts adverse move)
- In sandbox: synthetic random-walk data causes more SL hits than live
- In live: OB zone + engulfing entries have higher probability of respecting the zone

### Parallel Signal (BOTH)
- When NIFTY price is simultaneously near a **bullish OB** and a **bearish OB** (range-bound):
  - Bot places **both CE and PE** options simultaneously
  - Profits from whichever direction breaks out
  - Strategy label: `OB+Engulfing|BOTH`

### Token Management
- Use `TOKEN_TYPE=api` in `.env` for the **30-day API token** (recommended for live)
- `web` tokens expire in **24 hours** — bot warns when < 2 hours remain and blocks live start if expired

### Risk Disclaimer
> This bot is for **educational and research purposes**. Options trading involves
> substantial risk of loss. **Always test in sandbox first.** The authors are not
> responsible for any financial losses. Never risk money you cannot afford to lose.

---

## 📦 Dependencies

```txt
dhanhq          # DhanHQ broker SDK
pandas          # DataFrame and candle processing
numpy           # Numerical operations
scipy           # Black-Scholes normal CDF
pandas_ta       # Technical indicators (reference)
requests        # HTTP calls (Telegram, Yahoo Finance, Dhan REST)
python-dotenv   # Load .env credentials
flask           # TradingView webhook receiver
websockets      # DhanHQ WebSocket live feed transport
```

Install all:
```bash
pip install -r requirements.txt
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "feat: description"`
4. Push and open a Pull Request

---

<div align="center">

**Built with ❤️ for algorithmic trading on Indian markets**

[![GitHub](https://img.shields.io/badge/GitHub-tejak6958-181717?style=for-the-badge&logo=github)](https://github.com/tejak6958/Dhan_api_trading)

</div>
