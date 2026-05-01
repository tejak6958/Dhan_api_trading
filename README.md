# Dhan Trading Bot

Sandbox paper-trading bot for NIFTY & BANKNIFTY using the Dhan HQ API.

## What this project does
- Polls Dhan sandbox intraday candle data every 60 seconds
- Runs three signal strategies: Order Block, Breakout Trend Follower, EMA/RSI
- Applies option Greeks filters before taking a trade
- Selects ATM CE/PE options for NIFTY and BANKNIFTY
- Sends Telegram alerts for entries, exits, and reports
- Uses paper orders only; no real capital is placed in sandbox mode
- Caches `scrip_master.csv` locally and refreshes it weekly

---

## Current repository structure
```
Dhan_api.py
requirements.txt
README.md
.gitignore
```

## Required files (do not commit)
- `.env`  ← your credentials and bot settings
- `scrip_master.csv`  ← downloaded automatically at runtime
- `sandbox_trades.csv`  ← paper trades log
- `sandbox_backtest_report.txt`  ← daily report output

---

## Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
```

### 2. Create a virtual environment
```bash
python -m venv venv
```

Activate it:
```bash
# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Create your `.env` file
Create a file named `.env` in the project folder with:
```
CLIENT_ID=your_dhan_client_id
ACCESS_TOKEN=your_dhan_access_token
BOT_TOKEN=your_telegram_bot_token
CHAT_ID=your_telegram_chat_id
```

### 5. Run the bot
```bash
python Dhan_api.py
```

### 6. Notes about sandbox mode
- `Dhan_api.py` is already configured for sandbox mode using:
  `SANDBOX_BASE_URL = "https://sandbox.dhan.co/v2"`
- Dhan sandbox does not support WebSocket in this script; it uses historical candle polling.
- The script performs paper trading only; there is no real-money order placement.

---

## Dependency summary
The bot relies on:
- `dhanhq`
- `pandas`
- `numpy`
- `pandas_ta`
- `scipy`
- `requests`
- `python-dotenv`

---

## GitHub visibility / hiding code
- If your repository is public, the code is visible to anyone.
- You cannot hide source code in a public GitHub repository once it is committed.
- To keep code private, use a private GitHub repository or do not push it to GitHub.
- Use `.gitignore` to prevent local secrets like `.env` from being committed.

---

## Important reminders
- Never commit `.env` or any credentials file.
- Do not publish your `ACCESS_TOKEN`, `CLIENT_ID`, `BOT_TOKEN`, or `CHAT_ID`.
- This script is designed for sandbox/paper trading, not live capital deployment.
