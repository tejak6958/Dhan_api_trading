# Dhan Trading Bot

Automated options trading bot for NIFTY & BANKNIFTY using Dhan HQ API.

## Features
- Live WebSocket feed from Dhan
- EMA + RSI signal generation
- Delta & Gamma Greeks filter
- ATM option selection (CE/PE)
- Telegram alerts for every trade
- Sandbox mode for safe testing
- Local scrip master cache (weekly refresh)
- End-of-day backtest report to Telegram

---

## Folder Structure
```
dhan-bot/
├── dhan_bot_v2.py       ← main bot
├── requirements.txt     ← all dependencies
├── .env                 ← your credentials (never committed)
├── .gitignore           ← files excluded from Git
├── scrip_master.csv     ← auto-downloaded, excluded from Git
├── trades.csv           ← generated at runtime
└── README.md
```

---

## Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/dhan-bot.git
cd dhan-bot
```

### 2. Create a virtual environment (recommended)
```bash
python -m venv venv

# Activate on Windows
venv\Scripts\activate

# Activate on Mac/Linux
source venv/bin/activate
```

### 3. Install all dependencies
```bash
pip install -r requirements.txt
```

### 4. Create your .env file
Create a file named `.env` in the project folder:
```
CLIENT_ID=your_dhan_client_id
ACCESS_TOKEN=your_dhan_access_token
BOT_TOKEN=your_telegram_bot_token
CHAT_ID=your_telegram_chat_id
```

### 5. Run in Sandbox (safe test mode)
Make sure `USE_SANDBOX = True` in `dhan_bot_v2.py`, then:
```bash
python dhan_bot_v2.py
```

### 6. Switch to Live trading
Set `USE_SANDBOX = False` in `dhan_bot_v2.py` only after sandbox testing is successful.

---

## How to create Git repository (first time only)

```bash
# Inside your project folder
git init
git add .
git commit -m "Initial commit"

# Push to GitHub (create repo on github.com first)
git remote add origin https://github.com/YOUR_USERNAME/dhan-bot.git
git push -u origin main
```

---

## Important Notes
- Never commit your `.env` file or share your ACCESS_TOKEN
- Sandbox mode uses paper trades — no real money
- Scrip master CSV auto-refreshes every 7 days
- Bot auto-generates daily report at 15:30 and sends to Telegram
