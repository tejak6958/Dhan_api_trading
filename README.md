# Dhan Trading Bot

Sandbox paper-trading bot for NIFTY & BANKNIFTY using the Dhan HQ API.

## What this project does
- Polls Dhan sandbox intraday candle data every 60 seconds
- Runs three signal strategies: Order Block, Breakout Trend Follower, EMA/RSI
- Applies option Greeks filters (Delta / Gamma) before taking a trade
- Selects ATM CE/PE option symbols for NIFTY and BANKNIFTY
- Simulates paper orders only; no real capital is placed in sandbox mode
- Sends Telegram alerts for entries, exits, daily report, and health checks
- Caches `scrip_master.csv` locally and refreshes it weekly
- Writes runtime status and errors to `dhan_bot.log`
- Writes sandbox trade logs and end-of-day backtest summaries
- Uses Dhan historical candle polling because sandbox WebSocket is unavailable

## Implementation details
- Single entry point: `Dhan_api.py`
- Uses `pandas`, `numpy`, `pandas_ta`, `scipy`, `requests`, `python-dotenv`
- Loads `CLIENT_ID`, `ACCESS_TOKEN`, `BOT_TOKEN`, `CHAT_ID` from `.env`
- Supports NIFTY and BANKNIFTY index IDs with separate state tracking
- Exits trades automatically on stop loss or target movement
- Produces daily sandbox report and Telegram notification
