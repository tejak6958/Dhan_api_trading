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
