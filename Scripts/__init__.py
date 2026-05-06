"""
Scripts package — DhanBot helper modules.

Making this an explicit Python package (via __init__.py) ensures:
  1. Reliable absolute imports from any working directory
     (e.g. `from Scripts.greeks_options import combined_signal`)
  2. Prevents Python from silently picking a wrong namespace
     package if another 'Scripts' directory appears on sys.path.
  3. IDEs (PyCharm, VS Code) and linters (pylint, flake8, mypy)
     treat the folder as a proper package for code-completion
     and type-checking.
  4. pytest discovers test modules correctly when tests live
     alongside package code.
  5. Enables relative imports within the package if needed in future.

Modules:
    greeks_options   — Black-Scholes Greeks, ATM option selector,
                       combined_signal aggregator
    slippage         — Sandbox slippage config & paper-fill simulator
    dhan_historical  — Historical + live-LTP fetch for sandbox polling
    tick_recorder    — Thread-safe CSV tick recorder (sandbox + live)
    webhook_trade    — Flask webhook receiver + core execute_signal()
    websocket_feed   — DhanHQ MarketFeed WebSocket callback factory
"""
