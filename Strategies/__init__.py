"""
Strategies package — DhanBot trading strategy modules.

Making this an explicit Python package (via __init__.py) ensures:
  1. Reliable absolute imports from any working directory
     (e.g. `from Strategies.strategies_order_block import order_block_signal`)
  2. Prevents Python from silently picking a wrong namespace
     package if another 'Strategies' directory appears on sys.path.
  3. IDEs (PyCharm, VS Code) and linters (pylint, flake8, mypy)
     treat the folder as a proper package for code-completion
     and type-checking.
  4. pytest discovers test modules correctly when tests live
     alongside package code.
  5. Enables relative imports within the package if needed in future.

Modules:
    strategies_order_block      — Institutional Order Block detection
                                  (bullish/bearish OB + engulfing patterns).
                                  Primary hard-gate signal source in
                                  greeks_options.combined_signal().
    strategies_breakout_trend   — Swing breakout + MA trend-filter strategy
                                  (BTF). Used as a confirmation vote
                                  alongside the Order Block gate.
    strategies_ema_rsi          — EMA/RSI strategy (DISABLED per [Item vii]).
                                  Kept for reference / future re-activation.
"""
