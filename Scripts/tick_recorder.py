"""
==============================================================
  DhanBot / tick_recorder.py
  [Item v] TICK DATA RECORDER

  Used in BOTH Dhan_api.py (sandbox) AND dhan_live.py (live).

  ITEM viii ANSWER — Should this be in dhan_live.py? YES.
  In live mode, record_tick() captures every WebSocket tick
  (~1/sec), which is far more valuable than 1-min candle bars:
    - Validates whether SL was hit between candle closes
    - Enables slippage analysis (signal LTP vs actual fill)
    - Provides real market data for strategy refinement
  Runs in daemon threads so zero impact on order latency.

  SANDBOX usage:
    tick_rec = TickRecorder(mode="sandbox")
    tick_rec.record_candles(index, df)    # after fetch_candles()

  LIVE usage:
    tick_rec = TickRecorder(mode="live")
    tick_rec.record_tick(index, ltp, open_p, high_p, low_p)
                                           # inside on_message()
==============================================================
"""

import os
import csv
import threading
from datetime import datetime


class TickRecorder:
    """
    Thread-safe local CSV tick recorder for sandbox and live bots.

    File naming:
        ticks_sandbox_YYYYMMDD.csv   <- sandbox bot
        ticks_live_YYYYMMDD.csv      <- live bot
    Rolls over to a new file automatically each trading day.
    """

    def __init__(self, mode: str = "sandbox", output_dir: str = "."):
        self.mode       = mode.lower()
        self.output_dir = output_dir
        self._lock      = threading.Lock()
        self._csv_path  = None
        self._ready     = False
        self._init_csv()

    # ------------------------------------------------------------------ init

    def _build_path(self) -> str:
        today = datetime.now().strftime("%Y%m%d")
        return os.path.join(self.output_dir,
                            f"ticks_{self.mode}_{today}.csv")

    def _init_csv(self):
        path = self._build_path()
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "timestamp", "index", "open", "high",
                    "low", "close", "volume", "source"
                ])
        self._csv_path = path
        self._ready    = True
        print(f"[TickRecorder] Initialised -> {path}")

    def _ensure_daily_rollover(self):
        if self._build_path() != self._csv_path:
            self._ready = False
            self._init_csv()

    # ------------------------------------------------------------------ API

    def record_tick(self, index: str, ltp: float,
                    open_p: float = None, high_p: float = None,
                    low_p: float  = None, volume: int = 0):
        """
        Record a single WebSocket tick (live mode).
        Non-blocking — spawns daemon thread.
        """
        if not self._ready:
            return
        self._ensure_daily_rollover()
        ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        row = [
            ts, index,
            open_p if open_p is not None else ltp,
            high_p if high_p is not None else ltp,
            low_p  if low_p  is not None else ltp,
            ltp, volume, "websocket"
        ]
        threading.Thread(target=self._write_row,
                         args=(row,), daemon=True).start()

    def record_candles(self, index: str, df):
        """
        Append all rows from a candle poll DataFrame (sandbox mode).
        Non-blocking — spawns daemon thread.
        """
        if not self._ready or df is None or df.empty:
            return
        self._ensure_daily_rollover()

        def _write():
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with self._lock:
                with open(self._csv_path, "a", newline="",
                          encoding="utf-8") as f:
                    w = csv.writer(f)
                    for _, row in df.iterrows():
                        w.writerow([
                            ts, index,
                            row.get("open",   ""),
                            row.get("high",   ""),
                            row.get("low",    ""),
                            row.get("close",  ""),
                            row.get("volume", 0),
                            "candle_poll"
                        ])
        threading.Thread(target=_write, daemon=True).start()

    def _write_row(self, row: list):
        with self._lock:
            with open(self._csv_path, "a", newline="",
                      encoding="utf-8") as f:
                csv.writer(f).writerow(row)

    @property
    def csv_path(self) -> str:
        return self._csv_path or ""
