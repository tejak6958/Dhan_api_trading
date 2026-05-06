"""
==============================================================
  DhanBot / websocket_feed.py
  [Item iii] 5-MIN CANDLE GATE  [Item iv] 5% SL  [Item vii] NO RSI/EMA
  [Item v]   PARALLEL BUY+SELL handling

  Used exclusively by dhan_live.py (live trading).
  Dhan_api.py (sandbox) uses candle polling instead.

  CHANGES:
    [Item iii] candle_ticks default bumped to 300 (5 min at ~1 tick/sec).
               In dhan_live.py CANDLE_TICKS = 300 drives this.
               The on_message callback also now evaluates
               bullish/bearish engulfing on the rolling 5-min bar buffer.

    [Item iv]  SL = fill_price * 0.95 (5% initial stop loss).
               Target = fill_price * 1.10 (10%).
               Both EOD exit and regular exit updated.

    [Item v]   "BOTH" signal from order_block_signal() now handled:
               when parallel bull+bear OBs confirm, both a CE and PE
               order are placed (one per leg). Tracked in separate
               data["both_leg"] sub-dict.

    [Item vii] combined_signal_fn no longer calls ema_rsi_confirmation.
               The stripping happens in greeks_options.combined_signal —
               websocket_feed receives only OB + Breakout votes.
==============================================================
"""

import logging
import threading
import pandas as pd
from datetime import datetime
from dhanhq.marketfeed import MarketFeed

from Strategies.strategies_order_block import bullish_engulfing, bearish_engulfing

logger = logging.getLogger("DhanBot")


# ── WEBSOCKET FEED SETUP ──────────────────────────────────────

def build_feed(dhan_context, index_ids: dict, on_message_fn):
    """
    Build and return a DhanHQ MarketFeed instance subscribed
    to Quote data for all index instruments.
    """
    try:
        subscriptions = [
            (MarketFeed.INDEX, sid, MarketFeed.Quote)
            for sid in index_ids.values()
        ]
        logger.info(f"[WS FEED] Subscribing INDEX quotes: "
                    f"{list(index_ids.keys())}")
    except AttributeError:
        logger.error("[WS FEED] MarketFeed.INDEX not found — "
                     "falling back to MarketFeed.NSE (older SDK)")
        subscriptions = [
            (MarketFeed.NSE, sid, MarketFeed.Quote)
            for sid in index_ids.values()
        ]

    return MarketFeed(dhan_context, subscriptions, on_message=on_message_fn)


# ── WEBSOCKET CALLBACK ────────────────────────────────────────

def make_on_message(indices: dict, index_ids: dict,
                    candle_ticks: int,
                    combined_signal_fn, select_option_fn,
                    place_order_fn, send_alert_fn,
                    log_info_fn, log_error_fn,
                    log_trade_fn, show_dashboard_fn,
                    run_session_report_fn, check_daily_loss_fn,
                    is_market_open_fn,
                    lot_sizes: dict, min_delta: float, max_gamma: float,
                    order_lock: threading.Lock,
                    backtest_trades: list,
                    dhan,
                    tick_rec=None):
    """
    Factory returning the on_message WebSocket callback.

    [Item iii] candle_ticks should be 300 in dhan_live.py (5-min gate).
    [Item iv]  SL = 5%, Target = 10% on every entry.
    [Item v]   "BOTH" signal places two legs (CE + PE).
    [Item vii] RSI/EMA stripped from combined_signal_fn upstream.
    """

    state = {
        "total_pnl"  : 0.0,
        "trade_count": 0,
        "win_count"  : 0,
        "eod_done"   : False,
    }

    def _place_entry(index, data, ltp, signal, strat_label, opt_sid,
                     name, expiry_str, delta, gamma):
        """
        Inner helper: place one entry leg and update data dict.
        [Item iv] SL = 5%, Target = 10%.
        Returns True if order succeeded.
        """
        qty  = lot_sizes[index]
        resp = place_order_fn(opt_sid, qty, dhan.BUY, name, ltp=ltp)

        if resp and resp.get("status") in ("success", "pending"):
            fill_price = resp.get("fill_price", ltp)
            slippage   = round(fill_price - ltp, 2)
            sl         = round(fill_price * 0.95, 2)   # [Item iv] 5% SL
            target     = round(fill_price * 1.10, 2)   # [Item iv] 10% target

            data.update({
                "in_trade"    : True,
                "entry"       : fill_price,
                "sl"          : sl,
                "target"      : target,
                "opt_sid"     : opt_sid,
                "qty"         : qty,
                "name"        : name,
                "delta"       : delta,
                "gamma"       : gamma,
                "signal"      : signal,
                "strategy"    : strat_label,
            })
            send_alert_fn(
                f"LIVE ENTRY | {index} | {strat_label}\n"
                f"Signal: {signal} | Option: {name}\n"
                f"Signal LTP: Rs.{ltp:.2f}  "
                f"Fill: Rs.{fill_price:.2f} (slip {slippage:+.2f})\n"
                f"Expiry: {expiry_str}  "
                f"Delta: {delta:.3f}  Gamma: {gamma:.5f}\n"
                f"SL: Rs.{sl:.2f} (-5%)  "
                f"Target: Rs.{target:.2f} (+10%)  "
                f"Qty: {qty} | LIVE AUTO"
            )
            log_info_fn(
                f"[{index}] ENTRY {signal} {name} | fill={fill_price} "
                f"sl={sl:.2f} target={target:.2f} strat={strat_label}"
            )
            return True
        else:
            data["order_placed"] = False
            log_error_fn(f"[{index}] Order rejected: {resp}")
            send_alert_fn(f"ORDER REJECTED | {index} | {name}\n"
                          f"Resp: {resp}\nWill retry on next signal.")
            return False

    def on_message(instance, message):
        now = datetime.now()

        # ── EOD FORCE-EXIT at 15:25 ──────────────────────────
        if now.hour == 15 and now.minute >= 25 and not state["eod_done"]:
            for index, data in indices.items():
                if data["in_trade"]:
                    log_info_fn(f"[WS] EOD force-exit for {index}")
                    with order_lock:
                        if not data["in_trade"]:
                            continue
                        eod_ltp  = float(message.get("last_traded_price",
                                                     data["entry"]))
                        eod_resp = place_order_fn(
                            data["opt_sid"], data["qty"],
                            dhan.SELL, data["name"], ltp=eod_ltp)
                        exit_fill     = eod_resp.get("fill_price", eod_ltp)
                        exit_slippage = round(exit_fill - eod_ltp, 2)
                        pnl           = (exit_fill - data["entry"]) * data["qty"]

                        state["total_pnl"]   += pnl
                        state["trade_count"] += 1
                        if pnl > 0:
                            state["win_count"] += 1

                        backtest_trades.append([
                            datetime.now(), data["name"], data["strategy"],
                            "EOD EXIT", data["entry"], exit_fill,
                            pnl, data["delta"], data["gamma"]
                        ])
                        log_trade_fn([
                            datetime.now(), data["name"], data["strategy"],
                            "EOD EXIT", data["entry"], exit_fill,
                            round(pnl, 2), data["delta"], data["gamma"]
                        ])
                        send_alert_fn(
                            f"EOD EXIT | {index} | {data['name']}\n"
                            f"Entry: Rs.{data['entry']:.2f}  "
                            f"Exit Fill: Rs.{exit_fill:.2f} "
                            f"(slip {exit_slippage:+.2f})\n"
                            f"PnL: Rs.{pnl:.2f} | Mode: LIVE"
                        )
                        data.update({"in_trade": False, "order_placed": False})
            run_session_report_fn()
            state["eod_done"] = True
            return

        if not is_market_open_fn():
            return
        if check_daily_loss_fn(state["total_pnl"]):
            return

        # ── Extract tick ─────────────────────────────────────
        sid = str(message.get("security_id", ""))
        ltp = message.get("last_traded_price")
        if not ltp:
            return

        for index, idx_sid in index_ids.items():
            if sid != idx_sid:
                continue

            data = indices[index]
            data["tick_count"] += 1

            if tick_rec is not None:
                tick_rec.record_tick(
                    index, float(ltp),
                    open_p=message.get("open_price"),
                    high_p=message.get("high_price"),
                    low_p =message.get("low_price"),
                    volume=message.get("volume", 0),
                )

            candle = {
                "open" : message.get("open_price",  ltp),
                "high" : message.get("high_price",  ltp),
                "low"  : message.get("low_price",   ltp),
                "close": ltp,
            }
            data["buffer"].append(candle)
            if len(data["buffer"]) > 500:
                data["buffer"].pop(0)

            # ── [Item iii] 5-min tick gate ───────────────────
            # candle_ticks = 300 in dhan_live.py -> evaluate every 5 min
            if data["tick_count"] % candle_ticks != 0:
                continue
            if len(data["buffer"]) < 60:
                log_info_fn(f"[{index}] Warming up: "
                            f"{len(data['buffer'])}/60 bars")
                continue

            df  = pd.DataFrame(data["buffer"])
            ltp = float(df.iloc[-1]["close"])
            log_info_fn(f"[{index}] 5-min gate | LTP={ltp:.2f} | "
                        f"bars={len(data['buffer'])}")

            # ── [Item iii] Engulfing check on latest 5-min bars
            bull_eng = bullish_engulfing(df)
            bear_eng = bearish_engulfing(df)
            if bull_eng:
                log_info_fn(f"[{index}] Bullish Engulfing detected")
            if bear_eng:
                log_info_fn(f"[{index}] Bearish Engulfing detected")

            # ── EXIT CHECK ───────────────────────────────────
            if data["in_trade"]:
                exit_flag, reason = False, ""
                if ltp <= data["sl"]:
                    exit_flag, reason = True, "SL HIT"
                elif ltp >= data["target"]:
                    exit_flag, reason = True, "TARGET HIT"

                if exit_flag:
                    with order_lock:
                        if not data["in_trade"]:
                            continue
                        exit_resp     = place_order_fn(
                            data["opt_sid"], data["qty"],
                            dhan.SELL, data["name"], ltp=ltp)
                        exit_fill     = exit_resp.get("fill_price", ltp)
                        exit_slippage = round(exit_fill - ltp, 2)
                        pnl           = (exit_fill - data["entry"]) * data["qty"]

                        state["total_pnl"]   += pnl
                        state["trade_count"] += 1
                        if pnl > 0:
                            state["win_count"] += 1

                        backtest_trades.append([
                            datetime.now(), data["name"], data["strategy"],
                            reason, data["entry"], exit_fill,
                            pnl, data["delta"], data["gamma"]
                        ])
                        log_trade_fn([
                            datetime.now(), data["name"], data["strategy"],
                            reason, data["entry"], exit_fill,
                            round(pnl, 2), data["delta"], data["gamma"]
                        ])
                        send_alert_fn(
                            f"EXIT | {index} | {data['strategy']}\n"
                            f"Reason: {reason} | Option: {data['name']}\n"
                            f"Entry: Rs.{data['entry']:.2f}  "
                            f"Exit LTP: Rs.{ltp:.2f}  "
                            f"Fill: Rs.{exit_fill:.2f} "
                            f"(slip {exit_slippage:+.2f})\n"
                            f"PnL: Rs.{pnl:.2f}  "
                            f"Total: Rs.{state['total_pnl']:.2f} | LIVE"
                        )
                        data.update({"in_trade": False, "order_placed": False})
                        show_dashboard_fn(state["trade_count"],
                                          state["win_count"],
                                          state["total_pnl"])
                continue

            # ── ENTRY CHECK ──────────────────────────────────
            # [Item vii] combined_signal_fn no longer includes RSI/EMA
            signal, strat_label = combined_signal_fn(df, index, ltp)
            if not signal or data["order_placed"]:
                continue

            # ── [Item v] Handle BOTH (parallel CE + PE) ──────
            if signal == "BOTH":
                log_info_fn(f"[{index}] BOTH signal — parallel CE+PE entry")
                for direction in ("BUY", "SELL"):
                    with order_lock:
                        if data["in_trade"]:
                            break
                        opt_sid, name, expiry_str, delta, gamma = \
                            select_option_fn(index, ltp, direction)
                        if not opt_sid:
                            send_alert_fn(
                                f"BOTH: No {direction} option near "
                                f"{ltp:.0f} for {index}")
                            continue
                        if abs(delta) < min_delta or gamma > max_gamma:
                            continue
                        data["order_placed"] = True
                        _place_entry(index, data, ltp, direction,
                                     strat_label, opt_sid, name,
                                     expiry_str, delta, gamma)
                show_dashboard_fn(state["trade_count"],
                                  state["win_count"],
                                  state["total_pnl"])
                continue

            # ── Standard single-direction entry ──────────────
            with order_lock:
                if data["in_trade"] or data["order_placed"]:
                    continue

                opt_sid, name, expiry_str, delta, gamma = select_option_fn(
                    index, ltp, signal)

                if not opt_sid:
                    send_alert_fn(
                        f"No ATM option near {ltp:.0f} for {index}")
                    continue

                if abs(delta) < min_delta:
                    send_alert_fn(
                        f"{index} {name}: |delta|={abs(delta):.2f} "
                        f"< {min_delta} -- skip OTM")
                    continue
                if gamma > max_gamma:
                    send_alert_fn(
                        f"{index} {name}: gamma={gamma:.5f} "
                        f"> {max_gamma} -- skip near-expiry")
                    continue

                data["order_placed"] = True
                ok = _place_entry(index, data, ltp, signal,
                                  strat_label, opt_sid, name,
                                  expiry_str, delta, gamma)
                if ok:
                    show_dashboard_fn(state["trade_count"],
                                      state["win_count"],
                                      state["total_pnl"])

    return on_message, state