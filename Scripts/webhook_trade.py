"""
==============================================================
  DhanBot / webhook_trade.py
  [Item iv] WEBHOOK RECEIVER + CORE TRADE LOGIC

  CHANGES:
    [Item iv] SL changed from 25% (fill * 0.75) to 5% (fill * 0.95).
              Target adjusted to 10% (fill * 1.10) from 50%.
              Rationale: 5% SL works in live since sandbox uses
              historical/synthetic data where price swings are
              exaggerated. A tighter SL preserves capital on
              real market moves and is a better live default.
    [Item v]  "BOTH" signal handling added — when OB strategy
              returns "BOTH" (parallel buy+sell), execute_signal
              is called with direction the caller specifies; the
              "BOTH" routing lives in Dhan_api.py / dhan_live.py.

  Shared by Dhan_api.py (sandbox, paper orders)
         and dhan_live.py (live, real orders).

  execute_signal() is the single entry-point for both the
  polling loop and the webhook receiver.
==============================================================
"""

import threading
import logging

logger = logging.getLogger("DhanBot")

try:
    from flask import Flask, request as flask_request, jsonify
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    logger.warning("Flask not installed -- webhook receiver disabled. "
                   "Install with: pip install flask")


# ── CORE TRADE LOGIC ─────────────────────────────────────────

def execute_signal(index: str, ltp: float, signal: str,
                   strat_label: str, context: dict,
                   source: str = "poll"):
    """
    Shared entry logic called by BOTH the polling/WebSocket loop
    AND the webhook receiver.

    [Item iv] SL = fill_price * 0.95  (5% stop loss)
              Target = fill_price * 1.10 (10% target)
              Previously SL=0.75 (25%), Target=1.50 (50%).

    [Item v]  Handles signal="BUY" or "SELL" only. "BOTH" is
              resolved by the caller (Dhan_api / websocket_feed)
              which calls execute_signal twice with each direction.

    Args:
        index       : "NIFTY" or "BANKNIFTY"
        ltp         : underlying index price at signal time
        signal      : "BUY" or "SELL" (or None -> no-op)
        strat_label : comma-separated strategy names that fired
        context     : dict with all required callables and state
        source      : "poll" | "webhook" — for logging/alert labels
    """
    if not signal or signal not in ("BUY", "SELL"):
        return

    indices          = context["indices"]
    order_lock       = context["order_lock"]
    select_option_fn = context["select_option_fn"]
    place_order_fn   = context["place_order_fn"]
    fetch_premium_fn = context.get("fetch_premium_fn")
    send_alert_fn    = context["send_alert_fn"]
    log_info_fn      = context["log_info_fn"]
    lot_sizes        = context["lot_sizes"]
    min_delta        = context["min_delta"]
    max_gamma        = context["max_gamma"]
    mode             = context.get("mode", "sandbox").upper()

    data = indices[index]

    if data["order_placed"]:
        return

    with order_lock:
        if data["in_trade"] or data["order_placed"]:
            return

        opt_sid, name, expiry_str, delta, gamma = select_option_fn(
            index, ltp, signal)

        if not opt_sid:
            send_alert_fn(f"No ATM option near strike {ltp:.0f} for {index}")
            return

        abs_delta = abs(delta)
        if abs_delta < min_delta:
            send_alert_fn(f"{index} {name}: delta {abs_delta:.2f} "
                          f"< {min_delta} -- skip (OTM)")
            return
        if gamma > max_gamma:
            send_alert_fn(f"{index} {name}: gamma {gamma:.5f} "
                          f"> {max_gamma} -- skip (near expiry)")
            return

        qty = lot_sizes[index]
        data["order_placed"] = True

        if fetch_premium_fn is not None:
            order_ltp = fetch_premium_fn(opt_sid, index)
        else:
            order_ltp = ltp

        resp       = place_order_fn(opt_sid, qty, "BUY", name, ltp=order_ltp)
        fill_price = resp.get("fill_price", order_ltp)
        slippage   = round(fill_price - order_ltp, 2)

        # [Item iv] SL = 5% below fill, Target = 10% above fill
        sl     = round(fill_price * 0.95, 2)   # was 0.75 (25%)
        target = round(fill_price * 1.10, 2)   # was 1.50 (50%)

        data.update({
            "in_trade" : True,
            "entry"    : fill_price,
            "sl"       : sl,
            "target"   : target,
            "opt_sid"  : opt_sid,
            "qty"      : qty,
            "name"     : name,
            "delta"    : delta,
            "gamma"    : gamma,
            "signal"   : signal,
            "strategy" : strat_label,
        })

        send_alert_fn(
            f"ENTRY | {index} | {strat_label} | {mode}\n"
            f"Signal: {signal} | Option: {name}\n"
            f"Signal LTP: Rs.{ltp:.2f}  "
            f"Fill: Rs.{fill_price:.2f} (slip {slippage:+.2f})\n"
            f"Expiry: {expiry_str}  "
            f"Delta: {delta:.3f}  Gamma: {gamma:.5f}\n"
            f"SL: Rs.{sl:.2f} (-5%)  Target: Rs.{target:.2f} (+10%)  "
            f"Qty: {qty} | Source: {source.upper()}"
        )
        log_info_fn(
            f"execute_signal: {index} {signal} {name} "
            f"fill={fill_price} sl={sl:.2f} target={target:.2f} "
            f"source={source} mode={mode}"
        )


# ── WEBHOOK RECEIVER ──────────────────────────────────────────

def start_webhook(context: dict, index_ids: dict,
                  is_market_open_fn, market_status_reason_fn,
                  check_daily_loss_fn=None,
                  port: int = 5001,
                  mode: str = "SANDBOX"):
    """
    Create and start a Flask webhook server in a daemon thread.

    Accepts TradingView POST alerts and routes them to execute_signal().
    Same Greeks filter and order logic as the polling/WebSocket path.

    Endpoints:
        GET  /         -> health check
        GET  /status   -> watch-only bot state (no orders)
        POST /webhook  -> TradingView alert -> execute_signal()

    TradingView alert JSON body:
        {"index":"NIFTY","signal":"BUY","ltp":{{close}},"strategy":"OB"}

    Args:
        context              : same context dict as execute_signal()
        index_ids            : {"NIFTY": "13", "BANKNIFTY": "25"}
        is_market_open_fn    : callable -> bool
        market_status_reason_fn: callable -> str
        check_daily_loss_fn  : callable(total_pnl) -> bool [live only]
        port                 : Flask port (5001 sandbox, 5002 live)
        mode                 : "SANDBOX" or "LIVE" (label only)

    Returns:
        threading.Thread (already started) or None if Flask unavailable
    """
    if not FLASK_AVAILABLE:
        print("Flask not installed -- webhook disabled. pip install flask")
        return None

    indices      = context["indices"]
    pnl_state    = context.get("pnl_state", {})
    log_info_fn  = context["log_info_fn"]
    log_error_fn = context.get("log_error_fn", logger.error)

    app = Flask(f"dhan_{mode.lower()}_webhook")

    @app.route("/", methods=["GET"])
    def root():
        return jsonify({
            "status" : "running",
            "bot"    : f"Dhan {mode} Bot",
            "mode"   : mode,
            "market" : market_status_reason_fn(),
            "endpoints": {
                "POST /webhook": "Send TradingView alert",
                "GET  /status" : "Watch-only bot state",
            }
        }), 200

    @app.route("/favicon.ico", methods=["GET"])
    def favicon():
        return "", 204

    @app.route("/status", methods=["GET"])
    def status():
        state_out = {
            idx: {
                "in_trade"   : d["in_trade"],
                "entry"      : d["entry"],
                "sl"         : d["sl"],
                "target"     : d["target"],
                "option_name": d["name"],
                "strategy"   : d["strategy"],
                "signal"     : d["signal"],
            }
            for idx, d in indices.items()
        }
        return jsonify({
            "mode"      : mode,
            "market"    : market_status_reason_fn(),
            "total_pnl" : round(pnl_state.get("total_pnl", 0), 2),
            "trades"    : pnl_state.get("trade_count", 0),
            "wins"      : pnl_state.get("win_count", 0),
            "indices"   : state_out,
        }), 200

    @app.route("/webhook", methods=["POST"])
    def webhook():
        try:
            payload  = flask_request.get_json(force=True)
            index    = str(payload.get("index",    "")).upper()
            signal   = str(payload.get("signal",   "")).upper()
            ltp      = float(payload.get("ltp",    0))
            strategy = str(payload.get("strategy", "TradingView"))

            log_info_fn(f"[WEBHOOK] index={index} signal={signal} "
                        f"ltp={ltp} strategy={strategy}")

            if index not in index_ids:
                return jsonify({"status": "error",
                                "reason": f"Unknown index: {index}"}), 400
            if signal not in ("BUY", "SELL"):
                return jsonify({"status": "error",
                                "reason": f"Invalid signal: {signal}"}), 400
            if ltp <= 0:
                return jsonify({"status": "error",
                                "reason": "ltp must be > 0"}), 400
            if not is_market_open_fn():
                return jsonify({"status": "skipped",
                                "reason": market_status_reason_fn()}), 200
            if check_daily_loss_fn and check_daily_loss_fn(
                    pnl_state.get("total_pnl", 0)):
                return jsonify({"status": "skipped",
                                "reason": "daily loss limit hit"}), 200

            execute_signal(index, ltp, signal, strategy,
                           context=context, source="webhook")

            return jsonify({"status": "ok", "index": index,
                            "signal": signal, "ltp": ltp}), 200

        except Exception as e:
            log_error_fn(f"[WEBHOOK] Error: {e}")
            return jsonify({"status": "error", "reason": str(e)}), 500

    def _start():
        log_info_fn(f"[WEBHOOK] {mode} server starting on port {port}")
        print(f"[WEBHOOK] {mode}: http://0.0.0.0:{port}/webhook")
        print(f"[STATUS]  {mode}: http://0.0.0.0:{port}/status")
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

    t = threading.Thread(target=_start, daemon=True,
                         name=f"Webhook_{mode}")
    t.start()
    return t