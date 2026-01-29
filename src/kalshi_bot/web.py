import logging
import os
import threading
import time
from collections import deque
from pathlib import Path

logging.basicConfig(level=logging.WARNING, format="%(name)s %(levelname)s: %(message)s")

from flask import Flask, render_template, request, jsonify, redirect, url_for

from kalshi_bot import db
from kalshi_bot.config import load_config
from kalshi_bot.client import create_client
from kalshi_bot.scanner import scan
from kalshi_bot.whale import run_whale_strategy

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "kalshi-bot-dev-key")

# ---------------------------------------------------------------------------
# Shared state for background whale-trade
# ---------------------------------------------------------------------------

_whale_state = {
    "running": False,
    "thread": None,
    "logs": deque(maxlen=500),
    "stop_requested": False,
}
_whale_lock = threading.Lock()


def _get_client():
    """Create an authenticated Kalshi client (cached on app config)."""
    if "kalshi_client" not in app.config:
        cfg = load_config()
        app.config["kalshi_client"] = create_client(cfg)
    return app.config["kalshi_client"]


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard():
    db.init_db()
    try:
        client = _get_client()
        bal_data = client.get_balance()
        balance_cents = bal_data.get("balance", 0)
        db.log_balance(balance_cents)
    except Exception as e:
        balance_cents = 0

    open_positions = db.get_open_positions()
    stats = db.get_stats()

    # Unrealized P&L from open positions
    total_unrealized = 0
    try:
        client = _get_client()
        for p in open_positions:
            try:
                m = client.get_market(ticker=p["ticker"])
                if p["side"] == "yes":
                    current = m.get("yes_bid", 0) or 0
                else:
                    current = m.get("no_bid", 0) or 0
                total_unrealized += int(p["quantity"] * (current - p["avg_entry_price_cents"]))
            except Exception:
                pass
    except Exception:
        pass

    realized = stats["realized_pnl_cents"]
    total_pnl = realized + total_unrealized

    return render_template(
        "dashboard.html",
        balance_cents=balance_cents,
        unrealized_cents=total_unrealized,
        realized_cents=realized,
        total_pnl_cents=total_pnl,
        open_count=len(open_positions),
        total_trades=stats["total_orders"],
        win_rate=stats["win_rate"],
        profit_factor=stats["profit_factor"],
    )


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

@app.route("/positions")
def positions():
    db.init_db()
    open_positions = db.get_open_positions()

    enriched = []
    try:
        client = _get_client()
        for p in open_positions:
            entry = p["avg_entry_price_cents"]
            qty = p["quantity"]
            try:
                m = client.get_market(ticker=p["ticker"])
                if p["side"] == "yes":
                    current = m.get("yes_bid", 0) or 0
                else:
                    current = m.get("no_bid", 0) or 0
            except Exception:
                current = 0
            unrealized = int(qty * (current - entry))
            enriched.append({
                **p,
                "current_price": current,
                "unrealized_cents": unrealized,
            })
    except Exception:
        enriched = [{**p, "current_price": 0, "unrealized_cents": 0} for p in open_positions]

    return render_template("positions.html", positions=enriched)


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

@app.route("/trades")
def trades():
    db.init_db()
    ticker = request.args.get("ticker", "").strip() or None
    limit = request.args.get("limit", 50, type=int)
    trade_list = db.get_trade_history(limit=limit, ticker=ticker)
    return render_template(
        "trades.html",
        trades=trade_list,
        filter_ticker=ticker or "",
        filter_limit=limit,
    )


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

@app.route("/scanner")
def scanner():
    db.init_db()
    results = []
    scan_stats = {}
    error = None
    try:
        client = _get_client()
        results, scan_stats = scan(client, min_price=95, min_volume=1000, top_n=30, use_cache=True)
    except Exception as e:
        error = str(e)
    return render_template("scanner.html", results=results, scan_stats=scan_stats, error=error)


@app.route("/scanner/refresh", methods=["POST"])
def scanner_refresh():
    try:
        client = _get_client()
        results, scan_stats = scan(client, min_price=95, min_volume=1000, top_n=30, use_cache=False)
        data = []
        for m in results:
            data.append({
                "ticker": m.get("ticker", "?"),
                "side": m["signal_side"],
                "price": m["signal_price"],
                "volume_24h": m.get("volume_24h", 0),
                "volume": m.get("volume", 0),
                "open_interest": m.get("open_interest", 0),
                "event": m.get("event_ticker", ""),
                "tier": m.get("tier", 3),
            })
        return jsonify({"ok": True, "count": len(data), "scan_stats": scan_stats, "results": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ---------------------------------------------------------------------------
# Control Panel
# ---------------------------------------------------------------------------

@app.route("/control")
def control():
    with _whale_lock:
        running = _whale_state["running"]
    return render_template("control.html", running=running)


@app.route("/control/start", methods=["POST"])
def control_start():
    with _whale_lock:
        if _whale_state["running"]:
            return redirect(url_for("control"))
        _whale_state["running"] = True
        _whale_state["stop_requested"] = False
        _whale_state["logs"].clear()

    dry_run = request.form.get("dry_run") == "on"
    prefixes_raw = request.form.get("prefixes", "KXNFL,KXNBA,KXBTC,KXETH")
    prefixes = tuple(p.strip() for p in prefixes_raw.split(",") if p.strip())

    def _log(msg):
        _whale_state["logs"].append(msg)

    def _run():
        try:
            db.init_db()
            client = _get_client()
            _log(f"Starting whale strategy ({'DRY RUN' if dry_run else 'LIVE'})...")
            run_whale_strategy(
                client,
                prefixes=prefixes,
                dry_run=dry_run,
                log=_log,
            )
            _log("Strategy run complete.")
        except Exception as e:
            _log(f"ERROR: {e}")
        finally:
            with _whale_lock:
                _whale_state["running"] = False

    t = threading.Thread(target=_run, daemon=True)
    _whale_state["thread"] = t
    t.start()

    return redirect(url_for("control"))


@app.route("/control/stop", methods=["POST"])
def control_stop():
    with _whale_lock:
        _whale_state["stop_requested"] = True
        _whale_state["logs"].append("Stop requested — will finish current operation...")
    return redirect(url_for("control"))


@app.route("/control/logs")
def control_logs():
    with _whale_lock:
        running = _whale_state["running"]
        logs = list(_whale_state["logs"])
    return jsonify({"running": running, "logs": logs})


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/api/balance")
def api_balance():
    try:
        client = _get_client()
        bal = client.get_balance()
        return jsonify({"ok": True, "balance_cents": bal.get("balance", 0)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

def main():
    db.init_db()
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)


if __name__ == "__main__":
    main()
