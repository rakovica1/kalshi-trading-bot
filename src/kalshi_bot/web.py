import logging
import os
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(level=logging.WARNING, format="%(name)s %(levelname)s: %(message)s")

import functools
import hmac

from flask import Flask, render_template, request, jsonify, redirect, url_for, session, abort

from kalshi_bot import db
from kalshi_bot.config import load_config
from kalshi_bot.client import create_client
from kalshi_bot.whale import run_whale_strategy
from kalshi_bot.scanner import scan

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

# ---------------------------------------------------------------------------
# Shared state for background scanner
# ---------------------------------------------------------------------------

_scan_state = {
    "running": False,
    "thread": None,
    "error": None,
}
_scan_lock = threading.Lock()


def _require_control_password(f):
    """Decorator: require CONTROL_PASSWORD session auth for protected routes."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        password = os.environ.get("CONTROL_PASSWORD", "")
        if not password:
            # No password configured — block access entirely
            abort(403)
        if not session.get("control_authed"):
            if request.is_json or request.headers.get("X-Requested-With"):
                return jsonify({"ok": False, "error": "Authentication required"}), 401
            return redirect(url_for("control_login"))
        return f(*args, **kwargs)
    return wrapper


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

_EST = timezone(timedelta(hours=-5))


def _utc_to_est(utc_str):
    """Convert a 'YYYY-MM-DD HH:MM:SS' UTC string to EST."""
    try:
        dt = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.astimezone(_EST).strftime("%Y-%m-%d %I:%M:%S %p EST")
    except Exception:
        return utc_str


@app.route("/trades")
def trades():
    db.init_db()
    ticker = request.args.get("ticker", "").strip() or None
    limit = request.args.get("limit", 50, type=int)
    trade_list = db.get_trade_history(limit=limit, ticker=ticker)
    for t in trade_list:
        if t.get("created_at"):
            t["created_at"] = _utc_to_est(t["created_at"])
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
    with _scan_lock:
        scanning = _scan_state["running"]
        scan_error = _scan_state["error"]
    results, scan_stats, scanned_at = db.get_scan_results()
    return render_template(
        "scanner.html",
        results=results,
        scan_stats=scan_stats,
        scanned_at=scanned_at,
        scanning=scanning,
        scan_error=scan_error,
    )


@app.route("/scanner/start", methods=["POST"])
def scanner_start():
    with _scan_lock:
        if _scan_state["running"]:
            return jsonify({"ok": False, "error": "Scan already running"})
        _scan_state["running"] = True
        _scan_state["error"] = None

    def _run_scan():
        try:
            db.init_db()
            client = _get_client()
            results, stats = scan(client, min_price=95, min_volume=1000, top_n=5000)
            db.save_scan_results(results, stats)
        except Exception as e:
            with _scan_lock:
                _scan_state["error"] = str(e)
        finally:
            with _scan_lock:
                _scan_state["running"] = False

    t = threading.Thread(target=_run_scan, daemon=True)
    _scan_state["thread"] = t
    t.start()
    return jsonify({"ok": True})


@app.route("/scanner/status")
def scanner_status():
    with _scan_lock:
        running = _scan_state["running"]
        error = _scan_state["error"]
    return jsonify({"running": running, "error": error})


# ---------------------------------------------------------------------------
# Control Panel — auth
# ---------------------------------------------------------------------------

@app.route("/control/login", methods=["GET", "POST"])
def control_login():
    password = os.environ.get("CONTROL_PASSWORD", "")
    if not password:
        abort(403)
    if request.method == "POST":
        submitted = request.form.get("password", "")
        if hmac.compare_digest(submitted, password):
            session["control_authed"] = True
            return redirect(url_for("control"))
        return render_template("login.html", error="Incorrect password")
    return render_template("login.html", error=None)


@app.route("/control/logout", methods=["POST"])
def control_logout():
    session.pop("control_authed", None)
    return redirect(url_for("control_login"))


# ---------------------------------------------------------------------------
# Control Panel
# ---------------------------------------------------------------------------

@app.route("/control")
@_require_control_password
def control():
    with _whale_lock:
        running = _whale_state["running"]
    return render_template("control.html", running=running)


@app.route("/control/start", methods=["POST"])
@_require_control_password
def control_start():
    with _whale_lock:
        if _whale_state["running"]:
            return redirect(url_for("control"))
        _whale_state["running"] = True
        _whale_state["stop_requested"] = False
        _whale_state["logs"].clear()

    dry_run = request.form.get("dry_run") == "on"
    tier1_only = request.form.get("tier1_only") == "on"
    max_positions = request.form.get("max_positions", 1, type=int)
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
                tier1_only=tier1_only,
                max_positions=max_positions,
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
@_require_control_password
def control_stop():
    with _whale_lock:
        _whale_state["stop_requested"] = True
        _whale_state["logs"].append("Stop requested — will finish current operation...")
    return redirect(url_for("control"))


@app.route("/control/logs")
@_require_control_password
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
