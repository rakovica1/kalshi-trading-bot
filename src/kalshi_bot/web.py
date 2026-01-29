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
from kalshi_bot.ticker import decode_ticker

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "kalshi-bot-dev-key")
app.jinja_env.filters["decode_ticker"] = decode_ticker

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
    total_fees = stats["total_fees_cents"]
    net_pnl = total_pnl - total_fees

    return render_template(
        "dashboard.html",
        balance_cents=balance_cents,
        unrealized_cents=total_unrealized,
        realized_cents=realized,
        total_pnl_cents=total_pnl,
        total_fees_cents=total_fees,
        net_pnl_cents=net_pnl,
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
            close_time = ""
            try:
                m = client.get_market(ticker=p["ticker"])
                if p["side"] == "yes":
                    current = m.get("yes_bid", 0) or 0
                else:
                    current = m.get("no_bid", 0) or 0
                close_time = m.get("close_time") or m.get("expected_expiration_time") or ""
            except Exception:
                current = 0
            unrealized = int(qty * (current - entry))
            # Format opened_at for display
            opened_at = p.get("opened_at", "")
            if opened_at and isinstance(opened_at, str):
                opened_at_display = _utc_to_est(opened_at)
            elif hasattr(opened_at, "strftime"):
                opened_at_display = _utc_to_est(opened_at)
            else:
                opened_at_display = str(opened_at)
            enriched.append({
                **p,
                "current_price": current,
                "unrealized_cents": unrealized,
                "opened_at_display": opened_at_display,
                "close_time": close_time,
            })
    except Exception:
        enriched = [{
            **p,
            "current_price": 0,
            "unrealized_cents": 0,
            "opened_at_display": _utc_to_est(p.get("opened_at", "")),
            "close_time": "",
        } for p in open_positions]

    return render_template("positions.html", positions=enriched)


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

_EST = timezone(timedelta(hours=-5))


def _utc_to_est(val):
    """Convert a UTC timestamp (string or datetime) to EST display string."""
    try:
        if isinstance(val, datetime):
            dt = val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
        else:
            dt = datetime.strptime(str(val), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.astimezone(_EST).strftime("%Y-%m-%d %I:%M:%S %p EST")
    except Exception:
        return str(val)


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
# Charts
# ---------------------------------------------------------------------------

def _fetch_candlestick_history(client, tickers, hours=24):
    """Fetch candlestick history for a list of tickers via the Kalshi API.

    Uses 1-minute candles for <6h, 1-hour candles otherwise.
    Returns dict mapping ticker -> list of {ts, yes_bid, yes_ask, no_bid, no_ask}.
    """
    import time as _time
    now = int(_time.time())
    start = now - int(hours * 3600)
    interval = 1 if hours <= 6 else 60

    result = {}
    try:
        raw = client.batch_get_market_candlesticks(
            tickers=tickers, start_ts=start, end_ts=now,
            period_interval=interval,
        )
        # raw is dict: ticker -> list of candlestick dicts
        for ticker, candles in raw.items():
            points = []
            for c in candles:
                ts = c.get("end_period_ts", 0)
                yes_bid_d = c.get("yes_bid") or {}
                yes_ask_d = c.get("yes_ask") or {}
                points.append({
                    "ts": ts,
                    "yes_bid": yes_bid_d.get("close", 0) or 0,
                    "yes_ask": yes_ask_d.get("close", 0) or 0,
                    "yes_bid_high": yes_bid_d.get("high", 0) or 0,
                    "yes_bid_low": yes_bid_d.get("low", 0) or 0,
                    "volume": c.get("volume", 0) or 0,
                })
            points.sort(key=lambda p: p["ts"])
            result[ticker] = points
    except Exception:
        pass
    return result


def _build_position_data(client, open_positions, candle_history):
    """Enrich open positions with current prices and chart history."""
    enriched = []
    for p in open_positions:
        entry = p["avg_entry_price_cents"]
        qty = p["quantity"]
        try:
            m = client.get_market(ticker=p["ticker"])
            if p["side"] == "yes":
                bid = m.get("yes_bid", 0) or 0
                ask = m.get("yes_ask", 0) or 0
            else:
                bid = m.get("no_bid", 0) or 0
                ask = m.get("no_ask", 0) or 0
            close_time = m.get("close_time") or m.get("expected_expiration_time") or ""
            db.log_price_snapshot(p["ticker"], p["side"], bid, ask)
        except Exception:
            bid = 0
            ask = 0
            close_time = ""
        unrealized = int(qty * (bid - entry))

        # Build history from candlestick data
        history = []
        candles = candle_history.get(p["ticker"], [])
        bid_high = bid if bid > 0 else 0
        bid_low = bid if bid > 0 else 100
        for c in candles:
            if p["side"] == "yes":
                h_bid = c.get("yes_bid", 0)
                h_ask = c.get("yes_ask", 0)
                h_high = c.get("yes_bid_high", 0) or h_bid
                h_low = c.get("yes_bid_low", 0) or h_bid
            else:
                h_bid = max(0, 100 - (c.get("yes_ask", 0) or 0))
                h_ask = max(0, 100 - (c.get("yes_bid", 0) or 0))
                h_high = max(0, 100 - (c.get("yes_bid_low", 0) or 0)) if c.get("yes_bid_low") else h_bid
                h_low = max(0, 100 - (c.get("yes_bid_high", 0) or 0)) if c.get("yes_bid_high") else h_bid
            if h_bid > 0:
                bid_high = max(bid_high, h_high if h_high > 0 else h_bid)
                bid_low = min(bid_low, h_low if h_low > 0 else h_bid)
            history.append({
                "ts": c["ts"],
                "bid_cents": h_bid,
                "ask_cents": h_ask,
            })

        if bid_low > bid_high:
            bid_low = bid_high

        enriched.append({
            **p,
            "current_bid": bid,
            "current_ask": ask,
            "unrealized_cents": unrealized,
            "close_time": close_time,
            "history": history,
            "bid_high": bid_high,
            "bid_low": bid_low,
        })
    return enriched


@app.route("/charts")
def charts():
    db.init_db()
    open_positions = db.get_open_positions()

    enriched = []
    try:
        client = _get_client()
        tickers = [p["ticker"] for p in open_positions]
        candle_history = _fetch_candlestick_history(client, tickers, hours=24) if tickers else {}
        enriched = _build_position_data(client, open_positions, candle_history)
    except Exception:
        enriched = [{
            **p,
            "current_bid": 0,
            "current_ask": 0,
            "unrealized_cents": 0,
            "close_time": "",
            "history": [],
        } for p in open_positions]

    # Cleanup old snapshots periodically
    try:
        db.cleanup_old_snapshots(hours=48)
    except Exception:
        pass

    return render_template("charts.html", positions=enriched)


@app.route("/api/charts/prices")
def api_charts_prices():
    """Return current prices + candlestick history for all open positions."""
    db.init_db()
    open_positions = db.get_open_positions()
    try:
        client = _get_client()
        tickers = [p["ticker"] for p in open_positions]
        candle_history = _fetch_candlestick_history(client, tickers, hours=24) if tickers else {}
        enriched = _build_position_data(client, open_positions, candle_history)
        result = []
        for pos in enriched:
            result.append({
                "ticker": pos["ticker"],
                "side": pos["side"],
                "entry_cents": pos["avg_entry_price_cents"],
                "quantity": pos["quantity"],
                "current_bid": pos["current_bid"],
                "current_ask": pos["current_ask"],
                "unrealized_cents": pos["unrealized_cents"],
                "close_time": pos["close_time"],
                "history": pos["history"],
                "bid_high": pos["bid_high"],
                "bid_low": pos["bid_low"],
            })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})
    return jsonify({"ok": True, "positions": result})


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
    if scanned_at:
        scanned_at = _utc_to_est(scanned_at)
    from kalshi_bot.scanner import format_close_time, hours_until_close
    for r in results:
        r["close_time_fmt"] = format_close_time(r.get("close_time", ""))
        r["hours_left"] = hours_until_close(r.get("close_time", ""))
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
            results, stats = scan(client, min_price=95, min_volume=10000, top_n=500)
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
# Execute — auth
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
# Execute
# ---------------------------------------------------------------------------

@app.route("/control")
@_require_control_password
def control():
    defaults = {
        "prefixes": "KXNFL,KXNBA,KXBTC,KXETH",
        "max_positions": 5,
        "max_hours": 24,
        "cooldown_minutes": 1,
        "continuous": True,
        "tier1_only": True,
        "dry_run": True,
    }
    with _whale_lock:
        running = _whale_state["running"]
        settings = _whale_state.get("settings", defaults)
    return render_template("control.html", running=running, settings=settings)


@app.route("/control/start", methods=["POST"])
@_require_control_password
def control_start():
    with _whale_lock:
        if _whale_state["running"]:
            return redirect(url_for("control"))
        _whale_state["running"] = True
        _whale_state["stop_requested"] = False
        _whale_state["logs"].clear()

    import time as _time

    dry_run = request.form.get("dry_run") == "on"
    tier1_only = request.form.get("tier1_only") == "on"
    continuous = request.form.get("continuous") == "on"

    # Parse max_positions — default to 5 if missing or invalid
    try:
        max_positions = int(request.form.get("max_positions", "5"))
        if max_positions < 1:
            max_positions = 1
        elif max_positions > 50:
            max_positions = 50
    except (ValueError, TypeError):
        max_positions = 5

    # Parse cooldown
    try:
        cooldown_minutes = float(request.form.get("cooldown_minutes", "1"))
        if cooldown_minutes < 0.1:
            cooldown_minutes = 0.1
    except (ValueError, TypeError):
        cooldown_minutes = 1.0

    prefixes_raw = request.form.get("prefixes", "KXNFL,KXNBA,KXBTC,KXETH")
    prefixes = tuple(p.strip() for p in prefixes_raw.split(",") if p.strip())

    # Store form values so the UI preserves them after redirect
    # Display whole-number floats as integers (1.0 -> 1, 0.5 -> 0.5)
    _cd = int(cooldown_minutes) if cooldown_minutes == int(cooldown_minutes) else cooldown_minutes
    with _whale_lock:
        _whale_state["settings"] = {
            "prefixes": ",".join(prefixes),
            "max_positions": max_positions,
            "max_hours": None,  # set below after parsing
            "cooldown_minutes": _cd,
            "continuous": continuous,
            "tier1_only": tier1_only,
            "dry_run": dry_run,
        }

    # Parse max hours — default to 1.0 if empty
    try:
        max_hours_raw = request.form.get("max_hours_to_expiration", "").strip()
        max_hours = float(max_hours_raw) if max_hours_raw else 24.0
        if max_hours < 0.1:
            max_hours = 0.1
    except (ValueError, TypeError):
        max_hours = 24.0

    _mh = int(max_hours) if max_hours == int(max_hours) else max_hours
    with _whale_lock:
        _whale_state["settings"]["max_hours"] = _mh

    def _log(msg):
        _whale_state["logs"].append(msg)

    def _is_stop_requested():
        with _whale_lock:
            return _whale_state["stop_requested"]

    def _run():
        from kalshi_bot.scanner import StopRequested

        try:
            db.init_db()
            client = _get_client()

            _log(f"Config: max_positions={max_positions}, max_hours={max_hours}, "
                 f"cooldown={cooldown_minutes}min, continuous={continuous}, "
                 f"tier1_only={tier1_only}, dry_run={dry_run}")

            strategy_kwargs = dict(
                prefixes=prefixes,
                dry_run=dry_run,
                tier1_only=tier1_only,
                max_positions=max_positions,
                max_hours_to_expiration=max_hours,
                log=_log,
                stop_check=_is_stop_requested,
            )

            if not continuous:
                _log(f"Starting sniper ({'DRY RUN' if dry_run else 'LIVE'})...")
                run_whale_strategy(client, **strategy_kwargs)
                _log("Strategy run complete.")
                return

            # --- Continuous mode ---
            cooldown_sec = cooldown_minutes * 60
            trades_placed = 0
            _log(f"[CONTINUOUS] Starting ({'DRY RUN' if dry_run else 'LIVE'}) — "
                 f"max {max_positions} positions, {cooldown_minutes}min cooldown")

            round_num = 0
            while True:
                if _is_stop_requested():
                    _log(f"[CONTINUOUS] Stop requested. Finishing.")
                    break

                round_num += 1
                open_count = db.count_open_positions()

                if open_count >= max_positions:
                    _log(f"[CONTINUOUS] All {max_positions} positions filled. Stopping.")
                    break

                remaining = max_positions - open_count
                _log(f"[CONTINUOUS] Round {round_num} — "
                     f"{open_count}/{max_positions} filled, "
                     f"{remaining} slot{'s' if remaining != 1 else ''} remaining")

                result = run_whale_strategy(client, **strategy_kwargs)

                if result.get("traded", 0) > 0:
                    trades_placed += 1
                    open_now = db.count_open_positions()
                    _log(f"[CONTINUOUS] Trade {trades_placed} complete. "
                         f"{open_now}/{max_positions} positions filled.")
                    if open_now >= max_positions:
                        _log(f"[CONTINUOUS] All {max_positions} positions filled. Stopping.")
                        break
                else:
                    reason = result.get("stopped_reason")
                    if reason == "daily_loss":
                        _log(f"[CONTINUOUS] Daily loss limit hit. Stopping.")
                        break
                    if reason == "max_positions":
                        _log(f"[CONTINUOUS] All {max_positions} positions filled. Stopping.")
                        break

                _log(f"[CONTINUOUS] Waiting {cooldown_minutes} min before next scan...")
                for _ in range(int(cooldown_sec)):
                    if _is_stop_requested():
                        _log(f"[CONTINUOUS] Stop requested during cooldown.")
                        break
                    _time.sleep(1)
                else:
                    continue
                break  # stop was requested during cooldown

            _log(f"[CONTINUOUS] Done — {round_num} rounds, {trades_placed} trades placed, "
                 f"{db.count_open_positions()}/{max_positions} positions")
        except StopRequested:
            _log("Strategy stopped by user.")
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
        _whale_state["logs"].append("Stop requested — stopping...")
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
