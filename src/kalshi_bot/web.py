import logging
import os
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def _market_position_value(market_data, side):
    """Determine current value per contract for a position.

    For settled markets, returns 100 if position side matches result, 0 otherwise.
    For active markets, returns the current bid price.
    """
    result = market_data.get("result", "")
    status = market_data.get("status", "")

    # Market is settled/finalized
    if result:
        return (100, True) if result == side else (0, True)

    # Active market — use current bid
    if side == "yes":
        return (market_data.get("yes_bid", 0) or 0, False)
    else:
        return (market_data.get("no_bid", 0) or 0, False)


def _batch_fetch_markets(client, tickers):
    """Fetch multiple markets in parallel. Returns dict: ticker -> market data."""
    if not tickers:
        return {}
    results = {}
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(client.get_market, ticker=t): t for t in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                results[ticker] = future.result()
            except Exception:
                results[ticker] = None
    return results


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard():
    db.init_db()
    balance_cents = 0
    balance_timestamp = None
    try:
        client = _get_client()
        bal_data = client.get_balance()
        # balance = available cash; portfolio_value = market value of open positions
        balance_cents = bal_data.get("balance", 0)
        portfolio_value_cents = bal_data.get("portfolio_value", 0)
        db.log_balance(balance_cents)
        balance_timestamp = datetime.now(timezone.utc).astimezone(_EST).strftime("%I:%M:%S %p EST")
    except Exception as e:
        balance_cents = 0
        portfolio_value_cents = 0

    open_positions = db.get_open_positions()
    stats = db.get_stats()

    # Unrealized P&L from open positions (and auto-close settled ones)
    total_unrealized_bid = 0
    total_unrealized_ask = 0
    portfolio_ask_value = 0
    try:
        client = _get_client()
        market_map = _batch_fetch_markets(client, [p["ticker"] for p in open_positions])
        for p in open_positions:
            m = market_map.get(p["ticker"])
            if m is None:
                continue
            current, is_settled = _market_position_value(m, p["side"])
            if is_settled:
                db.close_position_settled(
                    p["ticker"], p["side"],
                    settlement_value_cents=current,
                )
            else:
                qty = p["quantity"]
                entry = p["avg_entry_price_cents"]
                # Bid-based (what you'd get selling now)
                total_unrealized_bid += int(qty * (current - entry))
                # Ask-based
                if p["side"] == "yes":
                    ask = m.get("yes_ask", 0) or 0
                else:
                    ask = m.get("no_ask", 0) or 0
                ask_val = ask if ask else current
                total_unrealized_ask += int(qty * (ask_val - entry))
                portfolio_ask_value += int(qty * ask_val)
    except Exception:
        pass

    # Re-fetch stats after auto-closing any settled positions
    stats = db.get_stats()

    # Deposits/withdrawals for display
    total_deposits, deposit_count = db.get_total_deposits()
    total_withdrawals, withdrawal_count = db.get_total_withdrawals()

    # Net P&L from trade history: realized P&L - fees
    total_fees = stats["total_fees_cents"]
    total_invested = stats["total_invested_cents"]
    realized_pnl = stats["realized_pnl_cents"]
    net_pnl = realized_pnl - total_fees
    roi_pct = (net_pnl / total_invested * 100) if total_invested > 0 else 0.0

    # Daily P&L history for chart
    daily_pnl = db.get_daily_pnl(days=90)

    return render_template(
        "dashboard.html",
        balance_cents=balance_cents,
        total_balance_cents=balance_cents + portfolio_ask_value,
        balance_timestamp=balance_timestamp,
        total_deposits_cents=total_deposits,
        deposit_count=deposit_count,
        total_withdrawals_cents=total_withdrawals,
        withdrawal_count=withdrawal_count,
        unrealized_bid_cents=total_unrealized_bid,
        unrealized_ask_cents=total_unrealized_ask,
        total_fees_cents=total_fees,
        net_pnl_cents=net_pnl,
        portfolio_value_cents=portfolio_value_cents,
        roi_pct=roi_pct,
        total_invested_cents=total_invested,
        open_count=db.count_open_positions(),
        total_trades=stats["total_orders"],
        win_rate=stats["win_rate"],
        profit_factor=stats["profit_factor"],
        daily_pnl=daily_pnl,
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
        market_map = _batch_fetch_markets(client, [p["ticker"] for p in open_positions])
        for p in open_positions:
            entry = p["avg_entry_price_cents"]
            qty = p["quantity"]
            close_time = ""
            is_settled = False
            m = market_map.get(p["ticker"])
            if m is not None:
                current, is_settled = _market_position_value(m, p["side"])
                close_time = m.get("close_time") or m.get("expected_expiration_time") or ""
                if is_settled:
                    db.close_position_settled(
                        p["ticker"], p["side"],
                        settlement_value_cents=current,
                    )
            else:
                current = 0
                m = {}
            unrealized_bid = int(qty * (current - entry))
            if p["side"] == "yes":
                ask = m.get("yes_ask", 0) or 0
            else:
                ask = m.get("no_ask", 0) or 0
            unrealized_ask = int(qty * ((ask if ask else current) - entry))
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
                "ask_price": ask,
                "unrealized_bid_cents": unrealized_bid,
                "unrealized_ask_cents": unrealized_ask,
                "unrealized_cents": unrealized_bid,
                "opened_at_display": opened_at_display,
                "close_time": close_time,
                "is_settled": is_settled,
            })
    except Exception:
        enriched = [{
            **p,
            "current_price": 0,
            "unrealized_cents": 0,
            "opened_at_display": _utc_to_est(p.get("opened_at", "")),
            "close_time": "",
            "is_settled": False,
        } for p in open_positions]

    # Fetch closed positions
    closed_positions = db.get_closed_positions()
    closed_enriched = []
    for p in closed_positions:
        opened_at = p.get("opened_at", "")
        if opened_at and isinstance(opened_at, str):
            opened_at_display = _utc_to_est(opened_at)
        elif hasattr(opened_at, "strftime"):
            opened_at_display = _utc_to_est(opened_at)
        else:
            opened_at_display = str(opened_at)
        closed_at = p.get("closed_at", "")
        if closed_at and isinstance(closed_at, str):
            closed_at_display = _utc_to_est(closed_at)
        elif hasattr(closed_at, "strftime"):
            closed_at_display = _utc_to_est(closed_at)
        else:
            closed_at_display = str(closed_at) if closed_at else ""
        closed_enriched.append({
            **p,
            "opened_at_display": opened_at_display,
            "closed_at_display": closed_at_display,
        })

    # Compute position totals
    total_value = sum(p["current_price"] * p["quantity"] for p in enriched)
    total_cost = sum(int(p["avg_entry_price_cents"] * p["quantity"]) for p in enriched)
    total_unrealized_bid = sum(p["unrealized_bid_cents"] for p in enriched)
    total_unrealized_ask = sum(p["unrealized_ask_cents"] for p in enriched)

    # Aggregated portfolio P&L history
    portfolio_snapshots = db.get_portfolio_snapshots(hours=48)

    return render_template(
        "positions.html",
        positions=enriched,
        closed_positions=closed_enriched,
        total_value_cents=total_value,
        total_cost_cents=total_cost,
        total_unrealized_bid_cents=total_unrealized_bid,
        total_unrealized_ask_cents=total_unrealized_ask,
        portfolio_snapshots=portfolio_snapshots,
    )


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
@_require_control_password
def trades():
    db.init_db()
    ticker = request.args.get("ticker", "").strip() or None
    limit = request.args.get("limit", 50, type=int)
    trade_list = db.get_trade_history(limit=limit, ticker=ticker)

    # Build settlement lookup from positions table
    all_positions = db.get_all_positions()
    # Map (ticker, side) -> position info
    pos_map = {}
    for p in all_positions:
        key = (p["ticker"], p["side"])
        pos_map[key] = p

    for t in trade_list:
        if t.get("created_at"):
            t["created_at"] = _utc_to_est(t["created_at"])

        # Determine settlement status
        fill_count = t.get("fill_count", 0) or 0
        if fill_count <= 0 or t.get("status") == "failed":
            t["settlement"] = "na"
            t["settlement_label"] = "—"
        else:
            pos = pos_map.get((t["ticker"], t["side"]))
            if pos and pos.get("is_closed"):
                pnl = pos.get("realized_pnl_cents", 0)
                if pnl > 0:
                    t["settlement"] = "won"
                    t["settlement_label"] = "WON · 100¢"
                elif pnl < 0:
                    t["settlement"] = "lost"
                    t["settlement_label"] = "LOST · 0¢"
                else:
                    t["settlement"] = "even"
                    t["settlement_label"] = "EVEN"
            else:
                t["settlement"] = "pending"
                t["settlement_label"] = "Pending"

    # Compute trade totals (only filled trades)
    filled_trades = [t for t in trade_list if (t.get("fill_count") or 0) > 0]
    total_traded_cents = sum(t["fill_count"] * t["price_cents"] for t in filled_trades)
    total_fees_cents = sum(t.get("fee_cents", 0) or 0 for t in filled_trades)

    # Total invested from closed positions only
    total_invested_cents = sum(
        p.get("total_cost_cents", 0)
        for p in all_positions if p.get("is_closed")
    )
    # Realized P&L from all closed positions
    realized_pnl_cents = sum(
        p.get("realized_pnl_cents", 0)
        for p in all_positions if p.get("is_closed")
    )
    trade_net_pnl_cents = realized_pnl_cents - total_fees_cents
    trade_roi_pct = (trade_net_pnl_cents / total_invested_cents * 100) if total_invested_cents > 0 else 0.0

    # Group by date for daily subtotals
    daily_groups = {}
    for t in filled_trades:
        # Extract date from formatted EST string (YYYY-MM-DD ...)
        date_str = str(t.get("created_at", ""))[:10]
        if date_str not in daily_groups:
            daily_groups[date_str] = {"date": date_str, "count": 0, "cost_cents": 0, "fees_cents": 0}
        daily_groups[date_str]["count"] += 1
        daily_groups[date_str]["cost_cents"] += t["fill_count"] * t["price_cents"]
        daily_groups[date_str]["fees_cents"] += t.get("fee_cents", 0) or 0
    daily_summary = sorted(daily_groups.values(), key=lambda x: x["date"], reverse=True)

    return render_template(
        "trades.html",
        trades=trade_list,
        filter_ticker=ticker or "",
        filter_limit=limit,
        total_traded_cents=total_traded_cents,
        total_fees_cents=total_fees_cents,
        total_invested_cents=total_invested_cents,
        realized_pnl_cents=realized_pnl_cents,
        trade_net_pnl_cents=trade_net_pnl_cents,
        trade_roi_pct=trade_roi_pct,
        filled_count=len(filled_trades),
        daily_summary=daily_summary,
        import_result=request.args.get("import_result"),
        import_count=request.args.get("import_count", 0, type=int),
    )


@app.route("/trades/import", methods=["POST"])
@_require_control_password
def trades_import():
    """Import trades from an uploaded Kalshi CSV file."""
    db.init_db()
    file = request.files.get("csv_file")
    if not file or not file.filename:
        return redirect(url_for("trades", import_result="error"))

    try:
        content = file.read().decode("utf-8-sig")
        imported, skipped = db.import_trades_from_csv(content, clear_existing=True)
        return redirect(url_for("trades", import_result="success", import_count=imported))
    except Exception as e:
        return redirect(url_for("trades", import_result="error"))


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


def _build_position_data(market_map, open_positions, candle_history):
    """Enrich open positions with current prices and chart history."""
    enriched = []
    for p in open_positions:
        entry = p["avg_entry_price_cents"]
        qty = p["quantity"]
        is_settled = False
        m = market_map.get(p["ticker"])
        if m is not None:
            bid, is_settled = _market_position_value(m, p["side"])
            if is_settled:
                ask = bid  # settled: bid == ask == settlement value
                db.close_position_settled(
                    p["ticker"], p["side"],
                    settlement_value_cents=bid,
                )
            else:
                if p["side"] == "yes":
                    ask = m.get("yes_ask", 0) or 0
                else:
                    ask = m.get("no_ask", 0) or 0
                db.log_price_snapshot(p["ticker"], p["side"], bid, ask)
            close_time = m.get("close_time") or m.get("expected_expiration_time") or ""
        else:
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
            "is_settled": is_settled,
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
        market_map = _batch_fetch_markets(client, tickers)
        enriched = _build_position_data(market_map, open_positions, candle_history)
    except Exception:
        enriched = [{
            **p,
            "current_bid": 0,
            "current_ask": 0,
            "unrealized_cents": 0,
            "close_time": "",
            "history": [],
            "is_settled": False,
        } for p in open_positions]

    # Separate open vs just-settled positions
    active_positions = [p for p in enriched if not p.get("is_settled")]
    just_settled = [p for p in enriched if p.get("is_settled")]

    # Fetch previously closed positions from DB
    closed_positions = db.get_closed_positions()
    closed_enriched = []
    for p in closed_positions:
        closed_enriched.append({
            **p,
            "current_bid": 100 if p["realized_pnl_cents"] >= 0 else 0,
            "current_ask": 100 if p["realized_pnl_cents"] >= 0 else 0,
            "unrealized_cents": p["realized_pnl_cents"],
            "close_time": "",
            "history": [],
            "bid_high": 0,
            "bid_low": 0,
            "is_settled": True,
        })

    # Merge just-settled with closed
    all_closed = just_settled + closed_enriched

    # Cleanup old snapshots periodically
    try:
        db.cleanup_old_snapshots(hours=48)
    except Exception:
        pass

    # Compute portfolio totals for active positions
    portfolio_value = sum(p["current_bid"] * p["quantity"] for p in active_positions)
    portfolio_cost = sum(int(p["avg_entry_price_cents"] * p["quantity"]) for p in active_positions)
    portfolio_unrealized = sum(p["unrealized_cents"] for p in active_positions)

    return render_template(
        "charts.html",
        positions=active_positions,
        closed_positions=all_closed,
        portfolio_value_cents=portfolio_value,
        portfolio_cost_cents=portfolio_cost,
        portfolio_unrealized_cents=portfolio_unrealized,
    )


@app.route("/api/charts/prices")
def api_charts_prices():
    """Return current prices + candlestick history for all open positions."""
    db.init_db()
    open_positions = db.get_open_positions()
    try:
        client = _get_client()
        tickers = [p["ticker"] for p in open_positions]
        candle_history = _fetch_candlestick_history(client, tickers, hours=24) if tickers else {}
        market_map = _batch_fetch_markets(client, tickers)
        enriched = _build_position_data(market_map, open_positions, candle_history)
        result = []
        for pos in enriched:
            if pos.get("is_settled"):
                continue  # Don't include settled positions in live refresh
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
    # Build set of tickers with open positions
    open_tickers = {p["ticker"] for p in db.get_open_positions()}
    return render_template(
        "scanner.html",
        results=results,
        scan_stats=scan_stats,
        scanned_at=scanned_at,
        scanning=scanning,
        scan_error=scan_error,
        open_tickers=open_tickers,
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
# Arbitrage Scanner
# ---------------------------------------------------------------------------

_arb_state = {"running": False, "results": [], "logs": [], "scanned_at": None}
_arb_lock = threading.Lock()


@app.route("/arbitrage")
def arbitrage_page():
    with _arb_lock:
        running = _arb_state["running"]
        results = list(_arb_state["results"])
        logs = list(_arb_state["logs"])
        scanned_at = _arb_state["scanned_at"]
    return render_template("arbitrage.html",
                           running=running, results=results,
                           logs=logs, scanned_at=scanned_at)


@app.route("/arbitrage/scan", methods=["POST"])
def arbitrage_scan():
    with _arb_lock:
        if _arb_state["running"]:
            return redirect(url_for("arbitrage_page"))
        _arb_state["running"] = True
        _arb_state["logs"].clear()
        _arb_state["results"].clear()

    def _log(msg):
        _arb_state["logs"].append(msg)

    client = _get_client()

    def _run():
        from kalshi_bot.arbitrage import run_arbitrage_scan
        try:
            opps = run_arbitrage_scan(
                client, log=_log,
                min_profit_cents=1, quantity=10,
                check_orderbook=True, max_orderbook_checks=50,
            )
            with _arb_lock:
                _arb_state["results"] = opps
                _arb_state["scanned_at"] = __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception as e:
            import traceback
            _log(f"[FAIL] Error: {e}")
            _log(f"[FAIL] {traceback.format_exc()}")
        finally:
            with _arb_lock:
                _arb_state["running"] = False

    import threading as _threading
    t = _threading.Thread(target=_run, daemon=True)
    t.start()
    return redirect(url_for("arbitrage_page"))


@app.route("/arbitrage/status")
def arbitrage_status():
    with _arb_lock:
        return jsonify({
            "running": _arb_state["running"],
            "results": _arb_state["results"],
            "logs": _arb_state["logs"],
            "scanned_at": _arb_state["scanned_at"],
            "count": len(_arb_state["results"]),
        })


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

@app.route("/control")
@_require_control_password
def control():
    defaults = {
        "max_positions": 10,
        "dry_run": True,
        "with_ai": True,
    }
    with _whale_lock:
        running = _whale_state["running"]
        settings = _whale_state.get("settings", defaults)
    open_positions = db.get_open_positions()
    return render_template("control.html", running=running, settings=settings,
                           positions=open_positions)


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
    with_ai = request.form.get("with_ai") == "on"

    # Parse max_positions — default to 10 if missing or invalid
    try:
        max_positions = int(request.form.get("max_positions", "10"))
        if max_positions < 1:
            max_positions = 1
        elif max_positions > 50:
            max_positions = 50
    except (ValueError, TypeError):
        max_positions = 10

    # Hardcoded default (removed from UI)
    max_hours = 24.0

    # Store form values so the UI preserves them after redirect
    with _whale_lock:
        _whale_state["settings"] = {
            "max_positions": max_positions,
            "dry_run": dry_run,
            "with_ai": with_ai,
        }

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

            ai_tag = ", ai=ON" if with_ai else ""
            _log(f"[INFO] Config: max_positions={max_positions}, max_hours={max_hours}, "
                 f"dry_run={dry_run}{ai_tag}")

            strategy_kwargs = dict(
                prefixes=None,
                dry_run=dry_run,
                max_positions=max_positions,
                max_hours_to_expiration=max_hours,
                log=_log,
                stop_check=_is_stop_requested,
                with_ai=with_ai,
            )

            trades_placed = 0
            mode = "DRY RUN" if dry_run else "LIVE"
            _log(f"[HEAD] Starting [{mode}] — max {max_positions} positions")

            round_num = 0
            while True:
                if _is_stop_requested():
                    _log(f"[WARN] Stop requested. Finishing.")
                    break

                round_num += 1
                open_count = db.count_open_positions()

                if open_count >= max_positions:
                    _log(f"[FILL] All {max_positions} positions filled. Stopping.")
                    break

                remaining = max_positions - open_count
                _log(f"[HEAD] Round {round_num} — "
                     f"{open_count}/{max_positions} filled, "
                     f"{remaining} slot{'s' if remaining != 1 else ''} remaining")

                result = run_whale_strategy(client, **strategy_kwargs)

                if result.get("traded", 0) > 0:
                    trades_placed += 1
                    open_now = db.count_open_positions()
                    _log(f"[FILL] Trade {trades_placed} complete. "
                         f"{open_now}/{max_positions} positions filled.")
                    if open_now >= max_positions:
                        _log(f"[FILL] All {max_positions} positions filled. Stopping.")
                        break
                else:
                    reason = result.get("stopped_reason")
                    if reason == "daily_loss":
                        _log(f"[FAIL] Daily loss limit hit. Stopping.")
                        break
                    if reason == "max_positions":
                        _log(f"[FILL] All {max_positions} positions filled. Stopping.")
                        break
                    # No trade — wait 60s then rescan
                    _log(f"[WARN] No targets right now. Retrying in 60s...")
                    for _ in range(60):
                        if _is_stop_requested():
                            _log(f"[WARN] Stop requested. Finishing.")
                            break
                        import time as _time
                        _time.sleep(1)
                    else:
                        continue
                    break  # stop was requested during wait

            _log(f"[HEAD] Done — {round_num} rounds, {trades_placed} trades placed, "
                 f"{db.count_open_positions()}/{max_positions} positions")
        except StopRequested:
            _log("[WARN] Strategy stopped by user.")
        except Exception as e:
            _log(f"[FAIL] ERROR: {e}")
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
