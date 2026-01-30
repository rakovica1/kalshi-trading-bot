"""Backtest engine for the whale (sniper) strategy.

Runs the strategy against historical settled markets from the Kalshi API.
No DB writes, no live-trading imports — only uses `client` for API calls.

Performance optimizations:
- Disk cache: settled markets are cached to ~/.cache/nightrader/ by date,
  so repeated backtests with the same date range skip the API entirely.
- Date chunking: the date range is split into per-day chunks fetched in
  parallel threads.
- Parallel page fetching: each chunk fetches pages concurrently after the
  first cursor is obtained.
"""

import hashlib
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta, date as date_type
from pathlib import Path


# ---------------------------------------------------------------------------
# Disk cache
# ---------------------------------------------------------------------------

_CACHE_DIR = Path.home() / ".cache" / "nightrader" / "settled_markets"


def _cache_path(day: date_type) -> Path:
    """Return the cache file path for a given date."""
    return _CACHE_DIR / f"{day.isoformat()}.json"


def _load_cached_day(day: date_type):
    """Load cached markets for a date, or None if not cached."""
    path = _cache_path(day)
    if not path.exists():
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _save_cached_day(day: date_type, markets: list):
    """Save markets for a date to disk cache."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(day)
    try:
        with open(path, "w") as f:
            json.dump(markets, f, separators=(",", ":"))
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Strategy helpers
# ---------------------------------------------------------------------------

def _assign_tier(ask_price):
    """Assign a tier based on ask price.

    Tier 0 (Skip):  ask >= 99c
    Tier 1 (Best):  ask == 98c
    Tier 2 (Good):  ask 96-97c
    Tier 3 (Okay):  ask <= 95c
    """
    if ask_price >= 99:
        return 0
    elif ask_price == 98:
        return 1
    elif ask_price >= 96:
        return 2
    else:
        return 3


def _calc_spread_pct(bid, ask):
    """Calculate bid/ask spread as a percentage of the midpoint."""
    if not bid or not ask or ask <= bid:
        return 0.0
    mid = (bid + ask) / 2.0
    return ((ask - bid) / mid) * 100.0


# ---------------------------------------------------------------------------
# Parallel market fetching
# ---------------------------------------------------------------------------

def _fetch_day(client, day, log, stop_check):
    """Fetch settled markets for a single day, using disk cache if available.

    Uses parallel page fetching within the day for additional speed.
    """
    if stop_check and stop_check():
        return []

    cached = _load_cached_day(day)
    if cached is not None:
        return cached

    min_ts = int(datetime.combine(day, datetime.min.time(),
                                   tzinfo=timezone.utc).timestamp())
    max_ts = int(datetime.combine(day, datetime.max.time(),
                                   tzinfo=timezone.utc).timestamp())

    markets = client.get_all_markets(
        status="settled",
        min_close_ts=min_ts,
        max_close_ts=max_ts,
    )

    # Only cache days that are fully in the past (settled data won't change)
    today = datetime.now(timezone.utc).date()
    if day < today:
        _save_cached_day(day, markets)

    return markets


def fetch_settled_markets(client, start_date, end_date, log, stop_check,
                          progress_cb=None):
    """Fetch settled markets from Kalshi API within a date range.

    Splits the range into per-day chunks and fetches them in parallel,
    using disk cache for previously fetched days.
    """
    # Build list of days
    days = []
    current = start_date
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)

    if not days:
        return []

    # Check which days are cached vs need fetching
    cached_days = []
    fetch_days = []
    for day in days:
        if _load_cached_day(day) is not None:
            cached_days.append(day)
        else:
            fetch_days.append(day)

    if cached_days:
        log(f"[INFO] {len(cached_days)} days cached, {len(fetch_days)} to fetch")
    else:
        log(f"[INFO] Fetching {len(days)} days of settled markets...")

    all_markets = []

    # Load cached days (instant)
    for day in cached_days:
        data = _load_cached_day(day)
        if data:
            all_markets.extend(data)

    # Fetch uncached days in parallel
    if fetch_days:
        max_workers = min(8, len(fetch_days))
        completed = 0
        total_to_fetch = len(fetch_days)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_fetch_day, client, day, log, stop_check): day
                for day in fetch_days
            }
            for future in as_completed(futures):
                if stop_check and stop_check():
                    break
                day = futures[future]
                try:
                    day_markets = future.result()
                    all_markets.extend(day_markets)
                    completed += 1
                    if progress_cb:
                        pct = 5 + int(15 * completed / total_to_fetch)
                        progress_cb(pct, f"Fetched {completed}/{total_to_fetch} days...")
                except Exception as e:
                    log(f"[WARN] Failed to fetch {day}: {e}")
                    completed += 1

    log(f"[INFO] Total: {len(all_markets)} settled markets across {len(days)} days")
    return all_markets


def _filter_whale_candidates(markets, params):
    """Filter markets using whale strategy criteria, mirroring scanner.py logic.

    Settled markets have zeroed-out orderbooks, so we use:
    - `volume` (lifetime) instead of `volume_24h` (reset to 0 after settlement)
    - `previous_yes_bid/ask` as fallback when `yes_bid/ask` are 0
    - `last_price` as final fallback for the ask price

    params keys: min_ask, max_ask, min_volume_24h, max_spread_pct,
                 top_n_dollar_vol
    Returns list of candidate dicts with signal info.
    """
    min_ask = params.get("min_ask", 95)
    max_ask = params.get("max_ask", 98)
    min_volume = params.get("min_volume_24h", 10000)
    max_spread = params.get("max_spread_pct", 5.0)
    top_n = params.get("top_n_dollar_vol", 200)

    # For settled markets: use `volume` (lifetime) since `volume_24h` is 0
    sorted_markets = sorted(markets,
                            key=lambda m: (m.get("volume", 0) or 0),
                            reverse=True)

    candidates = []
    for m in sorted_markets:
        # Use lifetime volume for settled markets (volume_24h is always 0)
        vol = m.get("volume", 0) or 0
        if vol < min_volume:
            continue

        # Use current bid/ask, falling back to previous (pre-settlement) values
        yes_bid = m.get("yes_bid", 0) or m.get("previous_yes_bid", 0) or 0
        yes_ask = m.get("yes_ask", 0) or m.get("previous_yes_ask", 0) or 0
        no_bid = m.get("no_bid", 0) or 0
        no_ask = m.get("no_ask", 0) or 0

        # Use last_price as fallback for ask if both are missing
        if not yes_ask and not no_ask:
            last_price = m.get("last_price", 0) or 0
            if last_price > 0:
                yes_ask = last_price

        # Infer missing ask from opposite bid
        if not yes_ask and no_bid:
            yes_ask = 100 - no_bid
        if not no_ask and yes_bid:
            no_ask = 100 - yes_bid

        result = m.get("result", "")

        # Check YES side
        if yes_ask and min_ask <= yes_ask <= max_ask:
            tier = _assign_tier(yes_ask)
            if tier == 0:
                continue
            spread = _calc_spread_pct(yes_bid, yes_ask)
            if spread > max_spread:
                continue
            dollar_vol = int(vol * yes_ask) // 100
            candidates.append({
                "ticker": m.get("ticker", ""),
                "event_ticker": m.get("event_ticker", ""),
                "signal_side": "yes",
                "signal_ask": yes_ask,
                "signal_bid": yes_bid,
                "volume_24h": vol,
                "dollar_24h": dollar_vol,
                "tier": tier,
                "spread_pct": round(spread, 2),
                "result": result,
                "close_time": m.get("close_time") or m.get("expected_expiration_time") or "",
            })
        # Check NO side
        elif no_ask and min_ask <= no_ask <= max_ask:
            tier = _assign_tier(no_ask)
            if tier == 0:
                continue
            spread = _calc_spread_pct(no_bid, no_ask)
            if spread > max_spread:
                continue
            dollar_vol = int(vol * no_ask) // 100
            candidates.append({
                "ticker": m.get("ticker", ""),
                "event_ticker": m.get("event_ticker", ""),
                "signal_side": "no",
                "signal_ask": no_ask,
                "signal_bid": no_bid,
                "volume_24h": vol,
                "dollar_24h": dollar_vol,
                "tier": tier,
                "spread_pct": round(spread, 2),
                "result": result,
                "close_time": m.get("close_time") or m.get("expected_expiration_time") or "",
            })

    # Apply dollar volume rank filter
    candidates.sort(key=lambda x: x["dollar_24h"], reverse=True)
    candidates = candidates[:top_n]

    return candidates


def run_backtest(client, start_date, end_date, params, log, stop_check,
                 progress_cb=None):
    """Run the whale strategy backtest against settled markets.

    Returns dict with keys: trades, summary, equity_curve, daily_breakdown.
    """
    if progress_cb:
        progress_cb(5, "Fetching settled markets...")

    if stop_check and stop_check():
        return None

    markets = fetch_settled_markets(client, start_date, end_date, log,
                                    stop_check, progress_cb=progress_cb)

    if stop_check and stop_check():
        return None

    if progress_cb:
        progress_cb(20, "Filtering whale candidates...")

    candidates = _filter_whale_candidates(markets, params)
    log(f"[INFO] Found {len(candidates)} qualifying candidates")

    if not candidates:
        log("[WARN] No candidates found matching criteria")
        return {
            "trades": [],
            "summary": _empty_summary(),
            "equity_curve": [],
            "daily_breakdown": [],
        }

    # Simulation parameters
    position_size_cents = params.get("position_size_cents", 1000)
    fee_per_contract = params.get("fee_per_contract", 1)

    trades = []
    equity = 0
    peak_equity = 0
    max_drawdown = 0
    total_wins = 0
    total_losses = 0
    total_cost = 0
    total_revenue = 0
    total_fees = 0
    win_streak = 0
    loss_streak = 0
    max_win_streak = 0
    max_loss_streak = 0
    wins_pnl = 0
    losses_pnl = 0
    equity_curve = []
    daily_map = {}

    total_candidates = len(candidates)

    for i, c in enumerate(candidates):
        if stop_check and stop_check():
            log("[WARN] Backtest stopped by user")
            break

        if progress_cb:
            pct = 20 + int(70 * (i + 1) / total_candidates)
            progress_cb(pct, f"Simulating trade {i + 1}/{total_candidates}...")

        entry_price = c["signal_ask"]
        if entry_price <= 0:
            continue

        contracts = position_size_cents // entry_price
        if contracts <= 0:
            continue

        cost = contracts * entry_price
        fees = contracts * fee_per_contract
        result_side = (c.get("result") or "").lower()
        signal_side = c["signal_side"].lower()

        won = result_side == signal_side
        revenue = contracts * 100 if won else 0
        pnl = revenue - cost - fees

        equity += pnl
        total_cost += cost
        total_revenue += revenue
        total_fees += fees

        if won:
            total_wins += 1
            wins_pnl += pnl
            win_streak += 1
            loss_streak = 0
            max_win_streak = max(max_win_streak, win_streak)
        else:
            total_losses += 1
            losses_pnl += pnl
            loss_streak += 1
            win_streak = 0
            max_loss_streak = max(max_loss_streak, loss_streak)

        peak_equity = max(peak_equity, equity)
        drawdown = peak_equity - equity
        max_drawdown = max(max_drawdown, drawdown)

        # Extract date for daily breakdown
        close_time = c.get("close_time", "")
        trade_date = _extract_date(close_time)

        trade_record = {
            "num": len(trades) + 1,
            "date": trade_date,
            "ticker": c["ticker"],
            "side": c["signal_side"].upper(),
            "entry": entry_price,
            "qty": contracts,
            "cost": cost,
            "fee": fees,
            "result": "WON" if won else "LOST",
            "revenue": revenue,
            "pnl": pnl,
            "equity": equity,
            "tier": c["tier"],
        }
        trades.append(trade_record)
        equity_curve.append({"x": len(trades), "y": equity, "date": trade_date})

        # Daily breakdown
        if trade_date not in daily_map:
            daily_map[trade_date] = {
                "date": trade_date, "trades": 0, "wins": 0, "losses": 0,
                "pnl": 0, "cost": 0, "revenue": 0, "fees": 0,
            }
        day = daily_map[trade_date]
        day["trades"] += 1
        day["wins"] += 1 if won else 0
        day["losses"] += 0 if won else 1
        day["pnl"] += pnl
        day["cost"] += cost
        day["revenue"] += revenue
        day["fees"] += fees

    # Compute summary
    total_trades = total_wins + total_losses
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0.0
    total_pnl = equity
    roi_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0.0
    avg_win = (wins_pnl / total_wins) if total_wins > 0 else 0
    avg_loss = (losses_pnl / total_losses) if total_losses > 0 else 0
    expectancy = (total_pnl / total_trades) if total_trades > 0 else 0
    gross_profit = wins_pnl if wins_pnl > 0 else 0
    gross_loss = abs(losses_pnl) if losses_pnl < 0 else 0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (
        float("inf") if gross_profit > 0 else 0.0
    )

    summary = {
        "total_trades": total_trades,
        "total_wins": total_wins,
        "total_losses": total_losses,
        "win_rate": round(win_rate, 1),
        "total_pnl": total_pnl,
        "total_pnl_dollars": round(total_pnl / 100, 2),
        "roi_pct": round(roi_pct, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else "Inf",
        "max_drawdown": max_drawdown,
        "max_drawdown_dollars": round(max_drawdown / 100, 2),
        "avg_win": round(avg_win, 1),
        "avg_win_dollars": round(avg_win / 100, 2),
        "avg_loss": round(avg_loss, 1),
        "avg_loss_dollars": round(avg_loss / 100, 2),
        "expectancy": round(expectancy, 1),
        "expectancy_dollars": round(expectancy / 100, 2),
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "total_fees": total_fees,
        "total_fees_dollars": round(total_fees / 100, 2),
        "total_cost": total_cost,
        "total_cost_dollars": round(total_cost / 100, 2),
        "total_revenue": total_revenue,
        "total_revenue_dollars": round(total_revenue / 100, 2),
    }

    daily_breakdown = sorted(daily_map.values(), key=lambda d: d["date"])

    if progress_cb:
        progress_cb(100, "Backtest complete")
    log(f"[INFO] Backtest complete: {total_trades} trades, "
        f"P&L: {_fmt_cents(total_pnl)}, Win rate: {win_rate:.1f}%")

    return {
        "trades": trades,
        "summary": summary,
        "equity_curve": equity_curve,
        "daily_breakdown": daily_breakdown,
    }


def _empty_summary():
    """Return an empty summary dict."""
    return {
        "total_trades": 0, "total_wins": 0, "total_losses": 0,
        "win_rate": 0.0, "total_pnl": 0, "total_pnl_dollars": 0.0,
        "roi_pct": 0.0, "profit_factor": 0.0,
        "max_drawdown": 0, "max_drawdown_dollars": 0.0,
        "avg_win": 0, "avg_win_dollars": 0.0,
        "avg_loss": 0, "avg_loss_dollars": 0.0,
        "expectancy": 0, "expectancy_dollars": 0.0,
        "max_win_streak": 0, "max_loss_streak": 0,
        "total_fees": 0, "total_fees_dollars": 0.0,
        "total_cost": 0, "total_cost_dollars": 0.0,
        "total_revenue": 0, "total_revenue_dollars": 0.0,
    }


def _extract_date(close_time_str):
    """Extract a YYYY-MM-DD date string from a close_time value."""
    if not close_time_str:
        return "unknown"
    try:
        return close_time_str[:10]
    except Exception:
        return "unknown"


def _fmt_cents(cents):
    """Format cents as a signed dollar string."""
    val = cents / 100
    if val >= 0:
        return "+$%.2f" % val
    return "-$%.2f" % (-val,)
