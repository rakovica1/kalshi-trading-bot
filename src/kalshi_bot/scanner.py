import time
import threading
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


class StopRequested(Exception):
    """Raised when a stop signal is detected during scanning."""
    pass


# Cache for raw market data — 5-minute TTL
_market_cache = {"ts": 0, "markets": [], "ttl": 300}
_cache_lock = threading.Lock()

# Background refresh state
_bg_refresh = {"thread": None, "running": False, "client": None}
_bg_lock = threading.Lock()


def _fetch_page(client, page_size, status, cursor, min_close_ts, max_close_ts):
    """Fetch a single page of markets. Used by parallel fetcher."""
    import json
    kwargs = {"limit": page_size, "status": status}
    if cursor:
        kwargs["cursor"] = cursor
    if min_close_ts is not None:
        kwargs["min_close_ts"] = min_close_ts
    if max_close_ts is not None:
        kwargs["max_close_ts"] = max_close_ts
    resp = client._market_api.get_markets_without_preload_content(**kwargs)
    raw = resp.data if hasattr(resp, "data") else resp
    if not raw:
        return [], None
    data = json.loads(raw)
    markets = data.get("markets", [])
    next_cursor = data.get("cursor")
    return markets, next_cursor


def _fetch_all_markets(client, status="open", page_size=1000, stop_check=None,
                       close_window_hours=48):
    """Fetch open markets from the API with server-side time filtering.

    Uses min_close_ts/max_close_ts to only fetch markets closing within
    close_window_hours, dramatically reducing the number of markets fetched
    (from ~930k to a few thousand). Results are cached for 5 minutes.

    After the first sequential page (to get the cursor), remaining pages
    are fetched in parallel using a thread pool.
    """
    with _cache_lock:
        age = time.time() - _market_cache["ts"]
        if _market_cache["markets"] and age < _market_cache["ttl"]:
            return _market_cache["markets"], True

    # Server-side time filter: only markets closing within the window
    now_ts = int(time.time())
    min_close_ts = now_ts
    max_close_ts = now_ts + int(close_window_hours * 3600)

    # First page — sequential to get cursor
    first_markets, cursor = _fetch_page(
        client, page_size, status, None, min_close_ts, max_close_ts
    )

    if stop_check and stop_check():
        raise StopRequested()

    all_raw = list(first_markets)

    # Fetch remaining pages in parallel batches
    if cursor and first_markets:
        # We need cursors sequentially, but can process results in parallel
        # Use sequential pagination with the client wrapper
        while cursor:
            if stop_check and stop_check():
                raise StopRequested()
            page, cursor = _fetch_page(
                client, page_size, status, cursor, min_close_ts, max_close_ts
            )
            if not page:
                break
            all_raw.extend(page)

    # Slim down to only fields we need
    markets = []
    for m in all_raw:
        if stop_check and stop_check():
            raise StopRequested()
        markets.append({
            "ticker": m.get("ticker", "?"),
            "event_ticker": m.get("event_ticker", ""),
            "volume_24h": m.get("volume_24h", 0) or 0,
            "volume": m.get("volume", 0) or 0,
            "open_interest": m.get("open_interest", 0) or 0,
            "yes_bid": m.get("yes_bid", 0) or 0,
            "yes_ask": m.get("yes_ask", 0) or 0,
            "no_bid": m.get("no_bid", 0) or 0,
            "no_ask": m.get("no_ask", 0) or 0,
            "close_time": m.get("close_time") or m.get("expected_expiration_time") or "",
        })

    with _cache_lock:
        _market_cache["markets"] = markets
        _market_cache["ts"] = time.time()

    return markets, False


def _bg_refresh_loop():
    """Background thread that refreshes the market cache periodically."""
    while True:
        with _bg_lock:
            if not _bg_refresh["running"]:
                break
            client = _bg_refresh["client"]
        if client is None:
            break

        with _cache_lock:
            age = time.time() - _market_cache["ts"]
            needs_refresh = age >= _market_cache["ttl"] * 0.8  # Refresh at 80% of TTL

        if needs_refresh:
            try:
                _fetch_all_markets(client, close_window_hours=48)
                logger.debug("Background cache refresh complete")
            except Exception as e:
                logger.warning("Background cache refresh failed: %s", e)

        # Sleep in 1s increments so we can stop quickly
        for _ in range(60):
            with _bg_lock:
                if not _bg_refresh["running"]:
                    return
            time.sleep(1)


def start_background_refresh(client):
    """Start background thread to keep market cache warm."""
    with _bg_lock:
        if _bg_refresh["running"]:
            return
        _bg_refresh["client"] = client
        _bg_refresh["running"] = True
        t = threading.Thread(target=_bg_refresh_loop, daemon=True)
        _bg_refresh["thread"] = t
        t.start()


def stop_background_refresh():
    """Stop the background refresh thread."""
    with _bg_lock:
        _bg_refresh["running"] = False


def _assign_tier(ask_price):
    """Assign a tier based on ask price (what you actually pay to enter).

    Tier 0 (Skip):  ask >= 99c — unprofitable (0¢ profit after 1¢ fee)
    Tier 1 (Best):  ask == 98c — 1¢ profit after fees
    Tier 2 (Good):  ask 96-97c — 2-3¢ profit
    Tier 3 (Okay):  ask <= 95c — 4¢+ profit
    """
    if ask_price >= 99:
        return 0
    elif ask_price == 98:
        return 1
    elif ask_price >= 96:
        return 2
    else:
        return 3


# Simple in-memory cache
_scan_cache = {"ts": 0, "results": [], "stats": {}, "ttl": 300}


def _calc_spread_pct(bid, ask):
    """Calculate bid/ask spread as a percentage of the midpoint.

    Returns 0.0 if bid or ask is missing/zero.
    """
    if not bid or not ask or ask <= bid:
        return 0.0
    mid = (bid + ask) / 2.0
    return ((ask - bid) / mid) * 100.0


_EST = timezone(timedelta(hours=-5))


def _parse_close_time(raw):
    """Parse a close_time string into a UTC datetime, or None."""
    if not raw:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def format_close_time(raw):
    """Format a close_time string into a human-friendly EST string.

    Returns e.g. "2h 15m", "Tomorrow 3:00 PM", "Feb 15", "Jan 29, 8:00 PM EST".
    """
    dt = _parse_close_time(raw)
    if dt is None:
        return "—"
    now = datetime.now(timezone.utc)
    est_dt = dt.astimezone(_EST)
    est_now = now.astimezone(_EST)
    delta = dt - now

    if delta.total_seconds() <= 0:
        return "Closed"

    total_hours = delta.total_seconds() / 3600

    if total_hours < 24:
        hours = int(total_hours)
        minutes = int((delta.total_seconds() % 3600) / 60)
        if hours == 0:
            return f"{minutes}m"
        return f"{hours}h {minutes}m"

    if est_dt.date() == (est_now + timedelta(days=1)).date():
        return f"Tomorrow {est_dt.strftime('%-I:%M %p')}"

    if delta.days < 180:
        return est_dt.strftime("%b %-d, %-I:%M %p")

    return est_dt.strftime("%b %-d, %Y")


def hours_until_close(raw):
    """Return hours remaining until market close, or None if unknown."""
    dt = _parse_close_time(raw)
    if dt is None:
        return None
    delta = dt - datetime.now(timezone.utc)
    if delta.total_seconds() <= 0:
        return 0.0
    return delta.total_seconds() / 3600.0


# Qualification thresholds for premium trade execution
QUALIFIED_MIN_DOLLAR_24H = 10_000
QUALIFIED_MAX_SPREAD_PCT = 5.0
QUALIFIED_TOP_N_DOLLAR = 200
QUALIFIED_MAX_HOURS = 24.0


def scan(client, min_price=95, ticker_prefixes=None, min_volume=10000,
         use_cache=False, top_n=30, stop_check=None):
    """Find markets where YES or NO bid is >= min_price.

    Fetches markets closing within 48h from the API (server-side filtered),
    sorts by 24h volume descending, then filters the top_n most active
    markets by prefix, volume, and price. Each result is assigned a tier.

    Each result also gets a `qualified` flag: True when ALL of these hold:
      - Top 200 by 24h dollar volume
      - >= $10,000 in 24h dollar volume
      - Bid/ask spread <= 5%
      - Expires within 24 hours

    min_volume applies to volume_24h (24-hour trading volume).

    Returns (results, stats) tuple.
    """
    # Return cached results if fresh enough
    if use_cache and _scan_cache["stats"]:
        age = time.time() - _scan_cache["ts"]
        if age < _scan_cache["ttl"]:
            return _scan_cache["results"], _scan_cache["stats"]

    # Start background refresh if not already running
    start_background_refresh(client)

    prefixes_upper = [p.upper() for p in ticker_prefixes] if ticker_prefixes else None

    # 1. Fetch markets closing within 48h (server-side filtered)
    all_markets, from_cache = _fetch_all_markets(
        client, stop_check=stop_check, close_window_hours=48
    )
    total_fetched = len(all_markets)

    # 2. Sort by 24h volume descending — most recent activity first
    all_markets.sort(key=lambda m: m["volume_24h"], reverse=True)

    # 3. Take only top_n by 24h volume for filtering
    candidates = all_markets[:top_n]

    results = []
    passed_prefix = 0
    passed_volume = 0
    passed_price = 0

    for m in candidates:
        # Cheapest checks first: prefix filter
        if prefixes_upper:
            event_ticker = (m.get("event_ticker") or "").upper()
            if not any(event_ticker.startswith(p) for p in prefixes_upper):
                continue

        passed_prefix += 1

        # Volume filter (cheap int comparison)
        if m["volume_24h"] < min_volume:
            continue

        passed_volume += 1

        yes_bid = m["yes_bid"]
        yes_ask = m["yes_ask"]
        no_bid = m["no_bid"]
        no_ask = m["no_ask"]

        # Infer missing ask from opposite bid: on Kalshi, yes_ask = 100 - no_bid
        if not yes_ask and no_bid:
            yes_ask = 100 - no_bid
        if not no_ask and yes_bid:
            no_ask = 100 - yes_bid

        close_time_raw = m.get("close_time", "")

        hrs_left = hours_until_close(close_time_raw)

        if yes_ask and min_price <= yes_ask <= 98:
            passed_price += 1
            tier = _assign_tier(yes_ask)
            dollar_24h = int(m["volume_24h"] * yes_ask) // 100
            spread_pct = _calc_spread_pct(yes_bid, yes_ask)
            results.append({
                "ticker": m["ticker"],
                "event_ticker": m["event_ticker"],
                "signal_side": "yes",
                "signal_price": yes_bid,
                "signal_ask": yes_ask,
                "volume_24h": m["volume_24h"],
                "dollar_24h": dollar_24h,
                "volume": m["volume"],
                "open_interest": m["open_interest"],
                "yes_bid": yes_bid,
                "no_bid": no_bid,
                "tier": tier,
                "spread_pct": round(spread_pct, 2),
                "close_time": close_time_raw,
                "close_time_fmt": format_close_time(close_time_raw),
                "hours_left": hrs_left,
            })
        elif no_ask and min_price <= no_ask <= 98:
            passed_price += 1
            tier = _assign_tier(no_ask)
            dollar_24h = int(m["volume_24h"] * no_ask) // 100
            spread_pct = _calc_spread_pct(no_bid, no_ask)
            results.append({
                "ticker": m["ticker"],
                "event_ticker": m["event_ticker"],
                "signal_side": "no",
                "signal_price": no_bid,
                "signal_ask": no_ask,
                "volume_24h": m["volume_24h"],
                "dollar_24h": dollar_24h,
                "volume": m["volume"],
                "open_interest": m["open_interest"],
                "yes_bid": yes_bid,
                "no_bid": no_bid,
                "tier": tier,
                "spread_pct": round(spread_pct, 2),
                "close_time": close_time_raw,
                "close_time_fmt": format_close_time(close_time_raw),
                "hours_left": hrs_left,
            })

    # Determine dollar-volume rank and qualification status
    by_dollar = sorted(results, key=lambda x: x["dollar_24h"], reverse=True)
    dollar_ranks = {r["ticker"]: rank + 1 for rank, r in enumerate(by_dollar)}

    qualified_count = 0
    count_tier1 = 0
    count_top20 = 0
    count_dollar_vol = 0
    count_spread = 0
    count_expires = 0
    for r in results:
        rank = dollar_ranks[r["ticker"]]
        r["dollar_rank"] = rank

        # Cheapest checks first for qualification
        is_profitable = r["tier"] > 0
        if not is_profitable:
            r["qualified"] = False
            r["fail_reasons"] = ["tier0"]
            continue

        is_spread = r["spread_pct"] <= QUALIFIED_MAX_SPREAD_PCT
        is_dollar = r["dollar_24h"] >= QUALIFIED_MIN_DOLLAR_24H
        is_top_n = rank <= QUALIFIED_TOP_N_DOLLAR
        hrs = r.get("hours_left")
        is_expiring = hrs is not None and 0 < hrs <= QUALIFIED_MAX_HOURS

        if r["tier"] == 1:
            count_tier1 += 1
        if is_top_n:
            count_top20 += 1
        if is_dollar:
            count_dollar_vol += 1
        if is_spread:
            count_spread += 1
        if is_expiring:
            count_expires += 1

        fail_reasons = []
        if not is_top_n:
            fail_reasons.append("rank")
        if not is_dollar:
            fail_reasons.append("volume")
        if not is_spread:
            fail_reasons.append("spread")
        if not is_expiring:
            fail_reasons.append("expiry")
        r["fail_reasons"] = fail_reasons
        r["qualified"] = len(fail_reasons) == 0
        if r["qualified"]:
            qualified_count += 1

    # Sort: qualified first (grouped by tier), then non-qualified by tier -> price
    results.sort(key=lambda x: (
        0 if x["qualified"] else 1,
        x["tier"],
        -x["signal_price"],
        -x["dollar_24h"],
        x.get("spread_pct", 99),
    ))

    stats = {
        "total_fetched": total_fetched,
        "top_n": top_n,
        "scanned": len(candidates),
        "passed_prefix": passed_prefix,
        "passed_volume": passed_volume,
        "passed_price": passed_price,
        "count_tier1": count_tier1,
        "count_top20": count_top20,
        "count_dollar_vol": count_dollar_vol,
        "count_spread": count_spread,
        "count_expires": count_expires,
        "qualified": qualified_count,
        "min_price": min_price,
        "min_volume": min_volume,
        "prefixes": ticker_prefixes or [],
        "cached": from_cache,
    }

    # Update cache
    _scan_cache["ts"] = time.time()
    _scan_cache["results"] = results
    _scan_cache["stats"] = stats

    return results, stats
