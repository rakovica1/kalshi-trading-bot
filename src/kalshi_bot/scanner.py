import json
import time
from datetime import datetime, timezone, timedelta


class StopRequested(Exception):
    """Raised when a stop signal is detected during scanning."""
    pass


# Cache for raw market data to avoid re-fetching 500k+ markets every scan
_market_cache = {"ts": 0, "markets": [], "ttl": 120}


def _fetch_all_markets(client, status="open", page_size=1000, stop_check=None):
    """Fetch all open markets from the API, paginating with max page size.

    Returns a list of market dicts with only the fields we need,
    to keep memory usage low. Results are cached for 120 seconds.
    """
    # Return cached if fresh
    age = time.time() - _market_cache["ts"]
    if _market_cache["markets"] and age < _market_cache["ttl"]:
        return _market_cache["markets"], True

    markets = []
    cursor = None
    while True:
        if stop_check and stop_check():
            raise StopRequested()
        kwargs = {"limit": page_size, "status": status}
        if cursor:
            kwargs["cursor"] = cursor
        resp = client._market_api.get_markets_without_preload_content(**kwargs)
        data = json.loads(resp.data)
        page = data.get("markets", [])
        for m in page:
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
        cursor = data.get("cursor")
        if not cursor or not page:
            break

    _market_cache["markets"] = markets
    _market_cache["ts"] = time.time()
    return markets, False


def _assign_tier(price):
    """Assign a tier based on signal price.

    Tier 0 (Skip):  99c — unprofitable (0¢ profit after 1¢ fee)
    Tier 1 (Best):  98c — 1¢ profit after fees
    Tier 2 (Good):  96-97c — 2-3¢ profit
    Tier 3 (Okay):  95c — 4¢ profit
    """
    if price >= 99:
        return 0
    elif price == 98:
        return 1
    elif price >= 96:
        return 2
    else:
        return 3


# Simple in-memory cache
_scan_cache = {"ts": 0, "results": [], "stats": {}, "ttl": 120}


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

    Fetches ALL open markets from the API, sorts by 24h volume descending,
    then filters the top_n most active markets by prefix, volume, and price.
    Each result is assigned a tier (1/2/3) based on price.

    Each result also gets a `qualified` flag: True when ALL of these hold:
      - Top 200 by 24h dollar volume
      - >= $10,000 in 24h dollar volume
      - Bid/ask spread < 5%
      - Expires within 24 hours

    min_volume applies to volume_24h (24-hour trading volume).

    Returns (results, stats) tuple.
    """
    # Return cached results if fresh enough
    if use_cache and _scan_cache["stats"]:
        age = time.time() - _scan_cache["ts"]
        if age < _scan_cache["ttl"]:
            return _scan_cache["results"], _scan_cache["stats"]

    prefixes_upper = [p.upper() for p in ticker_prefixes] if ticker_prefixes else None

    # 1. Fetch all open markets (cached for 60s)
    all_markets, from_cache = _fetch_all_markets(client, stop_check=stop_check)
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
        if prefixes_upper:
            event_ticker = (m.get("event_ticker") or "").upper()
            if not any(event_ticker.startswith(p) for p in prefixes_upper):
                continue

        passed_prefix += 1

        if m["volume_24h"] < min_volume:
            continue

        passed_volume += 1

        yes_bid = m["yes_bid"]
        yes_ask = m["yes_ask"]
        no_bid = m["no_bid"]
        no_ask = m["no_ask"]

        close_time_raw = m.get("close_time", "")

        hrs_left = hours_until_close(close_time_raw)

        if yes_bid >= min_price:
            passed_price += 1
            tier = _assign_tier(yes_bid)
            dollar_24h = int(m["volume_24h"] * yes_bid) // 100
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
        elif no_bid >= min_price:
            passed_price += 1
            tier = _assign_tier(no_bid)
            dollar_24h = int(m["volume_24h"] * no_bid) // 100
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
        is_top_n = rank <= QUALIFIED_TOP_N_DOLLAR
        is_dollar = r["dollar_24h"] >= QUALIFIED_MIN_DOLLAR_24H
        is_spread = r["spread_pct"] < QUALIFIED_MAX_SPREAD_PCT
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
        is_profitable = r["tier"] > 0
        r["qualified"] = is_profitable and is_top_n and is_dollar and is_spread and is_expiring
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
