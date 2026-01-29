import json
import time


def _fetch_all_markets(client, status="open", page_size=1000):
    """Fetch all open markets from the API, paginating with max page size.

    Returns a list of market dicts with only the fields we need,
    to keep memory usage low.
    """
    markets = []
    cursor = None
    while True:
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
            })
        cursor = data.get("cursor")
        if not cursor or not page:
            break
    return markets


def _assign_tier(price):
    """Assign a tier based on signal price.

    Tier 1 (Best):  98-99c+
    Tier 2 (Good):  96-97c
    Tier 3 (Okay):  95c
    """
    if price >= 98:
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


# Qualification thresholds for premium trade execution
QUALIFIED_MIN_DOLLAR_24H = 50_000
QUALIFIED_MAX_SPREAD_PCT = 5.0
QUALIFIED_TOP_N_DOLLAR = 20


def scan(client, min_price=95, ticker_prefixes=None, min_volume=1000,
         use_cache=False, top_n=30):
    """Find markets where YES or NO bid is >= min_price.

    Fetches ALL open markets from the API, sorts by 24h volume descending,
    then filters the top_n most active markets by prefix, volume, and price.
    Each result is assigned a tier (1/2/3) based on price.

    Each result also gets a `qualified` flag: True when ALL of these hold:
      - Tier 1 (price >= 98c)
      - Top 20 by 24h dollar volume
      - >= $50,000 in 24h dollar volume
      - Bid/ask spread < 5%

    min_volume applies to volume_24h (24-hour trading volume).

    Returns (results, stats) tuple.
    """
    # Return cached results if fresh enough
    if use_cache and _scan_cache["stats"]:
        age = time.time() - _scan_cache["ts"]
        if age < _scan_cache["ttl"]:
            return _scan_cache["results"], _scan_cache["stats"]

    prefixes_upper = [p.upper() for p in ticker_prefixes] if ticker_prefixes else None

    # 1. Fetch all open markets
    all_markets = _fetch_all_markets(client)
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
                "volume_24h": m["volume_24h"],
                "dollar_24h": dollar_24h,
                "volume": m["volume"],
                "open_interest": m["open_interest"],
                "yes_bid": yes_bid,
                "no_bid": no_bid,
                "tier": tier,
                "spread_pct": round(spread_pct, 2),
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
                "volume_24h": m["volume_24h"],
                "dollar_24h": dollar_24h,
                "volume": m["volume"],
                "open_interest": m["open_interest"],
                "yes_bid": yes_bid,
                "no_bid": no_bid,
                "tier": tier,
                "spread_pct": round(spread_pct, 2),
            })

    # Determine dollar-volume rank and qualification status
    by_dollar = sorted(results, key=lambda x: x["dollar_24h"], reverse=True)
    dollar_ranks = {r["ticker"]: rank + 1 for rank, r in enumerate(by_dollar)}

    qualified_count = 0
    for r in results:
        rank = dollar_ranks[r["ticker"]]
        r["dollar_rank"] = rank
        r["qualified"] = (
            r["tier"] == 1
            and rank <= QUALIFIED_TOP_N_DOLLAR
            and r["dollar_24h"] >= QUALIFIED_MIN_DOLLAR_24H
            and r["spread_pct"] < QUALIFIED_MAX_SPREAD_PCT
        )
        if r["qualified"]:
            qualified_count += 1

    # Sort: qualified first, then by price desc -> $volume desc -> spread asc
    # Within non-qualified: tier asc -> price desc -> volume desc
    results.sort(key=lambda x: (
        0 if x["qualified"] else 1,
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
        "qualified": qualified_count,
        "min_price": min_price,
        "min_volume": min_volume,
        "prefixes": ticker_prefixes or [],
    }

    # Update cache
    _scan_cache["ts"] = time.time()
    _scan_cache["results"] = results
    _scan_cache["stats"] = stats

    return results, stats
