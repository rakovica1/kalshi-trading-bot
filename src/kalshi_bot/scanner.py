import json
import time


def _iter_markets(client, status="open", page_size=200, max_markets=500):
    """Yield markets one page at a time, up to max_markets total."""
    cursor = None
    count = 0
    while True:
        kwargs = {"limit": min(page_size, max_markets - count), "status": status}
        if cursor:
            kwargs["cursor"] = cursor
        resp = client._market_api.get_markets_without_preload_content(**kwargs)
        data = json.loads(resp.data)
        markets = data.get("markets", [])
        for m in markets:
            yield m
            count += 1
            if count >= max_markets:
                return
        cursor = data.get("cursor")
        if not cursor or not markets:
            break


# Simple in-memory cache
_scan_cache = {"ts": 0, "results": [], "stats": {}, "ttl": 120}

MAX_MARKETS = 500


def scan(client, min_price=99, ticker_prefixes=None, min_volume=100,
         use_cache=False, max_markets=MAX_MARKETS):
    """Find markets where YES or NO bid is >= min_price.

    Streams markets in pages to keep memory low.
    Returns (results, scanned_count) tuple.
    """
    # Return cached results if fresh enough
    if use_cache and _scan_cache["stats"]:
        age = time.time() - _scan_cache["ts"]
        if age < _scan_cache["ttl"]:
            return _scan_cache["results"], _scan_cache["stats"]

    prefixes_upper = [p.upper() for p in ticker_prefixes] if ticker_prefixes else None
    results = []
    scanned = 0
    passed_prefix = 0
    passed_volume = 0
    passed_price = 0

    for m in _iter_markets(client, page_size=200, max_markets=max_markets):
        scanned += 1

        if prefixes_upper:
            event_ticker = (m.get("event_ticker") or "").upper()
            if not any(event_ticker.startswith(p) for p in prefixes_upper):
                continue

        passed_prefix += 1

        if m.get("volume", 0) < min_volume:
            continue

        passed_volume += 1

        yes_bid = m.get("yes_bid", 0) or 0
        no_bid = m.get("no_bid", 0) or 0

        if yes_bid >= min_price:
            passed_price += 1
            results.append({
                "ticker": m.get("ticker", "?"),
                "event_ticker": m.get("event_ticker", ""),
                "signal_side": "yes",
                "signal_price": yes_bid,
                "volume": m.get("volume", 0),
                "yes_bid": yes_bid,
                "no_bid": no_bid,
            })
        elif no_bid >= min_price:
            passed_price += 1
            results.append({
                "ticker": m.get("ticker", "?"),
                "event_ticker": m.get("event_ticker", ""),
                "signal_side": "no",
                "signal_price": no_bid,
                "volume": m.get("volume", 0),
                "yes_bid": yes_bid,
                "no_bid": no_bid,
            })

    results.sort(key=lambda x: (x["signal_price"], x.get("volume", 0)), reverse=True)

    stats = {
        "scanned": scanned,
        "passed_prefix": passed_prefix,
        "passed_volume": passed_volume,
        "passed_price": passed_price,
        "min_price": min_price,
        "min_volume": min_volume,
        "prefixes": ticker_prefixes or [],
    }

    # Update cache
    _scan_cache["ts"] = time.time()
    _scan_cache["results"] = results
    _scan_cache["stats"] = stats

    return results, stats
