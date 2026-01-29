import json
import time


def _iter_markets(client, status="open", page_size=200):
    """Yield markets one page at a time to avoid loading all into memory."""
    cursor = None
    while True:
        kwargs = {"limit": page_size, "status": status}
        if cursor:
            kwargs["cursor"] = cursor
        resp = client._market_api.get_markets_without_preload_content(**kwargs)
        data = json.loads(resp.data)
        markets = data.get("markets", [])
        for m in markets:
            yield m
        cursor = data.get("cursor")
        if not cursor or not markets:
            break


# Simple in-memory cache: (timestamp, results)
_scan_cache = {"ts": 0, "results": [], "ttl": 120}


def scan(client, min_price=99, ticker_prefixes=None, min_volume=100, use_cache=False):
    """Find markets where YES or NO bid is >= min_price.

    Streams markets in pages to keep memory low.
    """
    # Return cached results if fresh enough
    if use_cache and _scan_cache["results"]:
        age = time.time() - _scan_cache["ts"]
        if age < _scan_cache["ttl"]:
            return _scan_cache["results"]

    prefixes_upper = [p.upper() for p in ticker_prefixes] if ticker_prefixes else None
    results = []

    for m in _iter_markets(client, page_size=200):
        if m.get("volume", 0) < min_volume:
            continue

        if prefixes_upper:
            event_ticker = (m.get("event_ticker") or "").upper()
            if not any(event_ticker.startswith(p) for p in prefixes_upper):
                continue

        yes_bid = m.get("yes_bid", 0) or 0
        no_bid = m.get("no_bid", 0) or 0

        if yes_bid >= min_price:
            # Only keep the fields we need, not the entire market dict
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

    # Update cache
    _scan_cache["ts"] = time.time()
    _scan_cache["results"] = results

    return results
