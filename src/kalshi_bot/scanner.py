def scan(client, min_price=99, ticker_prefixes=None, min_volume=100):
    """Find markets where YES or NO bid is >= min_price."""
    all_markets = client.get_all_markets(status="open")

    prefixes_upper = [p.upper() for p in ticker_prefixes] if ticker_prefixes else None
    results = []

    for m in all_markets:
        if m.get("volume", 0) < min_volume:
            continue

        if prefixes_upper:
            event_ticker = (m.get("event_ticker") or "").upper()
            if not any(event_ticker.startswith(p) for p in prefixes_upper):
                continue

        yes_bid = m.get("yes_bid", 0) or 0
        no_bid = m.get("no_bid", 0) or 0

        if yes_bid >= min_price:
            m["signal_side"] = "yes"
            m["signal_price"] = yes_bid
            results.append(m)
        elif no_bid >= min_price:
            m["signal_side"] = "no"
            m["signal_price"] = no_bid
            results.append(m)

    results.sort(key=lambda x: (x["signal_price"], x.get("volume", 0)), reverse=True)
    return results
