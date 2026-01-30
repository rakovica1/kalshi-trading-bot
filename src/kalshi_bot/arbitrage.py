"""Kalshi arbitrage detection and execution.

Adapted from https://github.com/vladmeer/kalshi-arbitrage-bot

Two arbitrage types:

1. **Probability arbitrage** — When YES_ask + NO_ask < 100c, buying both
   sides guarantees profit (one side always pays 100c at settlement).
   When YES_bid + NO_bid > 100c, selling both sides guarantees profit.

2. **Orderbook spread** — When the best bid on one side exceeds the best
   ask on the other side (after adjusting for YES+NO=100 equivalence),
   you can buy low and sell high for instant profit.

All profits are calculated net of Kalshi's tiered fee structure.
"""

import math
import time

# ---------------------------------------------------------------------------
# Fee calculator — Kalshi's official formula
# ---------------------------------------------------------------------------

def taker_fee(price_cents, quantity):
    """Calculate taker fee per Kalshi formula: ceil(0.07 * C * P * (1-P)).

    price_cents: contract price in cents (1-99)
    quantity: number of contracts
    Returns fee in cents.
    """
    p = price_cents / 100.0
    fee = 0.07 * quantity * p * (1 - p)
    return math.ceil(fee * 100)  # convert dollars to cents, round up


def maker_fee(price_cents, quantity):
    """Calculate maker fee: ceil(0.0175 * C * P * (1-P))."""
    p = price_cents / 100.0
    fee = 0.0175 * quantity * p * (1 - p)
    return math.ceil(fee * 100)


def net_profit_buy_both(yes_ask, no_ask, quantity):
    """Net profit from buying YES + NO (both settle to 100c combined).

    Revenue: 100c * quantity (guaranteed)
    Cost: (yes_ask + no_ask) * quantity + fees
    """
    cost_cents = (yes_ask + no_ask) * quantity
    revenue_cents = 100 * quantity
    gross = revenue_cents - cost_cents

    # Fees for buying both sides (taker — crossing the ask)
    yes_fee = taker_fee(yes_ask, quantity)
    no_fee = taker_fee(no_ask, quantity)
    total_fees = yes_fee + no_fee

    return gross - total_fees


def net_profit_sell_both(yes_bid, no_bid, quantity):
    """Net profit from selling YES + NO at bid prices.

    Revenue: (yes_bid + no_bid) * quantity
    Cost: 100c * quantity (obligation at settlement) + fees
    """
    revenue_cents = (yes_bid + no_bid) * quantity
    cost_cents = 100 * quantity
    gross = revenue_cents - cost_cents

    yes_fee = taker_fee(yes_bid, quantity)
    no_fee = taker_fee(no_bid, quantity)
    total_fees = yes_fee + no_fee

    return gross - total_fees


# ---------------------------------------------------------------------------
# Opportunity detection
# ---------------------------------------------------------------------------

def scan_probability_arb(markets, min_profit_cents=1, quantity=10):
    """Scan markets for probability arbitrage.

    Returns list of opportunities sorted by profit (highest first).
    Each opportunity is a dict with details.
    """
    opportunities = []

    for m in markets:
        ticker = m.get("ticker", "")
        yes_bid = m.get("yes_bid", 0) or 0
        yes_ask = m.get("yes_ask", 100) or 100
        no_bid = m.get("no_bid", 0) or 0
        no_ask = m.get("no_ask", 100) or 100

        # Type 1: Buy both — YES_ask + NO_ask < 100
        buy_total = yes_ask + no_ask
        if buy_total < 100:
            profit = net_profit_buy_both(yes_ask, no_ask, quantity)
            if profit >= min_profit_cents:
                opportunities.append({
                    "type": "buy_both",
                    "ticker": ticker,
                    "yes_ask": yes_ask,
                    "no_ask": no_ask,
                    "total_cost": buy_total,
                    "gross_edge": 100 - buy_total,
                    "net_profit_cents": profit,
                    "quantity": quantity,
                    "description": (
                        f"BUY YES@{yes_ask}c + NO@{no_ask}c = {buy_total}c "
                        f"→ profit {profit}c on {quantity} contracts"
                    ),
                })

        # Type 2: Sell both — YES_bid + NO_bid > 100
        sell_total = yes_bid + no_bid
        if sell_total > 100:
            profit = net_profit_sell_both(yes_bid, no_bid, quantity)
            if profit >= min_profit_cents:
                opportunities.append({
                    "type": "sell_both",
                    "ticker": ticker,
                    "yes_bid": yes_bid,
                    "no_bid": no_bid,
                    "total_revenue": sell_total,
                    "gross_edge": sell_total - 100,
                    "net_profit_cents": profit,
                    "quantity": quantity,
                    "description": (
                        f"SELL YES@{yes_bid}c + NO@{no_bid}c = {sell_total}c "
                        f"→ profit {profit}c on {quantity} contracts"
                    ),
                })

    opportunities.sort(key=lambda x: x["net_profit_cents"], reverse=True)
    return opportunities


def scan_orderbook_arb(client, tickers, min_profit_cents=1, max_quantity=100):
    """Scan orderbooks for spread arbitrage.

    Looks for cases where you can buy at the ask and sell at a higher bid
    on the complementary side (since YES + NO = 100c at settlement).

    Args:
        client: KalshiBotClient with get_market_orderbook()
        tickers: list of ticker strings to check
        min_profit_cents: minimum net profit to report
        max_quantity: max contracts per trade

    Returns list of opportunities.
    """
    opportunities = []

    for ticker in tickers:
        try:
            book = client.get_market_orderbook(ticker, depth=5)
        except Exception:
            continue

        yes_bids = book.get("yes", [])  # [[price, qty], ...]
        no_bids = book.get("no", [])

        # On Kalshi, the orderbook returns bids for YES and NO sides.
        # YES ask = 100 - NO bid, NO ask = 100 - YES bid
        # Look for: YES best bid > implied YES ask (100 - NO best bid)
        # That means: YES_bid + NO_bid > 100

        if yes_bids and no_bids:
            best_yes_bid = yes_bids[0][0] if yes_bids[0] else 0
            best_no_bid = no_bids[0][0] if no_bids[0] else 0
            yes_bid_qty = yes_bids[0][1] if yes_bids[0] else 0
            no_bid_qty = no_bids[0][1] if no_bids[0] else 0

            if best_yes_bid + best_no_bid > 100:
                qty = min(yes_bid_qty, no_bid_qty, max_quantity)
                if qty > 0:
                    profit = net_profit_sell_both(best_yes_bid, best_no_bid, qty)
                    if profit >= min_profit_cents:
                        opportunities.append({
                            "type": "orderbook_sell",
                            "ticker": ticker,
                            "yes_bid": best_yes_bid,
                            "no_bid": best_no_bid,
                            "quantity": qty,
                            "net_profit_cents": profit,
                            "description": (
                                f"SELL YES@{best_yes_bid}c({yes_bid_qty}) + "
                                f"NO@{best_no_bid}c({no_bid_qty}) "
                                f"→ profit {profit}c on {qty} contracts"
                            ),
                        })

        # Also check implied asks
        # YES ask levels are equivalent to NO bid complement
        # If we can buy YES cheap and it resolves, or buy both sides cheap
        # This is already covered by probability arb above

    opportunities.sort(key=lambda x: x["net_profit_cents"], reverse=True)
    return opportunities


# ---------------------------------------------------------------------------
# Full scan
# ---------------------------------------------------------------------------

def run_arbitrage_scan(client, log=print, min_profit_cents=1,
                       quantity=10, check_orderbook=True,
                       max_orderbook_checks=50, stop_check=None):
    """Full arbitrage scan across all open markets.

    Args:
        client: KalshiBotClient
        log: logging callable
        min_profit_cents: minimum net profit to report
        quantity: default contract quantity for profit calculation
        check_orderbook: whether to also scan orderbooks (slower)
        max_orderbook_checks: limit orderbook API calls
        stop_check: callable returning True to abort

    Returns list of all opportunities found.
    """
    log("[HEAD] Arbitrage Scanner")
    log("[INFO] Fetching all open markets...")

    markets = client.get_all_markets(status="open")
    log(f"[INFO] {len(markets)} markets fetched")

    # Extract price data
    priced_markets = []
    for m in markets:
        yes_bid = m.get("yes_bid") or 0
        yes_ask = m.get("yes_ask") or 0
        no_bid = m.get("no_bid") or 0
        no_ask = m.get("no_ask") or 0

        # Skip markets with no price data
        if yes_bid == 0 and yes_ask == 0 and no_bid == 0 and no_ask == 0:
            continue

        # Infer missing prices
        if not no_ask and yes_bid:
            no_ask = 100 - yes_bid
        if not yes_ask and no_bid:
            yes_ask = 100 - no_bid
        if not no_bid and yes_ask:
            no_bid = 100 - yes_ask
        if not yes_bid and no_ask:
            yes_bid = 100 - no_ask

        priced_markets.append({
            "ticker": m.get("ticker", ""),
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "volume_24h": m.get("volume_24h", 0) or 0,
        })

    log(f"[INFO] {len(priced_markets)} markets with price data")

    # 1. Probability arbitrage scan (fast — uses existing price data)
    prob_opps = scan_probability_arb(priced_markets, min_profit_cents, quantity)
    if prob_opps:
        log(f"[FILL] Found {len(prob_opps)} probability arbitrage opportunities!")
        for opp in prob_opps[:10]:
            log(f"[FILL] {opp['ticker']}: {opp['description']}")
    else:
        log("[INFO] No probability arbitrage found")

    # 2. Orderbook spread scan (slower — requires per-market API calls)
    book_opps = []
    if check_orderbook:
        if stop_check and stop_check():
            log("[WARN] Stopped before orderbook scan")
            return prob_opps

        # Sort by volume, check highest-volume markets first
        priced_markets.sort(key=lambda m: m["volume_24h"], reverse=True)
        tickers_to_check = [m["ticker"] for m in priced_markets[:max_orderbook_checks]]
        log(f"[INFO] Scanning {len(tickers_to_check)} orderbooks...")

        book_opps = scan_orderbook_arb(
            client, tickers_to_check, min_profit_cents, quantity
        )

        if book_opps:
            log(f"[FILL] Found {len(book_opps)} orderbook spread opportunities!")
            for opp in book_opps[:10]:
                log(f"[FILL] {opp['ticker']}: {opp['description']}")
        else:
            log("[INFO] No orderbook spread arbitrage found")

    all_opps = prob_opps + book_opps
    log(f"[HEAD] Scan complete — {len(all_opps)} total opportunities")
    return all_opps
