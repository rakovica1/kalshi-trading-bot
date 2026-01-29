import time

from kalshi_bot import db
from kalshi_bot.scanner import scan
from kalshi_bot.sizing import calculate_position


def _split_into_chunks(total, chunk_count):
    """Split total contracts into chunk_count pieces (min 1 each)."""
    if total <= 0:
        return []
    n = min(chunk_count, total)
    base = total // n
    remainder = total % n
    chunks = [base] * n
    for i in range(remainder):
        chunks[i] += 1
    return chunks


def run_whale_strategy(
    client,
    prefixes=("KXNFL", "KXNBA", "KXBTC", "KXETH"),
    min_price=95,
    min_volume=1000,
    risk_pct=0.01,
    max_positions=10,
    daily_loss_pct=0.05,
    chunk_count=3,
    chunk_delay_sec=10,
    dry_run=True,
    tier1_only=True,
    log=print,
):
    """Run the whale trading strategy.

    1. Scans all markets automatically
    2. Filters to QUALIFIED markets (Tier 1 + top 20 $vol + $50k + <5% spread)
    3. Ranks by: highest price -> highest $volume -> tightest spread
    4. Picks the #1 ranked market
    5. Places the trade (or simulates in dry-run mode)

    Returns a summary dict with counts of actions taken.
    """
    mode = "DRY RUN" if dry_run else "LIVE"
    log(f"\n{'='*60}")
    log(f"  WHALE STRATEGY [{mode}]")
    log(f"{'='*60}")

    # 1. Fetch balance
    bal_data = client.get_balance()
    balance_cents = bal_data.get("balance", 0)
    db.log_balance(balance_cents)
    log(f"  Balance:        ${balance_cents / 100:.2f}")
    log(f"  Risk per trade: {risk_pct*100:.0f}% = ${balance_cents * risk_pct / 100:.2f}")

    # 2. Daily loss check
    starting = db.get_today_starting_balance()
    if starting is None:
        starting = balance_cents
    daily_loss = starting - balance_cents
    max_daily_loss = int(starting * daily_loss_pct)
    log(f"  Daily loss:     ${daily_loss / 100:.2f} / ${max_daily_loss / 100:.2f} limit")

    if daily_loss >= max_daily_loss:
        log(f"\n  STOPPED: Daily loss limit reached ({daily_loss_pct*100:.0f}%)")
        return {"scanned": 0, "skipped": 0, "traded": 0, "orders": 0, "stopped_reason": "daily_loss"}

    # 3. Position count check
    open_count = db.count_open_positions()
    log(f"  Open positions: {open_count} / {max_positions} max")

    if open_count >= max_positions:
        log(f"\n  STOPPED: Max positions reached ({max_positions})")
        return {"scanned": 0, "skipped": 0, "traded": 0, "orders": 0, "stopped_reason": "max_positions"}

    # 4. Scan markets
    prefix_list = list(prefixes)
    log(f"\n  Scanning all markets...")
    log(f"  Prefixes: {','.join(prefix_list)}")
    log(f"  Min price: {min_price}c  Min 24h vol: {min_volume}")
    results, scan_stats = scan(
        client, min_price=min_price, ticker_prefixes=prefix_list,
        min_volume=min_volume, top_n=5000,
    )
    # Save scan results for web dashboard
    db.save_scan_results(results, scan_stats)

    total_found = len(results)
    qualified_count = scan_stats.get("qualified", 0)
    log(f"  Found {total_found} markets, {qualified_count} qualified")

    # 5. Filter to qualified markets only
    if tier1_only:
        candidates = [r for r in results if r.get("qualified")]
    else:
        candidates = list(results)

    if not candidates:
        log(f"\n  No {'qualified ' if tier1_only else ''}markets found. Nothing to trade.")
        log(f"{'='*60}\n")
        return {"scanned": total_found, "skipped": 0, "traded": 0, "orders": 0, "stopped_reason": None}

    # 6. Remove markets we already hold
    existing_tickers = db.get_position_tickers()
    available = []
    held = []
    for c in candidates:
        if c["ticker"] in existing_tickers:
            held.append(c)
        else:
            available.append(c)

    if held:
        log(f"\n  Skipping {len(held)} already-held position{'s' if len(held) != 1 else ''}:")
        for h in held:
            log(f"    {h['ticker']} ({h['signal_side'].upper()} @ {h['signal_price']}c)")

    if not available:
        log(f"\n  All {len(candidates)} qualified markets are already held. Nothing to trade.")
        log(f"{'='*60}\n")
        return {"scanned": total_found, "skipped": len(held), "traded": 0, "orders": 0, "stopped_reason": None}

    # 7. Rank: highest price -> highest $volume -> tightest spread
    available.sort(key=lambda m: (
        -m["signal_price"],
        -m["dollar_24h"],
        m.get("spread_pct", 99),
    ))

    log(f"\n  Ranking {len(available)} available qualified markets:")
    log(f"  {'#':>3}  {'TICKER':<35} {'SIDE':<4} {'PRICE':>5} {'24H $':>10} {'SPREAD':>7} {'RANK':>5}")
    log(f"  {'-'*75}")
    for i, m in enumerate(available):
        marker = " >> " if i == 0 else "    "
        log(f"  {marker}{i+1:>1}. {m['ticker']:<35} {m['signal_side'].upper():<4} "
            f"{m['signal_price']:>4}c ${m['dollar_24h']:>8,} "
            f"{m.get('spread_pct', 0):>6.1f}% #{m.get('dollar_rank', 0):>3}")

    # 8. Select #1 ranked market
    selected = available[0]
    ticker = selected["ticker"]
    side = selected["signal_side"]
    price = selected["signal_price"]
    dollar_24h = selected["dollar_24h"]
    spread = selected.get("spread_pct", 0)

    log(f"\n  Selected {ticker} at {price}c "
        f"(${dollar_24h:,} volume, {spread:.1f}% spread)")

    # 9. Calculate position size
    total_contracts = calculate_position(balance_cents, price, risk_pct)
    if total_contracts <= 0:
        log(f"  SKIP: Insufficient balance for even 1 contract at {price}c")
        log(f"{'='*60}\n")
        return {"scanned": total_found, "skipped": len(held) + 1, "traded": 0, "orders": 0, "stopped_reason": "no_budget"}

    chunks = _split_into_chunks(total_contracts, chunk_count)
    cost_cents = total_contracts * price
    log(f"  Position: {total_contracts} contracts x {price}c = ${cost_cents / 100:.2f}")
    log(f"  Execution: {len(chunks)} chunks ({', '.join(str(c) for c in chunks)})")

    summary = {
        "scanned": total_found,
        "skipped": len(held),
        "traded": 0,
        "orders": 0,
        "stopped_reason": None,
        "selected_ticker": ticker,
    }

    # 10. Execute trade
    if dry_run:
        log(f"\n  DRY RUN — would place {len(chunks)} orders for {total_contracts} {side.upper()} contracts on {ticker} at {price}c")
        summary["traded"] = 1
        summary["orders"] = len(chunks)
    else:
        log(f"\n  PLACING {len(chunks)} orders for {total_contracts} {side.upper()} on {ticker}...")
        chunk_success = 0
        for i, chunk_qty in enumerate(chunks):
            try:
                result = client.create_order(
                    ticker=ticker,
                    side=side,
                    action="buy",
                    count=chunk_qty,
                    price=price,
                )

                order_id = result.get("order_id")
                fill_count = result.get("fill_count", 0)
                remaining = result.get("remaining_count", 0)

                if fill_count > 0 and remaining == 0:
                    status = "filled"
                elif fill_count > 0:
                    status = "partial"
                else:
                    status = "submitted"

                db.log_trade(
                    ticker=ticker,
                    side=side,
                    action="buy",
                    count=chunk_qty,
                    price_cents=price,
                    status=status,
                    order_id=order_id,
                    fill_count=fill_count,
                    remaining_count=remaining,
                )

                if fill_count > 0:
                    db.update_position_on_buy(ticker, side, fill_count, price)

                chunk_success += 1
                log(f"    Chunk {i+1}/{len(chunks)}: {chunk_qty} contracts -> {status} (fills={fill_count})")

            except Exception as e:
                db.log_trade(
                    ticker=ticker,
                    side=side,
                    action="buy",
                    count=chunk_qty,
                    price_cents=price,
                    status="failed",
                    error_message=str(e),
                )
                log(f"    Chunk {i+1}/{len(chunks)}: FAILED — {e}")

            # Delay between chunks
            if i < len(chunks) - 1:
                time.sleep(chunk_delay_sec)

        if chunk_success > 0:
            summary["traded"] = 1
        summary["orders"] = len(chunks)

    # 11. Summary
    log(f"\n{'='*60}")
    log(f"  SUMMARY [{mode}]")
    log(f"  Market:  {ticker} {side.upper()} @ {price}c")
    log(f"  Scanned: {summary['scanned']}  Qualified: {len(candidates)}  "
        f"Traded: {summary['traded']}  Orders: {summary['orders']}")
    if summary["stopped_reason"]:
        log(f"  Stopped: {summary['stopped_reason']}")
    log(f"{'='*60}\n")

    return summary
