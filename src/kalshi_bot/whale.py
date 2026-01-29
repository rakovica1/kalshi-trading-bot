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
    min_price=99,
    min_volume=1000,
    risk_pct=0.01,
    max_positions=10,
    daily_loss_pct=0.05,
    chunk_count=3,
    chunk_delay_sec=10,
    dry_run=True,
    log=print,
):
    """Run the whale trading strategy.

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
    log(f"  Balance:       ${balance_cents / 100:.2f}")
    log(f"  Risk per trade: {risk_pct*100:.0f}% = ${balance_cents * risk_pct / 100:.2f}")

    # 2. Daily loss check
    starting = db.get_today_starting_balance()
    if starting is None:
        starting = balance_cents
    daily_loss = starting - balance_cents
    max_daily_loss = int(starting * daily_loss_pct)
    log(f"  Daily loss:    ${daily_loss / 100:.2f} / ${max_daily_loss / 100:.2f} limit")

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
    log(f"\n  Scanning: prefixes={','.join(prefix_list)} min_price={min_price}c min_vol={min_volume}")
    results, scan_stats = scan(client, min_price=min_price, ticker_prefixes=prefix_list, min_volume=min_volume, max_markets=5000)
    log(f"  Found {len(results)} qualifying markets (scanned {scan_stats.get('scanned', '?')})")

    if not results:
        return {"scanned": 0, "skipped": 0, "traded": 0, "orders": 0, "stopped_reason": None}

    # 5. Trade each market
    existing_tickers = db.get_position_tickers()
    summary = {"scanned": len(results), "skipped": 0, "traded": 0, "orders": 0, "stopped_reason": None}
    dry_run_traded = 0  # track simulated trades for position limit in dry run

    log(f"\n  {'TICKER':<40} {'SIDE':<5} {'PRICE':>5} {'CONTRACTS':>10} {'CHUNKS':>7} {'ACTION':<10}")
    log(f"  {'-'*80}")

    for m in results:
        ticker = m.get("ticker", "?")
        side = m["signal_side"]
        price = m["signal_price"]

        # Skip if already holding this ticker
        if ticker in existing_tickers:
            log(f"  {ticker:<40} {side.upper():<5} {price:>4}c {'—':>10} {'—':>7} SKIP (held)")
            summary["skipped"] += 1
            continue

        # Re-check position limit
        current_open = db.count_open_positions() + dry_run_traded
        if current_open >= max_positions:
            log(f"\n  STOPPED: Max positions reached mid-run ({max_positions})")
            summary["stopped_reason"] = "max_positions"
            break

        # Re-check daily loss (skip API call in dry run — no real money spent)
        if dry_run:
            fresh_bal = balance_cents
        else:
            fresh_bal = client.get_balance().get("balance", 0)
            current_loss = starting - fresh_bal
            if current_loss >= max_daily_loss:
                log(f"\n  STOPPED: Daily loss limit reached mid-run")
                summary["stopped_reason"] = "daily_loss"
                break

        # Calculate position size
        total_contracts = calculate_position(fresh_bal, price, risk_pct)
        if total_contracts <= 0:
            log(f"  {ticker:<40} {side.upper():<5} {price:>4}c {0:>10} {'—':>7} SKIP (no budget)")
            summary["skipped"] += 1
            continue

        chunks = _split_into_chunks(total_contracts, chunk_count)

        prefix = f"  {ticker:<40} {side.upper():<5} {price:>4}c {total_contracts:>10} {len(chunks):>7} "

        if dry_run:
            log(prefix + "DRY RUN")
            summary["traded"] += 1
            summary["orders"] += len(chunks)
            existing_tickers.add(ticker)
            dry_run_traded += 1
            continue

        # Place chunked orders
        log(prefix + "PLACING...")
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
            summary["traded"] += 1
            existing_tickers.add(ticker)
        summary["orders"] += len(chunks)

    # 6. Summary
    log(f"\n{'='*60}")
    log(f"  SUMMARY [{mode}]")
    log(f"  Scanned: {summary['scanned']}  Traded: {summary['traded']}  "
        f"Skipped: {summary['skipped']}  Orders: {summary['orders']}")
    if summary["stopped_reason"]:
        log(f"  Stopped early: {summary['stopped_reason']}")
    log(f"{'='*60}\n")

    return summary
