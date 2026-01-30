from kalshi_bot import db
from kalshi_bot.scanner import scan, format_close_time, hours_until_close, StopRequested
from kalshi_bot.sizing import calculate_position


def run_whale_strategy(
    client,
    prefixes=("KXNFL", "KXNBA", "KXBTC", "KXETH"),
    min_price=95,
    min_volume=10000,
    risk_pct=0.01,
    max_positions=10,
    daily_loss_pct=0.05,
    dry_run=True,
    max_hours_to_expiration=24.0,
    log=print,
    stop_check=None,
):
    """Last-Minute Sniper strategy.

    Ultra-short-term, instant-execution strategy targeting markets that
    resolve within the hour. Uses MARKET orders at the current ask price
    for immediate fills — no chunked/staggered execution.

    Pipeline:
      1. Scan all markets
      2. Filter to QUALIFIED (Top 200 $vol + $10k+ + ≤5% spread + ≤24h exp)
      3. Filter by expiration window (default: 24 hours)
      4. Rank by: soonest expiration -> highest price -> highest $volume
      5. Select #1 ranked market (closest to resolving)
      6. Place aggressive limit @ 98c (fills instantly, auto-cancels if resting)

    Returns a summary dict with counts of actions taken.
    """
    mode = "DRY RUN" if dry_run else "LIVE"
    exp_label = f"{max_hours_to_expiration}h" if max_hours_to_expiration is not None else "no limit"
    log(f"\n{'='*60}")
    log(f"  LAST-MINUTE SNIPER [{mode}]")
    log(f"  Expiration window: {exp_label}")
    log(f"  Order type: Aggressive limit @ 98c (auto-cancel if resting)")
    log(f"{'='*60}")

    # 1. Fetch balance
    bal_data = client.get_balance()
    balance_cents = bal_data.get("balance", 0)
    db.log_balance(balance_cents)
    log(f"  Balance:        ${balance_cents / 100:.2f}")
    log(f"  Risk per trade: {risk_pct*100:.0f}% = ${balance_cents * risk_pct / 100:.2f}")

    # 2. Daily loss check (realized trading losses only, ignores deposits/withdrawals)
    daily_loss = db.get_today_trading_loss()
    max_daily_loss = int(balance_cents * daily_loss_pct)
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
    prefix_list = list(prefixes) if prefixes else None
    log(f"\n  Scanning all markets...")
    if prefix_list:
        log(f"  Prefixes: {','.join(prefix_list)}")
    log(f"  Min price: {min_price}c  Min 24h vol: {min_volume}")
    results, scan_stats = scan(
        client, min_price=min_price, ticker_prefixes=prefix_list,
        min_volume=min_volume, top_n=500, use_cache=True,
        stop_check=stop_check,
    )
    if scan_stats.get("cached"):
        log(f"  (Using cached market data)")
    # Save scan results for web dashboard
    db.save_scan_results(results, scan_stats)

    total_found = len(results)
    qualified_count = scan_stats.get("qualified", 0)
    log(f"  Found {total_found} markets, {qualified_count} qualified")

    # 5. Filter to qualified markets only
    candidates = [r for r in results if r.get("qualified")]

    if not candidates:
        log(f"\n  No qualified markets found. Nothing to trade.")
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

    # 6b. Skip 99¢ markets (unprofitable after 1¢ fee)
    before_fee_filter = len(available)
    available = [c for c in available if c["signal_price"] < 99]
    fee_filtered = before_fee_filter - len(available)
    if fee_filtered:
        log(f"\n  Skipping {fee_filtered} market{'s' if fee_filtered != 1 else ''} at 99¢ (unprofitable after 1¢ fee)")

    if not available:
        log(f"\n  All remaining markets are at 99¢ (unprofitable after fees). Nothing to trade.")
        log(f"{'='*60}\n")
        return {"scanned": total_found, "skipped": len(held) + fee_filtered, "traded": 0, "orders": 0, "stopped_reason": None}

    # 7. Filter by expiration window
    if max_hours_to_expiration is not None:
        log(f"\n  Expiration filter: within {max_hours_to_expiration}h")
        before_exp = len(available)
        filtered = []
        expired_out = []
        for c in available:
            hrs = c.get("hours_left")
            if hrs is None:
                expired_out.append((c, "unknown"))
            elif hrs <= 0:
                expired_out.append((c, "closed"))
            elif hrs > max_hours_to_expiration:
                expired_out.append((c, f"{hrs:.0f}h"))
            else:
                filtered.append(c)
        if expired_out:
            log(f"  Filtered out {len(expired_out)} market{'s' if len(expired_out) != 1 else ''} beyond {max_hours_to_expiration}h:")
            for c, reason in expired_out[:5]:
                log(f"    {c['ticker']} — {reason}")
            if len(expired_out) > 5:
                log(f"    ... and {len(expired_out) - 5} more")
        available = filtered
        if not available:
            log(f"\n  No qualified markets expiring within {max_hours_to_expiration}h. Nothing to snipe.")
            log(f"  Tip: Use --max-hours-to-expiration to widen the window.")
            log(f"{'='*60}\n")
            return {"scanned": total_found, "skipped": len(held) + before_exp, "traded": 0, "orders": 0, "stopped_reason": "no_expiring"}

    # 8. Rank all qualified: safest tier first, then soonest expiration
    available.sort(key=lambda m: (
        m.get("tier", 3),
        m.get("hours_left") if m.get("hours_left") is not None else 9999,
        -m["signal_price"],
        -m["dollar_24h"],
    ))
    log(f"\n  Ranking {len(available)} qualified targets (T1 → T2 → T3):")
    log(f"  {'#':>3}  {'TICKER':<35} {'SIDE':<4} {'BID':>4} {'ASK':>4} {'24H $':>10} {'SPREAD':>7} {'EXPIRES':>10}")
    log(f"  {'-'*85}")
    for i, m in enumerate(available):
        marker = " >> " if i == 0 else "    "
        hrs = m.get("hours_left")
        exp_str = f"{hrs:.0f}h" if hrs is not None and hrs >= 1 else f"{int((hrs or 0) * 60)}m"
        ask = m.get("signal_ask", 0)
        log(f"  {marker}{i+1:>1}. {m['ticker']:<35} {m['signal_side'].upper():<4} "
            f"{m['signal_price']:>3}c {ask:>3}c ${m['dollar_24h']:>8,} "
            f"{m.get('spread_pct', 0):>6.1f}% {exp_str:>10}")

    summary = {
        "scanned": total_found,
        "skipped": len(held),
        "traded": 0,
        "orders": 0,
        "stopped_reason": None,
        "selected_ticker": None,
    }

    # 9. Iterate through all candidates until one fills
    for idx, selected in enumerate(available):
        if stop_check and stop_check():
            log(f"\n  Stop requested.")
            summary["stopped_reason"] = "stopped"
            break

        ticker = selected["ticker"]
        side = selected["signal_side"]
        bid_price = selected["signal_price"]
        ask_price = selected.get("signal_ask", 0)
        spread = selected.get("spread_pct", 0)
        sel_close = selected.get("close_time_fmt") or format_close_time(selected.get("close_time", ""))

        log(f"\n  TARGET #{idx+1}/{len(available)}: {ticker}")
        log(f"  Side:    {side.upper()}")
        log(f"  Bid:     {bid_price}c")
        log(f"  Ask:     {ask_price}c")
        log(f"  Spread:  {spread:.1f}%")
        log(f"  Expires: {sel_close}")

        # Slippage warning for wide spreads
        if spread >= 3.0:
            log(f"  WARNING: Wide spread ({spread:.1f}%). May execute "
                f"at {ask_price}c vs bid {bid_price}c ({ask_price - bid_price}c slippage).")

        # Use ask price for position sizing estimate
        est_price = ask_price if ask_price > 0 else bid_price

        # 10. Calculate position size
        total_contracts = calculate_position(balance_cents, est_price, risk_pct)
        if total_contracts <= 0:
            log(f"  SKIP: Insufficient balance for even 1 contract at ~{est_price}c")
            continue

        est_cost = total_contracts * est_price
        log(f"  ORDER: {total_contracts} contracts @ 98c limit (est ~{est_price}c each = ~${est_cost / 100:.2f})")

        summary["selected_ticker"] = ticker

        # 11. Execute order
        if dry_run:
            log(f"  DRY RUN — would place {total_contracts} "
                f"{side.upper()} contracts on {ticker} @ 98c limit")
            summary["traded"] = 1
            summary["orders"] += 1
            break

        log(f"  PLACING ORDER: {total_contracts} {side.upper()} on {ticker} @ 98c limit...")
        try:
            result = client.create_order(
                ticker=ticker,
                side=side,
                action="buy",
                count=total_contracts,
                price=98,
            )

            log(f"  API response: {result}")

            order_id = result.get("order_id")
            api_status = result.get("status", "unknown")
            fill_count = result.get("fill_count", 0)
            remaining = result.get("remaining_count", 0)
            taker_fill_cost = result.get("taker_fill_cost", 0)
            taker_fees = result.get("taker_fees", 0)

            log(f"  Order ID:     {order_id}")
            log(f"  API status:   {api_status}")
            log(f"  Filled:       {fill_count}/{total_contracts}")
            log(f"  Remaining:    {remaining}")
            log(f"  Fill cost:    {taker_fill_cost}c (${taker_fill_cost / 100:.2f})")
            log(f"  Taker fees:   {taker_fees}c (${taker_fees / 100:.2f})")

            # Auto-cancel if order is resting
            if api_status == "resting" and remaining > 0 and order_id:
                log(f"  Resting with {remaining} unfilled — cancelling and trying next...")
                try:
                    client.cancel_order(order_id)
                    log(f"  Cancelled resting order {order_id}")
                    api_status = "canceled"
                except Exception as cancel_err:
                    log(f"  Failed to cancel resting order: {cancel_err}")

            status = api_status
            actual_fees = taker_fees if taker_fees > 0 else fill_count * 1
            if fill_count > 0 and taker_fill_cost > 0:
                actual_entry = int(taker_fill_cost / fill_count)
            else:
                actual_entry = est_price
                if fill_count > 0:
                    log(f"  WARNING: taker_fill_cost=0 but fill_count={fill_count}. "
                        f"Using est_price={est_price}c as fallback.")

            db.log_trade(
                ticker=ticker,
                side=side,
                action="buy",
                count=total_contracts,
                price_cents=actual_entry,
                status=status,
                order_id=order_id,
                fill_count=fill_count,
                remaining_count=remaining,
                fee_cents=actual_fees,
            )

            summary["orders"] += 1

            if fill_count > 0:
                db.update_position_on_buy(ticker, side, fill_count, actual_entry)
                summary["traded"] = 1
                log(f"  FILLED: {fill_count} contracts on {ticker}")
                break
            else:
                log(f"  No fill on {ticker} — trying next candidate...")
                continue

        except Exception as e:
            db.log_trade(
                ticker=ticker,
                side=side,
                action="buy",
                count=total_contracts,
                price_cents=est_price,
                status="failed",
                error_message=str(e),
                fee_cents=0,
            )
            log(f"  ORDER FAILED: {e} — trying next candidate...")
            summary["orders"] += 1
            continue

    # 12. Summary
    sel_ticker = summary.get("selected_ticker") or "none"
    log(f"\n{'='*60}")
    log(f"  SUMMARY [{mode}]")
    log(f"  Strategy:    Last-Minute Sniper")
    log(f"  Candidates:  {len(available)}  Orders attempted: {summary['orders']}  Filled: {summary['traded']}")
    if summary["stopped_reason"]:
        log(f"  Stopped: {summary['stopped_reason']}")
    log(f"{'='*60}\n")

    return summary
