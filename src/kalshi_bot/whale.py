import time

from kalshi_bot import db
from kalshi_bot.ai import detect_category
from kalshi_bot.scanner import scan, format_close_time, hours_until_close, StopRequested
from kalshi_bot.sizing import calculate_position


def _check_price_velocity(client, ticker, event_ticker, side, current_ask,
                          window_sec=19, max_move_pct=10.0, log=print):
    """Return True if the price spiked too fast, indicating possible manipulation.

    Fetches the last 3 minutes of 1-minute candles and compares the price
    from ~window_sec ago to the current live ask.  If the ask-side price
    rose more than max_move_pct in that window, the trade should be skipped.

    Uses the *open* of the most recent candle (≈ price at the start of the
    current minute) as a proxy for the price ~19-60 seconds ago.  If a
    prior candle's close is available and more recent, that is preferred.
    """
    try:
        now = int(time.time())
        start = now - 180  # 3 minutes back
        candles = client.get_market_candlesticks(
            ticker=ticker,
            series_ticker=event_ticker,
            start_ts=start,
            end_ts=now,
            period_interval=1,  # 1-minute candles (finest available)
        )
        if not candles:
            return False  # no data — allow trade

        # Sort by timestamp ascending
        candles.sort(key=lambda c: c.get("end_period_ts", 0))

        # Determine which price field to use based on our side
        ask_key = f"{side}_ask"

        # Get the reference price: the open of the most recent candle or
        # close of the prior candle — whichever is farther back in time
        ref_price = None

        if len(candles) >= 2:
            # Prior candle's close is ~1-2 minutes old — good reference
            prior = candles[-2]
            ask_d = prior.get(ask_key) or {}
            ref_price = ask_d.get("close") or ask_d.get("open")

        if ref_price is None and candles:
            # Fall back to the current candle's open
            latest = candles[-1]
            ask_d = latest.get(ask_key) or {}
            ref_price = ask_d.get("open")

        if not ref_price or ref_price <= 0:
            return False  # no usable reference price

        # Calculate percentage move
        move_pct = ((current_ask - ref_price) / ref_price) * 100

        if move_pct > max_move_pct:
            log(f"[VELOCITY] {ticker} — price spiked {ref_price}c → {current_ask}c "
                f"({move_pct:+.1f}%) in last ~{window_sec}s, skipping (limit {max_move_pct}%)")
            return True

        return False

    except Exception as e:
        log(f"[WARN] Price velocity check failed for {ticker}: {e}")
        return False  # fail open — don't block trades on API errors


def run_whale_strategy(
    client,
    prefixes=("KXNFL", "KXNBA", "KXBTC", "KXETH"),
    min_price=95,
    min_volume=10000,
    risk_pct=0.01,
    max_positions=10,
    daily_loss_pct=0.05,
    dry_run=True,
    max_hours_to_expiration=2.0,
    log=print,
    stop_check=None,
    with_ai=True,
    min_confidence=75,
    exclude_categories=None,
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
    ai_tag = " + AI" if with_ai else ""
    log(f"[HEAD] Sniper [{mode}{ai_tag}] — limit 98c, exp {max_hours_to_expiration or '∞'}h")

    # 1. Fetch balance
    bal_data = client.get_balance()
    balance_cents = bal_data.get("balance", 0)
    db.log_balance(balance_cents)

    # 2. Daily loss check
    daily_loss = db.get_today_trading_loss()
    max_daily_loss = int(balance_cents * daily_loss_pct)
    log(f"[INFO] Balance ${balance_cents / 100:.2f} | Risk {risk_pct*100:.0f}% = ${balance_cents * risk_pct / 100:.2f} | Loss ${daily_loss / 100:.2f}/${max_daily_loss / 100:.2f}")

    if daily_loss >= max_daily_loss:
        log(f"[FAIL] Daily loss limit reached ({daily_loss_pct*100:.0f}%)")
        return {"scanned": 0, "skipped": 0, "traded": 0, "orders": 0, "stopped_reason": "daily_loss"}

    # 3. Position count check
    open_count = db.count_open_positions()
    if open_count >= max_positions:
        log(f"[WARN] Max positions reached ({open_count}/{max_positions})")
        return {"scanned": 0, "skipped": 0, "traded": 0, "orders": 0, "stopped_reason": "max_positions"}

    # 4. Scan markets
    prefix_list = list(prefixes) if prefixes else None
    log(f"[INFO] Scanning markets...")
    results, scan_stats = scan(
        client, min_price=min_price, ticker_prefixes=prefix_list,
        min_volume=min_volume, top_n=500, use_cache=True,
        stop_check=stop_check, exclude_categories=exclude_categories,
    )
    if scan_stats.get("cached"):
        log(f"[INFO] (cached data)")
    db.save_scan_results(results, scan_stats)

    total_found = len(results)
    qualified_count = scan_stats.get("qualified", 0)
    log(f"[INFO] Found {total_found} markets, {qualified_count} qualified")

    # 5. Filter to qualified markets only
    candidates = [r for r in results if r.get("qualified")]

    if not candidates:
        log(f"[WARN] No qualified markets found")
        return {"scanned": total_found, "skipped": 0, "traded": 0, "orders": 0, "stopped_reason": None}

    # 5b. Exclude categories (e.g. crypto)
    if exclude_categories:
        before = len(candidates)
        candidates = [
            c for c in candidates
            if detect_category(c.get("event_ticker", "")) not in exclude_categories
        ]
        excluded = before - len(candidates)
        if excluded:
            log(f"[INFO] Excluded {excluded} {', '.join(exclude_categories)} market{'s' if excluded != 1 else ''}")

    # 6. Remove already-held, 99c, and out-of-window markets
    existing_tickers = db.get_position_tickers()
    available = [c for c in candidates if c["ticker"] not in existing_tickers]
    held = len(candidates) - len(available)
    if held:
        log(f"[INFO] Skipping {held} already-held position{'s' if held != 1 else ''}")

    available = [c for c in available if c.get("signal_ask", 100) <= 98]

    if max_hours_to_expiration is not None:
        def _within_expiry(c):
            hrs = c.get("hours_left")
            if hrs is None or hrs <= 0:
                return False
            # Tighter spread allows longer expiration window
            limit = 10.0 if c.get("spread_pct", 99) <= 2.5 else max_hours_to_expiration
            return hrs <= limit
        available = [c for c in available if _within_expiry(c)]

    if not available:
        log(f"[WARN] No tradeable markets after filters (held={held})")
        return {"scanned": total_found, "skipped": held, "traded": 0, "orders": 0, "stopped_reason": None}

    # 7. Rank: safest tier first, soonest expiration
    available.sort(key=lambda m: (
        m.get("tier", 3),
        m.get("hours_left") if m.get("hours_left") is not None else 9999,
        -m["signal_price"],
        -m["dollar_24h"],
    ))
    log(f"[INFO] {len(available)} targets ranked (T1→T2→T3)")

    summary = {
        "scanned": total_found,
        "skipped": held,
        "traded": 0,
        "orders": 0,
        "stopped_reason": None,
        "selected_ticker": None,
    }

    # 8. Iterate through all candidates until one fills
    for idx, selected in enumerate(available):
        if stop_check and stop_check():
            summary["stopped_reason"] = "stopped"
            break

        ticker = selected["ticker"]
        side = selected["signal_side"]
        bid_price = selected["signal_price"]
        ask_price = selected.get("signal_ask", 0)
        spread = selected.get("spread_pct", 0)
        est_price = ask_price if ask_price > 0 else bid_price
        total_contracts = calculate_position(balance_cents, est_price, risk_pct)

        if total_contracts <= 0:
            log(f"[SKIP] #{idx+1}/{len(available)} {ticker} {side.upper()} {bid_price}c/{ask_price}c — insufficient balance")
            continue

        # Re-check current ask before ordering
        try:
            live = client.get_market(ticker=ticker)
            live_ask = live.get(f"{side}_ask", 0) or 0
            live_bid = live.get(f"{side}_bid", 0) or 0
            if live_ask < min_price or live_ask > 98:
                log(f"[SKIP] #{idx+1}/{len(available)} {ticker} {side.upper()} — live ask {live_ask}c outside {min_price}-98c range")
                continue
            # Update prices to live values
            ask_price = live_ask
            bid_price = live_bid
            est_price = ask_price
            total_contracts = calculate_position(balance_cents, est_price, risk_pct)
            if total_contracts <= 0:
                log(f"[SKIP] #{idx+1}/{len(available)} {ticker} {side.upper()} — insufficient balance at live ask {ask_price}c")
                continue
        except Exception as e:
            log(f"[WARN] #{idx+1}/{len(available)} {ticker} — failed to re-check price: {e}")

        # Price velocity filter — reject markets with suspicious rapid spikes
        if _check_price_velocity(client, ticker, selected.get("event_ticker", ""),
                                 side, ask_price, log=log):
            continue

        spread_warn = f" (spread {spread:.1f}%)" if spread >= 3.0 else ""
        prefix = f"#{idx+1}/{len(available)}"

        summary["selected_ticker"] = ticker

        # Directional filter for crypto price markets
        event_prefix = selected.get("event_ticker", "").upper()
        if any(event_prefix.startswith(p) for p in ("KXBTC", "KXETH")):
            from kalshi_bot.ticker import extract_strike_price
            from kalshi_bot.ai import fetch_crypto_context
            strike = extract_strike_price(ticker)
            if strike is not None:
                crypto = fetch_crypto_context()
                asset_key = "btc_usd" if "BTC" in event_prefix else "eth_usd"
                asset_name = "BTC" if "BTC" in event_prefix else "ETH"
                spot = crypto.get(asset_key)
                if spot is not None:
                    buffer = 0.005  # 0.5%
                    if side == "no" and spot > strike * (1 + buffer):
                        log(f"[SKIP] {prefix} {ticker} — contrarian NO: {asset_name} ${spot:,.0f} above strike ${strike:,.0f}")
                        continue
                    if side == "yes" and spot < strike * (1 - buffer):
                        log(f"[SKIP] {prefix} {ticker} — contrarian YES: {asset_name} ${spot:,.0f} below strike ${strike:,.0f}")
                        continue

        # AI analysis gate
        if with_ai:
            from kalshi_bot.ai import analyze_market
            ai_result = analyze_market(selected, log=log)

            ai_side = (ai_result.get("expected_outcome") or "").upper()
            our_side = side.upper()
            ai_confidence = ai_result.get("confidence", 0)

            # Check AI agrees with our side
            if ai_side not in ("UNKNOWN", "") and ai_side != our_side:
                log(f"[AI] SKIP {prefix} {ticker} — AI says {ai_side}, we want {our_side}")
                continue

            # Check confidence threshold
            if ai_confidence > 0 and ai_confidence < min_confidence:
                log(f"[AI] SKIP {prefix} {ticker} — confidence {ai_confidence}% < {min_confidence}%")
                continue

            # Check AI explicit recommendation
            if not ai_result.get("should_trade", True):
                log(f"[AI] SKIP {prefix} {ticker} — AI recommends skip")
                continue

            if ai_confidence > 0:
                log(f"[AI] PASS {prefix} {ticker} — confidence {ai_confidence}%, approved")

        limit_price = ask_price if ask_price > 0 else 98

        if dry_run:
            log(f"[FILL] {prefix} {ticker} {side.upper()} {bid_price}c/{ask_price}c — DRY RUN {total_contracts}x @ {limit_price}c{spread_warn}")
            summary["traded"] = 1
            summary["orders"] += 1
            break

        # Place order at the current ask price
        try:
            result = client.create_order(
                ticker=ticker, side=side, action="buy",
                count=total_contracts, price=limit_price,
            )

            order_id = result.get("order_id")
            api_status = result.get("status", "unknown")
            fill_count = result.get("fill_count", 0)
            remaining = result.get("remaining_count", 0)
            taker_fill_cost = result.get("taker_fill_cost", 0)
            taker_fees = result.get("taker_fees", 0)

            # Auto-cancel resting orders
            if api_status == "resting" and remaining > 0 and order_id:
                try:
                    client.cancel_order(order_id)
                    api_status = "canceled"
                except Exception:
                    pass

            status = api_status
            actual_fees = taker_fees if taker_fees > 0 else fill_count * 1
            if fill_count > 0 and taker_fill_cost > 0:
                actual_entry = int(taker_fill_cost / fill_count)
            else:
                actual_entry = est_price

            db.log_trade(
                ticker=ticker, side=side, action="buy",
                count=total_contracts, price_cents=actual_entry,
                status=status, order_id=order_id,
                fill_count=fill_count, remaining_count=remaining,
                fee_cents=actual_fees,
            )

            summary["orders"] += 1

            if fill_count > 0:
                db.update_position_on_buy(ticker, side, fill_count, actual_entry)
                summary["traded"] = 1
                log(f"[FILL] {prefix} {ticker} {side.upper()} {bid_price}c/{ask_price}c — FILLED {fill_count}x @ {actual_entry}c{spread_warn}")
                break
            else:
                log(f"[SKIP] {prefix} {ticker} {side.upper()} {bid_price}c/{ask_price}c — no fill, cancelled{spread_warn}")
                continue

        except Exception as e:
            db.log_trade(
                ticker=ticker, side=side, action="buy",
                count=total_contracts, price_cents=est_price,
                status="failed", error_message=str(e), fee_cents=0,
            )
            log(f"[FAIL] {prefix} {ticker} — {e}")
            summary["orders"] += 1
            continue

    # 9. Summary
    log(f"[HEAD] Scan complete — {len(available)} candidates, {summary['orders']} orders, {summary['traded']} filled")

    return summary
