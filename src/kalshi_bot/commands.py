import sys
from pathlib import Path

import click

from kalshi_bot.config import load_config
from kalshi_bot.client import create_client
from datetime import datetime, timezone
from kalshi_bot.scanner import scan, format_close_time, _parse_close_time, hours_until_close
from kalshi_bot.sizing import calculate_position
from kalshi_bot.whale import run_whale_strategy
from kalshi_bot import db


def _get_client(config_path):
    cfg = load_config(Path(config_path))
    click.echo(f"Connecting to Kalshi ({cfg['environment']})...")
    return create_client(cfg)


@click.group()
@click.option(
    "--config",
    "config_path",
    default="config.yaml",
    help="Path to config file.",
    show_default=True,
)
@click.pass_context
def cli(ctx, config_path):
    """Kalshi trading bot CLI."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path
    db.init_db()


@cli.command()
@click.pass_context
def balance(ctx):
    """Show account balance."""
    try:
        client = _get_client(ctx.obj["config_path"])
        data = client.get_balance()
        cents = data.get("balance", 0)
        db.log_balance(cents)
        click.echo(f"Balance: ${cents / 100:.2f}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option("--limit", default=20, help="Number of markets to fetch.", show_default=True)
@click.option("--status", default="open", help="Market status filter.", show_default=True)
@click.pass_context
def markets(ctx, limit, status):
    """List markets."""
    try:
        client = _get_client(ctx.obj["config_path"])
        items = client.get_markets(limit=limit, status=status)
        for m in items:
            ticker = m.get("ticker", "?")
            title = m.get("title", "")
            yes_bid = m.get("yes_bid", "—")
            click.echo(f"  {ticker:<30} yes_bid={yes_bid}  {title}")
        click.echo(f"\n({len(items)} markets shown)")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("ticker")
@click.pass_context
def market(ctx, ticker):
    """Show details for a specific market by TICKER."""
    try:
        client = _get_client(ctx.obj["config_path"])
        m = client.get_market(ticker=ticker)
        click.echo(f"Ticker:        {m.get('ticker')}")
        click.echo(f"Title:         {m.get('title')}")
        click.echo(f"Status:        {m.get('status')}")
        click.echo(f"Yes Bid:       {m.get('yes_bid', '—')}")
        click.echo(f"Yes Ask:       {m.get('yes_ask', '—')}")
        click.echo(f"No Bid:        {m.get('no_bid', '—')}")
        click.echo(f"No Ask:        {m.get('no_ask', '—')}")
        click.echo(f"Volume:        {m.get('volume', '—')}")
        click.echo(f"Open Interest: {m.get('open_interest', '—')}")
        click.echo(f"Close Time:    {m.get('close_time', '—')}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("ticker")
@click.option("--side", required=True, type=click.Choice(["yes", "no"]), help="Side to trade.")
@click.option("--action", default="buy", type=click.Choice(["buy", "sell"]), help="Buy or sell.", show_default=True)
@click.option("--count", required=True, type=int, help="Number of contracts.")
@click.option("--price", required=True, type=click.IntRange(1, 99), help="Limit price in cents (1-99).")
@click.option("--yes", "skip_confirm", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def order(ctx, ticker, side, action, count, price, skip_confirm):
    """Place a limit order on TICKER."""
    try:
        client = _get_client(ctx.obj["config_path"])

        cost_cents = price * count
        click.echo(f"\nOrder Summary:")
        click.echo(f"  Ticker:    {ticker}")
        click.echo(f"  Action:    {action.upper()} {side.upper()}")
        click.echo(f"  Contracts: {count}")
        click.echo(f"  Price:     {price}c per contract")
        click.echo(f"  Max cost:  ${cost_cents / 100:.2f}")

        if not skip_confirm:
            click.confirm("\nPlace this order?", abort=True)

        result = client.create_order(
            ticker=ticker,
            side=side,
            action=action,
            count=count,
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

        # Log to database
        db.log_trade(
            ticker=ticker,
            side=side,
            action=action,
            count=count,
            price_cents=price,
            status=status,
            order_id=order_id,
            fill_count=fill_count,
            remaining_count=remaining,
        )

        # Update position tracking
        if fill_count > 0:
            if action == "buy":
                db.update_position_on_buy(ticker, side, fill_count, price)
            else:
                pnl = db.update_position_on_sell(ticker, side, fill_count, price)
                if pnl != 0:
                    click.echo(f"  Realized PnL: ${pnl / 100:+.2f}")

        click.echo(f"\nOrder placed!")
        click.echo(f"  Order ID:   {order_id}")
        click.echo(f"  Status:     {status}")
        click.echo(f"  Filled:     {fill_count}/{count}")
        click.echo(f"  Remaining:  {remaining}")

    except click.Abort:
        click.echo("Order cancelled.")
    except Exception as e:
        # Log failed order
        db.log_trade(
            ticker=ticker,
            side=side,
            action=action,
            count=count,
            price_cents=price,
            status="failed",
            error_message=str(e),
        )
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


_SORT_COLUMNS = ("tier", "price", "volume", "spread", "rank", "expiration", "open_int")

_SORT_LABELS = {
    "tier": "Tier (best first)",
    "price": "Price (highest first)",
    "volume": "24h $ Volume (highest first)",
    "spread": "Spread (tightest first)",
    "rank": "Dollar Rank (top first)",
    "expiration": "Expiration (soonest first)",
    "open_int": "Open Interest (highest first)",
}

_FAR_FUTURE = datetime(9999, 1, 1, tzinfo=timezone.utc)


def _sort_key(column, reverse=False):
    """Return a sort key function for the given column name.

    Each key returns a tuple so ties are broken consistently.
    The `reverse` param is handled by the caller via list.sort(reverse=).
    """
    def _exp_dt(m):
        return _parse_close_time(m.get("close_time", "")) or _FAR_FUTURE

    keys = {
        "tier": lambda m: (m.get("tier", 3), -m["signal_price"], -m.get("dollar_24h", 0)),
        "price": lambda m: (-m["signal_price"], -m.get("dollar_24h", 0), m.get("spread_pct", 99)),
        "volume": lambda m: (-m.get("dollar_24h", 0), -m["signal_price"]),
        "spread": lambda m: (m.get("spread_pct", 99), -m["signal_price"]),
        "rank": lambda m: (m.get("dollar_rank", 999), -m["signal_price"]),
        "expiration": lambda m: (_exp_dt(m), -m["signal_price"]),
        "open_int": lambda m: (-m.get("open_interest", 0), -m["signal_price"]),
    }
    return keys[column]


@cli.command("scan")
@click.option("--min-price", default=99, type=click.IntRange(1, 99), help="Minimum bid price in cents.", show_default=True)
@click.option("--min-volume", default=100, type=int, help="Minimum 24h volume.", show_default=True)
@click.option("--prefixes", default=None, help="Comma-separated event ticker prefixes (e.g. 'KXNFL,KXNBA,KXBTC,KXETH').")
@click.option("--show-sizing", is_flag=True, help="Show position sizing based on current balance.")
@click.option("--qualified-only", is_flag=True, help="Only show qualified markets (Tier 1 + top 20 $vol + $50k+ + <5% spread).")
@click.option("--sort-by", "sort_by", default=None, type=click.Choice(_SORT_COLUMNS, case_sensitive=False),
              help="Sort column: tier, price, volume, spread, rank, expiration, open_int.")
@click.option("--reverse", "reverse_sort", is_flag=True, help="Reverse the sort order.")
@click.pass_context
def scan_cmd(ctx, min_price, min_volume, prefixes, show_sizing, qualified_only, sort_by, reverse_sort):
    """Scan for high-probability markets.

    \b
    Sort examples:
      --sort-by expiration          Soonest closing first
      --sort-by volume --reverse    Lowest volume first
      --sort-by price               Highest price first
    """
    try:
        client = _get_client(ctx.obj["config_path"])

        prefix_list = [p.strip() for p in prefixes.split(",")] if prefixes else None

        click.echo(f"Scanning all open markets (min_price={min_price}c, min_24h_vol={min_volume})...")
        results, scan_stats = scan(client, min_price=min_price, ticker_prefixes=prefix_list, min_volume=min_volume, top_n=5000)

        # Save to DB so the web dashboard can display them
        db.save_scan_results(results, scan_stats)
        click.echo(f"Saved {len(results)} results to database for web dashboard.")

        qualified = [r for r in results if r.get("qualified")]
        non_qualified = [r for r in results if not r.get("qualified")]
        click.echo(f"Qualified (Tier 1 + Top 20 + $50k+ + <5% spread): {len(qualified)}")

        # Determine sort column
        if sort_by:
            active_sort = sort_by
        elif qualified_only:
            active_sort = "price"  # qualified: price -> volume -> spread
        else:
            active_sort = "tier"   # all markets: tier -> price -> volume

        key_fn = _sort_key(active_sort)
        qualified.sort(key=key_fn, reverse=reverse_sort)
        non_qualified.sort(key=key_fn, reverse=reverse_sort)
        results = qualified + non_qualified

        sort_label = _SORT_LABELS[active_sort]
        if reverse_sort:
            sort_label += " (reversed)"

        if qualified_only:
            display = qualified
            click.echo(f"Showing only qualified markets")
        else:
            display = results

        if not display:
            click.echo("No markets found matching criteria.")
            return

        balance_cents = None
        if show_sizing:
            data = client.get_balance()
            balance_cents = data.get("balance", 0)
            click.echo(f"Balance: ${balance_cents / 100:.2f} (1% risk = ${balance_cents * 0.01 / 100:.2f})\n")

        def _print_table(markets, header=None):
            if header:
                click.echo(f"\n  {header}")
                click.echo(f"  {'='*len(header)}")
            click.echo(f"  Sorted by: {sort_label}")
            click.echo(f"{'':>3} {'TICKER':<38} {'SIDE':<5} {'PRICE':>5} {'24H $':>8} {'RANK':>5} {'SPREAD':>7} {'24H VOL':>8} {'OI':>8} {'TIME LEFT':>10} {'CLOSES':>20} {'EVENT':>15} ", nl=False)
            if show_sizing:
                click.echo(f"{'CONTRACTS':>10}", nl=False)
            click.echo()
            click.echo("-" * (140 + (10 if show_sizing else 0)))

            for m in markets:
                ticker = m.get("ticker", "?")
                side = m["signal_side"]
                price = m["signal_price"]
                vol_24h = m.get("volume_24h", 0)
                dollar_24h = m.get("dollar_24h", 0)
                dollar_rank = m.get("dollar_rank", 0)
                spread_pct = m.get("spread_pct", 0)
                oi = m.get("open_interest", 0)
                event = m.get("event_ticker", "—")
                is_qualified = m.get("qualified", False)
                if len(event) > 15:
                    event = event[:14] + "~"

                close_fmt = m.get("close_time_fmt") or format_close_time(m.get("close_time", ""))
                hrs = m.get("hours_left")
                if hrs is None:
                    time_left_str = "—"
                elif hrs <= 0:
                    time_left_str = "closed"
                elif hrs < 1:
                    time_left_str = f"{int(hrs * 60)}m"
                elif hrs < 48:
                    time_left_str = f"{hrs:.0f}h"
                else:
                    time_left_str = f"{hrs / 24:.1f}d"
                badge = " * " if is_qualified else "   "
                rank_str = f"#{dollar_rank}"
                spread_str = f"{spread_pct:.1f}%"

                click.echo(f"{badge}{ticker:<38} {side.upper():<5} {price:>4}c ${dollar_24h:>7,} {rank_str:>5} {spread_str:>7} {vol_24h:>8} {oi:>8} {time_left_str:>10} {close_fmt:>20} {event:>15} ", nl=False)

                if show_sizing and balance_cents:
                    contracts = calculate_position(balance_cents, price)
                    click.echo(f"{contracts:>10}", nl=False)

                click.echo()

        if qualified_only:
            _print_table(display, f"QUALIFIED MARKETS ({len(display)})")
        else:
            _print_table(qualified, f"QUALIFIED FOR WHALE-TRADE ({len(qualified)})")
            if non_qualified:
                _print_table(non_qualified, f"OTHER MARKETS ({len(non_qualified)})")

        click.echo(f"\n({len(display)} markets shown, * = QUALIFIED for whale-trade)")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def positions(ctx):
    """Show open positions."""
    try:
        client = _get_client(ctx.obj["config_path"])
        items = client.get_positions()

        if not items:
            click.echo("No open positions.")
            return

        click.echo(f"{'TICKER':<40} {'SIDE':<5} {'QTY':>5} {'AVG PRICE':>10} {'MARKET PRICE':>13} {'VALUE':>8}")
        click.echo("-" * 85)

        for p in items:
            ticker = p.get("ticker", "?")
            yes_count = p.get("position", 0)
            if yes_count > 0:
                side = "YES"
                qty = yes_count
            elif yes_count < 0:
                side = "NO"
                qty = abs(yes_count)
            else:
                continue

            avg_price = p.get("average_price_paid", 0)
            market_price = p.get("market_price", 0)
            value = qty * market_price

            click.echo(
                f"  {ticker:<38} {side:<5} {qty:>5} "
                f"{avg_price:>9}c {market_price:>12}c {value / 100:>7.2f}"
            )

        click.echo(f"\n({len([p for p in items if p.get('position', 0) != 0])} positions)")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Database reporting commands
# ---------------------------------------------------------------------------

@cli.command("trade-history")
@click.option("--limit", default=50, help="Number of trades to show.", show_default=True)
@click.option("--ticker", default=None, help="Filter by ticker.")
@click.pass_context
def trade_history(ctx, limit, ticker):
    """Show past trades from the local database."""
    trades = db.get_trade_history(limit=limit, ticker=ticker)

    if not trades:
        click.echo("No trades recorded yet.")
        return

    click.echo(f"{'ID':>4} {'TIME':<20} {'TICKER':<35} {'ACTION':<10} {'QTY':>4} {'PRICE':>5} {'FILLS':>5} {'STATUS':<10}")
    click.echo("-" * 100)

    for t in trades:
        action_str = f"{t['action'].upper()} {t['side'].upper()}"
        click.echo(
            f"  {t['id']:>2} {t['created_at']:<20} {t['ticker']:<35} "
            f"{action_str:<10} {t['count']:>4} {t['price_cents']:>4}c "
            f"{t['fill_count']:>5} {t['status']:<10}"
        )
        if t.get("error_message"):
            click.echo(f"      Error: {t['error_message']}")

    click.echo(f"\n({len(trades)} trades shown)")


@cli.command()
@click.pass_context
def pnl(ctx):
    """Show profit and loss summary."""
    try:
        client = _get_client(ctx.obj["config_path"])

        # Current balance
        bal_data = client.get_balance()
        balance_cents = bal_data.get("balance", 0)
        db.log_balance(balance_cents)

        click.echo(f"Account Balance: ${balance_cents / 100:.2f}\n")

        # Open positions with unrealized P&L
        open_pos = db.get_open_positions()
        total_unrealized = 0

        if open_pos:
            click.echo(f"{'TICKER':<35} {'SIDE':<5} {'QTY':>5} {'ENTRY':>6} {'CURRENT':>8} {'UNREAL P&L':>11}")
            click.echo("-" * 75)

            for p in open_pos:
                ticker = p["ticker"]
                try:
                    m = client.get_market(ticker=ticker)
                    if p["side"] == "yes":
                        current = m.get("yes_bid", 0) or 0
                    else:
                        current = m.get("no_bid", 0) or 0
                except Exception:
                    current = 0

                entry = p["avg_entry_price_cents"]
                qty = p["quantity"]
                unrealized = int(qty * (current - entry))
                total_unrealized += unrealized

                click.echo(
                    f"  {ticker:<33} {p['side'].upper():<5} {qty:>5} "
                    f"{entry:>5.0f}c {current:>7}c ${unrealized / 100:>+9.2f}"
                )

            click.echo(f"\n  Unrealized P&L: ${total_unrealized / 100:+.2f}")
        else:
            click.echo("No open positions tracked.")

        # Realized P&L from closed positions
        closed = db.get_all_positions()
        realized = sum(p["realized_pnl_cents"] for p in closed)

        click.echo(f"\n  Realized P&L:   ${realized / 100:+.2f}")
        click.echo(f"  Total P&L:      ${(realized + total_unrealized) / 100:+.2f}")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def stats(ctx):
    """Show trading statistics."""
    s = db.get_stats()

    click.echo("Trading Statistics")
    click.echo("=" * 35)
    click.echo(f"  Total orders:      {s['total_orders']}")
    click.echo(f"  Filled orders:     {s['filled_orders']}")
    click.echo(f"  Failed orders:     {s['failed_orders']}")
    click.echo()
    click.echo(f"  Closed positions:  {s['closed_positions']}")
    click.echo(f"    Wins:            {s['wins']}")
    click.echo(f"    Losses:          {s['losses']}")
    click.echo(f"    Breakeven:       {s['breakeven']}")
    click.echo(f"    Win rate:        {s['win_rate']:.1f}%")
    click.echo()
    click.echo(f"  Realized P&L:      ${s['realized_pnl_cents'] / 100:+.2f}")
    click.echo(f"    Gross profit:    ${s['gross_profit_cents'] / 100:+.2f}")
    click.echo(f"    Gross loss:      ${s['gross_loss_cents'] / 100:-.2f}")
    pf = s['profit_factor']
    pf_str = f"{pf:.2f}" if pf != float("inf") else "∞"
    click.echo(f"    Profit factor:   {pf_str}")


@cli.command("whale-trade")
@click.option("--prefixes", default="KXNFL,KXNBA,KXBTC,KXETH", help="Comma-separated event ticker prefixes.", show_default=True)
@click.option("--min-price", default=95, type=click.IntRange(1, 99), help="Minimum bid price in cents for scanner.", show_default=True)
@click.option("--min-volume", default=1000, type=int, help="Minimum 24h volume for scanner.", show_default=True)
@click.option("--max-positions", default=10, type=int, help="Max concurrent positions.", show_default=True)
@click.option("--max-days-to-expiration", default=None, type=float, help="Only trade markets expiring within N days.")
@click.option("--max-hours-to-expiration", default=None, type=float, help="Only trade markets expiring within N hours (overrides --max-days).")
@click.option("--dry-run/--live", default=True, help="Simulate without placing real orders.", show_default=True)
@click.option("--tier1-only/--all-tiers", default=True, help="Only trade qualified Tier 1 markets.", show_default=True)
@click.option("--yes", "skip_confirm", is_flag=True, help="Skip confirmation prompt for live mode.")
@click.pass_context
def whale_trade(ctx, prefixes, min_price, min_volume, max_positions,
                max_days_to_expiration, max_hours_to_expiration,
                dry_run, tier1_only, skip_confirm):
    """Run fully autonomous whale trading strategy.

    Scans all markets, filters to qualified opportunities, ranks them,
    picks the best one, and places the trade automatically.

    Qualified = Tier 1 (>=98c) + top 20 by $vol + >=$50k daily + <5% spread.

    \b
    Expiration filters (faster capital turnover):
      --max-days-to-expiration 7     Only markets closing within 7 days
      --max-hours-to-expiration 48   Only markets closing within 48 hours

    Default is dry-run mode. Use --live to place real orders.
    """
    try:
        if not dry_run and not skip_confirm:
            click.confirm(
                "LIVE MODE: This will place real orders with real money. Continue?",
                abort=True,
            )

        # --max-hours takes priority; convert --max-days to hours
        max_hours = max_hours_to_expiration
        if max_hours is None and max_days_to_expiration is not None:
            max_hours = max_days_to_expiration * 24

        client = _get_client(ctx.obj["config_path"])
        prefix_list = tuple(p.strip() for p in prefixes.split(","))

        run_whale_strategy(
            client,
            prefixes=prefix_list,
            min_price=min_price,
            min_volume=min_volume,
            max_positions=max_positions,
            dry_run=dry_run,
            tier1_only=tier1_only,
            max_hours_to_expiration=max_hours,
            log=click.echo,
        )
    except click.Abort:
        click.echo("Aborted.")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
