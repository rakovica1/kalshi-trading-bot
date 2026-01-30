import csv
import io
import os
import sqlite3
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Database backend selection
# ---------------------------------------------------------------------------
# If DATABASE_URL is set (e.g. on Railway), use PostgreSQL.
# Otherwise fall back to local SQLite.

DATABASE_URL = os.environ.get("DATABASE_URL")
_use_pg = bool(DATABASE_URL)

DEFAULT_DB_PATH = Path(os.environ.get("KALSHI_DB_PATH", "kalshi_bot.db"))
_today = lambda: date.today().isoformat()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    count INTEGER NOT NULL,
    price_cents INTEGER NOT NULL,
    fee_cents INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    fill_count INTEGER DEFAULT 0,
    remaining_count INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    avg_entry_price_cents REAL NOT NULL DEFAULT 0,
    total_cost_cents INTEGER NOT NULL DEFAULT 0,
    realized_pnl_cents INTEGER NOT NULL DEFAULT 0,
    is_closed INTEGER NOT NULL DEFAULT 0,
    opened_at TEXT NOT NULL DEFAULT (datetime('now')),
    closed_at TEXT
);

CREATE TABLE IF NOT EXISTS balance_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    balance_cents INTEGER NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS daily_pnl (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    starting_balance_cents INTEGER,
    ending_balance_cents INTEGER,
    realized_pnl_cents INTEGER DEFAULT 0,
    trades_count INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS scan_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    event_ticker TEXT,
    signal_side TEXT NOT NULL,
    signal_price INTEGER NOT NULL,
    signal_ask INTEGER NOT NULL DEFAULT 0,
    tier INTEGER NOT NULL DEFAULT 3,
    volume_24h INTEGER NOT NULL DEFAULT 0,
    dollar_24h INTEGER NOT NULL DEFAULT 0,
    volume INTEGER NOT NULL DEFAULT 0,
    open_interest INTEGER NOT NULL DEFAULT 0,
    spread_pct REAL NOT NULL DEFAULT 0,
    dollar_rank INTEGER NOT NULL DEFAULT 0,
    qualified INTEGER NOT NULL DEFAULT 0,
    close_time TEXT NOT NULL DEFAULT '',
    scanned_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS scan_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_fetched INTEGER NOT NULL DEFAULT 0,
    top_n INTEGER NOT NULL DEFAULT 0,
    scanned INTEGER NOT NULL DEFAULT 0,
    passed_prefix INTEGER NOT NULL DEFAULT 0,
    passed_volume INTEGER NOT NULL DEFAULT 0,
    passed_price INTEGER NOT NULL DEFAULT 0,
    count_tier1 INTEGER NOT NULL DEFAULT 0,
    count_top20 INTEGER NOT NULL DEFAULT 0,
    count_dollar_vol INTEGER NOT NULL DEFAULT 0,
    count_spread INTEGER NOT NULL DEFAULT 0,
    count_expires INTEGER NOT NULL DEFAULT 0,
    qualified INTEGER NOT NULL DEFAULT 0,
    min_price INTEGER NOT NULL DEFAULT 0,
    min_volume INTEGER NOT NULL DEFAULT 0,
    prefixes TEXT NOT NULL DEFAULT '',
    scanned_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS price_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    bid_cents INTEGER NOT NULL DEFAULT 0,
    ask_cents INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS deposits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    amount_cents INTEGER NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS withdrawals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    amount_cents INTEGER NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

_PG_TABLES = [
    """CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        order_id TEXT,
        ticker TEXT NOT NULL,
        side TEXT NOT NULL,
        action TEXT NOT NULL,
        count INTEGER NOT NULL,
        price_cents INTEGER NOT NULL,
        fee_cents INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        fill_count INTEGER DEFAULT 0,
        remaining_count INTEGER DEFAULT 0,
        error_message TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS positions (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        side TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 0,
        avg_entry_price_cents REAL NOT NULL DEFAULT 0,
        total_cost_cents INTEGER NOT NULL DEFAULT 0,
        realized_pnl_cents INTEGER NOT NULL DEFAULT 0,
        is_closed INTEGER NOT NULL DEFAULT 0,
        opened_at TIMESTAMP NOT NULL DEFAULT NOW(),
        closed_at TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS balance_history (
        id SERIAL PRIMARY KEY,
        balance_cents INTEGER NOT NULL,
        recorded_at TIMESTAMP NOT NULL DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS daily_pnl (
        id SERIAL PRIMARY KEY,
        date TEXT NOT NULL UNIQUE,
        starting_balance_cents INTEGER,
        ending_balance_cents INTEGER,
        realized_pnl_cents INTEGER DEFAULT 0,
        trades_count INTEGER DEFAULT 0,
        wins INTEGER DEFAULT 0,
        losses INTEGER DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS scan_results (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        event_ticker TEXT,
        signal_side TEXT NOT NULL,
        signal_price INTEGER NOT NULL,
        signal_ask INTEGER NOT NULL DEFAULT 0,
        tier INTEGER NOT NULL DEFAULT 3,
        volume_24h INTEGER NOT NULL DEFAULT 0,
        dollar_24h INTEGER NOT NULL DEFAULT 0,
        volume INTEGER NOT NULL DEFAULT 0,
        open_interest INTEGER NOT NULL DEFAULT 0,
        spread_pct REAL NOT NULL DEFAULT 0,
        dollar_rank INTEGER NOT NULL DEFAULT 0,
        qualified INTEGER NOT NULL DEFAULT 0,
        close_time TEXT NOT NULL DEFAULT '',
        scanned_at TIMESTAMP NOT NULL DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS scan_meta (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        total_fetched INTEGER NOT NULL DEFAULT 0,
        top_n INTEGER NOT NULL DEFAULT 0,
        scanned INTEGER NOT NULL DEFAULT 0,
        passed_prefix INTEGER NOT NULL DEFAULT 0,
        passed_volume INTEGER NOT NULL DEFAULT 0,
        passed_price INTEGER NOT NULL DEFAULT 0,
        count_tier1 INTEGER NOT NULL DEFAULT 0,
        count_top20 INTEGER NOT NULL DEFAULT 0,
        count_dollar_vol INTEGER NOT NULL DEFAULT 0,
        count_spread INTEGER NOT NULL DEFAULT 0,
        count_expires INTEGER NOT NULL DEFAULT 0,
        qualified INTEGER NOT NULL DEFAULT 0,
        min_price INTEGER NOT NULL DEFAULT 0,
        min_volume INTEGER NOT NULL DEFAULT 0,
        prefixes TEXT NOT NULL DEFAULT '',
        scanned_at TIMESTAMP NOT NULL DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS price_snapshots (
        id SERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        side TEXT NOT NULL,
        bid_cents INTEGER NOT NULL DEFAULT 0,
        ask_cents INTEGER NOT NULL DEFAULT 0,
        recorded_at TIMESTAMP NOT NULL DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS deposits (
        id SERIAL PRIMARY KEY,
        amount_cents INTEGER NOT NULL,
        notes TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS withdrawals (
        id SERIAL PRIMARY KEY,
        amount_cents INTEGER NOT NULL,
        notes TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )""",
]


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _q(sql):
    """Translate ? placeholders to %s for PostgreSQL."""
    if _use_pg:
        return sql.replace("?", "%s")
    return sql


def _now_sql():
    """Return the SQL expression for current timestamp."""
    return "NOW()" if _use_pg else "datetime('now')"


def _connect(db_path=DEFAULT_DB_PATH):
    if _use_pg:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        return conn
    else:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn


def _execute(conn, sql, params=None):
    """Execute a query with automatic placeholder translation."""
    if _use_pg:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        cur = conn.cursor()
    cur.execute(_q(sql), params or ())
    return cur


def _fetchone(conn, sql, params=None):
    """Execute and return one row as a dict."""
    cur = _execute(conn, sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    return dict(row)


def _fetchall(conn, sql, params=None):
    """Execute and return all rows as a list of dicts."""
    cur = _execute(conn, sql, params)
    return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Init & migration
# ---------------------------------------------------------------------------

def init_db(db_path=DEFAULT_DB_PATH):
    """Create all tables if they don't exist."""
    if _use_pg:
        conn = _connect()
        for stmt in _PG_TABLES:
            conn.cursor().execute(stmt)
        _migrate_columns(conn, "trades", {
            "fee_cents": "INTEGER NOT NULL DEFAULT 0",
        })
        _migrate_columns(conn, "scan_results", {
            "spread_pct": "REAL NOT NULL DEFAULT 0",
            "dollar_rank": "INTEGER NOT NULL DEFAULT 0",
            "qualified": "INTEGER NOT NULL DEFAULT 0",
            "close_time": "TEXT NOT NULL DEFAULT ''",
            "signal_ask": "INTEGER NOT NULL DEFAULT 0",
        })
        _migrate_columns(conn, "scan_meta", {
            "qualified": "INTEGER NOT NULL DEFAULT 0",
            "count_tier1": "INTEGER NOT NULL DEFAULT 0",
            "count_top20": "INTEGER NOT NULL DEFAULT 0",
            "count_dollar_vol": "INTEGER NOT NULL DEFAULT 0",
            "count_spread": "INTEGER NOT NULL DEFAULT 0",
            "count_expires": "INTEGER NOT NULL DEFAULT 0",
        })
        conn.close()
    else:
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = _connect(db_path)
        conn.executescript(SCHEMA_SQLITE)
        _migrate_columns(conn, "trades", {
            "fee_cents": "INTEGER NOT NULL DEFAULT 0",
        })
        _migrate_columns(conn, "scan_results", {
            "spread_pct": "REAL NOT NULL DEFAULT 0",
            "dollar_rank": "INTEGER NOT NULL DEFAULT 0",
            "qualified": "INTEGER NOT NULL DEFAULT 0",
            "close_time": "TEXT NOT NULL DEFAULT ''",
            "signal_ask": "INTEGER NOT NULL DEFAULT 0",
        })
        _migrate_columns(conn, "scan_meta", {
            "qualified": "INTEGER NOT NULL DEFAULT 0",
            "count_tier1": "INTEGER NOT NULL DEFAULT 0",
            "count_top20": "INTEGER NOT NULL DEFAULT 0",
            "count_dollar_vol": "INTEGER NOT NULL DEFAULT 0",
            "count_spread": "INTEGER NOT NULL DEFAULT 0",
            "count_expires": "INTEGER NOT NULL DEFAULT 0",
        })
        conn.close()


def _migrate_columns(conn, table, columns):
    """Add columns to a table if they don't already exist."""
    if _use_pg:
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s AND table_schema = 'public'",
            (table,),
        )
        existing = {row[0] for row in cur.fetchall()}
    else:
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    for col, typedef in columns.items():
        if col not in existing:
            conn.cursor().execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

def log_trade(
    ticker,
    side,
    action,
    count,
    price_cents,
    status,
    order_id=None,
    fill_count=0,
    remaining_count=0,
    error_message=None,
    fee_cents=0,
    db_path=DEFAULT_DB_PATH,
):
    """Record an order attempt."""
    conn = _connect(db_path)
    _execute(conn,
        """INSERT INTO trades
           (order_id, ticker, side, action, count, price_cents, fee_cents, status,
            fill_count, remaining_count, error_message)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (order_id, ticker, side, action, count, price_cents, fee_cents, status,
         fill_count, remaining_count, error_message),
    )
    if not _use_pg:
        conn.commit()
    conn.close()


def get_trade_history(limit=50, ticker=None, db_path=DEFAULT_DB_PATH):
    """Return recent trades as a list of dicts."""
    conn = _connect(db_path)
    if ticker:
        rows = _fetchall(conn,
            "SELECT * FROM trades WHERE ticker = ? ORDER BY id DESC LIMIT ?",
            (ticker, limit),
        )
    else:
        rows = _fetchall(conn,
            "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
        )
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

def update_position_on_buy(ticker, side, qty, price_cents, db_path=DEFAULT_DB_PATH):
    """Update or create position after a buy fill."""
    conn = _connect(db_path)
    row = _fetchone(conn,
        "SELECT * FROM positions WHERE ticker = ? AND side = ? AND is_closed = 0",
        (ticker, side),
    )

    if row:
        old_qty = row["quantity"]
        old_cost = row["total_cost_cents"]
        new_qty = old_qty + qty
        new_cost = old_cost + (qty * price_cents)
        new_avg = new_cost / new_qty if new_qty > 0 else 0
        _execute(conn,
            """UPDATE positions
               SET quantity = ?, avg_entry_price_cents = ?, total_cost_cents = ?
               WHERE id = ?""",
            (new_qty, new_avg, new_cost, row["id"]),
        )
    else:
        total_cost = qty * price_cents
        _execute(conn,
            """INSERT INTO positions
               (ticker, side, quantity, avg_entry_price_cents, total_cost_cents)
               VALUES (?, ?, ?, ?, ?)""",
            (ticker, side, qty, price_cents, total_cost),
        )

    if not _use_pg:
        conn.commit()
    conn.close()


def update_position_on_sell(ticker, side, qty, sell_price_cents, db_path=DEFAULT_DB_PATH):
    """Update position after a sell fill. Calculates realized PnL."""
    conn = _connect(db_path)
    row = _fetchone(conn,
        "SELECT * FROM positions WHERE ticker = ? AND side = ? AND is_closed = 0",
        (ticker, side),
    )

    if not row:
        conn.close()
        return 0

    avg_entry = row["avg_entry_price_cents"]
    sell_qty = min(qty, row["quantity"])
    pnl = int(sell_qty * (sell_price_cents - avg_entry))

    new_qty = row["quantity"] - sell_qty
    new_cost = int(new_qty * avg_entry)
    total_pnl = row["realized_pnl_cents"] + pnl

    if new_qty <= 0:
        _execute(conn,
            f"""UPDATE positions
               SET quantity = 0, total_cost_cents = 0, realized_pnl_cents = ?,
                   is_closed = 1, closed_at = {_now_sql()}
               WHERE id = ?""",
            (total_pnl, row["id"]),
        )
    else:
        _execute(conn,
            """UPDATE positions
               SET quantity = ?, total_cost_cents = ?, realized_pnl_cents = ?
               WHERE id = ?""",
            (new_qty, new_cost, total_pnl, row["id"]),
        )

    if not _use_pg:
        conn.commit()
    conn.close()
    return pnl


def get_open_positions(db_path=DEFAULT_DB_PATH):
    """Return all open positions."""
    conn = _connect(db_path)
    rows = _fetchall(conn,
        "SELECT * FROM positions WHERE is_closed = 0 AND quantity > 0 ORDER BY opened_at DESC"
    )
    conn.close()
    return rows


def get_all_positions(db_path=DEFAULT_DB_PATH):
    """Return all positions (open and closed)."""
    conn = _connect(db_path)
    rows = _fetchall(conn,
        "SELECT * FROM positions ORDER BY opened_at DESC"
    )
    conn.close()
    return rows


def get_closed_positions(db_path=DEFAULT_DB_PATH):
    """Return all closed positions."""
    conn = _connect(db_path)
    rows = _fetchall(conn,
        "SELECT * FROM positions WHERE is_closed = 1 ORDER BY closed_at DESC"
    )
    conn.close()
    return rows


def close_position_settled(ticker, side, settlement_value_cents, db_path=DEFAULT_DB_PATH):
    """Close a position that has been settled by the market.

    settlement_value_cents: 100 if position side won, 0 if lost.
    Returns realized PnL in cents, or None if no matching position found.
    """
    conn = _connect(db_path)
    row = _fetchone(conn,
        "SELECT * FROM positions WHERE ticker = ? AND side = ? AND is_closed = 0 AND quantity > 0",
        (ticker, side),
    )
    if not row:
        conn.close()
        return None

    avg_entry = row["avg_entry_price_cents"]
    qty = row["quantity"]
    pnl = int(qty * (settlement_value_cents - avg_entry))
    total_pnl = row["realized_pnl_cents"] + pnl

    _execute(conn,
        f"""UPDATE positions
           SET quantity = 0, total_cost_cents = 0, realized_pnl_cents = ?,
               is_closed = 1, closed_at = {_now_sql()}
           WHERE id = ?""",
        (total_pnl, row["id"]),
    )
    if not _use_pg:
        conn.commit()
    conn.close()
    return pnl


# ---------------------------------------------------------------------------
# Balance
# ---------------------------------------------------------------------------

def log_balance(balance_cents, db_path=DEFAULT_DB_PATH):
    """Record a balance snapshot."""
    conn = _connect(db_path)
    _execute(conn,
        "INSERT INTO balance_history (balance_cents) VALUES (?)",
        (balance_cents,),
    )
    if not _use_pg:
        conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Whale strategy helpers
# ---------------------------------------------------------------------------

def get_position_tickers(db_path=DEFAULT_DB_PATH):
    """Return set of tickers that have open positions."""
    conn = _connect(db_path)
    rows = _fetchall(conn,
        "SELECT DISTINCT ticker FROM positions WHERE is_closed = 0 AND quantity > 0"
    )
    conn.close()
    return {r["ticker"] for r in rows}


def get_first_balance(db_path=DEFAULT_DB_PATH):
    """Return the very first balance snapshot ever recorded, or None."""
    conn = _connect(db_path)
    row = _fetchone(conn,
        "SELECT balance_cents FROM balance_history ORDER BY id ASC LIMIT 1"
    )
    conn.close()
    return row["balance_cents"] if row else None


def log_deposit(amount_cents, notes=None, db_path=DEFAULT_DB_PATH):
    """Record a deposit."""
    conn = _connect(db_path)
    _execute(conn,
        "INSERT INTO deposits (amount_cents, notes) VALUES (?, ?)",
        (amount_cents, notes),
    )
    if not _use_pg:
        conn.commit()
    conn.close()


def log_withdrawal(amount_cents, notes=None, db_path=DEFAULT_DB_PATH):
    """Record a withdrawal."""
    conn = _connect(db_path)
    _execute(conn,
        "INSERT INTO withdrawals (amount_cents, notes) VALUES (?, ?)",
        (amount_cents, notes),
    )
    if not _use_pg:
        conn.commit()
    conn.close()


def get_total_deposits(db_path=DEFAULT_DB_PATH):
    """Sum all deposits from the deposits table.

    Returns (total_deposits_cents, deposit_count).
    """
    conn = _connect(db_path)
    row = _fetchone(conn,
        "SELECT COALESCE(SUM(amount_cents), 0) as total, COUNT(*) as cnt FROM deposits"
    )
    conn.close()
    return (row["total"], row["cnt"])


def get_total_withdrawals(db_path=DEFAULT_DB_PATH):
    """Sum all withdrawals from the withdrawals table.

    Returns (total_withdrawals_cents, withdrawal_count).
    """
    conn = _connect(db_path)
    row = _fetchone(conn,
        "SELECT COALESCE(SUM(amount_cents), 0) as total, COUNT(*) as cnt FROM withdrawals"
    )
    conn.close()
    return (row["total"], row["cnt"])


def get_today_starting_balance(db_path=DEFAULT_DB_PATH):
    """Return the earliest balance snapshot for today, or None."""
    conn = _connect(db_path)
    if _use_pg:
        sql = "SELECT balance_cents FROM balance_history WHERE recorded_at::date = CURRENT_DATE ORDER BY id ASC LIMIT 1"
    else:
        sql = "SELECT balance_cents FROM balance_history WHERE date(recorded_at) = date('now') ORDER BY id ASC LIMIT 1"
    row = _fetchone(conn, sql)
    conn.close()
    return row["balance_cents"] if row else None


def get_today_trading_loss(db_path=DEFAULT_DB_PATH):
    """Return total realized trading loss for today (cents, always >= 0).

    Only counts negative realized P&L from positions closed today.
    Deposits and withdrawals are excluded.
    """
    conn = _connect(db_path)
    if _use_pg:
        sql = """SELECT COALESCE(SUM(realized_pnl_cents), 0) as total
                 FROM positions
                 WHERE is_closed = 1 AND realized_pnl_cents < 0
                   AND closed_at::date = CURRENT_DATE"""
    else:
        sql = """SELECT COALESCE(SUM(realized_pnl_cents), 0) as total
                 FROM positions
                 WHERE is_closed = 1 AND realized_pnl_cents < 0
                   AND date(closed_at) = date('now')"""
    row = _fetchone(conn, sql)
    conn.close()
    # Return as positive number (amount lost)
    return abs(row["total"])


def count_open_positions(db_path=DEFAULT_DB_PATH):
    """Return number of open positions."""
    conn = _connect(db_path)
    row = _fetchone(conn,
        "SELECT COUNT(*) as n FROM positions WHERE is_closed = 0 AND quantity > 0"
    )
    conn.close()
    return row["n"]


# ---------------------------------------------------------------------------
# Stats & P&L
# ---------------------------------------------------------------------------

def get_stats(db_path=DEFAULT_DB_PATH):
    """Return aggregate trading statistics."""
    conn = _connect(db_path)

    total = _fetchone(conn,
        "SELECT COUNT(*) as n FROM trades WHERE status != 'failed' AND fill_count > 0"
    )["n"]

    filled = _fetchone(conn,
        "SELECT COUNT(*) as n FROM trades WHERE status IN ('filled', 'partial') AND fill_count > 0"
    )["n"]

    failed = _fetchone(conn,
        "SELECT COUNT(*) as n FROM trades WHERE status = 'failed'"
    )["n"]

    realized_pnl = _fetchone(conn,
        "SELECT COALESCE(SUM(realized_pnl_cents), 0) as total FROM positions WHERE is_closed = 1"
    )["total"]

    open_realized = _fetchone(conn,
        "SELECT COALESCE(SUM(realized_pnl_cents), 0) as total FROM positions WHERE is_closed = 0"
    )["total"]

    wins = _fetchone(conn,
        "SELECT COUNT(*) as n FROM positions WHERE is_closed = 1 AND realized_pnl_cents > 0"
    )["n"]
    losses = _fetchone(conn,
        "SELECT COUNT(*) as n FROM positions WHERE is_closed = 1 AND realized_pnl_cents < 0"
    )["n"]
    breakeven = _fetchone(conn,
        "SELECT COUNT(*) as n FROM positions WHERE is_closed = 1 AND realized_pnl_cents = 0"
    )["n"]

    gross_profit = _fetchone(conn,
        "SELECT COALESCE(SUM(realized_pnl_cents), 0) as s FROM positions WHERE is_closed = 1 AND realized_pnl_cents > 0"
    )["s"]
    gross_loss = abs(_fetchone(conn,
        "SELECT COALESCE(SUM(realized_pnl_cents), 0) as s FROM positions WHERE is_closed = 1 AND realized_pnl_cents < 0"
    )["s"])

    total_fees = _fetchone(conn,
        "SELECT COALESCE(SUM(fee_cents), 0) as total FROM trades WHERE status != 'failed' AND fill_count > 0"
    )["total"]

    # Total invested = sum of (fill_count × price_cents) for trades with actual fills
    total_invested = _fetchone(conn,
        """SELECT COALESCE(SUM(fill_count * price_cents), 0) as total
           FROM trades
           WHERE action = 'buy' AND fill_count > 0
             AND status != 'failed'"""
    )["total"]

    conn.close()

    closed = wins + losses + breakeven
    win_rate = (wins / closed * 100) if closed > 0 else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0

    return {
        "total_orders": total,
        "filled_orders": filled,
        "failed_orders": failed,
        "closed_positions": closed,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": win_rate,
        "realized_pnl_cents": realized_pnl + open_realized,
        "gross_profit_cents": gross_profit,
        "gross_loss_cents": gross_loss,
        "profit_factor": profit_factor,
        "total_fees_cents": total_fees,
        "total_invested_cents": total_invested,
    }


def get_daily_pnl(days=30, db_path=DEFAULT_DB_PATH):
    """Return daily P&L breakdown for the last N days.

    Combines realized P&L from closed positions (by closed_at date)
    with fees from trades (by created_at date).
    Returns list of dicts sorted by date ascending.
    """
    conn = _connect(db_path)

    if _use_pg:
        date_fn = "closed_at::date"
        trade_date_fn = "created_at::date"
        day_filter = f"closed_at >= NOW() - INTERVAL '{days} days'"
        trade_day_filter = f"created_at >= NOW() - INTERVAL '{days} days'"
    else:
        date_fn = "date(closed_at)"
        trade_date_fn = "date(created_at)"
        day_filter = f"closed_at >= datetime('now', '-{days} days')"
        trade_day_filter = f"created_at >= datetime('now', '-{days} days')"

    # Realized P&L by close date
    pnl_rows = _fetchall(conn, f"""
        SELECT {date_fn} as day,
               COALESCE(SUM(realized_pnl_cents), 0) as realized_cents,
               COUNT(*) as positions_closed
        FROM positions
        WHERE is_closed = 1 AND {day_filter}
        GROUP BY {date_fn}
        ORDER BY day ASC
    """)

    # Fees and trade count by trade date
    fee_rows = _fetchall(conn, f"""
        SELECT {trade_date_fn} as day,
               COALESCE(SUM(fee_cents), 0) as fees_cents,
               COUNT(*) as trade_count
        FROM trades
        WHERE status != 'failed' AND {trade_day_filter}
        GROUP BY {trade_date_fn}
        ORDER BY day ASC
    """)

    conn.close()

    # Merge into a single dict keyed by date string
    daily = {}
    for row in pnl_rows:
        d = str(row["day"])
        daily.setdefault(d, {"date": d, "realized_cents": 0, "fees_cents": 0,
                             "trade_count": 0, "positions_closed": 0})
        daily[d]["realized_cents"] = row["realized_cents"]
        daily[d]["positions_closed"] = row["positions_closed"]

    for row in fee_rows:
        d = str(row["day"])
        daily.setdefault(d, {"date": d, "realized_cents": 0, "fees_cents": 0,
                             "trade_count": 0, "positions_closed": 0})
        daily[d]["fees_cents"] = row["fees_cents"]
        daily[d]["trade_count"] = row["trade_count"]

    # Sort by date and compute net P&L
    result = sorted(daily.values(), key=lambda x: x["date"])
    for r in result:
        r["net_cents"] = r["realized_cents"] - r["fees_cents"]

    return result


# ---------------------------------------------------------------------------
# Scan results (written by CLI, read by web dashboard)
# ---------------------------------------------------------------------------

def save_scan_results(results, stats, db_path=DEFAULT_DB_PATH):
    """Replace scan_results table with fresh results from CLI scan."""
    conn = _connect(db_path)
    _execute(conn, "DELETE FROM scan_results")
    for m in results:
        _execute(conn,
            """INSERT INTO scan_results
               (ticker, event_ticker, signal_side, signal_price, signal_ask, tier,
                volume_24h, dollar_24h, volume, open_interest,
                spread_pct, dollar_rank, qualified, close_time)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (m["ticker"], m.get("event_ticker", ""), m["signal_side"],
             m["signal_price"], m.get("signal_ask", 0), m.get("tier", 3),
             m.get("volume_24h", 0), m.get("dollar_24h", 0),
             m.get("volume", 0), m.get("open_interest", 0),
             m.get("spread_pct", 0), m.get("dollar_rank", 0),
             1 if m.get("qualified") else 0,
             m.get("close_time", "")),
        )
    _execute(conn, "DELETE FROM scan_meta")
    prefixes_str = ",".join(stats.get("prefixes", []))
    _execute(conn,
        """INSERT INTO scan_meta
           (id, total_fetched, top_n, scanned, passed_prefix, passed_volume,
            passed_price, count_tier1, count_top20, count_dollar_vol,
            count_spread, count_expires, qualified, min_price, min_volume, prefixes)
           VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (stats.get("total_fetched", 0), stats.get("top_n", 0),
         stats.get("scanned", 0), stats.get("passed_prefix", 0),
         stats.get("passed_volume", 0), stats.get("passed_price", 0),
         stats.get("count_tier1", 0), stats.get("count_top20", 0),
         stats.get("count_dollar_vol", 0), stats.get("count_spread", 0),
         stats.get("count_expires", 0), stats.get("qualified", 0),
         stats.get("min_price", 0), stats.get("min_volume", 0),
         prefixes_str),
    )
    if not _use_pg:
        conn.commit()
    conn.close()


def get_scan_results(db_path=DEFAULT_DB_PATH):
    """Read last scan results from DB. Returns (results, stats, scanned_at)."""
    conn = _connect(db_path)

    results = _fetchall(conn,
        "SELECT * FROM scan_results ORDER BY tier ASC, signal_price DESC, volume_24h DESC"
    )

    meta = _fetchone(conn, "SELECT * FROM scan_meta WHERE id = 1")
    if meta:
        prefixes_raw = meta.get("prefixes", "")
        scanned_at = meta["scanned_at"]
        # Convert datetime to string if PostgreSQL returns a datetime object
        if hasattr(scanned_at, "strftime"):
            scanned_at = scanned_at.strftime("%Y-%m-%d %H:%M:%S")
        stats = {
            "total_fetched": meta["total_fetched"],
            "top_n": meta["top_n"],
            "scanned": meta["scanned"],
            "passed_prefix": meta["passed_prefix"],
            "passed_volume": meta["passed_volume"],
            "passed_price": meta["passed_price"],
            "count_tier1": meta.get("count_tier1", 0),
            "count_top20": meta.get("count_top20", 0),
            "count_dollar_vol": meta.get("count_dollar_vol", 0),
            "count_spread": meta.get("count_spread", 0),
            "count_expires": meta.get("count_expires", 0),
            "qualified": meta.get("qualified", 0),
            "min_price": meta["min_price"],
            "min_volume": meta["min_volume"],
            "prefixes": [p for p in prefixes_raw.split(",") if p],
        }
    else:
        stats = {}
        scanned_at = None

    conn.close()

    # Recompute fail_reasons for each result (not stored in DB)
    from kalshi_bot.scanner import (QUALIFIED_TOP_N_DOLLAR, QUALIFIED_MIN_DOLLAR_24H,
                                    QUALIFIED_MAX_SPREAD_PCT, QUALIFIED_MAX_HOURS,
                                    hours_until_close)
    for r in results:
        if r.get("tier", 0) == 0:
            r["fail_reasons"] = ["tier0"]
            continue
        reasons = []
        if r.get("dollar_rank", 0) > QUALIFIED_TOP_N_DOLLAR:
            reasons.append("rank")
        if r.get("dollar_24h", 0) < QUALIFIED_MIN_DOLLAR_24H:
            reasons.append("volume")
        if r.get("spread_pct", 0) > QUALIFIED_MAX_SPREAD_PCT:
            reasons.append("spread")
        hrs = hours_until_close(r.get("close_time", ""))
        if hrs is None or hrs <= 0 or hrs > QUALIFIED_MAX_HOURS:
            reasons.append("expiry")
        r["fail_reasons"] = reasons

    return results, stats, scanned_at


# ---------------------------------------------------------------------------
# Price snapshots (for charts)
# ---------------------------------------------------------------------------

def log_price_snapshot(ticker, side, bid_cents, ask_cents, db_path=DEFAULT_DB_PATH):
    """Record a price snapshot for charting."""
    conn = _connect(db_path)
    _execute(conn,
        """INSERT INTO price_snapshots (ticker, side, bid_cents, ask_cents)
           VALUES (?, ?, ?, ?)""",
        (ticker, side, bid_cents, ask_cents),
    )
    if not _use_pg:
        conn.commit()
    conn.close()


def get_price_history(ticker, side, hours=24, db_path=DEFAULT_DB_PATH):
    """Return price snapshots for a ticker/side within the last N hours."""
    conn = _connect(db_path)
    if _use_pg:
        sql = """SELECT bid_cents, ask_cents, recorded_at
                 FROM price_snapshots
                 WHERE ticker = %s AND side = %s
                   AND recorded_at >= NOW() - INTERVAL '%s hours'
                 ORDER BY recorded_at ASC"""
        cur = conn.cursor()
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (ticker, side, hours))
        rows = [dict(r) for r in cur.fetchall()]
    else:
        sql = """SELECT bid_cents, ask_cents, recorded_at
                 FROM price_snapshots
                 WHERE ticker = ? AND side = ?
                   AND recorded_at >= datetime('now', ?)
                 ORDER BY recorded_at ASC"""
        rows = _fetchall(conn, sql, (ticker, side, f"-{hours} hours"))
    conn.close()
    # Normalize recorded_at to string
    for r in rows:
        if hasattr(r["recorded_at"], "strftime"):
            r["recorded_at"] = r["recorded_at"].strftime("%Y-%m-%dT%H:%M:%SZ")
    return rows


def cleanup_old_snapshots(hours=48, db_path=DEFAULT_DB_PATH):
    """Delete price snapshots older than N hours."""
    conn = _connect(db_path)
    if _use_pg:
        conn.cursor().execute(
            "DELETE FROM price_snapshots WHERE recorded_at < NOW() - INTERVAL '%s hours'",
            (hours,),
        )
    else:
        _execute(conn,
            "DELETE FROM price_snapshots WHERE recorded_at < datetime('now', ?)",
            (f"-{hours} hours",),
        )
        conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# CSV Import
# ---------------------------------------------------------------------------

def clear_all_trades(db_path=DEFAULT_DB_PATH):
    """Delete all trades from the database."""
    conn = _connect(db_path)
    _execute(conn, "DELETE FROM trades")
    if not _use_pg:
        conn.commit()
    conn.close()


def clear_all_positions(db_path=DEFAULT_DB_PATH):
    """Delete all positions from the database."""
    conn = _connect(db_path)
    _execute(conn, "DELETE FROM positions")
    if not _use_pg:
        conn.commit()
    conn.close()


def _parse_kalshi_csv_datetime(date_str):
    """Parse Kalshi CSV Original_Date to a UTC datetime string.

    Input format: 2026-01-29T22:46:06.724Z
    Output format: 2026-01-29 22:46:06
    """
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return date_str


def import_trades_from_csv(csv_content, clear_existing=True, db_path=DEFAULT_DB_PATH):
    """Import trades from a Kalshi CSV export.

    CSV columns: type, Market_Ticker, Market_Id, Original_Date, Price_In_Cents,
                 Amount_In_Dollars, Fee_In_Dollars, Traded_Time, Direction, Order_Type

    Returns (imported_count, skipped_count).
    """
    if clear_existing:
        clear_all_trades(db_path)
        clear_all_positions(db_path)

    # Handle BOM
    if csv_content.startswith("\ufeff"):
        csv_content = csv_content[1:]

    reader = csv.DictReader(io.StringIO(csv_content))
    imported = 0
    skipped = 0

    for row in reader:
        trade_type = (row.get("type") or "").strip()
        if not trade_type:
            continue  # skip empty rows

        ticker = (row.get("Market_Ticker") or "").strip()
        if not ticker:
            skipped += 1
            continue

        price_cents = int(row.get("Price_In_Cents", 0))
        direction = (row.get("Direction") or "").strip().lower()  # "yes" or "no"
        fee_dollars = float(row.get("Fee_In_Dollars", 0))
        fee_cents = int(round(fee_dollars * 100))
        # Amount_In_Dollars = number of contracts (each contract is worth $1 at max payout)
        count = max(1, int(float(row.get("Amount_In_Dollars", 1))))
        created_at = _parse_kalshi_csv_datetime(row.get("Original_Date", ""))
        market_id = (row.get("Market_Id") or "").strip()

        # Insert trade with correct timestamp
        conn = _connect(db_path)
        _execute(conn,
            """INSERT INTO trades
               (order_id, ticker, side, action, count, price_cents, fee_cents, status,
                fill_count, remaining_count, error_message, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (market_id, ticker, direction, "buy", count, price_cents, fee_cents,
             "executed", count, 0, None, created_at),
        )
        if not _use_pg:
            conn.commit()
        conn.close()

        # Also create/update the position
        update_position_on_buy(ticker, direction, count, price_cents, db_path=db_path)
        imported += 1

    return imported, skipped


def import_trades_from_csv_file(file_path, clear_existing=True, db_path=DEFAULT_DB_PATH):
    """Import trades from a Kalshi CSV file on disk.

    Returns (imported_count, skipped_count).
    """
    with open(file_path, "r", encoding="utf-8-sig") as f:
        content = f.read()
    return import_trades_from_csv(content, clear_existing=clear_existing, db_path=db_path)
