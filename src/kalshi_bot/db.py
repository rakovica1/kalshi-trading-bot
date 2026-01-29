import sqlite3
from datetime import date, datetime
from pathlib import Path

DEFAULT_DB_PATH = Path("kalshi_bot.db")
_today = lambda: date.today().isoformat()

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    action TEXT NOT NULL,
    count INTEGER NOT NULL,
    price_cents INTEGER NOT NULL,
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
"""


def _connect(db_path=DEFAULT_DB_PATH):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path=DEFAULT_DB_PATH):
    """Create all tables if they don't exist."""
    conn = _connect(db_path)
    conn.executescript(SCHEMA)
    conn.close()


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
    db_path=DEFAULT_DB_PATH,
):
    """Record an order attempt."""
    conn = _connect(db_path)
    conn.execute(
        """INSERT INTO trades
           (order_id, ticker, side, action, count, price_cents, status,
            fill_count, remaining_count, error_message)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (order_id, ticker, side, action, count, price_cents, status,
         fill_count, remaining_count, error_message),
    )
    conn.commit()
    conn.close()


def get_trade_history(limit=50, ticker=None, db_path=DEFAULT_DB_PATH):
    """Return recent trades as a list of dicts."""
    conn = _connect(db_path)
    if ticker:
        rows = conn.execute(
            "SELECT * FROM trades WHERE ticker = ? ORDER BY id DESC LIMIT ?",
            (ticker, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

def update_position_on_buy(ticker, side, qty, price_cents, db_path=DEFAULT_DB_PATH):
    """Update or create position after a buy fill."""
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT * FROM positions WHERE ticker = ? AND side = ? AND is_closed = 0",
        (ticker, side),
    ).fetchone()

    if row:
        old_qty = row["quantity"]
        old_cost = row["total_cost_cents"]
        new_qty = old_qty + qty
        new_cost = old_cost + (qty * price_cents)
        new_avg = new_cost / new_qty if new_qty > 0 else 0
        conn.execute(
            """UPDATE positions
               SET quantity = ?, avg_entry_price_cents = ?, total_cost_cents = ?
               WHERE id = ?""",
            (new_qty, new_avg, new_cost, row["id"]),
        )
    else:
        total_cost = qty * price_cents
        conn.execute(
            """INSERT INTO positions
               (ticker, side, quantity, avg_entry_price_cents, total_cost_cents)
               VALUES (?, ?, ?, ?, ?)""",
            (ticker, side, qty, price_cents, total_cost),
        )

    conn.commit()
    conn.close()


def update_position_on_sell(ticker, side, qty, sell_price_cents, db_path=DEFAULT_DB_PATH):
    """Update position after a sell fill. Calculates realized PnL."""
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT * FROM positions WHERE ticker = ? AND side = ? AND is_closed = 0",
        (ticker, side),
    ).fetchone()

    if not row:
        # No tracked position — just log it, don't error
        conn.close()
        return 0

    avg_entry = row["avg_entry_price_cents"]
    sell_qty = min(qty, row["quantity"])
    pnl = int(sell_qty * (sell_price_cents - avg_entry))

    new_qty = row["quantity"] - sell_qty
    new_cost = int(new_qty * avg_entry)
    total_pnl = row["realized_pnl_cents"] + pnl

    if new_qty <= 0:
        conn.execute(
            """UPDATE positions
               SET quantity = 0, total_cost_cents = 0, realized_pnl_cents = ?,
                   is_closed = 1, closed_at = datetime('now')
               WHERE id = ?""",
            (total_pnl, row["id"]),
        )
    else:
        conn.execute(
            """UPDATE positions
               SET quantity = ?, total_cost_cents = ?, realized_pnl_cents = ?
               WHERE id = ?""",
            (new_qty, new_cost, total_pnl, row["id"]),
        )

    conn.commit()
    conn.close()
    return pnl


def get_open_positions(db_path=DEFAULT_DB_PATH):
    """Return all open positions."""
    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT * FROM positions WHERE is_closed = 0 AND quantity > 0 ORDER BY opened_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_positions(db_path=DEFAULT_DB_PATH):
    """Return all positions (open and closed)."""
    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT * FROM positions ORDER BY opened_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Balance
# ---------------------------------------------------------------------------

def log_balance(balance_cents, db_path=DEFAULT_DB_PATH):
    """Record a balance snapshot."""
    conn = _connect(db_path)
    conn.execute(
        "INSERT INTO balance_history (balance_cents) VALUES (?)",
        (balance_cents,),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Whale strategy helpers
# ---------------------------------------------------------------------------

def get_position_tickers(db_path=DEFAULT_DB_PATH):
    """Return set of tickers that have open positions."""
    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM positions WHERE is_closed = 0 AND quantity > 0"
    ).fetchall()
    conn.close()
    return {r["ticker"] for r in rows}


def get_today_starting_balance(db_path=DEFAULT_DB_PATH):
    """Return the earliest balance snapshot for today, or None."""
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT balance_cents FROM balance_history WHERE date(recorded_at) = date('now') ORDER BY id ASC LIMIT 1"
    ).fetchone()
    conn.close()
    return row["balance_cents"] if row else None


def count_open_positions(db_path=DEFAULT_DB_PATH):
    """Return number of open positions."""
    conn = _connect(db_path)
    row = conn.execute(
        "SELECT COUNT(*) as n FROM positions WHERE is_closed = 0 AND quantity > 0"
    ).fetchone()
    conn.close()
    return row["n"]


# ---------------------------------------------------------------------------
# Stats & P&L
# ---------------------------------------------------------------------------

def get_stats(db_path=DEFAULT_DB_PATH):
    """Return aggregate trading statistics."""
    conn = _connect(db_path)

    total = conn.execute(
        "SELECT COUNT(*) as n FROM trades WHERE status != 'failed'"
    ).fetchone()["n"]

    filled = conn.execute(
        "SELECT COUNT(*) as n FROM trades WHERE status IN ('filled', 'partial') AND fill_count > 0"
    ).fetchone()["n"]

    failed = conn.execute(
        "SELECT COUNT(*) as n FROM trades WHERE status = 'failed'"
    ).fetchone()["n"]

    # Realized P&L from closed positions
    pnl_row = conn.execute(
        "SELECT COALESCE(SUM(realized_pnl_cents), 0) as total FROM positions WHERE is_closed = 1"
    ).fetchone()
    realized_pnl = pnl_row["total"]

    # Open position P&L
    open_pnl_row = conn.execute(
        "SELECT COALESCE(SUM(realized_pnl_cents), 0) as total FROM positions WHERE is_closed = 0"
    ).fetchone()
    open_realized = open_pnl_row["total"]

    # Win/loss from closed positions
    wins = conn.execute(
        "SELECT COUNT(*) as n FROM positions WHERE is_closed = 1 AND realized_pnl_cents > 0"
    ).fetchone()["n"]
    losses = conn.execute(
        "SELECT COUNT(*) as n FROM positions WHERE is_closed = 1 AND realized_pnl_cents < 0"
    ).fetchone()["n"]
    breakeven = conn.execute(
        "SELECT COUNT(*) as n FROM positions WHERE is_closed = 1 AND realized_pnl_cents = 0"
    ).fetchone()["n"]

    # Gross profit / gross loss for profit factor
    gross_profit = conn.execute(
        "SELECT COALESCE(SUM(realized_pnl_cents), 0) as s FROM positions WHERE is_closed = 1 AND realized_pnl_cents > 0"
    ).fetchone()["s"]
    gross_loss = abs(conn.execute(
        "SELECT COALESCE(SUM(realized_pnl_cents), 0) as s FROM positions WHERE is_closed = 1 AND realized_pnl_cents < 0"
    ).fetchone()["s"])

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
    }
