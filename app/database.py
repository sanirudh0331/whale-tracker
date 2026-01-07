import aiosqlite
from app.config import DATABASE_PATH

SCHEMA = """
-- Kalshi trades
CREATE TABLE IF NOT EXISTS kalshi_trades (
    id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    market_title TEXT,
    taker_side TEXT NOT NULL,
    count INTEGER NOT NULL,
    price INTEGER NOT NULL,
    usd_value REAL NOT NULL,
    timestamp INTEGER NOT NULL,
    is_whale INTEGER DEFAULT 0
);

-- Kalshi markets
CREATE TABLE IF NOT EXISTS kalshi_markets (
    ticker TEXT PRIMARY KEY,
    title TEXT,
    status TEXT,
    yes_price INTEGER,
    no_price INTEGER,
    volume_24h INTEGER DEFAULT 0,
    volume_avg REAL DEFAULT 0,
    open_interest INTEGER DEFAULT 0,
    last_updated INTEGER
);

-- Polymarket trades
CREATE TABLE IF NOT EXISTS polymarket_trades (
    id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    market_question TEXT,
    wallet TEXT NOT NULL,
    side TEXT NOT NULL,
    size REAL NOT NULL,
    price REAL NOT NULL,
    timestamp INTEGER NOT NULL,
    is_whale INTEGER DEFAULT 0
);

-- Polymarket wallets
CREATE TABLE IF NOT EXISTS polymarket_wallets (
    address TEXT PRIMARY KEY,
    total_volume REAL DEFAULT 0,
    trade_count INTEGER DEFAULT 0,
    first_seen INTEGER,
    last_seen INTEGER
);

-- Polymarket markets
CREATE TABLE IF NOT EXISTS polymarket_markets (
    id TEXT PRIMARY KEY,
    slug TEXT,
    question TEXT,
    volume_24h REAL DEFAULT 0,
    volume_avg REAL DEFAULT 0
);

-- Unified alerts
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    identifier TEXT,
    title TEXT,
    message TEXT NOT NULL,
    trade_size REAL,
    timestamp INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kalshi_trades_whale ON kalshi_trades(is_whale);
CREATE INDEX IF NOT EXISTS idx_kalshi_trades_ts ON kalshi_trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_poly_trades_whale ON polymarket_trades(is_whale);
CREATE INDEX IF NOT EXISTS idx_poly_trades_ts ON polymarket_trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_platform ON alerts(platform);
CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(timestamp);
"""


async def get_db():
    db = await aiosqlite.connect(DATABASE_PATH)
    db.row_factory = aiosqlite.Row
    return db


async def init_db():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()
