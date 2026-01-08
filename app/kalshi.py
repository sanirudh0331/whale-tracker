import time
import httpx
import aiosqlite
from datetime import datetime
from typing import Optional
from app.config import KALSHI_API_BASE, KALSHI_THRESHOLD, VOLUME_SPIKE_MULTIPLIER, DATABASE_PATH
from app.insider import calculate_insider_score, get_insider_label, get_insider_color


def detect_category(ticker: str, title: str) -> str:
    """
    Detect market category. Sports whales have strongest edge based on backtesting:
    - Sports: 98% win rate, +11% edge over expected
    - Other categories: ~67% win rate, close to expected
    """
    ticker_upper = ticker.upper()
    title_lower = title.lower()

    # Sports indicators - these have the highest predictive value
    sports_keywords = ['winner', 'wins by', 'total points', 'spread', 'game', 'match', 'vs']
    sports_tickers = ['NBA', 'NFL', 'MLB', 'NHL', 'NCAA', 'EPL', 'MLS', 'UFC', 'BOXING', 'NCAAMB', 'NCAAF', 'SERIE']

    if any(kw in title_lower for kw in sports_keywords):
        return "sports"
    if any(t in ticker_upper for t in sports_tickers):
        return "sports"

    # Crypto
    if any(c in ticker_upper for c in ['BTC', 'ETH', 'BITCOIN', 'ETHEREUM']):
        return "crypto"

    # Indices
    if any(i in title_lower for i in ['s&p', 'nasdaq', 'dow', 'inx']):
        return "indices"

    # Politics
    if any(p in title_lower for p in ['trump', 'biden', 'government', 'election', 'congress', 'fed chair']):
        return "politics"

    return "other"


class KalshiClient:
    def __init__(self):
        self.client = httpx.AsyncClient(
            base_url=KALSHI_API_BASE,
            timeout=30.0,
            headers={"Accept": "application/json"}
        )
        self.market_cache = {}

    async def close(self):
        await self.client.aclose()

    async def get_trades(self, limit: int = 1000, cursor: str = None) -> dict:
        try:
            params = {"limit": min(limit, 1000)}
            if cursor:
                params["cursor"] = cursor
            resp = await self.client.get("/markets/trades", params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Kalshi trades error: {e}")
            return {"trades": [], "cursor": ""}

    async def get_all_trades(self, max_trades: int = 2000) -> list[dict]:
        all_trades = []
        cursor = None
        while len(all_trades) < max_trades:
            data = await self.get_trades(limit=1000, cursor=cursor)
            trades = data.get("trades", [])
            if not trades:
                break
            all_trades.extend(trades)
            cursor = data.get("cursor", "")
            if not cursor:
                break
        return all_trades[:max_trades]

    async def get_markets(self, limit: int = 200, cursor: str = None) -> dict:
        try:
            params = {"limit": min(limit, 200), "status": "open"}
            if cursor:
                params["cursor"] = cursor
            resp = await self.client.get("/markets", params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Kalshi markets error: {e}")
            return {"markets": [], "cursor": ""}

    async def get_all_markets(self) -> list[dict]:
        all_markets = []
        cursor = None
        while True:
            data = await self.get_markets(limit=200, cursor=cursor)
            markets = data.get("markets", [])
            if not markets:
                break
            all_markets.extend(markets)
            cursor = data.get("cursor", "")
            if not cursor or len(all_markets) > 500:
                break
        return all_markets

    async def get_market(self, ticker: str) -> Optional[dict]:
        try:
            resp = await self.client.get(f"/markets/{ticker}")
            resp.raise_for_status()
            return resp.json().get("market")
        except Exception:
            return None

    def parse_trade(self, trade: dict) -> dict:
        yes_price = trade.get("yes_price", 50)
        no_price = trade.get("no_price", 50)
        count = trade.get("count", 0)
        taker_side = trade.get("taker_side", "yes")
        price_cents = yes_price if taker_side == "yes" else no_price
        usd_value = count * price_cents / 100

        created_time = trade.get("created_time", "")
        timestamp = 0
        if created_time:
            try:
                dt = datetime.fromisoformat(created_time.replace("Z", "+00:00"))
                timestamp = int(dt.timestamp())
            except Exception:
                pass

        return {
            "id": trade.get("trade_id", ""),
            "ticker": trade.get("ticker", ""),
            "taker_side": taker_side,
            "count": count,
            "price": price_cents,
            "usd_value": usd_value,
            "timestamp": timestamp
        }


async def fetch_kalshi_data() -> dict:
    """Fetch and process Kalshi trades and markets."""
    client = KalshiClient()
    whale_alerts = []
    volume_alerts = []

    try:
        # Process trades
        raw_trades = await client.get_all_trades(max_trades=2000)
        print(f"Kalshi: fetched {len(raw_trades)} trades")

        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row

            for raw in raw_trades:
                try:
                    trade = client.parse_trade(raw)
                    trade_id = trade.get("id", "")
                    if not trade_id:
                        continue

                    existing = await db.execute(
                        "SELECT id FROM kalshi_trades WHERE id = ?", (trade_id,)
                    )
                    if await existing.fetchone():
                        continue

                    ticker = trade.get("ticker", "")
                    usd_value = trade.get("usd_value", 0)

                    # Get market title
                    market_title = ticker
                    if ticker in client.market_cache:
                        market_title = client.market_cache[ticker]
                    else:
                        market = await client.get_market(ticker)
                        if market:
                            market_title = market.get("title", ticker)
                            client.market_cache[ticker] = market_title

                    is_whale = 1 if usd_value >= KALSHI_THRESHOLD else 0

                    # Calculate insider score for whale trades
                    insider_score = 0
                    if is_whale:
                        # Get market volume for liquidity score
                        market_vol = await db.execute(
                            "SELECT volume_24h FROM kalshi_markets WHERE ticker = ?", (ticker,)
                        )
                        vol_row = await market_vol.fetchone()
                        volume_24h = vol_row["volume_24h"] if vol_row else 0

                        score_data = await calculate_insider_score(
                            platform="kalshi",
                            usd_value=usd_value,
                            threshold=KALSHI_THRESHOLD,
                            price=trade.get("price", 50),
                            side=trade.get("taker_side", "yes"),
                            market_title=market_title,
                            market_id=ticker,
                            timestamp=trade.get("timestamp", 0),
                            volume_24h=volume_24h
                        )
                        insider_score = score_data["insider_score"]

                    category = detect_category(ticker, market_title)

                    await db.execute(
                        """INSERT OR IGNORE INTO kalshi_trades
                           (id, ticker, market_title, taker_side, count, price, usd_value, timestamp, is_whale, insider_score, category)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (trade_id, ticker, market_title, trade.get("taker_side"),
                         trade.get("count", 0), trade.get("price", 0),
                         usd_value, trade.get("timestamp", 0), is_whale, insider_score, category)
                    )

                    if is_whale:
                        side = trade.get("taker_side", "").upper()
                        category = detect_category(ticker, market_title)
                        is_sports = category == "sports"

                        # Sports whales have 98% win rate historically - flag them specially
                        prefix = "SPORTS WHALE" if is_sports else "WHALE"
                        alert_type = "sports_whale" if is_sports else "whale"
                        message = f"{prefix}: ${usd_value:,.0f} {side} on {market_title[:50]}"

                        await db.execute(
                            """INSERT INTO alerts (platform, alert_type, identifier, title, message, trade_size, timestamp)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            ("kalshi", alert_type, ticker, market_title, message, usd_value, int(time.time()))
                        )
                        whale_alerts.append({
                            "ticker": ticker,
                            "market_title": market_title,
                            "side": trade.get("taker_side"),
                            "usd_value": usd_value,
                            "category": category,
                            "is_sports": is_sports
                        })

                except Exception as e:
                    print(f"Kalshi trade error: {e}")
                    continue

            await db.commit()

        # Process markets
        markets = await client.get_all_markets()
        print(f"Kalshi: fetched {len(markets)} markets")

        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row

            for market in markets:
                try:
                    ticker = market.get("ticker", "")
                    if not ticker:
                        continue

                    title = market.get("title", "")
                    volume_24h = market.get("volume_24h", 0)
                    yes_price = market.get("yes_bid", 50)
                    no_price = market.get("no_bid", 50)
                    open_interest = market.get("open_interest", 0)

                    client.market_cache[ticker] = title

                    existing = await db.execute(
                        "SELECT volume_avg FROM kalshi_markets WHERE ticker = ?", (ticker,)
                    )
                    row = await existing.fetchone()

                    if row and row["volume_avg"] > 0:
                        avg = row["volume_avg"]
                        if volume_24h > avg * VOLUME_SPIKE_MULTIPLIER:
                            ratio = volume_24h / avg
                            message = f"{ratio:.1f}x volume spike on {title[:50]}"
                            await db.execute(
                                """INSERT INTO alerts (platform, alert_type, identifier, title, message, trade_size, timestamp)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                ("kalshi", "volume_spike", ticker, title, message, volume_24h, int(time.time()))
                            )
                            volume_alerts.append({"ticker": ticker, "ratio": ratio})

                    new_avg = (volume_24h + (row["volume_avg"] if row else 0)) / 2 if row else volume_24h
                    category = detect_category(ticker, title)

                    await db.execute(
                        """INSERT OR REPLACE INTO kalshi_markets
                           (ticker, title, status, yes_price, no_price, volume_24h, volume_avg, open_interest, last_updated, category)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (ticker, title, "open", yes_price, no_price, volume_24h, new_avg, open_interest, int(time.time()), category)
                    )

                except Exception as e:
                    print(f"Kalshi market error: {e}")
                    continue

            await db.commit()

    finally:
        await client.close()

    return {"whale_alerts": len(whale_alerts), "volume_alerts": len(volume_alerts)}


async def get_kalshi_whale_trades(limit: int = 30, insider_only: bool = False, min_threshold: int = 0, hours: int = 24, sort: str = "newest", hide_settled: bool = False) -> list[dict]:
    """
    Get whale trades with optional filters.
    - hours: Only trades from last X hours (e.g., 168 = 7 days)
    - hide_settled: If True, hide trades on markets that have already settled
    - insider_only: If True, only show trades with insider_score >= 50
    """
    if min_threshold <= 0:
        min_threshold = KALSHI_THRESHOLD

    min_timestamp = int(time.time()) - (hours * 3600)

    # Build ORDER BY clause based on sort parameter
    sort_clauses = {
        "newest": "t.timestamp DESC",
        "oldest": "t.timestamp ASC",
        "size_desc": "t.usd_value DESC",
        "size_asc": "t.usd_value ASC"
    }
    order_by = sort_clauses.get(sort, "t.timestamp DESC")

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Base query with JOIN to get market status
        base_query = """
            SELECT t.*, m.result, m.status as market_status
            FROM kalshi_trades t
            LEFT JOIN kalshi_markets m ON t.ticker = m.ticker
            WHERE t.usd_value >= ? AND t.timestamp >= ?
        """

        conditions = []
        params = [min_threshold, min_timestamp]

        if insider_only:
            conditions.append("t.insider_score >= 50")

        if hide_settled:
            conditions.append("(m.result IS NULL OR m.status = 'open')")

        if conditions:
            base_query += " AND " + " AND ".join(conditions)

        base_query += f" ORDER BY {order_by} LIMIT ?"
        params.append(limit)

        result = await db.execute(base_query, params)

        trades = []
        for row in await result.fetchall():
            trade = dict(row)
            trade["insider_label"] = get_insider_label(trade.get("insider_score", 0))
            trade["insider_color"] = get_insider_color(trade.get("insider_score", 0))
            trade["is_settled"] = trade.get("result") is not None
            trades.append(trade)
        return trades


async def get_kalshi_top_markets(limit: int = 15, include_settled: bool = False) -> list[dict]:
    """Get top markets by volume. By default only shows open markets in UI."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        if include_settled:
            result = await db.execute(
                """SELECT * FROM kalshi_markets ORDER BY volume_24h DESC LIMIT ?""",
                (limit,)
            )
        else:
            result = await db.execute(
                """SELECT * FROM kalshi_markets
                   WHERE status = 'open' OR result IS NULL
                   ORDER BY volume_24h DESC LIMIT ?""",
                (limit,)
            )
        return [dict(row) for row in await result.fetchall()]


async def get_kalshi_stats() -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row

        total = await db.execute("SELECT COUNT(*) as c FROM kalshi_trades")
        total = (await total.fetchone())["c"]

        whales = await db.execute("SELECT COUNT(*) as c FROM kalshi_trades WHERE is_whale = 1")
        whales = (await whales.fetchone())["c"]

        markets = await db.execute("SELECT COUNT(*) as c FROM kalshi_markets")
        markets = (await markets.fetchone())["c"]

        volume = await db.execute("SELECT COALESCE(SUM(usd_value), 0) as v FROM kalshi_trades WHERE is_whale = 1")
        volume = (await volume.fetchone())["v"]

        return {
            "total_trades": total,
            "whale_trades": whales,
            "markets_tracked": markets,
            "whale_volume": volume,
            "threshold": KALSHI_THRESHOLD
        }


async def check_settled_markets() -> dict:
    """
    Check markets in DB for settlement and record outcomes.
    This enables backtesting by capturing whale trade results.
    """
    client = KalshiClient()
    updated = 0
    checked = 0

    try:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row

            # Get markets without results that have whale trades
            result = await db.execute(
                """SELECT DISTINCT m.ticker FROM kalshi_markets m
                   JOIN kalshi_trades t ON m.ticker = t.ticker
                   WHERE m.result IS NULL AND t.is_whale = 1
                   LIMIT 100"""
            )
            tickers = [row["ticker"] for row in await result.fetchall()]

            print(f"Checking {len(tickers)} markets for settlement...")

            for ticker in tickers:
                checked += 1
                try:
                    market = await client.get_market(ticker)
                    if not market:
                        continue

                    status = market.get("status", "")
                    result_value = market.get("result")  # 'yes' or 'no'

                    if status in ("settled", "finalized") and result_value:
                        # Get settlement time
                        settlement_ts = market.get("settlement_ts", "")
                        settlement_time = 0
                        if settlement_ts:
                            try:
                                from datetime import datetime
                                dt = datetime.fromisoformat(settlement_ts.replace("Z", "+00:00"))
                                settlement_time = int(dt.timestamp())
                            except Exception:
                                settlement_time = int(time.time())

                        await db.execute(
                            """UPDATE kalshi_markets
                               SET status = ?, result = ?, settlement_time = ?
                               WHERE ticker = ?""",
                            (status, result_value, settlement_time, ticker)
                        )
                        updated += 1
                        print(f"  {ticker}: {result_value.upper()}")

                except Exception as e:
                    print(f"  Error checking {ticker}: {e}")
                    continue

            await db.commit()

    finally:
        await client.close()

    print(f"Updated {updated}/{checked} markets with settlement data")
    return {"checked": checked, "updated": updated}


async def get_kalshi_whale_performance() -> dict:
    """
    Calculate whale trade performance on settled markets.
    Returns win rate and edge for backtesting analysis.
    """
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Get whale trades with settlement data
        result = await db.execute(
            """SELECT t.*, m.result, m.category as market_category
               FROM kalshi_trades t
               JOIN kalshi_markets m ON t.ticker = m.ticker
               WHERE t.is_whale = 1 AND m.result IS NOT NULL"""
        )
        trades = [dict(row) for row in await result.fetchall()]

        if not trades:
            return {"settled_trades": 0, "message": "No settled whale trades yet"}

        wins = 0
        losses = 0
        total_expected = 0
        total_pnl = 0
        by_category = {}

        for t in trades:
            side = t["taker_side"]
            result_val = t["result"]
            price = t["price"]
            usd = t["usd_value"]
            category = t.get("category") or t.get("market_category") or "other"

            won = (side == result_val)
            expected_prob = price / 100
            total_expected += expected_prob

            contracts = usd / (price / 100) if price > 0 else 0
            if won:
                wins += 1
                pnl = contracts * (100 - price) / 100
            else:
                losses += 1
                pnl = -usd

            total_pnl += pnl

            if category not in by_category:
                by_category[category] = {"wins": 0, "losses": 0, "pnl": 0}
            by_category[category]["wins" if won else "losses"] += 1
            by_category[category]["pnl"] += pnl

        total = wins + losses
        actual_wr = wins / total if total > 0 else 0
        expected_wr = total_expected / total if total > 0 else 0
        edge = actual_wr - expected_wr

        return {
            "settled_trades": total,
            "wins": wins,
            "losses": losses,
            "actual_win_rate": round(actual_wr * 100, 1),
            "expected_win_rate": round(expected_wr * 100, 1),
            "edge": round(edge * 100, 1),
            "total_pnl": round(total_pnl, 2),
            "by_category": by_category
        }
