import time
import httpx
import aiosqlite
from typing import Optional
from app.config import POLYMARKET_GAMMA_API, POLYMARKET_THRESHOLD, VOLUME_SPIKE_MULTIPLIER, DATABASE_PATH
from app.insider import calculate_insider_score, get_insider_label, get_insider_color


class PolymarketClient:
    def __init__(self):
        self.client = httpx.AsyncClient(
            base_url=POLYMARKET_GAMMA_API,
            timeout=30.0,
            headers={"Accept": "application/json"}
        )
        self.market_cache = {}

    async def close(self):
        await self.client.aclose()

    async def get_markets(self, limit: int = 100, offset: int = 0) -> list[dict]:
        try:
            resp = await self.client.get(
                "/markets",
                params={"limit": limit, "offset": offset, "active": "true", "closed": "false"}
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Polymarket markets error: {e}")
            return []

    async def get_all_markets(self) -> list[dict]:
        all_markets = []
        offset = 0
        while True:
            markets = await self.get_markets(limit=100, offset=offset)
            if not markets:
                break
            all_markets.extend(markets)
            if len(markets) < 100 or offset > 400:
                break
            offset += 100
        return all_markets

    async def get_top_volume_markets(self, limit: int = 50) -> list[dict]:
        try:
            resp = await self.client.get(
                "/markets",
                params={"limit": limit, "order": "volume24hr", "ascending": "false", "active": "true"}
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Polymarket top markets error: {e}")
            return []

    async def get_market_by_id(self, market_id: str) -> Optional[dict]:
        try:
            resp = await self.client.get(f"/markets/{market_id}")
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None


async def fetch_polymarket_data() -> dict:
    """Fetch and process Polymarket high-volume markets."""
    client = PolymarketClient()
    whale_alerts = []
    volume_alerts = []

    try:
        # Get high-volume markets with price data
        markets = await client.get_top_volume_markets(limit=50)
        print(f"Polymarket: fetched {len(markets)} active markets")

        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row

            for market in markets:
                try:
                    market_id = market.get("conditionId", market.get("condition_id", market.get("id", "")))
                    if not market_id:
                        continue

                    question = market.get("question", "Unknown")
                    slug = market.get("slug", "")
                    volume_24h = float(market.get("volume24hr", 0) or 0)

                    # Parse outcome prices (Yes/No)
                    outcomes = market.get("outcomes", ["Yes", "No"])
                    outcome_prices = market.get("outcomePrices", ["0.5", "0.5"])

                    # Parse prices
                    try:
                        if isinstance(outcome_prices, str):
                            outcome_prices = outcome_prices.strip("[]").replace('"', '').split(",")
                        yes_price = float(outcome_prices[0]) if outcome_prices else 0.5
                        no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 1 - yes_price
                    except:
                        yes_price, no_price = 0.5, 0.5

                    # Determine dominant side based on price movement
                    last_price = float(market.get("lastTradePrice", 0.5) or 0.5)
                    price_change = float(market.get("oneDayPriceChange", 0) or 0)

                    # If price went up, more people buying YES; if down, more buying NO
                    if price_change > 0.02:
                        side = f"YES @ {yes_price*100:.0f}%"
                    elif price_change < -0.02:
                        side = f"NO @ {no_price*100:.0f}%"
                    else:
                        side = f"YES {yes_price*100:.0f}% / NO {no_price*100:.0f}%"

                    # Create trade record for high-volume markets
                    if volume_24h >= POLYMARKET_THRESHOLD:
                        trade_id = f"mkt-{market_id}-{int(time.time())//3600}"

                        existing = await db.execute(
                            "SELECT id FROM polymarket_trades WHERE id = ?", (trade_id,)
                        )
                        if not await existing.fetchone():
                            # Calculate insider score
                            score_data = await calculate_insider_score(
                                platform="polymarket",
                                usd_value=volume_24h,
                                threshold=POLYMARKET_THRESHOLD,
                                price=yes_price,
                                side="activity",
                                market_title=question,
                                market_id=market_id,
                                timestamp=int(time.time()),
                                volume_24h=volume_24h
                            )
                            insider_score = score_data["insider_score"]

                            await db.execute(
                                """INSERT OR IGNORE INTO polymarket_trades
                                   (id, market_id, market_question, wallet, side, size, price, timestamp, is_whale, insider_score)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                (trade_id, market_id, question, "market", side,
                                 volume_24h, yes_price, int(time.time()), 1, insider_score)
                            )

                            message = f"${volume_24h:,.0f} vol - {side}: {question[:35]}"
                            await db.execute(
                                """INSERT INTO alerts (platform, alert_type, identifier, title, message, trade_size, timestamp)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                ("polymarket", "whale", market_id, question, message, volume_24h, int(time.time()))
                            )
                            whale_alerts.append({
                                "market_id": market_id,
                                "question": question,
                                "side": side,
                                "volume_24h": volume_24h
                            })

                    # Update market data
                    existing = await db.execute(
                        "SELECT volume_avg FROM polymarket_markets WHERE id = ?", (market_id,)
                    )
                    row = await existing.fetchone()

                    if row and row["volume_avg"] > 0:
                        avg = row["volume_avg"]
                        if volume_24h > avg * VOLUME_SPIKE_MULTIPLIER:
                            ratio = volume_24h / avg
                            volume_alerts.append({"market_id": market_id, "ratio": ratio})

                    new_avg = (volume_24h + (row["volume_avg"] if row else 0)) / 2 if row else volume_24h

                    await db.execute(
                        """INSERT OR REPLACE INTO polymarket_markets
                           (id, slug, question, volume_24h, volume_avg)
                           VALUES (?, ?, ?, ?, ?)""",
                        (market_id, slug, question, volume_24h, new_avg)
                    )

                except Exception as e:
                    print(f"Polymarket market error: {e}")
                    continue

            await db.commit()

    finally:
        await client.close()

    return {"whale_alerts": len(whale_alerts), "volume_alerts": len(volume_alerts)}


async def get_polymarket_whale_trades(limit: int = 30, insider_only: bool = False, min_threshold: int = 0, hours: int = 24, sort: str = "newest") -> list[dict]:
    if min_threshold <= 0:
        min_threshold = POLYMARKET_THRESHOLD

    min_timestamp = int(time.time()) - (hours * 3600)

    # Build ORDER BY clause based on sort parameter
    sort_clauses = {
        "newest": "timestamp DESC",
        "oldest": "timestamp ASC",
        "size_desc": "size DESC",
        "size_asc": "size ASC"
    }
    order_by = sort_clauses.get(sort, "timestamp DESC")

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        if insider_only:
            result = await db.execute(
                f"""SELECT * FROM polymarket_trades
                   WHERE size >= ? AND insider_score >= 50 AND timestamp >= ?
                   ORDER BY {order_by} LIMIT ?""",
                (min_threshold, min_timestamp, limit)
            )
        else:
            result = await db.execute(
                f"""SELECT * FROM polymarket_trades
                   WHERE size >= ? AND timestamp >= ?
                   ORDER BY {order_by} LIMIT ?""",
                (min_threshold, min_timestamp, limit)
            )
        rows = await result.fetchall()
        trades = []
        for row in rows:
            trade = dict(row)
            wallet = trade.get("wallet", "")
            trade["wallet_short"] = f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) > 10 else wallet
            trade["insider_label"] = get_insider_label(trade.get("insider_score", 0))
            trade["insider_color"] = get_insider_color(trade.get("insider_score", 0))
            trades.append(trade)
        return trades


async def get_polymarket_top_markets(limit: int = 15) -> list[dict]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        result = await db.execute(
            """SELECT * FROM polymarket_markets ORDER BY volume_24h DESC LIMIT ?""",
            (limit,)
        )
        return [dict(row) for row in await result.fetchall()]


async def get_polymarket_stats() -> dict:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row

        total = await db.execute("SELECT COUNT(*) as c FROM polymarket_trades")
        total = (await total.fetchone())["c"]

        whales = await db.execute("SELECT COUNT(*) as c FROM polymarket_trades WHERE is_whale = 1")
        whales = (await whales.fetchone())["c"]

        markets = await db.execute("SELECT COUNT(*) as c FROM polymarket_markets")
        markets = (await markets.fetchone())["c"]

        volume = await db.execute("SELECT COALESCE(SUM(size), 0) as v FROM polymarket_trades WHERE is_whale = 1")
        volume = (await volume.fetchone())["v"]

        return {
            "total_trades": total,
            "whale_trades": whales,
            "markets_tracked": markets,
            "whale_volume": volume,
            "threshold": POLYMARKET_THRESHOLD
        }
