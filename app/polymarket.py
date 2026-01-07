import time
import httpx
import aiosqlite
from typing import Optional
from app.config import POLYMARKET_GAMMA_API, POLYMARKET_THRESHOLD, VOLUME_SPIKE_MULTIPLIER, DATABASE_PATH
from app.insider import calculate_insider_score, get_insider_label, get_insider_color


POLYMARKET_DATA_API = "https://data-api.polymarket.com"


class PolymarketClient:
    def __init__(self):
        self.client = httpx.AsyncClient(
            base_url=POLYMARKET_GAMMA_API,
            timeout=30.0,
            headers={"Accept": "application/json"}
        )
        self.data_client = httpx.AsyncClient(
            base_url=POLYMARKET_DATA_API,
            timeout=30.0,
            headers={"Accept": "application/json"}
        )
        self.market_cache = {}

    async def close(self):
        await self.client.aclose()
        await self.data_client.aclose()

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

    async def get_trades(self, limit: int = 500) -> list[dict]:
        """Fetch recent trades from public data API."""
        try:
            resp = await self.data_client.get("/trades", params={"limit": limit})
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            print(f"Polymarket trades error: {e}")
            return []


async def fetch_polymarket_data() -> dict:
    """Fetch and process real Polymarket trades."""
    client = PolymarketClient()
    whale_alerts = []
    volume_alerts = []

    try:
        # Get real trades from data API
        raw_trades = await client.get_trades(limit=500)
        print(f"Polymarket: fetched {len(raw_trades)} real trades")

        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row

            for trade in raw_trades:
                try:
                    # Use transaction hash as unique ID
                    trade_id = trade.get("transactionHash", "")
                    if not trade_id:
                        continue

                    # Check if already exists
                    existing = await db.execute(
                        "SELECT id FROM polymarket_trades WHERE id = ?", (trade_id,)
                    )
                    if await existing.fetchone():
                        continue

                    # Parse trade data
                    size = float(trade.get("size", 0))
                    price = float(trade.get("price", 0.5))
                    usd_value = size * price

                    side = trade.get("side", "BUY").upper()
                    outcome = trade.get("outcome", "")  # e.g., "Yes", "No", "Trump", "Up"

                    # Combine side + outcome for clear display
                    display_side = f"{side} {outcome}" if outcome else side

                    wallet = trade.get("proxyWallet", "")
                    market_id = trade.get("conditionId", "")
                    question = trade.get("title", "Unknown")
                    slug = trade.get("slug", "")
                    timestamp = int(trade.get("timestamp", time.time()))

                    is_whale = 1 if usd_value >= POLYMARKET_THRESHOLD else 0

                    # Calculate insider score for whale trades
                    insider_score = 0
                    if is_whale:
                        market_vol = await db.execute(
                            "SELECT volume_24h FROM polymarket_markets WHERE id = ?", (market_id,)
                        )
                        vol_row = await market_vol.fetchone()
                        volume_24h = vol_row["volume_24h"] if vol_row else 0

                        score_data = await calculate_insider_score(
                            platform="polymarket",
                            usd_value=usd_value,
                            threshold=POLYMARKET_THRESHOLD,
                            price=price,
                            side=side,
                            market_title=question,
                            market_id=market_id,
                            timestamp=timestamp,
                            volume_24h=volume_24h
                        )
                        insider_score = score_data["insider_score"]

                    await db.execute(
                        """INSERT OR IGNORE INTO polymarket_trades
                           (id, market_id, market_question, wallet, side, size, price, timestamp, is_whale, insider_score)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (trade_id, market_id, question, wallet, display_side,
                         usd_value, price, timestamp, is_whale, insider_score)
                    )

                    if is_whale:
                        message = f"${usd_value:,.0f} {display_side}: {question[:35]}"
                        await db.execute(
                            """INSERT INTO alerts (platform, alert_type, identifier, title, message, trade_size, timestamp)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            ("polymarket", "whale", market_id, question, message, usd_value, int(time.time()))
                        )
                        whale_alerts.append({
                            "market_id": market_id,
                            "question": question,
                            "side": display_side,
                            "usd_value": usd_value
                        })

                except Exception as e:
                    print(f"Polymarket trade error: {e}")
                    continue

            await db.commit()

        # Also update market volume data
        markets = await client.get_top_volume_markets(limit=30)
        async with aiosqlite.connect(DATABASE_PATH) as db:
            for market in markets:
                try:
                    market_id = market.get("conditionId", market.get("id", ""))
                    if not market_id:
                        continue
                    question = market.get("question", "")
                    slug = market.get("slug", "")
                    volume_24h = float(market.get("volume24hr", 0) or 0)

                    existing = await db.execute(
                        "SELECT volume_avg FROM polymarket_markets WHERE id = ?", (market_id,)
                    )
                    row = await existing.fetchone()
                    new_avg = (volume_24h + (row["volume_avg"] if row else 0)) / 2 if row else volume_24h

                    await db.execute(
                        """INSERT OR REPLACE INTO polymarket_markets
                           (id, slug, question, volume_24h, volume_avg)
                           VALUES (?, ?, ?, ?, ?)""",
                        (market_id, slug, question, volume_24h, new_avg)
                    )
                except:
                    continue
            await db.commit()

    finally:
        await client.close()

    return {"whale_alerts": len(whale_alerts), "volume_alerts": len(volume_alerts)}


async def get_polymarket_whale_trades(limit: int = 30, insider_only: bool = False, min_threshold: int = 0, hours: int = 24) -> list[dict]:
    if min_threshold <= 0:
        min_threshold = POLYMARKET_THRESHOLD

    min_timestamp = int(time.time()) - (hours * 3600)

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        if insider_only:
            result = await db.execute(
                """SELECT * FROM polymarket_trades
                   WHERE size >= ? AND insider_score >= 50 AND timestamp >= ?
                   ORDER BY insider_score DESC, timestamp DESC LIMIT ?""",
                (min_threshold, min_timestamp, limit)
            )
        else:
            result = await db.execute(
                """SELECT * FROM polymarket_trades
                   WHERE size >= ? AND timestamp >= ?
                   ORDER BY timestamp DESC LIMIT ?""",
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
