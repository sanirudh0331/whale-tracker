"""
Insider Trading Detection Heuristics

Scoring based on:
- Size outlier (how big vs other whales)
- Contrarian bet (betting against consensus)
- Low liquidity (obscure markets easier to have edge)
- Timing cluster (multiple whales in short window)
- Event market (FDA, court, political events)
"""

import re
import aiosqlite
from app.config import DATABASE_PATH

# Event-related keywords that suggest potential insider knowledge
EVENT_KEYWORDS = [
    # Regulatory/Legal
    'fda', 'approval', 'court', 'ruling', 'verdict', 'trial', 'judge',
    'sec', 'ftc', 'doj', 'indictment', 'settlement', 'lawsuit',
    # Political
    'announce', 'resign', 'nomination', 'confirm', 'veto', 'executive order',
    'pardon', 'impeach', 'electoral', 'delegate',
    # Corporate
    'earnings', 'merger', 'acquisition', 'ipo', 'bankruptcy',
    # Other time-sensitive
    'deadline', 'vote', 'decision', 'result', 'winner', 'before',
]


def calculate_size_score(usd_value: float, threshold: float) -> float:
    """
    Score based on how much larger than threshold.
    2x threshold = 50%, 5x = 80%, 10x+ = 100%
    """
    ratio = usd_value / threshold
    if ratio <= 1:
        return 0.0
    elif ratio <= 2:
        return 0.3
    elif ratio <= 5:
        return 0.3 + (ratio - 2) * 0.167  # 0.3 to 0.8
    elif ratio <= 10:
        return 0.8 + (ratio - 5) * 0.04  # 0.8 to 1.0
    else:
        return 1.0


def calculate_contrarian_score(price: float, side: str, platform: str) -> float:
    """
    Score based on betting against consensus.
    Buying YES at 10% or NO at 90% is very contrarian.
    """
    if platform == "kalshi":
        # Kalshi price is in cents (0-100)
        prob = price / 100.0
    else:
        # Polymarket price is 0-1
        prob = price if price <= 1 else price / 100.0

    # Normalize side
    side_lower = side.lower()
    is_yes = side_lower in ('yes', 'buy')

    if is_yes:
        # Buying YES - more contrarian at lower probabilities
        if prob <= 0.15:
            return 1.0
        elif prob <= 0.25:
            return 0.7
        elif prob <= 0.35:
            return 0.4
        else:
            return 0.0
    else:
        # Buying NO - more contrarian at higher probabilities
        if prob >= 0.85:
            return 1.0
        elif prob >= 0.75:
            return 0.7
        elif prob >= 0.65:
            return 0.4
        else:
            return 0.0


def calculate_event_score(market_title: str) -> float:
    """
    Score based on event-related keywords.
    More keywords = higher chance of time-sensitive insider info.
    """
    if not market_title:
        return 0.0

    title_lower = market_title.lower()
    matches = sum(1 for kw in EVENT_KEYWORDS if kw in title_lower)

    if matches >= 3:
        return 1.0
    elif matches == 2:
        return 0.7
    elif matches == 1:
        return 0.4
    else:
        return 0.0


def calculate_liquidity_score(volume_24h: float, platform: str) -> float:
    """
    Score based on market liquidity.
    Lower liquidity = easier to have informational edge.
    """
    if platform == "kalshi":
        # Kalshi volumes are smaller
        if volume_24h <= 1000:
            return 1.0
        elif volume_24h <= 5000:
            return 0.7
        elif volume_24h <= 20000:
            return 0.4
        else:
            return 0.1
    else:
        # Polymarket volumes are larger
        if volume_24h <= 10000:
            return 1.0
        elif volume_24h <= 50000:
            return 0.7
        elif volume_24h <= 200000:
            return 0.4
        else:
            return 0.1


async def calculate_timing_cluster_score(
    platform: str,
    market_id: str,
    timestamp: int,
    window_minutes: int = 30
) -> float:
    """
    Score based on other whale trades on same market within time window.
    Multiple whales converging = potential coordinated insider activity.
    """
    window_seconds = window_minutes * 60

    async with aiosqlite.connect(DATABASE_PATH) as db:
        if platform == "kalshi":
            result = await db.execute(
                """SELECT COUNT(*) as cnt FROM kalshi_trades
                   WHERE ticker = ? AND is_whale = 1
                   AND timestamp BETWEEN ? AND ?""",
                (market_id, timestamp - window_seconds, timestamp + window_seconds)
            )
        else:
            result = await db.execute(
                """SELECT COUNT(*) as cnt FROM polymarket_trades
                   WHERE market_id = ? AND is_whale = 1
                   AND timestamp BETWEEN ? AND ?""",
                (market_id, timestamp - window_seconds, timestamp + window_seconds)
            )

        row = await result.fetchone()
        count = row[0] if row else 0

    # Exclude the trade itself
    other_whales = max(0, count - 1)

    if other_whales >= 3:
        return 1.0
    elif other_whales == 2:
        return 0.7
    elif other_whales == 1:
        return 0.4
    else:
        return 0.0


async def calculate_insider_score(
    platform: str,
    usd_value: float,
    threshold: float,
    price: float,
    side: str,
    market_title: str,
    market_id: str,
    timestamp: int,
    volume_24h: float = 0
) -> dict:
    """
    Calculate composite insider probability score.
    Returns dict with total score and component breakdown.
    """
    size_score = calculate_size_score(usd_value, threshold)
    contrarian_score = calculate_contrarian_score(price, side, platform)
    event_score = calculate_event_score(market_title)
    liquidity_score = calculate_liquidity_score(volume_24h, platform)
    timing_score = await calculate_timing_cluster_score(platform, market_id, timestamp)

    # Weighted combination
    total_score = (
        0.20 * size_score +
        0.25 * contrarian_score +
        0.15 * liquidity_score +
        0.20 * timing_score +
        0.20 * event_score
    )

    return {
        "insider_score": round(total_score * 100, 1),
        "size_score": round(size_score * 100, 1),
        "contrarian_score": round(contrarian_score * 100, 1),
        "event_score": round(event_score * 100, 1),
        "liquidity_score": round(liquidity_score * 100, 1),
        "timing_score": round(timing_score * 100, 1),
    }


def get_insider_label(score: float) -> str:
    """Get human-readable label for insider score."""
    if score >= 70:
        return "High"
    elif score >= 50:
        return "Medium"
    elif score >= 30:
        return "Low"
    else:
        return "Unlikely"


def get_insider_color(score: float) -> str:
    """Get CSS color class for insider score."""
    if score >= 70:
        return "text-red-400"
    elif score >= 50:
        return "text-orange-400"
    elif score >= 30:
        return "text-yellow-400"
    else:
        return "text-gray-400"
