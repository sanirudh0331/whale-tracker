#!/usr/bin/env python3
"""
Deep dive into sports whale trades specifically.
Check if edge is real or just favorite bias.
"""

import httpx
import time
from collections import defaultdict

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
WHALE_THRESHOLD = 250


def is_sports(ticker: str, title: str) -> bool:
    """Detect if market is sports-related."""
    ticker_upper = ticker.upper()
    title_lower = title.lower()

    sports_keywords = ['winner', 'wins by', 'total points', 'spread', 'game', 'match', 'vs']
    sports_tickers = ['NBA', 'NFL', 'MLB', 'NHL', 'NCAA', 'EPL', 'MLS', 'UFC', 'BOXING', 'NCAAMB', 'NCAAF']

    if any(kw in title_lower for kw in sports_keywords):
        return True
    if any(t in ticker_upper for t in sports_tickers):
        return True
    return False


def run_analysis():
    print("=" * 80)
    print("SPORTS WHALE DEEP DIVE")
    print("=" * 80)
    print()

    sports_whales = []

    with httpx.Client(timeout=30) as client:
        # Fetch settled markets
        cursor = None
        markets = []

        print("Fetching settled markets...")
        for _ in range(40):
            params = {"status": "settled", "limit": 200}
            if cursor:
                params["cursor"] = cursor

            resp = client.get(f"{KALSHI_API}/markets", params=params)
            if resp.status_code != 200:
                break

            data = resp.json()
            batch = data.get("markets", [])
            markets.extend([m for m in batch if m.get("volume", 0) > 0])

            cursor = data.get("cursor")
            if not cursor:
                break
            time.sleep(0.1)

        print(f"Found {len(markets)} markets with volume")

        # Find sports markets and their whale trades
        sports_markets = [m for m in markets if is_sports(m["ticker"], m.get("title", ""))]
        print(f"Sports markets: {len(sports_markets)}")
        print()

        print("Fetching trades for sports markets...")
        for i, market in enumerate(sports_markets):
            ticker = market["ticker"]
            title = market.get("title", "")
            result = market.get("result")

            if not result:
                continue

            resp = client.get(f"{KALSHI_API}/markets/trades", params={"ticker": ticker, "limit": 500})
            if resp.status_code != 200:
                continue

            trades = resp.json().get("trades", [])

            for t in trades:
                count = t.get("count", 0)
                side = t.get("taker_side", "yes")
                yes_price = t.get("yes_price", 50)
                no_price = t.get("no_price", 50)
                price = yes_price if side == "yes" else no_price
                usd = count * price / 100

                if usd >= WHALE_THRESHOLD:
                    won = (side == result)
                    sports_whales.append({
                        "ticker": ticker,
                        "title": title,
                        "side": side,
                        "price": price,
                        "usd": usd,
                        "result": result,
                        "won": won
                    })

            if (i + 1) % 20 == 0:
                print(f"  Processed {i + 1}/{len(sports_markets)} sports markets...")

            time.sleep(0.05)

    print()
    print("=" * 80)
    print(f"FOUND {len(sports_whales)} SPORTS WHALE TRADES")
    print("=" * 80)

    if not sports_whales:
        print("No sports whale trades found.")
        return

    # Analysis
    wins = [w for w in sports_whales if w["won"]]
    losses = [w for w in sports_whales if not w["won"]]

    print(f"\nWins: {len(wins)} | Losses: {len(losses)}")
    print(f"Win rate: {len(wins)/len(sports_whales):.1%}")

    # Price distribution
    print()
    print("-" * 40)
    print("PRICE DISTRIBUTION (are they just betting favorites?)")
    print("-" * 40)

    price_buckets = defaultdict(lambda: {"count": 0, "wins": 0})
    for w in sports_whales:
        if w["price"] < 40:
            bucket = "Underdog (<40¢)"
        elif w["price"] < 60:
            bucket = "Tossup (40-60¢)"
        elif w["price"] < 80:
            bucket = "Lean Favorite (60-80¢)"
        else:
            bucket = "Heavy Favorite (80¢+)"

        price_buckets[bucket]["count"] += 1
        if w["won"]:
            price_buckets[bucket]["wins"] += 1

    for bucket in ["Underdog (<40¢)", "Tossup (40-60¢)", "Lean Favorite (60-80¢)", "Heavy Favorite (80¢+)"]:
        data = price_buckets[bucket]
        if data["count"] > 0:
            actual_wr = data["wins"] / data["count"]
            print(f"{bucket:25} | {data['count']:3} trades | {data['wins']}W | WR: {actual_wr:.0%}")

    # Edge by price bucket
    print()
    print("-" * 40)
    print("EDGE BY PRICE BUCKET (actual vs expected)")
    print("-" * 40)

    for bucket in ["Underdog (<40¢)", "Tossup (40-60¢)", "Lean Favorite (60-80¢)", "Heavy Favorite (80¢+)"]:
        trades_in_bucket = [w for w in sports_whales if
            (bucket == "Underdog (<40¢)" and w["price"] < 40) or
            (bucket == "Tossup (40-60¢)" and 40 <= w["price"] < 60) or
            (bucket == "Lean Favorite (60-80¢)" and 60 <= w["price"] < 80) or
            (bucket == "Heavy Favorite (80¢+)" and w["price"] >= 80)]

        if trades_in_bucket:
            actual_wr = sum(1 for t in trades_in_bucket if t["won"]) / len(trades_in_bucket)
            expected_wr = sum(t["price"]/100 for t in trades_in_bucket) / len(trades_in_bucket)
            edge = actual_wr - expected_wr
            print(f"{bucket:25} | Actual: {actual_wr:.0%} | Expected: {expected_wr:.0%} | Edge: {edge:+.0%}")

    # Show some example trades
    print()
    print("-" * 40)
    print("SAMPLE WINNING TRADES")
    print("-" * 40)
    for w in wins[:10]:
        print(f"  ${w['usd']:>7,.0f} | {w['side'].upper():3} @ {w['price']}¢ | {w['title'][:45]}")

    print()
    print("-" * 40)
    print("SAMPLE LOSING TRADES")
    print("-" * 40)
    for w in losses[:10]:
        print(f"  ${w['usd']:>7,.0f} | {w['side'].upper():3} @ {w['price']}¢ | {w['title'][:45]}")

    # Overall expected vs actual
    print()
    print("=" * 80)
    print("OVERALL SUMMARY")
    print("=" * 80)
    avg_price = sum(w["price"] for w in sports_whales) / len(sports_whales)
    expected_wr = sum(w["price"]/100 for w in sports_whales) / len(sports_whales)
    actual_wr = len(wins) / len(sports_whales)

    print(f"Average price paid:     {avg_price:.0f}¢")
    print(f"Expected win rate:      {expected_wr:.1%}")
    print(f"Actual win rate:        {actual_wr:.1%}")
    print(f"Edge:                   {actual_wr - expected_wr:+.1%}")
    print()

    if actual_wr - expected_wr > 0.05:
        print("VERDICT: Real edge - whales outperforming across price buckets")
    elif avg_price > 75:
        print("VERDICT: Mostly favorite bias - whales betting heavy favorites")
    else:
        print("VERDICT: Mixed signal - need more data")


if __name__ == "__main__":
    run_analysis()
