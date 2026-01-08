#!/usr/bin/env python3
"""
Historical blind backtest for whale trade detection.
Pulls settled markets and their trades from Kalshi API to test if
large trades predict outcomes better than random.
"""

import httpx
import time
import json
from datetime import datetime
from typing import Optional, List, Dict
from collections import defaultdict

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
WHALE_THRESHOLD = 10000  # USD


def fetch_settled_markets(client: httpx.Client, limit: int = 500) -> List[Dict]:
    """Fetch settled markets from Kalshi."""
    markets = []
    cursor = None

    print(f"Fetching settled markets...")
    while len(markets) < limit:
        params = {"status": "settled", "limit": 200}
        if cursor:
            params["cursor"] = cursor

        resp = client.get(f"{KALSHI_API}/markets", params=params)
        if resp.status_code != 200:
            print(f"  Error: {resp.status_code}")
            break

        data = resp.json()
        batch = data.get("markets", [])
        if not batch:
            break

        # Only keep markets with volume (had trades)
        batch = [m for m in batch if m.get("volume", 0) > 0]
        markets.extend(batch)

        cursor = data.get("cursor")
        if not cursor:
            break

        print(f"  Fetched {len(markets)} markets with volume...")
        time.sleep(0.1)

    return markets[:limit]


def fetch_market_trades(client: httpx.Client, ticker: str, limit: int = 1000) -> List[Dict]:
    """Fetch all trades for a specific market."""
    trades = []
    cursor = None

    while len(trades) < limit:
        params = {"ticker": ticker, "limit": 1000}
        if cursor:
            params["cursor"] = cursor

        try:
            resp = client.get(f"{KALSHI_API}/markets/trades", params=params)
            if resp.status_code != 200:
                break

            data = resp.json()
            batch = data.get("trades", [])
            if not batch:
                break

            trades.extend(batch)
            cursor = data.get("cursor")
            if not cursor:
                break
        except Exception as e:
            print(f"    Error fetching trades for {ticker}: {e}")
            break

        time.sleep(0.05)

    return trades


def parse_trade(trade: Dict) -> Dict:
    """Parse trade into normalized format with USD value."""
    yes_price = trade.get("yes_price", 50)
    no_price = trade.get("no_price", 50)
    count = trade.get("count", 0)
    taker_side = trade.get("taker_side", "yes")

    price_cents = yes_price if taker_side == "yes" else no_price
    usd_value = count * price_cents / 100

    return {
        "id": trade.get("trade_id", ""),
        "ticker": trade.get("ticker", ""),
        "taker_side": taker_side,
        "count": count,
        "price": price_cents,
        "usd_value": usd_value,
        "created_time": trade.get("created_time", "")
    }


def run_backtest(num_markets: int = 200, whale_threshold: float = WHALE_THRESHOLD):
    """Run blind historical backtest."""

    print("=" * 80)
    print("HISTORICAL BLIND BACKTEST - WHALE TRADE DETECTION")
    print(f"Whale threshold: ${whale_threshold:,.0f}")
    print("=" * 80)
    print()

    # Stats tracking
    stats = {
        "markets_analyzed": 0,
        "markets_with_whales": 0,
        "total_trades": 0,
        "whale_trades": 0,
        "whale_wins": 0,
        "whale_losses": 0,
        "whale_total_wagered": 0,
        "whale_total_pnl": 0,
        "whale_expected_wins": 0,  # Sum of probabilities
        "non_whale_wins": 0,
        "non_whale_losses": 0,
        "non_whale_expected_wins": 0,
    }

    # Track by market category
    whale_by_category = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0})

    with httpx.Client(timeout=30) as client:
        markets = fetch_settled_markets(client, limit=num_markets)
        print(f"\nFound {len(markets)} settled markets with volume")
        print()

        for i, market in enumerate(markets):
            ticker = market["ticker"]
            title = market.get("title", "")[:50]
            result = market.get("result")  # 'yes' or 'no'

            if not result:
                continue

            # Fetch trades for this market
            trades = fetch_market_trades(client, ticker)
            if not trades:
                continue

            stats["markets_analyzed"] += 1
            stats["total_trades"] += len(trades)

            market_has_whale = False

            for trade in trades:
                parsed = parse_trade(trade)
                usd_value = parsed["usd_value"]
                side = parsed["taker_side"]
                price = parsed["price"]

                won = (side == result)
                expected_prob = price / 100

                if usd_value >= whale_threshold:
                    # Whale trade
                    market_has_whale = True
                    stats["whale_trades"] += 1
                    stats["whale_total_wagered"] += usd_value
                    stats["whale_expected_wins"] += expected_prob

                    contracts = usd_value / (price / 100) if price > 0 else 0
                    if won:
                        stats["whale_wins"] += 1
                        pnl = contracts * (100 - price) / 100
                    else:
                        stats["whale_losses"] += 1
                        pnl = -usd_value

                    stats["whale_total_pnl"] += pnl

                    # Categorize
                    if "Bitcoin" in title or "BTC" in ticker:
                        cat = "Crypto"
                    elif "Ethereum" in title or "ETH" in ticker:
                        cat = "Crypto"
                    elif "S&P" in title or "Nasdaq" in title:
                        cat = "Indices"
                    elif "Winner" in title:
                        cat = "Sports"
                    elif "Trump" in title or "government" in title.lower():
                        cat = "Politics"
                    else:
                        cat = "Other"

                    whale_by_category[cat]["wins" if won else "losses"] += 1
                    whale_by_category[cat]["pnl"] += pnl
                else:
                    # Non-whale trade
                    stats["non_whale_expected_wins"] += expected_prob
                    if won:
                        stats["non_whale_wins"] += 1
                    else:
                        stats["non_whale_losses"] += 1

            if market_has_whale:
                stats["markets_with_whales"] += 1

            if (i + 1) % 20 == 0:
                print(f"  Processed {i + 1}/{len(markets)} markets... "
                      f"({stats['whale_trades']} whale trades found)")

    # Print results
    print()
    print("=" * 80)
    print("RESULTS")
    print("=" * 80)

    print(f"\nMarkets analyzed:       {stats['markets_analyzed']}")
    print(f"Markets with whales:    {stats['markets_with_whales']}")
    print(f"Total trades:           {stats['total_trades']:,}")
    print(f"Whale trades:           {stats['whale_trades']}")

    if stats["whale_trades"] > 0:
        whale_win_rate = stats["whale_wins"] / stats["whale_trades"]
        whale_expected_rate = stats["whale_expected_wins"] / stats["whale_trades"]
        whale_edge = whale_win_rate - whale_expected_rate

        print()
        print("-" * 40)
        print("WHALE TRADES (>= ${:,.0f})".format(whale_threshold))
        print("-" * 40)
        print(f"Total wagered:          ${stats['whale_total_wagered']:,.2f}")
        print(f"Wins / Losses:          {stats['whale_wins']} / {stats['whale_losses']}")
        print(f"Actual win rate:        {whale_win_rate:.1%}")
        print(f"Expected win rate:      {whale_expected_rate:.1%}")
        print(f"Edge:                   {whale_edge:+.1%}")
        print(f"Total P/L:              ${stats['whale_total_pnl']:+,.2f}")
        print(f"ROI:                    {stats['whale_total_pnl']/stats['whale_total_wagered']*100:+.1f}%")

        if whale_edge > 0.05:
            print(f"\n*** SIGNAL: Whales outperforming by {whale_edge:.1%} ***")
        elif whale_edge < -0.05:
            print(f"\n*** SIGNAL: Whales underperforming by {-whale_edge:.1%} ***")

    non_whale_total = stats["non_whale_wins"] + stats["non_whale_losses"]
    if non_whale_total > 0:
        non_whale_win_rate = stats["non_whale_wins"] / non_whale_total
        non_whale_expected = stats["non_whale_expected_wins"] / non_whale_total

        print()
        print("-" * 40)
        print("NON-WHALE TRADES (< ${:,.0f})".format(whale_threshold))
        print("-" * 40)
        print(f"Wins / Losses:          {stats['non_whale_wins']:,} / {stats['non_whale_losses']:,}")
        print(f"Actual win rate:        {non_whale_win_rate:.1%}")
        print(f"Expected win rate:      {non_whale_expected:.1%}")
        print(f"Edge:                   {non_whale_win_rate - non_whale_expected:+.1%}")

    if whale_by_category:
        print()
        print("-" * 40)
        print("WHALE PERFORMANCE BY CATEGORY")
        print("-" * 40)
        for cat, data in sorted(whale_by_category.items(), key=lambda x: -(x[1]["wins"] + x[1]["losses"])):
            total = data["wins"] + data["losses"]
            if total > 0:
                wr = data["wins"] / total
                print(f"{cat:12} | {data['wins']:3}W {data['losses']:3}L | "
                      f"WR: {wr:.0%} | P/L: ${data['pnl']:+,.0f}")

    print()
    print("=" * 80)

    return stats


if __name__ == "__main__":
    import sys

    num_markets = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    threshold = float(sys.argv[2]) if len(sys.argv) > 2 else WHALE_THRESHOLD

    print(f"Usage: python historical_backtest.py [num_markets] [whale_threshold]")
    print(f"Running with: {num_markets} markets, ${threshold:,.0f} threshold")
    print()

    run_backtest(num_markets=num_markets, whale_threshold=threshold)
