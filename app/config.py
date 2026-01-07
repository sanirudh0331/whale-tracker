import os

# Kalshi settings
KALSHI_THRESHOLD = int(os.getenv("KALSHI_THRESHOLD", "500"))
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Polymarket settings
POLYMARKET_THRESHOLD = int(os.getenv("POLYMARKET_THRESHOLD", "25000"))
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"

# Common settings
VOLUME_SPIKE_MULTIPLIER = float(os.getenv("VOLUME_SPIKE_MULTIPLIER", "2.5"))
FETCH_INTERVAL_MINUTES = int(os.getenv("FETCH_INTERVAL_MINUTES", "5"))
DASHBOARD_REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "300"))

DATABASE_PATH = os.getenv("DATABASE_PATH", "whales.db")
