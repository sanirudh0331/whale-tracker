# Whale Tracker

Unified dashboard for tracking large trades on Kalshi and Polymarket prediction markets.

## Features

- Real-time whale trade tracking for both platforms
- Platform toggle to switch between Kalshi and Polymarket
- Auto-refresh every 5 minutes
- Volume spike detection
- Top markets by volume

## Local Development

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run locally
python run.py
```

Visit http://localhost:8000

## Deploy to Render (Free)

1. Push to GitHub
2. Connect repo to Render
3. Create new Web Service
4. Render will auto-detect `render.yaml`

## Environment Variables

- `KALSHI_THRESHOLD`: Minimum USD for Kalshi whale trade (default: 500)
- `POLYMARKET_THRESHOLD`: Minimum USD for Polymarket whale (default: 25000)
- `FETCH_INTERVAL_MINUTES`: Data fetch interval (default: 5)
- `DASHBOARD_REFRESH_SECONDS`: UI refresh interval (default: 300)
