import time
import aiosqlite
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from app.config import (
    KALSHI_THRESHOLD,
    POLYMARKET_THRESHOLD,
    DASHBOARD_REFRESH_SECONDS,
    DATABASE_PATH
)
from app.database import init_db
from app.scheduler import start_scheduler, stop_scheduler, fetch_job
from app.kalshi import get_kalshi_whale_trades, get_kalshi_top_markets, get_kalshi_stats
from app.polymarket import get_polymarket_whale_trades, get_polymarket_top_markets, get_polymarket_stats


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    await init_db()
    start_scheduler()
    asyncio.create_task(fetch_job())
    yield
    stop_scheduler()


app = FastAPI(title="Whale Tracker", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


def timestamp_to_relative(ts: int) -> str:
    diff = int(time.time()) - ts
    if diff < 60:
        return "just now"
    elif diff < 3600:
        return f"{diff // 60}m ago"
    elif diff < 86400:
        return f"{diff // 3600}h ago"
    else:
        return f"{diff // 86400}d ago"


def format_number(n: float) -> str:
    if n >= 1_000_000:
        return f"${n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"${n/1_000:.1f}K"
    else:
        return f"${n:,.0f}"


templates.env.filters["timestamp_to_relative"] = timestamp_to_relative
templates.env.filters["format_number"] = format_number


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, platform: str = "kalshi", insider: str = "", threshold: int = 0, hours: int = 24):
    # Use custom threshold or default
    if threshold <= 0:
        threshold = KALSHI_THRESHOLD if platform == "kalshi" else POLYMARKET_THRESHOLD
    # Clamp hours to valid range
    if hours not in [1, 6, 24, 168]:
        hours = 24

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "platform": platform,
            "insider_only": insider == "1",
            "threshold": threshold,
            "hours": hours,
            "kalshi_threshold": KALSHI_THRESHOLD,
            "polymarket_threshold": POLYMARKET_THRESHOLD,
            "refresh_seconds": DASHBOARD_REFRESH_SECONDS
        }
    )


@app.get("/partials/whale-trades", response_class=HTMLResponse)
async def whale_trades_partial(request: Request, platform: str = "kalshi", insider: str = "", threshold: int = 0, hours: int = 24):
    insider_only = insider == "1"
    default_threshold = KALSHI_THRESHOLD if platform == "kalshi" else POLYMARKET_THRESHOLD
    if threshold <= 0:
        threshold = default_threshold
    if hours not in [1, 6, 24, 168]:
        hours = 24

    if platform == "kalshi":
        trades = await get_kalshi_whale_trades(limit=50, insider_only=insider_only, min_threshold=threshold, hours=hours)
    else:
        trades = await get_polymarket_whale_trades(limit=50, insider_only=insider_only, min_threshold=threshold, hours=hours)

    return templates.TemplateResponse(
        "components/whale_trades.html",
        {
            "request": request,
            "trades": trades,
            "platform": platform,
            "threshold": threshold,
            "insider_only": insider_only,
            "hours": hours
        }
    )


@app.get("/partials/top-markets", response_class=HTMLResponse)
async def top_markets_partial(request: Request, platform: str = "kalshi"):
    if platform == "kalshi":
        markets = await get_kalshi_top_markets(limit=15)
    else:
        markets = await get_polymarket_top_markets(limit=15)

    return templates.TemplateResponse(
        "components/top_markets.html",
        {
            "request": request,
            "markets": markets,
            "platform": platform
        }
    )


@app.get("/partials/alerts", response_class=HTMLResponse)
async def alerts_partial(request: Request, platform: str = "kalshi"):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        result = await db.execute(
            """SELECT * FROM alerts WHERE platform = ? ORDER BY timestamp DESC LIMIT 15""",
            (platform,)
        )
        alerts = [dict(row) for row in await result.fetchall()]

    return templates.TemplateResponse(
        "components/alerts.html",
        {
            "request": request,
            "alerts": alerts,
            "platform": platform
        }
    )


@app.get("/partials/stats", response_class=HTMLResponse)
async def stats_partial(request: Request, platform: str = "kalshi"):
    if platform == "kalshi":
        stats = await get_kalshi_stats()
    else:
        stats = await get_polymarket_stats()

    return templates.TemplateResponse(
        "components/stats.html",
        {
            "request": request,
            "stats": stats,
            "platform": platform
        }
    )


# API endpoints
@app.get("/api/whale-trades")
async def api_whale_trades(platform: str = "kalshi", insider: str = ""):
    insider_only = insider == "1"
    if platform == "kalshi":
        trades = await get_kalshi_whale_trades(limit=50, insider_only=insider_only)
    else:
        trades = await get_polymarket_whale_trades(limit=50, insider_only=insider_only)
    return {"trades": trades, "platform": platform, "insider_only": insider_only}


@app.get("/api/markets")
async def api_markets(platform: str = "kalshi"):
    if platform == "kalshi":
        markets = await get_kalshi_top_markets(limit=50)
    else:
        markets = await get_polymarket_top_markets(limit=50)
    return {"markets": markets, "platform": platform}


@app.get("/api/stats")
async def api_stats(platform: str = "kalshi"):
    if platform == "kalshi":
        stats = await get_kalshi_stats()
    else:
        stats = await get_polymarket_stats()
    return {"stats": stats, "platform": platform}


@app.post("/api/refresh")
async def trigger_refresh():
    await fetch_job()
    return {"status": "ok", "message": "Refresh triggered"}


@app.get("/health")
async def health():
    return {"status": "healthy"}
