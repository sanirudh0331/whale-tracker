from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.config import FETCH_INTERVAL_MINUTES
from app.kalshi import fetch_kalshi_data
from app.polymarket import fetch_polymarket_data

scheduler = AsyncIOScheduler()


async def fetch_job():
    """Periodic job to fetch data from both platforms."""
    print(f"Running fetch job...")
    try:
        kalshi_result = await fetch_kalshi_data()
        print(f"Kalshi: {kalshi_result['whale_alerts']} whales, {kalshi_result['volume_alerts']} spikes")
    except Exception as e:
        print(f"Kalshi fetch error: {e}")

    try:
        poly_result = await fetch_polymarket_data()
        print(f"Polymarket: {poly_result['whale_alerts']} whales, {poly_result['volume_alerts']} spikes")
    except Exception as e:
        print(f"Polymarket fetch error: {e}")


def start_scheduler():
    scheduler.add_job(
        fetch_job,
        "interval",
        minutes=FETCH_INTERVAL_MINUTES,
        id="fetch_data",
        replace_existing=True
    )
    scheduler.start()
    print(f"Scheduler started: fetching every {FETCH_INTERVAL_MINUTES} minutes")


def stop_scheduler():
    scheduler.shutdown()
