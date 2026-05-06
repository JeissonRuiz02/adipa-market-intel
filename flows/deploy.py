"""
adipa-market-intel — Prefect deployment registration

Registers scrape_prices with a 15-minute cron schedule on light-pool.
Creates the work pool if it doesn't exist yet (idempotent).

Note: the heavy pipeline (scraper.py + analyze.py) runs autonomously inside
worker-heavy via a 24-hour shell loop (entrypoint.sh) — it is not registered
as a Prefect deployment.
"""

from __future__ import annotations

import asyncio
import sys
import time

import httpx
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import Deployment
from prefect import get_client

MAX_RETRIES = 10
RETRY_DELAY = 5


async def wait_for_server() -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = httpx.get("http://prefect-server:4200/api/health", timeout=5)
            if r.status_code == 200:
                print(f"Prefect server ready (attempt {attempt})")
                return
        except Exception:
            pass
        print(f"  Waiting for Prefect server... ({attempt}/{MAX_RETRIES})")
        time.sleep(RETRY_DELAY)
    print("Prefect server did not respond — aborting")
    sys.exit(1)


async def ensure_work_pool(name: str) -> None:
    """Create the work pool if it doesn't exist. Idempotent."""
    async with get_client() as client:
        try:
            await client.read_work_pool(name)
            print(f"  Pool already exists: {name}")
        except Exception:
            try:
                await client.create_work_pool(
                    work_pool={"name": name, "type": "process"}
                )
                print(f"  Pool created: {name}")
            except Exception as e:
                print(f"  Pool {name}: {e}")


async def deploy_flows() -> None:
    sys.path.insert(0, "/app/flows")
    from scrape_prices import scrape_prices

    await ensure_work_pool("light-pool")

    schedule = CronSchedule(cron="*/15 * * * *", timezone="UTC")

    deployment = await Deployment.build_from_flow(
        flow=scrape_prices,
        name="scrape-prices-scheduled",
        work_pool_name="light-pool",
        schedules=[{"schedule": schedule}],
        description="Psychology course price check every 15 minutes",
        tags=["light", "scraping", "adipa"],
        version="1.0.0",
    )
    deployment_id = await deployment.apply()
    print(f"  Deployment registered: {deployment_id}")
    print("  Schedule: every 15 minutes (light-pool)")
    print("  Heavy pipeline: runs autonomously in worker-heavy every 24h via shell loop")


async def main() -> None:
    await wait_for_server()
    await deploy_flows()
    print("\nSetup complete — UI: http://localhost:4200")


if __name__ == "__main__":
    asyncio.run(main())
