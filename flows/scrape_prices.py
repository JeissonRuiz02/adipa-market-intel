"""
adipa-market-intel — light pipeline (Prefect flow)

Flow    : scrape-prices
Schedule: every 15 minutes via Prefect (light-pool)
Worker  : worker-light (httpx + BeautifulSoup)

Checks prices of known courses via plain HTTP (no JS rendering).
Results are UPSERTed into raw_courses.

The heavy worker (worker-heavy) runs Playwright once per day to scrape the
full catalog. This flow only refreshes prices for courses already known.
"""

from __future__ import annotations

import os
import re
import json
import time
from dataclasses import dataclass

import httpx
import psycopg2
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

DATABASE_URL = os.environ["DATABASE_URL"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-419,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TIMEOUT = httpx.Timeout(15.0, connect=5.0)

# Seed courses used for price verification.
# The full catalog is maintained by worker-heavy (Playwright).
KNOWN_COURSES = [
    # (source, external_id, url, fallback_price, currency)
    ("platzi",   "suscripcion-colombia",
     "https://platzi.com/precios/",           29.0, "USD"),
    ("coursera", "the-science-of-well-being",
     "https://www.coursera.org/learn/the-science-of-well-being", 49.0, "USD"),
    ("udemy",    "ansiedad-y-estres",
     "https://www.udemy.com/course/supera-el-estres-y-la-ansiedad/", 84.90, "USD"),
]


@dataclass
class PriceUpdate:
    source: str
    external_id: str
    price_original: float | None
    price_discount: float | None
    currency: str
    url: str
    reachable: bool


# ── Tasks ──────────────────────────────────────

@task(name="check-source-prices", retries=2, retry_delay_seconds=10)
def check_source_prices(client: httpx.Client) -> list[PriceUpdate]:
    """Fetch each known course URL and extract its current price from JSON-LD."""
    logger = get_run_logger()
    updates: list[PriceUpdate] = []

    for source, external_id, url, fallback_price, currency in KNOWN_COURSES:
        try:
            response = client.get(url, timeout=TIMEOUT, headers=HEADERS)
            reachable = response.status_code == 200

            price = None
            discount = None

            if reachable:
                ld_matches = re.findall(
                    r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
                    response.text, re.DOTALL
                )
                for raw in ld_matches:
                    try:
                        data = json.loads(raw)
                        offers = data.get("offers", {})
                        if isinstance(offers, dict):
                            price_str = str(offers.get("price", ""))
                            price = float(price_str) if price_str else None
                            currency = offers.get("priceCurrency", currency)
                    except Exception:
                        continue

                if price is None:
                    price = fallback_price

            updates.append(PriceUpdate(
                source=source,
                external_id=external_id,
                price_original=price or fallback_price,
                price_discount=discount,
                currency=currency,
                url=url,
                reachable=reachable,
            ))

            status = "ok" if reachable else "unreachable"
            logger.info(f"[{source}] {status} price=${price or fallback_price} {currency}")

        except httpx.HTTPError as e:
            logger.warning(f"[{source}] HTTP error: {e}")
            updates.append(PriceUpdate(
                source=source,
                external_id=external_id,
                price_original=fallback_price,
                price_discount=None,
                currency=currency,
                url=url,
                reachable=False,
            ))

    return updates


@task(name="upsert-price-updates")
def upsert_price_updates(updates: list[PriceUpdate]) -> int:
    """
    Upsert price data into raw_courses.
    If the course doesn't exist yet (worker-heavy adds full metadata), inserts
    a minimal record so the price is captured immediately.
    """
    logger = get_run_logger()

    if not updates:
        return 0

    sql = """
        INSERT INTO raw_courses (
            source, external_id, title, category,
            price_original, price_discount, currency, url, scraped_at
        ) VALUES (
            %(source)s, %(external_id)s, %(title)s, 'psicología',
            %(price_original)s, %(price_discount)s, %(currency)s, %(url)s, NOW()
        )
        ON CONFLICT (source, external_id) DO UPDATE SET
            price_original = EXCLUDED.price_original,
            price_discount = EXCLUDED.price_discount,
            currency       = EXCLUDED.currency,
            scraped_at     = EXCLUDED.scraped_at
    """

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                records = [
                    {
                        "source":         u.source,
                        "external_id":    u.external_id,
                        "title":          u.external_id.replace("-", " ").title(),
                        "price_original": u.price_original,
                        "price_discount": u.price_discount,
                        "currency":       u.currency,
                        "url":            u.url,
                    }
                    for u in updates
                ]
                cur.executemany(sql, records)
                count = cur.rowcount
        logger.info(f"Prices updated: {count} records")
        return count
    finally:
        conn.close()


@task(name="log-light-execution")
def log_execution(
    flow_run_id: str,
    updates: list[PriceUpdate],
    duration_ms: int,
) -> None:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                for u in updates:
                    cur.execute(
                        """
                        INSERT INTO scrape_log
                            (pipeline, source, flow_run_id, status,
                             courses_upserted, duration_ms)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        ("light", u.source, flow_run_id,
                         "success" if u.reachable else "unreachable",
                         1, duration_ms),
                    )
    finally:
        conn.close()


# ── Flow ───────────────────────────────────────

@flow(
    name="scrape-prices",
    description=(
        "Light pipeline: price check every 15 min via HTTP. "
        "Full Playwright catalog scrape runs in worker-heavy every 24h."
    ),
    retries=1,
    retry_delay_seconds=30,
)
def scrape_prices() -> None:
    logger = get_run_logger()

    try:
        ctx = get_run_context()
        flow_run_id = str(ctx.flow_run.id)
    except Exception:
        flow_run_id = "local"

    logger.info(f"Starting scrape_prices — flow_run_id={flow_run_id}")
    start = time.monotonic()

    with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
        updates = check_source_prices(client)

    upsert_price_updates(updates)

    duration_ms = int((time.monotonic() - start) * 1000)
    log_execution(flow_run_id, updates, duration_ms)

    reachable = sum(1 for u in updates if u.reachable)
    logger.info(
        f"scrape_prices done in {duration_ms}ms — "
        f"{reachable}/{len(updates)} sources reachable"
    )


if __name__ == "__main__":
    scrape_prices()
