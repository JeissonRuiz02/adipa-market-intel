"""
adipa-market-intel — heavy pipeline orchestration (Prefect flow)

This flow can be run manually from the heavy worker:
    docker exec adipa_worker_heavy python /app/flows/analyze_market.py

In normal operation the heavy worker runs scraper.py and analyze.py directly
via entrypoint.sh on a 24-hour shell loop — this file is the Prefect-wrapped
version for on-demand runs with full audit trail.

Steps:
    1. Verify raw_courses has data (fast-fail if light pipeline hasn't run yet)
    2. Delegate all heavy processing to workers/heavy/analyze.py (Polars logic)
    3. Confirm market_report was written for today
    4. Log outcome in scrape_log regardless of success or failure
"""

from __future__ import annotations

import os
import sys
import time

import psycopg2
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

DATABASE_URL = os.environ["DATABASE_URL"]

sys.path.insert(0, "/app/workers/heavy")


# ── Tasks ──────────────────────────────────────

@task(name="check-raw-data")
def check_raw_data() -> int:
    """
    Fail fast if raw_courses is empty.
    Prevents running expensive Polars analysis with no input data.
    """
    logger = get_run_logger()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_courses")
            count = cur.fetchone()[0]
    finally:
        conn.close()

    logger.info(f"raw_courses has {count} records")

    if count == 0:
        raise ValueError(
            "raw_courses is empty — run scrape_prices at least once before analyze_market."
        )

    return count


@task(
    name="run-polars-analysis",
    retries=2,
    retry_delay_seconds=60,
)
def run_polars_analysis() -> dict[str, int]:
    """
    Dynamic import of analyze.py so this flow file stays Polars-free.
    analyze.py only exists (and Polars is only installed) in worker-heavy.
    """
    logger = get_run_logger()

    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "analyze",
        "/app/workers/heavy/analyze.py",
    )
    analyze = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(analyze)

    logger.info("Starting Polars analysis...")
    results = analyze.run_analysis()
    logger.info(f"Analysis complete: {results}")

    return results


@task(name="log-heavy-execution")
def log_heavy_execution(
    flow_run_id: str,
    status: str,
    results: dict[str, int],
    duration_ms: int,
    error_message: str | None = None,
) -> None:
    """One scrape_log row per analyzed source for fine-grained auditing."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                if results:
                    for source, total in results.items():
                        cur.execute(
                            """
                            INSERT INTO scrape_log
                                (pipeline, source, flow_run_id, status,
                                 courses_upserted, error_message, duration_ms)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            ("heavy", source, flow_run_id, status,
                             total, error_message, duration_ms),
                        )
                else:
                    cur.execute(
                        """
                        INSERT INTO scrape_log
                            (pipeline, source, flow_run_id, status,
                             courses_upserted, error_message, duration_ms)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        ("heavy", None, flow_run_id, status,
                         0, error_message, duration_ms),
                    )
    finally:
        conn.close()


@task(name="verify-report-written")
def verify_report_written() -> None:
    """Confirm at least one market_report row exists for today after the UPSERT."""
    logger = get_run_logger()

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source, total_courses, avg_price_usd
                FROM market_report
                WHERE report_date = CURRENT_DATE
                ORDER BY source
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise ValueError("market_report has no rows for today — UPSERT may have failed")

    for source, total, avg_price in rows:
        logger.info(
            f"market_report[{source}]: {total} courses · avg_price_usd=${avg_price}"
        )


# ── Flow ───────────────────────────────────────

@flow(
    name="analyze-market",
    description=(
        "Heavy pipeline: daily market analysis with Polars. "
        "Reads raw_courses from the light pipeline and writes market_report."
    ),
    retries=1,
    retry_delay_seconds=120,
)
def analyze_market() -> None:
    logger = get_run_logger()

    try:
        ctx = get_run_context()
        flow_run_id = str(ctx.flow_run.id)
    except Exception:
        flow_run_id = "local"

    logger.info(f"Starting analyze_market — flow_run_id={flow_run_id}")
    start = time.monotonic()

    raw_count = check_raw_data()
    logger.info(f"Processing {raw_count} raw courses")

    results: dict[str, int] = {}
    status = "success"
    error_message = None

    try:
        results = run_polars_analysis()
        verify_report_written()

    except Exception as exc:
        status = "error"
        error_message = str(exc)
        logger.error(f"Analysis error: {exc}")
        raise

    finally:
        duration_ms = int((time.monotonic() - start) * 1000)
        log_heavy_execution(
            flow_run_id=flow_run_id,
            status=status,
            results=results,
            duration_ms=duration_ms,
            error_message=error_message,
        )
        logger.info(
            f"analyze_market finished in {duration_ms}ms "
            f"— status={status} · sources={list(results.keys())}"
        )


if __name__ == "__main__":
    analyze_market()
