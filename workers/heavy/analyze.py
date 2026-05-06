"""
adipa-market-intel — heavy pipeline: Polars analysis

Steps:
    1. Read raw_courses from Postgres using connectorx (zero-copy via Arrow)
    2. Normalize prices to USD with the Polars lazy API
    3. Compute per-source statistics (avg, median, percentiles, top courses)
    4. UPSERT results into market_report (idempotent on report_date + source)

Intentionally separated from flows/analyze_market.py:
    - This file contains pure data logic with no Prefect dependency
    - The Prefect flow only orchestrates; it never imports Polars directly
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone

import polars as pl
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]

# connectorx 0.4.x requires postgresql:// (no +asyncpg variant)
DATABASE_URL_CX = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://") \
    if "postgresql+asyncpg://" in DATABASE_URL else DATABASE_URL

# Fixed FX rates for USD normalization.
# In production these would come from a live FX API (e.g. Fixer.io).
FX_TO_USD: dict[str, float] = {
    "USD": 1.0,
    "EUR": 1.10,      # 1 EUR ≈ 1.10 USD  (Domestika)
    "CLP": 0.00109,   # 1 CLP ≈ 0.00109 USD
    "MXN": 0.058,     # 1 MXN ≈ 0.058 USD
    "COP": 0.00024,   # 1 COP ≈ 0.00024 USD
    "BRL": 0.19,      # 1 BRL ≈ 0.19 USD
    "ARS": 0.0011,    # 1 ARS ≈ 0.0011 USD
}


@dataclass
class SourceReport:
    report_date: date
    source: str
    total_courses: int
    courses_with_price: int
    avg_price_usd: float | None
    median_price_usd: float | None
    min_price_usd: float | None
    max_price_usd: float | None
    p25_price_usd: float | None
    p75_price_usd: float | None
    avg_duration_hours: float | None
    median_duration_hours: float | None
    avg_rating: float | None
    avg_reviews: float | None
    top_cheapest: list[dict]
    top_expensive: list[dict]
    top_rated: list[dict]
    by_level: dict[str, dict]


# ── Data loading ───────────────────────────────

def load_raw_courses() -> pl.DataFrame:
    """Read raw_courses via connectorx — Arrow transfer means zero Python-side copy."""
    import connectorx as cx

    query = """
        SELECT
            source, external_id, title, category,
            price_original, price_discount, currency,
            duration_hours, level, rating, reviews_count,
            students_count, url, scraped_at
        FROM raw_courses
        WHERE category = 'psicología'
          OR category IS NULL
        ORDER BY scraped_at DESC
    """

    return cx.read_sql(DATABASE_URL_CX, query, return_type="polars")


# ── Transformations ────────────────────────────

def normalize_prices(df: pl.DataFrame) -> pl.DataFrame:
    """Add price_usd column by converting each row's currency using FX_TO_USD."""
    fx_expr = (
        pl.when(pl.col("currency") == "USD").then(1.0)
        .when(pl.col("currency") == "EUR").then(FX_TO_USD["EUR"])
        .when(pl.col("currency") == "CLP").then(FX_TO_USD["CLP"])
        .when(pl.col("currency") == "MXN").then(FX_TO_USD["MXN"])
        .when(pl.col("currency") == "COP").then(FX_TO_USD["COP"])
        .when(pl.col("currency") == "BRL").then(FX_TO_USD["BRL"])
        .when(pl.col("currency") == "ARS").then(FX_TO_USD["ARS"])
        .otherwise(1.0)
        .alias("fx_rate")
    )

    # Use discount price when available, otherwise use list price
    effective_price = (
        pl.when(
            pl.col("price_discount").is_not_null()
            & (pl.col("price_discount") > 0)
        )
        .then(pl.col("price_discount"))
        .otherwise(pl.col("price_original"))
        .alias("price_effective")
    )

    return (
        df.lazy()
        .with_columns([fx_expr, effective_price])
        .with_columns([
            (pl.col("price_effective") * pl.col("fx_rate"))
            .round(2)
            .alias("price_usd")
        ])
        .collect()
    )


def compute_source_report(
    df: pl.DataFrame,
    source: str,
    report_date: date,
) -> SourceReport:
    """Compute all metrics for a single source using Polars lazy API."""
    source_df = (
        df.lazy()
        .filter(pl.col("source") == source)
        .collect()
    )

    total = len(source_df)
    priced_df = source_df.filter(pl.col("price_usd").is_not_null() & (pl.col("price_usd") > 0))
    courses_with_price = len(priced_df)

    price_stats: dict = {}
    if courses_with_price > 0:
        price_series = priced_df["price_usd"]
        price_stats = {
            "avg":    round(price_series.mean() or 0, 2),
            "median": round(price_series.median() or 0, 2),
            "min":    round(price_series.min() or 0, 2),
            "max":    round(price_series.max() or 0, 2),
            "p25":    round(price_series.quantile(0.25) or 0, 2),
            "p75":    round(price_series.quantile(0.75) or 0, 2),
        }

    duration_df = source_df.filter(pl.col("duration_hours").is_not_null())
    dur_stats: dict = {}
    if len(duration_df) > 0:
        dur_series = duration_df["duration_hours"]
        dur_stats = {
            "avg":    round(dur_series.mean() or 0, 1),
            "median": round(dur_series.median() or 0, 1),
        }

    rated_df = source_df.filter(pl.col("rating").is_not_null())
    avg_rating = round(rated_df["rating"].mean() or 0, 2) if len(rated_df) > 0 else None
    avg_reviews = round(rated_df["reviews_count"].mean() or 0, 1) if len(rated_df) > 0 else None

    return SourceReport(
        report_date=report_date,
        source=source,
        total_courses=total,
        courses_with_price=courses_with_price,
        avg_price_usd=price_stats.get("avg"),
        median_price_usd=price_stats.get("median"),
        min_price_usd=price_stats.get("min"),
        max_price_usd=price_stats.get("max"),
        p25_price_usd=price_stats.get("p25"),
        p75_price_usd=price_stats.get("p75"),
        avg_duration_hours=dur_stats.get("avg"),
        median_duration_hours=dur_stats.get("median"),
        avg_rating=avg_rating,
        avg_reviews=avg_reviews,
        top_cheapest=_top_cheapest(priced_df, n=5),
        top_expensive=_top_expensive(priced_df, n=5),
        top_rated=_top_rated(source_df, n=5),
        by_level=_by_level(source_df),
    )


def _top_cheapest(df: pl.DataFrame, n: int = 5) -> list[dict]:
    return (
        df.lazy()
        .filter(pl.col("price_usd") > 0)
        .sort("price_usd", descending=False)
        .limit(n)
        .select(["title", "price_usd", "url"])
        .collect()
        .to_dicts()
    )


def _top_expensive(df: pl.DataFrame, n: int = 5) -> list[dict]:
    return (
        df.lazy()
        .sort("price_usd", descending=True)
        .limit(n)
        .select(["title", "price_usd", "url"])
        .collect()
        .to_dicts()
    )


def _top_rated(df: pl.DataFrame, n: int = 5) -> list[dict]:
    return (
        df.lazy()
        .filter(pl.col("rating").is_not_null())
        .sort("rating", descending=True)
        .limit(n)
        .select(["title", "rating", "reviews_count", "url"])
        .collect()
        .to_dicts()
    )


def _by_level(df: pl.DataFrame) -> dict[str, dict]:
    """Return {level: {count, avg_price_usd}} for each non-null level."""
    result: dict[str, dict] = {}

    grouped = (
        df.lazy()
        .filter(pl.col("level").is_not_null())
        .group_by("level")
        .agg([
            pl.len().alias("count"),
            pl.col("price_usd").mean().round(2).alias("avg_price_usd"),
        ])
        .collect()
    )

    for row in grouped.to_dicts():
        result[row["level"]] = {
            "count":         row["count"],
            "avg_price_usd": row["avg_price_usd"],
        }

    return result


# ── Persistence ────────────────────────────────

def upsert_report(report: SourceReport) -> None:
    """
    Idempotent UPSERT into market_report.
    Re-running on the same day overwrites the previous row — never duplicates.
    """
    sql = """
        INSERT INTO market_report (
            report_date, source,
            total_courses, courses_with_price,
            avg_price_usd, median_price_usd, min_price_usd, max_price_usd,
            p25_price_usd, p75_price_usd,
            avg_duration_hours, median_duration_hours,
            avg_rating, avg_reviews,
            top_cheapest, top_expensive, top_rated,
            by_level, generated_at
        ) VALUES (
            %(report_date)s, %(source)s,
            %(total_courses)s, %(courses_with_price)s,
            %(avg_price_usd)s, %(median_price_usd)s, %(min_price_usd)s, %(max_price_usd)s,
            %(p25_price_usd)s, %(p75_price_usd)s,
            %(avg_duration_hours)s, %(median_duration_hours)s,
            %(avg_rating)s, %(avg_reviews)s,
            %(top_cheapest)s, %(top_expensive)s, %(top_rated)s,
            %(by_level)s, NOW()
        )
        ON CONFLICT (report_date, source) DO UPDATE SET
            total_courses         = EXCLUDED.total_courses,
            courses_with_price    = EXCLUDED.courses_with_price,
            avg_price_usd         = EXCLUDED.avg_price_usd,
            median_price_usd      = EXCLUDED.median_price_usd,
            min_price_usd         = EXCLUDED.min_price_usd,
            max_price_usd         = EXCLUDED.max_price_usd,
            p25_price_usd         = EXCLUDED.p25_price_usd,
            p75_price_usd         = EXCLUDED.p75_price_usd,
            avg_duration_hours    = EXCLUDED.avg_duration_hours,
            median_duration_hours = EXCLUDED.median_duration_hours,
            avg_rating            = EXCLUDED.avg_rating,
            avg_reviews           = EXCLUDED.avg_reviews,
            top_cheapest          = EXCLUDED.top_cheapest,
            top_expensive         = EXCLUDED.top_expensive,
            top_rated             = EXCLUDED.top_rated,
            by_level              = EXCLUDED.by_level,
            generated_at          = EXCLUDED.generated_at
    """

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "report_date":          report.report_date,
                    "source":               report.source,
                    "total_courses":        report.total_courses,
                    "courses_with_price":   report.courses_with_price,
                    "avg_price_usd":        report.avg_price_usd,
                    "median_price_usd":     report.median_price_usd,
                    "min_price_usd":        report.min_price_usd,
                    "max_price_usd":        report.max_price_usd,
                    "p25_price_usd":        report.p25_price_usd,
                    "p75_price_usd":        report.p75_price_usd,
                    "avg_duration_hours":   report.avg_duration_hours,
                    "median_duration_hours": report.median_duration_hours,
                    "avg_rating":           report.avg_rating,
                    "avg_reviews":          report.avg_reviews,
                    "top_cheapest":         json.dumps(report.top_cheapest),
                    "top_expensive":        json.dumps(report.top_expensive),
                    "top_rated":            json.dumps(report.top_rated),
                    "by_level":             json.dumps(report.by_level),
                })
    finally:
        conn.close()


# ── Entry point ────────────────────────────────

def run_analysis() -> dict[str, int]:
    """
    Run the full analysis for all sources.
    Returns {source: total_courses} for the caller to log.
    """
    today = datetime.now(timezone.utc).date()

    df = load_raw_courses()
    if df.is_empty():
        return {}

    df = normalize_prices(df)
    _update_price_usd(df)

    sources = df["source"].unique().to_list()
    results: dict[str, int] = {}

    for source in sources:
        report = compute_source_report(df, source, today)
        upsert_report(report)
        results[source] = report.total_courses

    return results


def _update_price_usd(df: pl.DataFrame) -> None:
    """Write normalized price_usd back to raw_courses for reference by the light pipeline."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                rows = (
                    df.lazy()
                    .filter(pl.col("price_usd").is_not_null())
                    .select(["price_usd", "source", "external_id"])
                    .collect()
                    .to_dicts()
                )
                cur.executemany(
                    """
                    UPDATE raw_courses
                    SET price_usd = %(price_usd)s
                    WHERE source = %(source)s
                      AND external_id = %(external_id)s
                    """,
                    rows,
                )
    finally:
        conn.close()


if __name__ == "__main__":
    results = run_analysis()
    print("Analysis complete:", results)
