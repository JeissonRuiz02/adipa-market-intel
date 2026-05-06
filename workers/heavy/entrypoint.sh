#!/bin/sh
# worker-heavy daily loop
# 1. scraper.py  — Playwright scrapes real data from all four sources
# 2. analyze.py  — Polars analyzes the accumulated raw_courses

set -e

echo "worker-heavy started — waiting 30s for Postgres..."
sleep 30

while true; do
    echo ""
    echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — starting daily cycle"

    echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') [1/2] Scraping with Playwright..."
    python /app/workers/heavy/scraper.py || echo "scraper.py failed — continuing to analyze.py"

    echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') [2/2] Analysis with Polars..."
    python /app/workers/heavy/analyze.py || echo "analyze.py failed"

    echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — cycle complete, sleeping 24h"
    sleep 86400
done
