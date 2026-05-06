-- adipa-market-intel — initial schema
-- Engine : PostgreSQL 16
-- Tables :
--   1. raw_courses   — written by the light pipeline (price checks) and
--                      the heavy pipeline (full Playwright scrape)
--   2. market_report — written by the heavy pipeline (Polars analysis)
--   3. scrape_log    — append-only execution audit for both pipelines

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ════════════════════════════════════════════
--  1. raw_courses
--     Both pipelines write here via UPSERT.
--     Idempotency: UNIQUE (source, external_id)
-- ════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS raw_courses (
    id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Origin
    source           TEXT         NOT NULL,  -- 'platzi' | 'coursera' | 'udemy' | 'domestika'
    external_id      TEXT         NOT NULL,  -- platform-native slug or id

    -- Course data
    title            TEXT         NOT NULL,
    category         TEXT,
    subcategory      TEXT,
    instructor       TEXT,
    description      TEXT,

    -- Price in original currency
    price_original   NUMERIC(12, 2),
    price_discount   NUMERIC(12, 2),
    currency         CHAR(3),                -- 'USD' | 'EUR' | 'CLP' | 'MXN' | 'COP' | ...
    price_usd        NUMERIC(12, 2),         -- normalized by the heavy pipeline

    -- Metadata
    duration_hours   NUMERIC(6, 1),
    level            TEXT,                   -- 'beginner' | 'intermediate' | 'advanced'
    rating           NUMERIC(3, 2),          -- 0.00 – 5.00
    reviews_count    INTEGER,
    students_count   INTEGER,
    url              TEXT,

    -- Control
    scraped_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_source_course UNIQUE (source, external_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_courses_source
    ON raw_courses (source);

CREATE INDEX IF NOT EXISTS idx_raw_courses_category
    ON raw_courses (category);

CREATE INDEX IF NOT EXISTS idx_raw_courses_scraped_at
    ON raw_courses (scraped_at DESC);

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_raw_courses_updated_at ON raw_courses;
CREATE TRIGGER trg_raw_courses_updated_at
    BEFORE UPDATE ON raw_courses
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();


-- ════════════════════════════════════════════
--  2. market_report
--     Written by the heavy pipeline once per day.
--     Idempotency: UNIQUE (report_date, source)
--     Re-running on the same day overwrites — never accumulates duplicates.
-- ════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS market_report (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Dimensions
    report_date           DATE        NOT NULL,
    source                TEXT        NOT NULL,

    -- Volume
    total_courses         INTEGER,
    courses_with_price    INTEGER,

    -- Prices in USD
    avg_price_usd         NUMERIC(10, 2),
    median_price_usd      NUMERIC(10, 2),
    min_price_usd         NUMERIC(10, 2),
    max_price_usd         NUMERIC(10, 2),
    p25_price_usd         NUMERIC(10, 2),
    p75_price_usd         NUMERIC(10, 2),

    -- Duration
    avg_duration_hours    NUMERIC(6, 1),
    median_duration_hours NUMERIC(6, 1),

    -- Quality
    avg_rating            NUMERIC(3, 2),
    avg_reviews           NUMERIC(10, 1),

    -- Top courses (JSONB keeps the schema flat)
    top_cheapest          JSONB,  -- [{title, price_usd, url}, ...]
    top_expensive         JSONB,
    top_rated             JSONB,  -- [{title, rating, reviews_count, url}, ...]

    -- Breakdown by level
    by_level              JSONB,  -- {beginner: {count, avg_price_usd}, ...}

    -- Control
    generated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_report_date_source UNIQUE (report_date, source)
);

CREATE INDEX IF NOT EXISTS idx_market_report_date
    ON market_report (report_date DESC);

CREATE INDEX IF NOT EXISTS idx_market_report_source
    ON market_report (source);


-- ════════════════════════════════════════════
--  3. scrape_log
--     Append-only audit log for every pipeline execution.
--     No idempotency — each run leaves exactly one row per source.
-- ════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS scrape_log (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline          TEXT        NOT NULL,  -- 'light' | 'heavy'
    source            TEXT,                  -- platform name
    flow_run_id       TEXT,                  -- Prefect run ID (null for shell-triggered runs)
    status            TEXT        NOT NULL,  -- 'success' | 'error' | 'unreachable'
    courses_upserted  INTEGER,
    error_message     TEXT,
    duration_ms       INTEGER,
    executed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scrape_log_executed_at
    ON scrape_log (executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_scrape_log_pipeline
    ON scrape_log (pipeline, status);
