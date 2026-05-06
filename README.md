# adipa-market-intel

> Automated market intelligence for ADIPA — scraping and analysis of competitor psychology course pricing.

---

## Table of contents

- [Problem](#problem)
- [Solution](#solution)
- [Architecture](#architecture)
- [Tech stack and design decisions](#tech-stack-and-design-decisions)
- [Repository structure](#repository-structure)
- [Running locally](#running-locally)
- [Verifying it works](#verifying-it-works)
- [VM deployment](#vm-deployment)
- [Idempotency](#idempotency)
- [Error handling and retries](#error-handling-and-retries)
- [Heavy pipeline isolation](#heavy-pipeline-isolation)
- [Next iteration](#next-iteration)

---

## Problem

ADIPA operates in Chile, Mexico, Colombia, and Argentina offering online psychology courses. To make informed pricing, content, and positioning decisions, they need to know what competitors are doing: what courses they offer, at what price, how long they are, and how students rate them.

Today that information doesn't exist in any automated form. Someone searches manually, when they remember, and by the time it reaches a decision it's already stale.

This system automates that market intelligence continuously.

---

## Solution

Two pipelines with distinct responsibilities running on Docker:

**Light pipeline** (`scrape_prices`) — runs every 15 minutes via Prefect. Checks prices of known courses from Platzi, Coursera, Udemy and Domestika via plain HTTP and UPSERTs the results into `raw_courses`. Takes seconds. Low resource usage.

**Heavy pipeline** (`scraper.py` + `analyze.py`) — runs once per day on a 24-hour shell loop inside `worker-heavy`. Scrapes the full catalog using Playwright (JS-rendered pages), then normalizes prices to USD with Polars and writes daily statistics to `market_report`. Takes minutes.

The heavy pipeline consumes what the light pipeline accumulates — they complement each other.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   docker compose                    │
│                                                     │
│  ┌─────────────┐        ┌──────────────────────┐   │
│  │   Postgres  │        │    Prefect Server    │   │
│  │  port 5432  │◄──────►│      port 4200       │   │
│  └─────────────┘        └──────────────────────┘   │
│         ▲                        ▲                  │
│         │                        │                  │
│  ┌──────┴──────┐       ┌─────────┴──────────┐      │
│  │worker-heavy │       │    worker-light     │      │
│  │  shell loop │       │     light-pool      │      │
│  │ Playwright  │       │   httpx — Prefect   │      │
│  │  + Polars   │       └────────────────────-┘      │
│  └─────────────┘                                    │
│                        ┌───────────────────┐        │
│                        │   flow-deployer   │        │
│                        │   (runs once)     │        │
│                        └───────────────────┘        │
└─────────────────────────────────────────────────────┘
```

**Data flow:**

```
Platzi ────┐
Coursera ──┤──► scraper.py ──► raw_courses ──► analyze.py ──► market_report
Udemy ─────┤    (Playwright      (Postgres)     (Polars          (Postgres)
Domestika ─┘     daily)                          daily)

Platzi ────┐
Coursera ──┤──► scrape_prices ──► raw_courses
Udemy ─────┘    (HTTP, every
                 15 min)
```

### How each worker runs

| Service | Trigger | Orchestrator |
|---|---|---|
| `worker-light` | Prefect schedule (every 15 min) | Prefect — `light-pool` |
| `worker-heavy` | `entrypoint.sh` shell loop | None — runs `scraper.py` then `analyze.py` directly every 24h |
| `flow-deployer` | `docker compose up` (once) | Registers `scrape-prices-scheduled` in Prefect, then exits |

### Scraping status per source

| Source | Method | Result |
|---|---|---|
| Coursera | Playwright + JSON-LD | Real data — titles, URLs, instructors |
| Platzi | Playwright + link extraction | Partial — 1 real course + fallback |
| Udemy | Playwright (captcha blocks) | Fallback — known courses with approximate data |
| Domestika | Playwright + CSS selectors | Fallback — known courses with approximate data |

Udemy and Domestika detect headless browsers and return captchas. The fallback contains real course titles and approximate prices documented in `workers/heavy/scraper.py`. In a second iteration this would be solved with authenticated sessions or rotating proxies.

### Postgres tables

| Table | Written by | Purpose |
|---|---|---|
| `raw_courses` | both pipelines | Primary store of scraped course data |
| `market_report` | heavy pipeline | Daily statistics per platform |
| `scrape_log` | both pipelines | Execution audit trail |

---

## Tech stack and design decisions

### Why Prefect and not Airflow?

Prefect wins in this context for three concrete reasons:

1. **Setup in minutes.** `docker compose up` and the UI is ready. Airflow requires configuring an executor, workers, a metadata database, and several additional services — easily 2 hours of setup for an 8-hour project.

2. **No separate DAG files.** In Prefect, flows are Python functions decorated with `@flow` and `@task`. The code is more readable, easier to test, and doesn't need a special DAGs directory.

3. **Modern API.** Prefect 2 uses `async` natively, has typed APIs throughout, and the UI UX is significantly better than Airflow 2.

### Why Polars and not Pandas?

Polars has concrete technical advantages for this use case:

- **Lazy API:** transformations are planned and optimized before execution. For datasets that grow over time, this makes a real difference.
- **Lower memory footprint:** Polars uses Apache Arrow internally. For the heavy pipeline running in a container limited to 1 GB, this matters.
- **Explicit expressions:** transformation code is more readable and less error-prone than Pandas indexing.

### Why the heavy pipeline doesn't use Prefect as scheduler

`prefect==2.19.9` conflicts with the version of pydantic installed in the same image (`TypeError: 'type' object is not iterable`). Rather than downgrade pydantic and risk breaking Polars or connectorx, the heavy worker runs on a 24-hour shell loop via `entrypoint.sh` — fully autonomous, no Prefect dependency.

This reinforces isolation: the heavy container has zero overlap with the orchestrator. `flows/analyze_market.py` exists as a Prefect-wrapped version of the same logic for on-demand manual runs. Migrating to Prefect 3.x (which resolves the pydantic conflict natively) is the clean fix for a second iteration.

### Why connectorx for reading Postgres?

`connectorx` reads directly from Postgres into a Polars DataFrame using Apache Arrow as the intermediate format — zero-copy. Alternatives like `psycopg2 → list of tuples → pl.DataFrame(...)` are slower and use more memory.

### FX rates

Prices are normalized to USD using fixed rates documented in `workers/heavy/analyze.py`. In production this would come from a live FX API (e.g. Fixer.io or ExchangeRate-API). Fixed rates are sufficient for demonstrating the normalization logic.

Supported currencies: `USD`, `EUR` (Domestika), `CLP`, `MXN`, `COP`, `BRL`, `ARS`.

---

## Repository structure

```
adipa-market-intel/
│
├── docker-compose.yml          # 5 services: postgres, prefect-server,
│                               # worker-light, worker-heavy, flow-deployer
├── .env.example                # Required environment variables
├── .gitignore
│
├── db/
│   └── init.sql                # Schema: raw_courses, market_report, scrape_log
│
├── flows/
│   ├── scrape_prices.py        # Light pipeline — Prefect @flow + @tasks
│   ├── analyze_market.py       # Prefect wrapper for manual/on-demand runs
│   └── deploy.py               # Registers scrape_prices schedule at startup
│
└── workers/
    ├── light/
    │   ├── Dockerfile          # FROM prefecthq/prefect:2-python3.11
    │   └── requirements.txt    # httpx, beautifulsoup4, psycopg2
    │
    └── heavy/
        ├── Dockerfile          # FROM python:3.11-slim  ← separate image
        ├── requirements.txt    # polars, connectorx, playwright, psycopg2
        ├── entrypoint.sh       # Shell loop: scraper.py → analyze.py every 24h
        ├── scraper.py          # Playwright scraper for all four sources
        └── analyze.py          # Polars analysis — no Prefect dependency
```

---

## Running locally

### Requirements

- Docker Desktop running
- `docker compose` v2+

### Steps

**1. Clone the repository**

```bash
git clone https://github.com/tu-usuario/adipa-market-intel.git
cd adipa-market-intel
```

**2. Set up environment variables**

```bash
cp .env.example .env
```

The default `.env` works locally without changes. To change the Postgres password, edit `POSTGRES_PASSWORD` before continuing.

**3. Start everything**

```bash
docker compose up --build
```

This starts in order:
1. Postgres (schema applied automatically)
2. Prefect Server
3. worker-light and worker-heavy
4. flow-deployer (registers the schedule and exits)

First run takes ~3 minutes for image downloads. Subsequent starts take seconds.

**4. Open the Prefect UI**

```
http://localhost:4200
```

You should see one registered deployment: `scrape-prices-scheduled`.

---

## Verifying it works

### Verify the Postgres schema

```bash
docker exec -it adipa_postgres psql -U adipa -d market_intel -c "\dt"
```

Expected output:

```
 Schema |     Name      | Type  | Owner
--------+---------------+-------+-------
 public | market_report | table | adipa
 public | raw_courses   | table | adipa
 public | scrape_log    | table | adipa
```

### Run the light pipeline manually

From the Prefect UI (`http://localhost:4200`) → Deployments → `scrape-prices-scheduled` → **Quick Run**.

Or from the terminal:

```bash
docker exec adipa_worker_light python /app/flows/scrape_prices.py
```

### Run the heavy pipeline manually

```bash
# Full Playwright scrape (writes to raw_courses)
docker exec adipa_worker_heavy python /app/workers/heavy/scraper.py

# Polars analysis (reads raw_courses, writes market_report)
docker exec adipa_worker_heavy python /app/workers/heavy/analyze.py
```

### Verify data was written

```bash
# Scraped courses
docker exec -it adipa_postgres psql -U adipa -d market_intel \
  -c "SELECT source, COUNT(*) as total, AVG(price_original) as avg_price FROM raw_courses GROUP BY source;"

# Market report
docker exec -it adipa_postgres psql -U adipa -d market_intel \
  -c "SELECT report_date, source, total_courses, avg_price_usd, median_price_usd FROM market_report ORDER BY report_date DESC;"

# Execution audit
docker exec -it adipa_postgres psql -U adipa -d market_intel \
  -c "SELECT pipeline, source, status, courses_upserted, duration_ms, executed_at FROM scrape_log ORDER BY executed_at DESC LIMIT 10;"
```

### Verify heavy pipeline isolation

```bash
# Polars must NOT exist in the light worker
docker exec adipa_worker_light pip show polars
# Expected: WARNING: Package(s) not found: polars

# Playwright must NOT exist in the light worker
docker exec adipa_worker_light pip show playwright
# Expected: WARNING: Package(s) not found: playwright

# Polars must exist in the heavy worker
docker exec adipa_worker_heavy pip show polars
# Expected: Name: polars / Version: 0.20.31
```

### Verify idempotency

```bash
# Run the scraper twice in a row
docker exec adipa_worker_heavy python /app/workers/heavy/scraper.py
docker exec adipa_worker_heavy python /app/workers/heavy/scraper.py

# Row count must not increase — UPSERT, not INSERT
docker exec -it adipa_postgres psql -U adipa -d market_intel \
  -c "SELECT source, COUNT(*) FROM raw_courses GROUP BY source;"
```

---

## VM deployment

The system is deployed on a GCP e2-medium instance (2 vCPU, 4 GB RAM).

Prefect UI: `http://34.71.29.154:4200`

### Setup on the VM

```bash
# 1. Connect to the VM
ssh user@YOUR_PUBLIC_IP

# 2. Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

# 3. Clone the repo
git clone https://github.com/tu-usuario/adipa-market-intel.git
cd adipa-market-intel

# 4. Configure environment
cp .env.example .env
nano .env
# → PUBLIC_HOST=YOUR_PUBLIC_IP

# 5. Open port 4200 in the VM firewall
# GCP: VPC Network → Firewall → Create rule → TCP 4200

# 6. Start
docker compose up --build -d

# 7. Verify
docker compose ps
```

---

## Idempotency

Idempotency is guaranteed by Postgres unique constraints, not by application logic.

**`raw_courses`** — `UNIQUE (source, external_id)`. The `ON CONFLICT DO UPDATE` updates price and metadata if the course already exists. Running any scraper 100 times produces exactly the same data as running it once.

**`market_report`** — `UNIQUE (report_date, source)`. If the heavy pipeline runs twice in the same day, the second run overwrites the first. No duplicate rows ever accumulate.

**`scrape_log`** — append-only by design. It's the historical execution record, not business data. Idempotency doesn't apply here — every run leaves a row.

---

## Error handling and retries

| Component | Policy |
|---|---|
| `@flow scrape_prices` | `retries=1, retry_delay_seconds=30` |
| `@task check-source-prices` | `retries=2, retry_delay_seconds=10` |
| `@flow analyze-market` (manual) | `retries=1, retry_delay_seconds=120` |
| `@task run-polars-analysis` (manual) | `retries=2, retry_delay_seconds=60` |
| `entrypoint.sh` (heavy, scheduled) | Shell `||` continues to next step on failure |

**Per-source resilience:** each source runs in its own try/except block in the heavy scraper. A Playwright failure or captcha on one source doesn't abort the remaining ones — it logs the error and falls back to known course data.

**Audit always:** `scrape_log` is written in a `finally` block (light pipeline) or after each source attempt (heavy scraper), covering both success and failure cases.

---

## Heavy pipeline isolation

The heavy worker uses a **completely separate Docker image** with no Prefect dependency:

| Image | Base | Libraries |
|---|---|---|
| `worker-light` | `prefecthq/prefect:2-python3.11` | httpx, beautifulsoup4, psycopg2 |
| `worker-heavy` | `python:3.11-slim` | polars, connectorx, playwright, psycopg2 |

`worker-heavy` starts from `python:3.11-slim` — not the Prefect image. Polars and Playwright exist only in this container. Verifiable with:

```bash
docker exec adipa_worker_light pip show polars
# WARNING: Package(s) not found: polars  ✓

docker exec adipa_worker_light pip show playwright
# WARNING: Package(s) not found: playwright  ✓
```

The heavy pipeline runs on a 24-hour shell loop (`entrypoint.sh`) that calls `scraper.py` and `analyze.py` directly — no Prefect involvement. This was a deliberate decision: `prefect==2.19.9` conflicts with pydantic v2 in the same image. Keeping Prefect out of the heavy container eliminates the conflict and reinforces isolation.

---

## Next iteration

**Functionality:**
- Integrate a live FX API (Fixer.io) instead of fixed rates
- Add more sources: MasterClass, LinkedIn Learning, other Spanish-language platforms
- Send the daily report to Slack with a week-over-week comparison
- Migrate to Prefect 3.x to resolve the pydantic conflict and schedule the heavy pipeline as a proper Prefect deployment
- Use authenticated Playwright sessions or rotating proxies to bypass Udemy and Domestika captchas

**Operations:**
- Nginx reverse proxy with basic auth in front of the Prefect UI
- Prefect alerts to Slack when a flow fails
- Environment variables for FX rates (remove hardcoding)

**Quality:**
- Unit tests for parsing and normalization functions
- Integration tests against a test database
- CI/CD with GitHub Actions to validate the compose before merge

---

*Developed as a technical assessment for ADIPA · 2026*