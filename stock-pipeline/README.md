# Project 01 · Stock Price Tracker

**Status:** ✅ Complete
**Airflow:** 3.2.1 · **Postgres:** 16 · **Python:** 3.13

Fetches daily closing prices for 5 US stocks using yfinance and loads them into Postgres on a daily schedule. The main focus of this project is understanding how Airflow thinks about time — `logical_date`, catchup, and backfilling.

---

## What It Builds

```
create_table → check_market_open → fetch_prices → validate_prices → store_prices
```

| Task | Type | What it does |
|---|---|---|
| `create_table` | `SQLExecuteQueryOperator` | Creates `stock_prices` table with upsert constraint — idempotent |
| `check_market_open` | `@task` | Skips weekends cleanly with `AirflowSkipException` |
| `fetch_prices` | `@task` | Downloads OHLCV data via yfinance for the `logical_date` |
| `validate_prices` | `@task` | Checks for negative prices and zero volume before storing |
| `store_prices` | `@task` | Upserts rows with `ON CONFLICT DO UPDATE` — re-runs never duplicate |

**Data source:** [yfinance](https://pypi.org/project/yfinance/) — free, no API key required  
**Table:** `stock_prices` in the `stocks` database  
**Stocks:** AAPL, GOOGL, MSFT, AMZN, META  
**Schedule:** `@daily` with `catchup=True`

---

## The Key Concept: logical_date

This is the most misunderstood concept in Airflow. A `@daily` DAG does not run *for* today — it runs *for yesterday*.

```
logical_date = 2026-05-01   →   fires at 2026-05-02 00:00 UTC
logical_date = 2026-05-02   →   fires at 2026-05-03 00:00 UTC
```

Airflow waits for the full interval to complete before processing it. When fetching stock prices, we pass `logical_date` to yfinance — not `datetime.now()`. This is what makes backfilling work: you can re-run May 1st a year later and still get the right data.

---

## Concepts Covered

**logical_date**
The data interval this run is processing. Always one interval behind the actual execution time. Never use `datetime.now()` inside a DAG — use `logical_date`.

**catchup=True**
On first start, Airflow automatically triggers one run for every missed day between `start_date` and today. This is how historical data loads automatically. Set `catchup=False` if you only care about future runs.

**retries + retry_delay**
```python
default_args={
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
```
yfinance calls can fail due to rate limits or network blips. Airflow retries automatically — waiting 5 minutes between attempts avoids hammering the API.

**on_failure_callback**
A Python function Airflow calls automatically when any task fails. Receives a `context` dict with the task, `logical_date`, and exception. In production this is where you'd send a Slack alert or page someone.

**AirflowSkipException**
Raising this marks a task (and all downstream tasks) as skipped — not failed. Used for weekends: a closed market is not an error. Using `ValueError` for expected conditions creates noise in your failure alerts.

**ON CONFLICT DO UPDATE (upsert)**
```sql
INSERT INTO stock_prices (symbol, open, ..., price_date)
VALUES (...)
ON CONFLICT (symbol, price_date)
DO UPDATE SET open = EXCLUDED.open, ...
```
The `UNIQUE` constraint on `(symbol, price_date)` means inserting the same day twice would normally error. The upsert says "update instead." Re-running any DAG run never duplicates rows — that's idempotency.

---

## Running It

### Prerequisites
- `docker network create shared-db` (once ever)
- infra/ running (`cd infra && docker compose up -d`)

### Start

```bash
cd stock-pipeline

# set up env vars (first time only)
echo "AIRFLOW_UID=$(id -u)" >> .env
python3 -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())" >> .env
echo "_PIP_ADDITIONAL_REQUIREMENTS=yfinance pandas apache-airflow-providers-postgres" >> .env

docker compose up -d
```

Wait for all containers to be healthy, then open `http://localhost:8081` (airflow / airflow).

### Add the Postgres connection

Admin → Connections → +

| Field | Value |
|---|---|
| Connection Id | `postgres` |
| Connection Type | `Postgres` |
| Host | `shared-postgres` |
| Database | `stocks` |
| Login | `admin` |
| Password | `admin123` |
| Port | `5432` |

### Create the stocks database

In pgAdmin (`http://localhost:5050`) → Query Tool:
```sql
CREATE DATABASE stocks;
```

### Unpause and run

```bash
docker compose exec airflow-scheduler airflow dags unpause stock_prices
```

With `catchup=True`, Airflow will immediately start triggering runs for every trading day since `start_date`.

---

## Verifying the Data

```sql
-- all loaded trading days
SELECT DISTINCT price_date FROM stock_prices ORDER BY price_date;

-- latest prices
SELECT symbol, close, price_date
FROM stock_prices
ORDER BY price_date DESC, symbol
LIMIT 10;

-- test idempotency: re-run a day, count should stay at 5
SELECT COUNT(*) FROM stock_prices WHERE price_date = '2026-05-01';
```

---

## Useful Commands

```bash
# clear all task states and re-queue
docker compose exec airflow-scheduler airflow tasks clear stock_prices -y

# trigger a specific historical date
docker compose exec airflow-scheduler airflow dags trigger stock_prices \
    --logical-date 2026-05-01T00:00:00+00:00

# check DAG is loaded without errors
docker compose exec airflow-scheduler airflow dags list | grep stock

# view worker logs live
docker compose logs airflow-worker -f

# stop everything (keeps data)
docker compose down

# stop and wipe (use when changing credentials)
docker compose down -v && rm -rf logs/ && mkdir logs
```

---

## Mistakes Made

**Wrong import path**
`airflow.providers.postgres.operators.postgres` no longer exists in newer provider versions. Use `airflow.providers.common.sql.operators.sql` instead.

**ValueError for weekends**
Using `ValueError` for expected conditions (closed market) marks runs as failed and triggers retries. `AirflowSkipException` is the right tool — skipping is not failing.

**Wiring inside a task function**
The dependency wiring (`create_table >> date`) must be at the DAG body level, not inside a `@task` function. Python treats indented code as part of the function body — it never executes during DAG parsing.

**Stale volumes after credential change**
Postgres ignores new credentials if a volume already exists. Always run `docker compose down -v` when changing database credentials during development.

---

## What's Next

Project 02 adds AWS S3 and Terraform — storing raw data in a data lake before loading to Postgres.