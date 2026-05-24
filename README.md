# Data Engineering Projects

A collection of production-grade data pipelines built while learning data engineering.
Each project introduces new tools and concepts — every one ships with a GitHub commit and a Medium article.

---

## Stack

| Tool | Version | Purpose |
|---|---|---|
| Apache Airflow | 3.2.1 | Pipeline orchestration |
| PostgreSQL | 16 | Local data storage |
| Snowflake | — | Cloud data warehouse (Project 6+) |
| dbt | — | Data transformations (Project 3+) |
| AWS S3 | — | Cloud data lake (Project 2+) |
| Terraform | — | Infrastructure as code (Project 2+) |
| GitHub Actions | — | CI/CD — lint, test, deploy DAGs (Project 5+) |
| pgAdmin | latest | Database UI |
| Docker Compose | v2 | Local infrastructure |
| Python | 3.13 | Pipeline logic |
| uv | latest | Python package management |

---

## Projects

| # | Project | Status | Concepts | Article |
|---|---|---|---|---|
| 00 | [Weather Pipeline](#project-00--weather-pipeline) | ✅ Done | Sensors, XComs, Hooks, Connections | [Medium](https://medium.com/@bilalnaseem19/i-built-my-first-data-pipeline-heres-what-nobody-tells-you) |
| 01 | [Stock Price Tracker](#project-01--stock-price-tracker) | ✅ Done | logical_date, scheduling, backfilling, retries, upsert | — |
| 02 | [Reddit → S3 + Terraform](#project-02--reddit-to-s3--terraform) | ⬜ Planned | AWS S3, IAM, S3Hook, data lake, Terraform | — |
| 03 | [dbt + Airflow Analytics](#project-03--dbt--airflow-analytics) | ⬜ Planned | ELT, dbt models, staging/marts, dbt tests | — |
| 04 | [Dynamic Multi-Source DAG](#project-04--dynamic-multi-source-dag) | ⬜ Planned | Dynamic DAGs, TaskFlow API, expand() | — |
| 05 | [Production Pipeline + CI/CD](#project-05--production-pipeline--cicd) | ⬜ Planned | Monitoring, SLAs, Slack alerting, GitHub Actions | — |
| 06 | [Snowflake Pipeline](#project-06--snowflake-pipeline) | ⬜ Planned | Cloud warehouse, external stages, dbt + Snowflake | — |
| 07 | [Capstone](#project-07--capstone) | ⬜ Planned | Everything combined — self-designed | — |

---

## How It Works

All projects share one Postgres instance and one Docker network:

```
docker network: shared-db
├── shared-postgres       ← your data (infra/)
├── pgadmin               ← database UI (infra/)
├── airflow-scheduler     ← runs DAGs (each project)
└── airflow-worker        ← executes tasks (each project)
```

Airflow containers reach Postgres by container name (`shared-postgres`) over the shared network.
Credentials never appear in code — only in Airflow Connections and `.env` files (both gitignored).

---

## Repository Structure

```
data-engineering-projects/
│
├── .github/
│   └── workflows/
│       └── deploy-dags.yml        ← CI/CD (added in Project 5)
│
├── infra/                          ← shared infrastructure, always running
│   ├── docker-compose.yml          ← Postgres + pgAdmin
│   └── .env                        ← GITIGNORED
│
├── terraform/                      ← AWS + Snowflake infra as code (Project 2+)
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars            ← GITIGNORED
│
├── dbt_project/                    ← shared dbt project (Project 3+)
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── dbt_project.yml
│
├── weather-pipeline/               ← Project 00
├── stock-pipeline/                 ← Project 01
├── reddit-pipeline/                ← Project 02
├── dynamic-pipeline/               ← Project 04
├── production-pipeline/            ← Project 05
├── snowflake-pipeline/             ← Project 06
├── capstone/                       ← Project 07
│
└── README.md
```

---

## Setup

### Prerequisites
- Docker Desktop running
- Python 3.10+
- uv (`pip install uv`)

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/data-engineering-projects.git
cd data-engineering-projects
```

### 2. Create the shared Docker network (once ever)
```bash
docker network create shared-db
```

### 3. Start shared infrastructure
```bash
cd infra
cp .env.example .env        # fill in your credentials
docker compose up -d
```

Services started:
- **pgAdmin** → `http://localhost:5050`
- **Postgres** → `localhost:5433`

### 4. Start a project
```bash
cd stock-pipeline

# generate required env vars
echo "AIRFLOW_UID=$(id -u)" >> .env
python3 -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())" >> .env

docker compose up -d

# watch init complete (2-3 minutes first time)
docker compose logs airflow-init -f
```

Airflow UI → `http://localhost:8081` (airflow / airflow)

---

## Project Details

### Project 00 · Weather Pipeline

**Status:** ✅ Complete
**Article:** [I Built My First Data Pipeline — Here's What Nobody Tells You](https://medium.com/@bilalnaseem19/i-built-my-first-data-pipeline-heres-what-nobody-tells-you)

**What it does:**
Fetches live weather data for Karachi from the Open-Meteo API every hour and loads it into Postgres.

**Pipeline:**
```
create_table → is_api_available → extract_weather → process_weather → store_weather
```

**Concepts:**
- `SQLExecuteQueryOperator` and Airflow Connections
- `@task.sensor` with `PokeReturnValue`
- `@task` decorator and XComs
- `PostgresHook` with `copy_expert` for bulk loading
- Implicit vs explicit task dependencies

**Data source:** [Open-Meteo](https://open-meteo.com/) (free, no API key)

**Table:** `weather_readings` in `weather_db`

---

### Project 01 · Stock Price Tracker

**Status:** ✅ Complete

**What it does:**
Fetches daily prices for 5 stocks (AAPL, GOOGL, MSFT, AMZN, META) using yfinance and loads them into Postgres on a daily schedule. Runs catchup automatically since May 1st — all historical trading days loaded on first start.

**Pipeline:**
```
create_table → check_market_open → fetch_prices → validate_prices → store_prices
```

**Concepts:**
- `logical_date` — the data interval Airflow is processing, always one interval behind execution time
- `schedule + catchup=True` — automatically triggers all missed runs since `start_date`
- `retries + retry_delay` — tasks retry 3 times with a 5-minute wait on failure
- `on_failure_callback` — custom function called on any task failure
- `AirflowSkipException` — skips weekend runs cleanly without marking them as failed
- `ON CONFLICT DO UPDATE` — idempotent upsert prevents duplicate rows on re-runs

**Data source:** [yfinance](https://pypi.org/project/yfinance/) (free, no API key)

**Table:** `stock_prices` in `stocks`

---

### Project 02 · Reddit to S3 + Terraform

**Status:** ⬜ Planned

**What it does:**
Fetches top posts from 3 subreddits and stores raw JSON in AWS S3 with date partitioning.
All AWS infrastructure provisioned with Terraform.

**Concepts:**
- Terraform: providers, resources, state, plan/apply/destroy
- AWS IAM and S3 as code
- Data lake pattern — raw/ never overwritten
- `S3Hook` and AWS Airflow Connection
- `S3KeySensor`

---

### Project 03 · dbt + Airflow Analytics

**Status:** ⬜ Planned

**What it does:**
Adds a dbt transformation layer on top of the stock price pipeline.
Raw prices → staging models → mart models (daily returns, moving averages, volatility).

**Concepts:**
- ELT vs ETL — why load raw first
- `source()`, `ref()`, model dependencies
- Staging vs mart layer separation
- `table` vs `view` vs `incremental` materializations
- dbt tests: `not_null`, `unique`, `accepted_values`
- `BashOperator` to trigger dbt from Airflow

---

### Project 04 · Dynamic Multi-Source DAG

**Status:** ⬜ Planned

**What it does:**
One DAG that generates tasks at runtime from a config file.
Adding a new data source requires only a config change — no DAG code change.

**Concepts:**
- Parse-time vs runtime task generation
- `expand()` for dynamic task mapping
- `TaskGroup` for visual organisation
- Airflow `Variable` for external config
- `BranchPythonOperator` and `trigger_rule`

---

### Project 05 · Production Pipeline + CI/CD

**Status:** ⬜ Planned

**What it does:**
Takes Project 04 and makes it production-grade.
Adds monitoring, Slack alerting, SLAs, data quality checks, and GitHub Actions CI/CD.

**Concepts:**
- `on_failure_callback` with Slack webhooks
- SLA miss alerting
- Great Expectations for data quality
- Dead letter queue pattern in S3
- GitHub Actions: lint → parse → deploy to S3

---

### Project 06 · Snowflake Pipeline

**Status:** ⬜ Planned

**What it does:**
Migrates the stock price + dbt pipeline from Postgres to Snowflake.
Loads raw data from S3 into Snowflake via external stages.

**Concepts:**
- Snowflake architecture vs Postgres
- External stages and `COPY INTO`
- `SnowflakeOperator` and Airflow Connection
- dbt profile swap: postgres → snowflake
- `VARIANT`, `FLATTEN`, `TIME_TRAVEL`
- Terraform Snowflake provider

---

### Project 07 · Capstone

**Status:** ⬜ Planned

**What it does:**
Self-designed project. Real data source, real question, full stack.

**Rules:**
- Must use 3+ tools from the stack
- Must answer a real question — not just move data
- README must explain the business problem first
- GitHub + Medium article mandatory

---

## Useful Commands

```bash
# test a single task without triggering the full DAG
docker exec -it <project>-airflow-scheduler-1 \
    airflow tasks test <dag_id> <task_id>

# get a shell inside the scheduler
docker exec -it <project>-airflow-scheduler-1 bash

# check which networks a container is on
docker inspect <container-name> | grep -A 20 "Networks"

# connect a container to shared-db manually
docker network connect shared-db <container-name>

# stop everything, keep data
docker compose down

# stop everything, wipe data (use when changing credentials)
docker compose down -v

# view logs for a service
docker compose logs airflow-scheduler -f
```

---

## Gitignored Files

```
.env                   ← Airflow and DB credentials
terraform.tfvars       ← AWS and Snowflake secrets
config/airflow.cfg     ← generated Airflow config (contains secrets)
logs/                  ← Airflow task logs (ephemeral)
__pycache__/           ← Python bytecode
```

Never commit credentials. Use `.env.example` files as templates.