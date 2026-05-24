# stock-pipeline/dags/stock_prices.py

import csv
import logging
import pendulum
import yfinance as yf

from datetime import timedelta
from pathlib import Path

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─── Constants ────────────────────────────────────────────────────────────────

STOCKS = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
TMP_FILE = Path("/tmp/stock_prices.csv")
CONN_ID = "postgres"

log = logging.getLogger(__name__)

# ─── Callbacks ────────────────────────────────────────────────────────────────

def on_failure(context):
    task_id = context["task_instance"].task_id
    logical_date = context["logical_date"]
    exception = context.get("exception")
    log.error(
        "Task failed | task_id=%s | logical_date=%s | exception=%s",
        task_id,
        logical_date,
        exception,
    )


# ─── DAG ──────────────────────────────────────────────────────────────────────

@dag(
    dag_id="stock_prices",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=True,
    max_active_runs=3,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure,
    },
    tags=["stocks", "project-01"],
)
def stock_prices():

    # ── Task 1: create_table ──────────────────────────────────────────────────

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS stock_prices (
                id          SERIAL PRIMARY KEY,
                symbol      TEXT        NOT NULL,
                open        NUMERIC,
                high        NUMERIC,
                low         NUMERIC,
                close       NUMERIC,
                volume      BIGINT,
                price_date  DATE        NOT NULL,
                fetched_at  TIMESTAMP   NOT NULL DEFAULT now(),
                CONSTRAINT  uq_symbol_date UNIQUE (symbol, price_date)
            );
        """,
    )

    # ── Task 2: check_market_open ─────────────────────────────────────────────

    @task
    def check_market_open(logical_date=None):
        weekday = logical_date.day_of_week  # 0=Monday, 6=Sunday
        if weekday >= 5:
            raise AirflowSkipException(
                f"Market closed on {logical_date.date()} "
                f"({'Saturday' if weekday == 5 else 'Sunday'}) — skipping."
            )
        log.info("Market open on %s (weekday=%s)", logical_date.date(), weekday)
        return str(logical_date.date())

    # ── Task 3: fetch_prices ──────────────────────────────────────────────────

    @task
    def fetch_prices(trade_date: str):
        start = trade_date
        end = str((pendulum.parse(trade_date) + timedelta(days=1)).date())

        log.info("Fetching prices for %s → %s", start, end)

        df = yf.download(
            tickers=STOCKS,
            start=start,
            end=end,
            auto_adjust=True,
            progress=False,
        )

        if df.empty:
            raise ValueError(f"yfinance returned no data for {trade_date}. Market holiday?")

        rows = []
        for symbol in STOCKS:
            try:
                row = {
                    "symbol":     symbol,
                    "open":       round(float(df["Open"][symbol].iloc[0]), 4),
                    "high":       round(float(df["High"][symbol].iloc[0]), 4),
                    "low":        round(float(df["Low"][symbol].iloc[0]), 4),
                    "close":      round(float(df["Close"][symbol].iloc[0]), 4),
                    "volume":     int(df["Volume"][symbol].iloc[0]),
                    "price_date": trade_date,
                }
                rows.append(row)
                log.info("%s close=%s", symbol, row["close"])
            except (KeyError, IndexError) as e:
                log.warning("Could not parse %s: %s", symbol, e)

        if not rows:
            raise ValueError(f"Parsed zero rows for {trade_date}")

        with open(TMP_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        log.info("Wrote %s rows to %s", len(rows), TMP_FILE)
        return str(TMP_FILE)

    # ── Task 4: validate_prices ───────────────────────────────────────────────

    @task
    def validate_prices(file_path: str):
        with open(file_path, newline="") as f:
            rows = list(csv.DictReader(f))

        errors = []
        for row in rows:
            symbol = row["symbol"]
            close = float(row["close"])
            volume = int(row["volume"])

            if close <= 0:
                errors.append(f"{symbol}: close price is {close}")
            if volume <= 0:
                errors.append(f"{symbol}: volume is {volume}")

        if errors:
            raise ValueError("Validation failed:\n" + "\n".join(errors))

        log.info("Validation passed for %s rows", len(rows))
        return file_path

    # ── Task 5: store_prices ──────────────────────────────────────────────────

    @task
    def store_prices(file_path: str):
        hook = PostgresHook(postgres_conn_id=CONN_ID)

        with open(file_path, newline="") as f:
            rows = list(csv.DictReader(f))

        conn = hook.get_conn()
        cursor = conn.cursor()

        upsert_sql = """
            INSERT INTO stock_prices
                (symbol, open, high, low, close, volume, price_date)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, price_date)
            DO UPDATE SET
                open       = EXCLUDED.open,
                high       = EXCLUDED.high,
                low        = EXCLUDED.low,
                close      = EXCLUDED.close,
                volume     = EXCLUDED.volume,
                fetched_at = now();
        """

        for row in rows:
            cursor.execute(upsert_sql, (
                row["symbol"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["price_date"],
            ))

        conn.commit()
        cursor.close()
        conn.close()
        log.info("Upserted %s rows into stock_prices", len(rows))

    # ── Wiring ────────────────────────────────────────────────────────────────

    date = check_market_open()
    file = fetch_prices(date)
    validated = validate_prices(file)
    store_prices(validated)

    create_table >> date


stock_prices()