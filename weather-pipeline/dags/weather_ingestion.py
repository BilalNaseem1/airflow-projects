from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests, csv
import pendulum


WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=24.8607&longitude=67.0011"
    "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
)


@dag(
        schedule='@hourly',
        catchup=False,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
)
def weather_ingestion():

    
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "postgres",
        sql = """
            CREATE TABLE IF NOT EXISTS weather_readings (
                id           SERIAL PRIMARY KEY,
                city         TEXT NOT NULL,
                temperature  NUMERIC,
                humidity     INTEGER,
                wind_speed   NUMERIC,
                weather_code INTEGER,
                recorded_at  TIMESTAMP
                );
        """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get(WEATHER_URL)
        if response.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=response.json())
        else:
            return PokeReturnValue(is_done=False, xcom_value=None)

    @task
    def extract_weather(raw):
        current = raw["current"]
        return {
            "city":         "Karachi",
            "temperature":  current["temperature_2m"],
            "humidity":     current["relative_humidity_2m"],
            "wind_speed":   current["wind_speed_10m"],
            "weather_code": current["weather_code"],
        }
    @task
    def process_weather(weather_data):
        weather_data["recorded_at"] = pendulum.now().isoformat()
        with open("/tmp/weather.csv", "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["city", "temperature", "humidity",
                            "wind_speed", "weather_code", "recorded_at"]
            )
            writer.writeheader()
            writer.writerow(weather_data)

    @task
    def store_weather():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="""
                COPY weather_readings
                    (city, temperature, humidity, wind_speed, weather_code, recorded_at)
                FROM STDIN WITH CSV HEADER
            """,
            filename="/tmp/weather.csv"
        )

   
    raw = is_api_available()
    weather_data = extract_weather(raw)

    create_table >> is_api_available() 
    process_weather(weather_data) >> store_weather()


weather_ingestion()