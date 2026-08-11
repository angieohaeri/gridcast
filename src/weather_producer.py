import os

from dotenv import load_dotenv
from kafka_client import build_producer, produce_json
from loguru import logger
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from gridcast.config import PROCESSED_DATA_DIR, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

HOURLY_VARS = ["temperature_2m", "precipitation", "wind_speed_10m", "cloud_cover"]


def poll_weather(openmeteo: openmeteo_requests.Client, zones: pd.DataFrame) -> list[dict]:
    records = []
    for _, row in zones.iterrows():
        params = {
            "latitude": row["lat"],
            "longitude": row["lon"],
            "current": HOURLY_VARS,
            "timezone": "UTC",
        }
        response = openmeteo.weather_api(os.environ["FORECAST_API"], params=params)[0]
        current = response.Current()
        records.append(
            {
                "time": pd.to_datetime(current.Time(), unit="s", utc=True),
                "zone": row["zone_id"],
                "temperature": current.Variables(0).Value(),
                "precipitation": current.Variables(1).Value(),
                "wind_speed": current.Variables(2).Value(),
                "cloud_cover": current.Variables(3).Value(),
            }
        )
    return records

@flow(name="weather_producer", description="Polls weather data every 20 min from Open-Meteo.", log_prints=True)
def main():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")

    cache_session = requests_cache.CachedSession(".cache", expire_after=300)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    records = poll_weather(openmeteo, zones)

    producer = build_producer()
    for record in records:
        produce_json(producer, "weather", key=record["zone"], record=record)
    producer.flush()
    logger.success(f"Produced {len(records)} weather messages")


if __name__ == "__main__":
    main()
