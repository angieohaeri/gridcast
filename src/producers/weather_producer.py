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
    # Open-Meteo accepts comma-joined coordinates and returns one response per station in
    # request order, so all 30 stations cost one call rather than 30 sequential ones.
    params = {
        "latitude": ",".join(str(v) for v in zones["lat"]),
        "longitude": ",".join(str(v) for v in zones["lon"]),
        "current": HOURLY_VARS,
        "timezone": "UTC",
    }
    responses = openmeteo.weather_api(os.environ["FORECAST_API"], params=params)

    readings = []
    for (_, row), response in zip(zones.iterrows(), responses):
        current = response.Current()
        readings.append(
            {
                "time": pd.to_datetime(current.Time(), unit="s", utc=True),
                "zone": row["zone_id"],
                "temperature": current.Variables(0).Value(),
                "precipitation": current.Variables(1).Value(),
                "wind_speed": current.Variables(2).Value(),
                "cloud_cover": current.Variables(3).Value(),
            }
        )

    # Seven zones are built from 2-3 stations (see pjm_weather_zones.csv) because their
    # load spans more than one climate. Averaging here keeps the weather table at one
    # reading per zone per poll, so observation_count stays comparable across zones.
    collapsed = pd.DataFrame(readings).groupby("zone", as_index=False).agg(
        time=("time", "min"),
        temperature=("temperature", "mean"),
        precipitation=("precipitation", "mean"),
        wind_speed=("wind_speed", "mean"),
        cloud_cover=("cloud_cover", "mean"),
    )
    return collapsed.to_dict(orient="records")

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
