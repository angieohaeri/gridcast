"""Backfill Open-Meteo history for all 20 zones, 30 stations.

Every zone is deleted and rewritten - the original four all changed station under the
degree-hour scoring, so old and new readings must not mix. Writes direct to the
hypertable, bypassing Kafka, as this is bounded history rather than a live feed.
"""

import os

from dotenv import load_dotenv
from loguru import logger
import openmeteo_requests
import pandas as pd
import psycopg2.extras
import requests_cache
from retry_requests import retry

from gridcast.config import (
    EXTERNAL_DATA_DIR,
    INTERIM_DATA_DIR,
    PROCESSED_DATA_DIR,
    PROJ_ROOT,
    get_connection,
    setup_logging,
)

load_dotenv(PROJ_ROOT / ".env")
setup_logging()

HOURLY_VARS = ["temperature_2m", "precipitation", "wind_speed_10m", "cloud_cover"]
START_DATE = "2023-01-01"
BATCH_SIZE = 10

INSERT = """
INSERT INTO weather (time, zone, temperature, precipitation, wind_speed, cloud_cover)
VALUES %s
"""

targets = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")

end_date = pd.Timestamp.utcnow().strftime("%Y-%m-%d")

cache_session = requests_cache.CachedSession(str(INTERIM_DATA_DIR / "openmeteo_backfill_cache"), expire_after=-1)
openmeteo = openmeteo_requests.Client(session=retry(cache_session, retries=5, backoff_factor=0.2))

frames = []
for start in range(0, len(targets), BATCH_SIZE):
    batch = targets.iloc[start : start + BATCH_SIZE]
    params = {
        "latitude": ",".join(str(v) for v in batch["lat"]),
        "longitude": ",".join(str(v) for v in batch["lon"]),
        "hourly": HOURLY_VARS,
        "start_date": START_DATE,
        "end_date": end_date,
        "timezone": "UTC",
    }
    responses = openmeteo.weather_api(os.environ["HISTORICAL_API"], params=params)

    for (_, row), response in zip(batch.iterrows(), responses):
        hourly = response.Hourly()
        frames.append(
            pd.DataFrame(
                {
                    "time": pd.date_range(
                        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                        freq=pd.Timedelta(seconds=hourly.Interval()),
                        inclusive="left",
                    ),
                    "zone": row["zone_id"],
                    "temperature": hourly.Variables(0).ValuesAsNumpy(),
                    "precipitation": hourly.Variables(1).ValuesAsNumpy(),
                    "wind_speed": hourly.Variables(2).ValuesAsNumpy(),
                    "cloud_cover": hourly.Variables(3).ValuesAsNumpy(),
                }
            )
        )
    logger.info(f"pulled {len(batch)} cities")

WEATHER_COLUMNS = ["time", "zone", "temperature", "precipitation", "wind_speed", "cloud_cover"]

# seven zones carry 2-3 stations; average so the hypertable stays one row per (time, zone)
weather = pd.concat(frames, ignore_index=True).dropna()
weather = weather.groupby(["time", "zone"], as_index=False).mean()[WEATHER_COLUMNS]
logger.info(f"{len(weather)} rows across {weather['zone'].nunique()} zones, through {weather['time'].max()}")

conn = get_connection()
cur = conn.cursor()

# covers re-stationed zones and any partial previous run
cur.execute("DELETE FROM weather WHERE zone = ANY(%s)", (sorted(set(targets["zone_id"])),))
logger.info(f"cleared {cur.rowcount} existing rows")

psycopg2.extras.execute_values(
    cur,
    INSERT,
    list(weather.itertuples(index=False, name=None)),
    page_size=5000,
)
logger.success(f"inserted {len(weather)} weather rows")

csv_path = EXTERNAL_DATA_DIR / "openmeteo_hourly_2023_present.csv"
weather.sort_values(["zone", "time"]).to_csv(csv_path, index=False)
logger.success(f"{csv_path.name}: {len(weather)} rows, {weather['zone'].nunique()} zones")

conn.close()
