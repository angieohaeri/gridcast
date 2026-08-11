import os

from dotenv import load_dotenv
from loguru import logger
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from gridcast.config import PROCESSED_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

HOURLY_VARS = ["temperature_2m", "precipitation", "wind_speed_10m", "cloud_cover"]

# One-off backfill for the outage window documented in references/decisions.md:
# weather_producer crash-looped on a missing zones CSV from 2026-08-09 04:00 UTC
# to 2026-08-10 04:52 UTC. Scoped to the calendar day the gap was reported for.
START_DATE = "2026-08-09"
END_DATE = "2026-08-09"

INSERT = """
INSERT INTO weather (time, zone, temperature, precipitation, wind_speed, cloud_cover)
VALUES (%(time)s, %(zone)s, %(temperature)s, %(precipitation)s, %(wind_speed)s, %(cloud_cover)s);
"""


def fetch_zone_hours(openmeteo: openmeteo_requests.Client, row: pd.Series) -> list[dict]:
    params = {
        "latitude": row["lat"],
        "longitude": row["lon"],
        "hourly": HOURLY_VARS,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "timezone": "UTC",
    }
    response = openmeteo.weather_api(os.environ["HISTORICAL_API"], params=params)[0]
    hourly = response.Hourly()
    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )
    return [
        {
            "time": t,
            "zone": row["zone_id"],
            "temperature": float(hourly.Variables(0).ValuesAsNumpy()[i]),
            "precipitation": float(hourly.Variables(1).ValuesAsNumpy()[i]),
            "wind_speed": float(hourly.Variables(2).ValuesAsNumpy()[i]),
            "cloud_cover": float(hourly.Variables(3).ValuesAsNumpy()[i]),
        }
        for i, t in enumerate(times)
    ]


def main():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")

    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT zone, date_trunc('hour', time) FROM weather WHERE time >= %s AND time < %s::date + 1",
        (START_DATE, END_DATE),
    )
    existing = {(zone, hour.isoformat()) for zone, hour in cur.fetchall()}

    written = 0
    for _, row in zones.iterrows():
        for record in fetch_zone_hours(openmeteo, row):
            hour_key = (record["zone"], record["time"].isoformat())
            if hour_key in existing:
                continue
            cur.execute(INSERT, record)
            written += 1

    conn.close()
    logger.success(f"Backfilled {written} weather rows for {START_DATE}")


if __name__ == "__main__":
    main()
