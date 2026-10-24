"""Fill hours where public.weather has no row for any zone - live polling outages.

Same fetch/insert as backfill_weather_2026_10.py (Open-Meteo historical hourly per station,
only zone-hours not already present), but the days come from the table instead of
constants: every UTC day containing an hour with zero rows.
"""

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

INSERT = """
INSERT INTO weather (time, zone, temperature, precipitation, wind_speed, cloud_cover)
VALUES (%(time)s, %(zone)s, %(temperature)s, %(precipitation)s, %(wind_speed)s, %(cloud_cover)s);
"""


def fetch_zone_hours(openmeteo: openmeteo_requests.Client, row: pd.Series, day: str) -> list[dict]:
    params = {
        "latitude": row["lat"],
        "longitude": row["lon"],
        "hourly": HOURLY_VARS,
        "start_date": day,
        "end_date": day,
        "timezone": "UTC",
    }
    hourly = openmeteo.weather_api(os.environ["HISTORICAL_API"], params=params)[0].Hourly()
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
    openmeteo = openmeteo_requests.Client(session=retry(requests_cache.CachedSession(".cache", expire_after=-1), retries=5, backoff_factor=0.2))

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        with h as (select generate_series(min(date_trunc('hour', time)), max(time), interval '1 hour') t from weather)
        select distinct (h.t at time zone 'UTC')::date from h
        where not exists (select 1 from weather w where date_trunc('hour', w.time) = h.t)
        order by 1
    """)
    days = [str(r[0]) for r in cur.fetchall()]
    logger.info(f"days with empty hours: {days}")

    written = 0
    for day in days:
        cur.execute("SELECT zone, date_trunc('hour', time) FROM weather WHERE time >= %s AND time < %s::date + 1", (day, day))
        existing = {(zone, hour.isoformat()) for zone, hour in cur.fetchall()}
        for _, row in zones.iterrows():
            for record in fetch_zone_hours(openmeteo, row, day):
                if (record["zone"], record["time"].isoformat()) in existing:
                    continue
                cur.execute(INSERT, record)
                written += 1
    conn.close()
    logger.success(f"Backfilled {written} weather rows across {len(days)} day(s)")


if __name__ == "__main__":
    main()
