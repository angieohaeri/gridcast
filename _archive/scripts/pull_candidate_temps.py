"""Pull hourly temperature history for every candidate city, 2023-01-01 -> present.

Temperature only - scoring needs nothing else. Batched via comma-joined coords.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

HERE = Path(__file__).parent
BATCH_SIZE = 10
END_DATE = "2026-08-11"

candidates = pd.read_csv(HERE / "candidates.csv")

cache_session = requests_cache.CachedSession(str(INTERIM_DATA_DIR / "openmeteo_candidate_cache"), expire_after=-1)
openmeteo = openmeteo_requests.Client(session=retry(cache_session, retries=5, backoff_factor=0.2))

frames = []
for start in range(0, len(candidates), BATCH_SIZE):
    batch = candidates.iloc[start : start + BATCH_SIZE]
    params = {
        "latitude": ",".join(str(v) for v in batch["lat"]),
        "longitude": ",".join(str(v) for v in batch["lon"]),
        "hourly": "temperature_2m",
        "start_date": "2023-01-01",
        "end_date": END_DATE,
        "timezone": "UTC",
    }
    responses = openmeteo.weather_api(os.environ["HISTORICAL_API"], params=params)

    for (_, row), response in zip(batch.iterrows(), responses, strict=True):
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
                    "zone_id": row["zone_id"],
                    "city": row["city"],
                    "temperature": hourly.Variables(0).ValuesAsNumpy(),
                }
            )
        )
    print(f"batch {start // BATCH_SIZE}: {len(batch)} cities")

temps = pd.concat(frames, ignore_index=True)
temps.to_parquet(INTERIM_DATA_DIR / "candidate_temps.parquet", index=False)
print(f"{len(temps)} rows, {temps['city'].nunique()} cities")
