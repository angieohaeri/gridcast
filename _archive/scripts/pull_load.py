"""Pull PJM metered hourly load for all zones, 2023-01-01 -> present.

Chunked monthly: the full range comes back empty, and error="ignore" hides it as
pd.concat's "No objects to concatenate" rather than the real error.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_load_hourly_2023_present.new.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

months = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="MS", tz=None)
bounds = list(months) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    chunk = pjm.get_load_metered_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    frames.append(chunk)
    print(f"{start:%Y-%m}: {len(chunk)} rows", flush=True)

load_hourly = pd.concat(frames, ignore_index=True)
load_hourly = load_hourly.drop_duplicates(subset=["Interval End", "Zone", "Load Area"])
load_hourly.to_csv(OUT, index=False)
print(f"\n{len(load_hourly)} rows -> {OUT}")
print(sorted(load_hourly["Zone"].unique()))
