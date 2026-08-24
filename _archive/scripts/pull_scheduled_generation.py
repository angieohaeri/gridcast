"""Pull PJM scheduled generation (rt_and_self_ecomax, hourly), 2023-01-01 -> present.

No gridstatus wrapper for this feed - calls gridstatus's PJM._get_pjm_json() directly
against the raw Data Miner 2 feed name instead (reuses its auth/retry/pagination
handling rather than writing a new HTTP client). Monthly chunks, same pattern as the
other Tier 1 pulls.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_scheduled_generation_2023_present.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

months = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="MS", tz=None)
bounds = list(months) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    chunk = pjm._get_pjm_json("rt_and_self_ecomax", start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), params={})
    frames.append(chunk)
    print(f"{start:%Y-%m}: {len(chunk)} rows", flush=True)

scheduled = pd.concat(frames, ignore_index=True)
scheduled = scheduled.drop_duplicates(subset=["Interval Start"])
scheduled.to_csv(OUT, index=False)
print(f"\n{len(scheduled)} rows -> {OUT}")
