"""Pull PJM day-ahead marginal value (hourly), 2023-01-01 -> present.

Hourly, not 5-min - monthly chunks are plenty, same as the load pull this pattern
is copied from.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_da_marginal_value_2023_present.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

months = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="MS", tz=None)
bounds = list(months) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    chunk = pjm.get_marginal_value_day_ahead_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    frames.append(chunk)
    print(f"{start:%Y-%m}: {len(chunk)} rows", flush=True)

marginal_value = pd.concat(frames, ignore_index=True)
marginal_value = marginal_value.drop_duplicates(subset=["Interval Start", "Monitored Facility", "Contingency Facility"])
marginal_value.to_csv(OUT, index=False)
print(f"\n{len(marginal_value)} rows -> {OUT}")
