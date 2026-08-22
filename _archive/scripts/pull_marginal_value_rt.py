"""Pull PJM real-time marginal value (5-min), 2023-01-01 -> present.

Chunked weekly: this is 5-min-granular across many constraints, much denser than the
hourly load pull this pattern is copied from - weekly keeps individual requests small
and resumable if PJM's API (already known flaky on large paginated pulls) times out.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_rt_marginal_value_2023_present.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

weeks = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="W-MON", tz=None)
bounds = list(weeks) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    chunk = pjm.get_marginal_value_real_time_5_min(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    frames.append(chunk)
    print(f"{start:%Y-%m-%d}: {len(chunk)} rows", flush=True)

marginal_value = pd.concat(frames, ignore_index=True)
marginal_value = marginal_value.drop_duplicates(subset=["Interval Start", "Monitored Facility", "Contingency Facility"])
marginal_value.to_csv(OUT, index=False)
print(f"\n{len(marginal_value)} rows -> {OUT}")
