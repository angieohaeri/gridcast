"""Pull PJM day-ahead transmission constraints (hourly events), 2023-01-01 -> present.

Which facility/contingency pairs bound in the day-ahead market and for how long - no
price magnitude, that's marginal_value_da's job. Monthly chunks, same pattern as the
marginal value pulls this is copied from.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_transmission_constraints_da_2023_present.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

months = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="MS", tz=None)
bounds = list(months) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    chunk = pjm.get_transmission_constraints_day_ahead_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    frames.append(chunk)
    print(f"{start:%Y-%m}: {len(chunk)} rows", flush=True)

constraints = pd.concat(frames, ignore_index=True)
constraints = constraints.drop_duplicates(subset=["Interval Start", "Monitored Facility", "Contingency Facility"])
constraints.to_csv(OUT, index=False)
print(f"\n{len(constraints)} rows -> {OUT}")
