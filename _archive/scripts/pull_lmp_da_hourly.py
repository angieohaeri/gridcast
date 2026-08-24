"""Pull PJM day-ahead hourly zonal LMP (da_hrl_lmps), 2023-01-01 -> present.

Zone-level (location_type=ZONE), not the full pnode set public.lmp uses - same grain
as public.lmp (zone x hour) but the DA market, for a DA-RT basis feature. Monthly
chunks, same pattern as the marginal value pulls this is copied from.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_lmp_da_hourly_2023_present.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

months = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="MS", tz=None)
bounds = list(months) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    chunk = pjm.get_lmp(
        start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        market=gs.Markets.DAY_AHEAD_HOURLY,
        location_type="ZONE",
    )
    frames.append(chunk)
    print(f"{start:%Y-%m}: {len(chunk)} rows", flush=True)

lmp_da = pd.concat(frames, ignore_index=True)
lmp_da = lmp_da.drop_duplicates(subset=["Interval End", "Location Short Name"])
lmp_da.to_csv(OUT, index=False)
print(f"\n{len(lmp_da)} rows -> {OUT}")
