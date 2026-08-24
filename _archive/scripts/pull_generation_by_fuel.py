"""Pull PJM generation by fuel type (hourly), 2023-01-01 -> present.

Hourly, RTO-wide - monthly chunks are plenty, same as the marginal value pulls this
pattern is copied from.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_generation_by_fuel_2023_present.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

months = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="MS", tz=None)
bounds = list(months) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    chunk = pjm.get_fuel_mix(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    frames.append(chunk)
    print(f"{start:%Y-%m}: {len(chunk)} rows", flush=True)

fuel_mix = pd.concat(frames, ignore_index=True)
fuel_mix = fuel_mix.drop_duplicates(subset=["Interval Start"])
fuel_mix.to_csv(OUT, index=False)
print(f"\n{len(fuel_mix)} rows -> {OUT}")
