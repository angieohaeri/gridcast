"""Pull PJM forecasted generation outages, 2023-01-01 -> present.

One row per (forecast execution, forecast target date) - small volume (~90 rows per
execution, roughly one execution per day), monthly chunks are plenty.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_forecasted_gen_outages_2023_present.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

months = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="MS", tz=None)
bounds = list(months) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    chunk = pjm.get_forecasted_generation_outages(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    frames.append(chunk)
    print(f"{start:%Y-%m}: {len(chunk)} rows", flush=True)

outages = pd.concat(frames, ignore_index=True)
outages.to_csv(OUT, index=False)
print(f"\n{len(outages)} rows -> {OUT}")
print(outages.columns.tolist())
print(outages.head())
