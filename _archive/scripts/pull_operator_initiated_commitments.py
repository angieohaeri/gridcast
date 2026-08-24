"""Pull PJM operator initiated commitments (ops_init_commit), 2023-01-01 -> present.

No gridstatus wrapper for this feed - calls gridstatus's PJM._get_pjm_json() directly
against the raw Data Miner 2 feed name instead (reuses its auth/retry/pagination
handling rather than writing a new HTTP client). Irregular event-level timestamps, not
hourly - monthly chunks are plenty, same pattern as the other Tier 1 pulls.
"""

from datetime import UTC, datetime
import os

from dotenv import load_dotenv
import gridstatus as gs
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR

load_dotenv()

OUT = INTERIM_DATA_DIR / "pjm_operator_initiated_commitments_2023_present.csv"

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

months = pd.date_range("2023-01-01", datetime.now(UTC).date(), freq="MS", tz=None)
bounds = list(months) + [pd.Timestamp(datetime.now(UTC).date())]

frames = []
for start, end in zip(bounds, bounds[1:]):
    try:
        chunk = pjm._get_pjm_json(
            "ops_init_commit",
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            params={},
        )
    except gs.NoDataFoundException:
        print(f"{start:%Y-%m}: 0 rows (no data)", flush=True)
        continue
    frames.append(chunk)
    print(f"{start:%Y-%m}: {len(chunk)} rows", flush=True)

commitments = pd.concat(frames, ignore_index=True)
commitments = commitments.drop_duplicates(subset=["Interval Start", "zone", "reason", "economic_max_mw"])
commitments.to_csv(OUT, index=False)
print(f"\n{len(commitments)} rows -> {OUT}")
