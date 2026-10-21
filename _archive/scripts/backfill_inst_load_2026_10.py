"""One-off backfill of PJM instantaneous load for the 2026-10-18..19 poller outage.

The weather and inst_load pollers dropped together, so inst_load had no readings from
2026-10-18 ~20:00 to 2026-10-19 ~13:00 UTC. PJM's inst_load feed still retains the
window (it keeps ~30 days), so this refills it. Scoped to the two calendar days the gap
spans; the upsert on (time, zone) makes re-runs idempotent.
"""

import os

from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import PROCESSED_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

START = "2026-10-18"
END = "2026-10-20"

# inst_load labels 3 zones differently than this project's zone_id codes
ZONE_RENAME = {"APS": "AP", "COMED": "CE", "DAYTON": "DAY", "PJM RTO": "RTO"}

# not zones: UG is an "underground asset" category; the 3 regional aggregates aren't part
# of this project's zone scheme. RTO is kept (see schema.sql).
DROP_COLUMNS = [
    "Time", "Interval End", "Load", "UG",
    "PJM MID ATLANTIC REGION", "PJM SOUTHERN REGION", "PJM WESTERN REGION",
]

UPSERT = """
INSERT INTO instantaneous_load (time, zone, instantaneous_load_mw)
VALUES %s
ON CONFLICT (time, zone) DO UPDATE SET
    instantaneous_load_mw = EXCLUDED.instantaneous_load_mw
"""


def main():
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    wide = pjm.get_load(START, END)
    wide = wide.rename(columns=ZONE_RENAME).drop(columns=DROP_COLUMNS)

    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
    zone_ids = {"RTO", *zones["zone_id"].unique()}
    found = set(wide.columns) - {"Interval Start"}
    assert found == zone_ids, f"zone mismatch: {found ^ zone_ids}"

    long = wide.melt(id_vars=["Interval Start"], var_name="zone", value_name="instantaneous_load_mw")
    long["time"] = long["Interval Start"].dt.tz_convert("UTC")
    rows = list(long[["time", "zone", "instantaneous_load_mw"]].itertuples(index=False, name=None))

    conn = get_connection()
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
    conn.close()
    logger.success(f"upserted {len(rows)} instantaneous_load rows for {START}..{END}")


if __name__ == "__main__":
    main()
