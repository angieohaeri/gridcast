"""Backfill PJM instantaneous load (inst_load) for the 20 in-scope zones + RTO.

inst_load only retains a trailing ~30 days (confirmed empirically 2026-08-22: 29 days
back returns data, 31+ raises NoDataFoundException) - unlike hrl_load_metered/rt_hrl_lmps,
there's no multi-year history available for this feed, so this pulls whatever's left of
the current window rather than a fixed start date.
"""

from datetime import UTC, datetime, timedelta
import os

from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, PROCESSED_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

# inst_load labels 3 zones differently than this project's zone_id codes
ZONE_RENAME = {"APS": "AP", "COMED": "CE", "DAYTON": "DAY", "PJM RTO": "RTO"}

# not zones: UG is an "underground asset" category, not a load area; the 3 regional
# aggregates aren't part of this project's zone scheme. RTO is kept (see schema.sql).
DROP_COLUMNS = [
    "Time", "Interval End", "Load", "UG",
    "PJM MID ATLANTIC REGION", "PJM SOUTHERN REGION", "PJM WESTERN REGION",
]

OUT = INTERIM_DATA_DIR / "pjm_instantaneous_load_30d.csv"

UPSERT = """
INSERT INTO instantaneous_load (time, zone, instantaneous_load_mw)
VALUES %s
ON CONFLICT (time, zone) DO UPDATE SET
    instantaneous_load_mw = EXCLUDED.instantaneous_load_mw
"""

pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

start = (datetime.now(UTC) - timedelta(days=30)).strftime("%Y-%m-%d")
end = datetime.now(UTC).strftime("%Y-%m-%d")
wide = pjm.get_load(start, end)
wide.to_csv(OUT, index=False)
logger.info(f"{len(wide)} 5-min rows, {wide['Interval Start'].min()} -> {wide['Interval Start'].max()} -> {OUT}")

wide = wide.rename(columns=ZONE_RENAME).drop(columns=DROP_COLUMNS)

zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
zone_ids = {"RTO", *zones["zone_id"].unique()}
found = set(wide.columns) - {"Interval Start"}
assert found == zone_ids, f"zone mismatch: {found ^ zone_ids}"

long = wide.melt(id_vars=["Interval Start"], var_name="zone", value_name="instantaneous_load_mw")
long["time"] = long["Interval Start"].dt.tz_convert("UTC")
logger.info(f"{len(long)} zone-intervals across {long['zone'].nunique()} zones")

rows = list(long[["time", "zone", "instantaneous_load_mw"]].itertuples(index=False, name=None))

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
logger.success(f"upserted {len(rows)} instantaneous_load rows")

cur.execute("select count(*), count(distinct zone), min(time), max(time) from instantaneous_load")
logger.info(f"instantaneous_load table: {cur.fetchone()}")
conn.close()
