"""Backfill PJM metered hourly load for all 20 in-scope zones, 2023-01-01 -> present.

Reads pull_load.py's snapshot and reshapes it exactly as poll_pjm_load does: sum Load
Area sub-areas per zone-hour, verified only if every sub-area is. Upserts on
(time, zone, source), so re-runs and the producer's own replay window are both safe.
"""


from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, PROCESSED_DATA_DIR, get_connection, setup_logging

setup_logging()

SNAPSHOT = INTERIM_DATA_DIR / "pjm_load_hourly_2023_present.csv"

UPSERT = """
INSERT INTO load (time, zone, source, demand_mw, demand_forecast_mw,
                  net_generation_mw, total_interchange_mw, is_verified)
VALUES %s
ON CONFLICT (time, zone, source) DO UPDATE SET
    demand_mw = EXCLUDED.demand_mw,
    is_verified = EXCLUDED.is_verified
"""

zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
zone_ids = ["RTO"] + zones["zone_id"].unique().tolist()

raw = pd.read_csv(SNAPSHOT)
raw["time"] = pd.to_datetime(raw["Interval End"], utc=True, format="mixed")
raw = raw[raw["Zone"].isin(zone_ids)]

load = raw.groupby(["time", "Zone"], as_index=False).agg(
    demand_mw=("MW", "sum"),
    is_verified=("Is Verified", "all"),
)
load = load.rename(columns={"Zone": "zone"})
load["source"] = "pjm"
logger.info(
    f"{len(load)} zone-hours across {load['zone'].nunique()} zones, "
    f"{load['time'].min()} -> {load['time'].max()}, "
    f"{(~load['is_verified']).sum()} unverified"
)

rows = [
    (r.time, r.zone, r.source, r.demand_mw, None, None, None, bool(r.is_verified))
    for r in load.itertuples(index=False)
]

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
logger.success(f"upserted {len(rows)} load rows")

cur.execute("select source, count(*), count(distinct zone) from load group by 1")
for row in cur.fetchall():
    logger.info(f"load table: {row}")
conn.close()

SNAPSHOT.rename(INTERIM_DATA_DIR / "pjm_load_hourly_2023_present.csv")
logger.success("snapshot promoted to pjm_load_hourly_2023_present.csv")
