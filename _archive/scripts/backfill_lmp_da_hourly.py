"""Backfill raw_lmp.lmp_da_hourly from pull_lmp_da_hourly.py's snapshot.

Same zone_to_location mapping as public.lmp's producer (src/producers/lmp_producer.py) -
MID-ATL/APS (aggregate), OVEC (out of scope), and PJM-RTO (hub) come back unmapped and
are dropped. time = Interval End (Hour Ending), matching public.lmp's convention.
"""

from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

setup_logging()

SNAPSHOT = INTERIM_DATA_DIR / "pjm_lmp_da_hourly_2023_present.csv"

ZONE_TO_LOCATION = {
    "AE": "AECO",
    "AEP": "AEP",
    "AP": "APS",
    "ATSI": "ATSI",
    "BC": "BGE",
    "CE": "COMED",
    "DAY": "DAY",
    "DEOK": "DEOK",
    "DOM": "DOM",
    "DPL": "DPL",
    "DUQ": "DUQ",
    "EKPC": "EKPC",
    "JC": "JCPL",
    "ME": "METED",
    "PE": "PECO",
    "PEP": "PEPCO",
    "PL": "PPL",
    "PN": "PENELEC",
    "PS": "PSEG",
    "RECO": "RECO",
}
LOCATION_TO_ZONE = {location: zone for zone, location in ZONE_TO_LOCATION.items()}

UPSERT = """
INSERT INTO raw_lmp.lmp_da_hourly (time, zone, lmp, congestion_price, marginal_loss_price)
VALUES %s
ON CONFLICT (time, zone) DO UPDATE SET
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price
"""

raw = pd.read_csv(SNAPSHOT)
raw = raw[raw["Location Short Name"].isin(LOCATION_TO_ZONE)].copy()
raw["zone"] = raw["Location Short Name"].map(LOCATION_TO_ZONE)
raw["time"] = pd.to_datetime(raw["Interval End"], utc=True)
raw = raw.where(raw.notna(), None)
raw = raw.drop_duplicates(subset=["time", "zone"])

logger.info(
    f"{len(raw)} rows, {raw['time'].min()} -> {raw['time'].max()}, {raw['zone'].nunique()} zones"
)

rows = list(raw[["time", "zone", "LMP", "Congestion", "Loss"]].itertuples(index=False, name=None))

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
conn.commit()
logger.success(f"upserted {len(rows)} rows")

cur.execute("select count(*), count(distinct zone), min(time), max(time) from raw_lmp.lmp_da_hourly")
logger.info(f"raw_lmp.lmp_da_hourly: {cur.fetchone()}")
conn.close()
