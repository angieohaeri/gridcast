"""Backfill raw_lmp.operator_initiated_commitments from pull_operator_initiated_commitments.py's snapshot.

Same zone_to_location mapping as public.lmp's producer (src/producers/lmp_producer.py) -
OVEC (out of scope) comes back unmapped and is dropped. No stable per-row id in the raw
feed, so duplicates on the full natural key collapse on upsert (see lmp_model_schema.sql
comment for why that's safe here).
"""

from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

setup_logging()

SNAPSHOT = INTERIM_DATA_DIR / "pjm_operator_initiated_commitments_2023_present.csv"

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
INSERT INTO raw_lmp.operator_initiated_commitments (datetime_beginning_utc, zone, economic_max_mw, reason)
VALUES %s
ON CONFLICT (datetime_beginning_utc, zone, reason, economic_max_mw) DO NOTHING
"""

raw = pd.read_csv(SNAPSHOT)
raw = raw[raw["zone"].isin(LOCATION_TO_ZONE)].copy()
raw["zone"] = raw["zone"].map(LOCATION_TO_ZONE)
raw["datetime_beginning_utc"] = pd.to_datetime(raw["Interval Start"], utc=True)
raw = raw.where(raw.notna(), None)
raw = raw.drop_duplicates(subset=["datetime_beginning_utc", "zone", "reason", "economic_max_mw"])

logger.info(
    f"{len(raw)} rows, {raw['datetime_beginning_utc'].min()} -> {raw['datetime_beginning_utc'].max()}, "
    f"{raw['zone'].nunique()} zones"
)

rows = list(raw[["datetime_beginning_utc", "zone", "economic_max_mw", "reason"]].itertuples(index=False, name=None))

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
conn.commit()
logger.success(f"upserted {len(rows)} rows")

cur.execute(
    "select count(*), count(distinct zone), min(datetime_beginning_utc), max(datetime_beginning_utc) "
    "from raw_lmp.operator_initiated_commitments"
)
logger.info(f"raw_lmp.operator_initiated_commitments: {cur.fetchone()}")
conn.close()
