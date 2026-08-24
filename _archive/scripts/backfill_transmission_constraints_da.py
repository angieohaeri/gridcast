"""Backfill raw_lmp.transmission_constraints_da from pull_transmission_constraints_da.py's snapshot.

"Day Ahead Congestion Event" from the raw feed is dropped - confirmed always identical
to Monitored Facility, redundant. Same dedup-before-upsert pattern as the marginal
value backfills, for the same chunk-boundary-overlap reason.
"""

from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

setup_logging()

SNAPSHOT = INTERIM_DATA_DIR / "pjm_transmission_constraints_da_2023_present.csv"

UPSERT = """
INSERT INTO raw_lmp.transmission_constraints_da
    (datetime_beginning_utc, datetime_ending_utc, duration_hours, monitored_facility, contingency_facility)
VALUES %s
ON CONFLICT (datetime_beginning_utc, monitored_facility, contingency_facility) DO UPDATE SET
    datetime_ending_utc = EXCLUDED.datetime_ending_utc,
    duration_hours = EXCLUDED.duration_hours
"""

raw = pd.read_csv(SNAPSHOT)
raw["datetime_beginning_utc"] = pd.to_datetime(raw["Interval Start"], utc=True)
raw["datetime_ending_utc"] = pd.to_datetime(raw["Interval End"], utc=True)
raw = raw.where(raw.notna(), None)
raw = raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])

logger.info(
    f"{len(raw)} rows, {raw['datetime_beginning_utc'].min()} -> {raw['datetime_beginning_utc'].max()}, "
    f"{raw['Monitored Facility'].nunique()} distinct monitored facilities"
)

rows = list(
    raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Duration", "Monitored Facility",
        "Contingency Facility",
    ]].itertuples(index=False, name=None)
)

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
conn.commit()
logger.success(f"upserted {len(rows)} rows")

cur.execute(
    "select count(*), min(datetime_beginning_utc), max(datetime_beginning_utc) "
    "from raw_lmp.transmission_constraints_da"
)
logger.info(f"raw_lmp.transmission_constraints_da: {cur.fetchone()}")
conn.close()
