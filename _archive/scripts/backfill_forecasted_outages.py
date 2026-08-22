"""Backfill raw_lmp.forecasted_generation_outages from pull_forecasted_outages.py's snapshot.

Interval Start is the forecast's target day (Eastern midnight -> next midnight), Publish
Time is when that forecast was made. Chunk-boundary overlap in the pull produced exact
duplicate rows - ON CONFLICT collapses them, no separate dedup needed.
"""

from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

setup_logging()

SNAPSHOT = INTERIM_DATA_DIR / "pjm_forecasted_gen_outages_2023_present.csv"

UPSERT = """
INSERT INTO raw_lmp.forecasted_generation_outages
    (forecast_execution_date, forecast_date, outage_mw_rto, outage_mw_west, outage_mw_other)
VALUES %s
ON CONFLICT (forecast_execution_date, forecast_date) DO UPDATE SET
    outage_mw_rto = EXCLUDED.outage_mw_rto,
    outage_mw_west = EXCLUDED.outage_mw_west,
    outage_mw_other = EXCLUDED.outage_mw_other
"""

raw = pd.read_csv(SNAPSHOT)
raw["forecast_execution_date"] = pd.to_datetime(raw["Publish Time"], utc=True)
raw["forecast_date"] = pd.to_datetime(raw["Interval Start"], utc=True).dt.date
raw = raw.drop_duplicates(subset=["forecast_execution_date", "forecast_date"])

logger.info(
    f"{len(raw)} rows, {raw['forecast_date'].min()} -> {raw['forecast_date'].max()} target dates, "
    f"{raw['forecast_execution_date'].nunique()} distinct forecast executions"
)

rows = list(
    raw[["forecast_execution_date", "forecast_date", "RTO MW", "West MW", "Other MW"]]
    .itertuples(index=False, name=None)
)

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
conn.commit()
logger.success(f"upserted {len(rows)} rows")

cur.execute("select count(*), min(forecast_date), max(forecast_date) from raw_lmp.forecasted_generation_outages")
logger.info(f"raw_lmp.forecasted_generation_outages: {cur.fetchone()}")
conn.close()
