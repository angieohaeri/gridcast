"""Backfill raw_lmp.generation_by_fuel from pull_generation_by_fuel.py's snapshot.

gridstatus returns fuel mix wide (one column per fuel type) - melted to long here to
match this project's other categorical time series.
"""

from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

setup_logging()

SNAPSHOT = INTERIM_DATA_DIR / "pjm_generation_by_fuel_2023_present.csv"

FUEL_COLUMNS = [
    "Coal", "Gas", "Hydro", "Multiple Fuels", "Nuclear", "Oil",
    "Other Renewables", "Solar", "Storage", "Wind",
]

UPSERT = """
INSERT INTO raw_lmp.generation_by_fuel (time, fuel_type, generation_mw)
VALUES %s
ON CONFLICT (time, fuel_type) DO UPDATE SET
    generation_mw = EXCLUDED.generation_mw
"""

raw = pd.read_csv(SNAPSHOT)
raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
raw = raw.drop_duplicates(subset=["time"])

long = raw.melt(id_vars=["time"], value_vars=FUEL_COLUMNS, var_name="fuel_type", value_name="generation_mw")
long = long.where(long.notna(), None)

logger.info(
    f"{len(long)} rows, {long['time'].min()} -> {long['time'].max()}, "
    f"{long['fuel_type'].nunique()} fuel types"
)

rows = list(long[["time", "fuel_type", "generation_mw"]].itertuples(index=False, name=None))

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
conn.commit()
logger.success(f"upserted {len(rows)} rows")

cur.execute("select count(*), min(time), max(time) from raw_lmp.generation_by_fuel")
logger.info(f"raw_lmp.generation_by_fuel: {cur.fetchone()}")
conn.close()
