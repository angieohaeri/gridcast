"""Backfill raw_lmp.natural_gas_fuel_cost from pull_natural_gas_fuel_cost.py's snapshot.

Drops EIA's census-region/national/territory rows (e.g. ENC, PCC, US, PR) - only real
states are kept. cost-per-btu-units dropped - constant ("dollars per million Btu")
across every row, not a data column. sectorid/fueltypeid/*Description columns dropped -
constant too, since the pull already scoped to NG + Electric Power sector.
"""

from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

setup_logging()

SNAPSHOT = INTERIM_DATA_DIR / "eia_natural_gas_fuel_cost_2023_present.csv"

# Non-state location codes returned alongside real states: census regions, national
# total, and Puerto Rico - not states, dropped.
NON_STATE_LOCATIONS = {
    "90", "ENC", "ESC", "MAT", "MTN", "NEW", "PCC", "PCN", "SAT", "WNC", "WSC", "PR", "US",
}

UPSERT = """
INSERT INTO raw_lmp.natural_gas_fuel_cost (period, location, cost_per_mmbtu)
VALUES %s
ON CONFLICT (period, location) DO UPDATE SET
    cost_per_mmbtu = EXCLUDED.cost_per_mmbtu
"""

raw = pd.read_csv(SNAPSHOT)
raw = raw[~raw["location"].isin(NON_STATE_LOCATIONS)].copy()
raw["period"] = pd.to_datetime(raw["period"], format="%Y-%m").dt.date
raw = raw.rename(columns={"cost-per-btu": "cost_per_mmbtu"})
raw = raw.where(raw.notna(), None)
raw = raw.drop_duplicates(subset=["period", "location"])

logger.info(
    f"{len(raw)} rows, {raw['period'].min()} -> {raw['period'].max()}, {raw['location'].nunique()} states"
)

rows = list(raw[["period", "location", "cost_per_mmbtu"]].itertuples(index=False, name=None))

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
conn.commit()
logger.success(f"upserted {len(rows)} rows")

cur.execute("select count(*), count(distinct location), min(period), max(period) from raw_lmp.natural_gas_fuel_cost")
logger.info(f"raw_lmp.natural_gas_fuel_cost: {cur.fetchone()}")
conn.close()
