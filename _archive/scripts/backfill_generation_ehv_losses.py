"""Backfill raw_lmp.generation_ehv_losses from pull_generation_ehv_losses.py's snapshot."""

from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

setup_logging()

SNAPSHOT = INTERIM_DATA_DIR / "pjm_generation_ehv_losses_2023_present.csv"

UPSERT = """
INSERT INTO raw_lmp.generation_ehv_losses (time, total_gen, total_losses)
VALUES %s
ON CONFLICT (time) DO UPDATE SET
    total_gen = EXCLUDED.total_gen,
    total_losses = EXCLUDED.total_losses
"""

raw = pd.read_csv(SNAPSHOT)
raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
raw = raw.where(raw.notna(), None)
raw = raw.drop_duplicates(subset=["time"])

logger.info(f"{len(raw)} rows, {raw['time'].min()} -> {raw['time'].max()}")

rows = list(raw[["time", "total_gen", "total_losses"]].itertuples(index=False, name=None))

conn = get_connection()
cur = conn.cursor()
psycopg2.extras.execute_values(cur, UPSERT, rows, page_size=5000)
conn.commit()
logger.success(f"upserted {len(rows)} rows")

cur.execute("select count(*), min(time), max(time) from raw_lmp.generation_ehv_losses")
logger.info(f"raw_lmp.generation_ehv_losses: {cur.fetchone()}")
conn.close()
