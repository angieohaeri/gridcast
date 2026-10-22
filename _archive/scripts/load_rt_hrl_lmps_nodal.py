"""Load the rt_hrl_lmps_nodal parquet checkpoints into raw_lmp.rt_hrl_lmps_nodal.

Checkpoints (backfill_rt_hrl_lmps_nodal.py) store hour-beginning UTC; the table stores hour
ending to match public.lmp / lmp_da_hourly, so time shifts +1h here. Rows before START are
dropped: PJM's pnode-filter cutoff rolled forward a day mid-backfill, so only some chunks
have 2024-10-21 - START gives every pnode the same first hour.

Each file is COPY'd into a temp staging table and upserted on (time, pnode_id), so reruns
are safe and refreshed *_partial months overwrite. DDL: src/consumers/lmp_model_schema.sql.
"""

import io

from dotenv import load_dotenv
from loguru import logger
import pandas as pd

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

SRC = INTERIM_DATA_DIR / "rt_hrl_lmps_nodal"
START = pd.Timestamp("2024-10-22", tz="US/Eastern").tz_convert("UTC")  # hour beginning
COLS = ["time", "pnode_id", "system_energy_price", "lmp", "congestion_price", "marginal_loss_price"]

conn = get_connection()
cur = conn.cursor()
cur.execute("create temp table stage (like raw_lmp.rt_hrl_lmps_nodal including defaults) on commit preserve rows;")

files = sorted(SRC.glob("*.parquet"))
total = 0
for i, f in enumerate(files, 1):
    df = pd.read_parquet(f)
    df = df[df["time"] >= START]
    if df.empty:
        continue
    df["time"] = df["time"] + pd.Timedelta(hours=1)
    buf = io.StringIO()
    df[COLS].to_csv(buf, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S+00")
    buf.seek(0)
    cur.execute("truncate stage;")
    cur.copy_expert(f"copy stage ({', '.join(COLS)}) from stdin with (format csv)", buf)
    cur.execute(f"""
        insert into raw_lmp.rt_hrl_lmps_nodal ({', '.join(COLS)})
        select {', '.join(COLS)} from stage
        on conflict (time, pnode_id) do update set
            system_energy_price = excluded.system_energy_price,
            lmp = excluded.lmp,
            congestion_price = excluded.congestion_price,
            marginal_loss_price = excluded.marginal_loss_price;
    """)
    total += len(df)
    if i % 25 == 0 or i == len(files):
        logger.info(f"[{i}/{len(files)}] {total:,} rows upserted")

cur.execute("select count(*), count(distinct pnode_id), min(time), max(time) from raw_lmp.rt_hrl_lmps_nodal;")
logger.success(f"raw_lmp.rt_hrl_lmps_nodal: {cur.fetchone()}")
conn.close()
