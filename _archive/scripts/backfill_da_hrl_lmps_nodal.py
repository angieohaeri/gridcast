"""Backfill day-ahead hourly LMPs (da_hrl_lmps) for the GNN node set into resumable parquet
checkpoints. A copy of backfill_rt_hrl_lmps_nodal.py with the feed and field names swapped -
same request limits; DA additionally has no verification lag (it clears the day before).

Node set: data/interim/lmp_backfill_nodes.parquet (build_backfill_node_list.py), sorted by
pnode_id and split into chunks of at most IDS_PER_CHUNK ids AND MAX_ID_PARAM_CHARS of
URL-encoded pnode_id parameter. The real limit is URL length, not id count: an over-long URL
returns 404 (or 414 when much longer). 150 mostly-10-digit ids (1,888 chars) failed every
time; 1,705 chars is the longest that has worked.

Window: Data Miner 2 only accepts a pnode_id filter on non-archived data - the last 731 days
for da_hrl_lmps (archived requests 400 with "invalid attribute(s) for archived data -
Pnode_Id"). Start one day inside that cutoff; end at the end of today (END_LAG_DAYS = -1:
today's DA prices posted yesterday).

Rate limit: free tier is 6 requests/min. gridstatus pages back-to-back and only backs off
after a 429, which has crashed earlier backfills (see backfill_lmp_rt_unverified_fivemin.py).
So every request is sized to a single page (chunk x WINDOW_DAYS x 24 <= 50,000 rows) and
spaced REQUEST_SPACING_S apart. A chunk-month that still fails is retried after a cooldown.

Checkpoints: one parquet per chunk per calendar month (EPT), written tmp-then-rename, so a
crash loses at most one file. Complete months are skipped on rerun; the current month is
written as *_partial.parquet and refreshed on every run.

Output: data/interim/da_hrl_lmps_nodal/chunk{NN}_{YYYY-MM}[_partial].parquet
"""

import logging
import os
import time

from dotenv import load_dotenv
import gridstatus as gs
from gridstatus.base import NoDataFoundException
from loguru import logger
import pandas as pd
import requests

from gridcast.config import INTERIM_DATA_DIR, setup_logging

load_dotenv()
setup_logging()
logging.getLogger("gridstatus").setLevel(logging.WARNING)  # its INFO lines log the API key

OUT_DIR = INTERIM_DATA_DIR / "da_hrl_lmps_nodal"
IDS_PER_CHUNK = 150
MAX_ID_PARAM_CHARS = 1705
WINDOW_DAYS = 13
REQUEST_SPACING_S = 10.5
ARCHIVE_DAYS = 731
END_LAG_DAYS = -1
CHUNK_RETRIES = 4
CHUNK_RETRY_COOLDOWN_S = 90
FIELDS = "datetime_beginning_utc,pnode_id,system_energy_price_da,total_lmp_da,congestion_price_da,marginal_loss_price_da,row_is_current"

OUT_DIR.mkdir(parents=True, exist_ok=True)
pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)

nodes = pd.read_parquet(INTERIM_DATA_DIR / "lmp_backfill_nodes.parquet")["pnode_id"].sort_values().tolist()
chunks, cur = [], []
for pid in nodes:
    nxt = cur + [pid]
    if len(nxt) > IDS_PER_CHUNK or sum(len(str(x)) for x in nxt) + 3 * (len(nxt) - 1) > MAX_ID_PARAM_CHARS:
        chunks.append(cur)
        nxt = [pid]
    cur = nxt
chunks.append(cur)

today = pd.Timestamp.now(tz="US/Eastern").normalize()
start = today - pd.Timedelta(days=ARCHIVE_DAYS - 1)
end = today - pd.Timedelta(days=END_LAG_DAYS)
months = pd.date_range(start.replace(day=1), end - pd.Timedelta(minutes=1), freq="MS", tz="US/Eastern")
logger.info(f"{len(nodes)} pnodes in {len(chunks)} chunks, {start.date()} -> {end.date()}, {len(months)} months")

last_request = 0.0
EMPTY = pd.DataFrame({
    "time": pd.Series(dtype="datetime64[ns, UTC]"),
    "pnode_id": pd.Series(dtype="int64"),
    **{c: pd.Series(dtype="float32") for c in ["system_energy_price", "lmp", "congestion_price", "marginal_loss_price"]},
})


def fetch(ids: list[int], lo: pd.Timestamp, hi: pd.Timestamp) -> pd.DataFrame:
    global last_request
    wait = REQUEST_SPACING_S - (time.monotonic() - last_request)
    if wait > 0:
        time.sleep(wait)
    last_request = time.monotonic()
    try:
        raw = pjm._get_pjm_json(
            "da_hrl_lmps",
            start=lo,
            end=hi - pd.Timedelta(minutes=1),  # end is inclusive
            params={"pnode_id": ";".join(map(str, ids)), "fields": FIELDS},
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
        )
    except NoDataFoundException:
        # whole chunk not yet effective in this window (60 of the node set started after 2024-10)
        return EMPTY
    raw = raw[raw["row_is_current"]]
    return pd.DataFrame({
        "time": raw["Interval Start"].dt.tz_convert("UTC"),
        "pnode_id": raw["pnode_id"].astype("int64"),
        "system_energy_price": raw["system_energy_price_da"].astype("float32"),
        "lmp": raw["total_lmp_da"].astype("float32"),
        "congestion_price": raw["congestion_price_da"].astype("float32"),
        "marginal_loss_price": raw["marginal_loss_price_da"].astype("float32"),
    })


def pull_month(ids: list[int], month: pd.Timestamp) -> pd.DataFrame:
    lo = max(month, start)
    hi = min(month + pd.offsets.MonthBegin(1), end)
    parts = []
    while lo < hi:
        nxt = min(lo + pd.Timedelta(days=WINDOW_DAYS), hi)
        parts.append(fetch(ids, lo, nxt))
        lo = nxt
    return pd.concat(parts).drop_duplicates(["time", "pnode_id"]).sort_values(["pnode_id", "time"])


done = 0
total = len(chunks) * len(months)
t0 = time.monotonic()
for k, ids in enumerate(chunks):
    for month in months:
        partial = month + pd.offsets.MonthBegin(1) > end
        path = OUT_DIR / f"chunk{k:02d}_{month:%Y-%m}{'_partial' if partial else ''}.parquet"
        done += 1
        if path.exists() and not partial:
            continue
        for attempt in range(1, CHUNK_RETRIES + 1):
            try:
                df = pull_month(ids, month)
                break
            except (RuntimeError, requests.exceptions.RequestException) as e:
                if attempt == CHUNK_RETRIES:
                    raise
                logger.warning(f"chunk{k:02d} {month:%Y-%m} attempt {attempt} failed ({str(e)[:80]}); cooling down {CHUNK_RETRY_COOLDOWN_S}s")
                time.sleep(CHUNK_RETRY_COOLDOWN_S)
        tmp = path.with_suffix(".tmp")
        df.to_parquet(tmp)
        os.replace(tmp, path)
        elapsed = time.monotonic() - t0
        logger.info(f"[{done}/{total}] {path.name}: {len(df)} rows, {df['pnode_id'].nunique()} pnodes ({elapsed / 60:.0f} min elapsed)")

logger.success(f"backfill complete: {len(list(OUT_DIR.glob('*.parquet')))} files in {OUT_DIR}")
