"""Backfill verified RT and DA hourly LMPs for GNN nodes that have no price history yet.

Data Miner 2 only takes a pnode_id
filter on the last 731 days, at most ~1,700 characters of ids per request, one 50,000-row page per
request, 6 requests/min. Rows go straight into the raw tables as hour ending (Interval End), the
same upsert the daily sync uses, so reruns are safe and the daily flows carry on from here.
"""

import logging
import os
import time

from dotenv import load_dotenv
import gridstatus as gs
from gridstatus.base import NoDataFoundException
from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import get_connection

load_dotenv()
logging.getLogger("gridstatus").setLevel(logging.WARNING)  # its INFO lines log the API key

ARCHIVE_DAYS = 731
MAX_ID_PARAM_CHARS = 1705
ROWS_PER_PAGE = 50_000
REQUEST_SPACING_S = 10.5
MARKETS = {"rt": "raw_lmp.lmp_rt", "da": "raw_lmp.lmp_da"}
COLUMNS = ["time", "pnode_id", "system_energy_price", "lmp", "congestion_price", "marginal_loss_price"]
RECENT_DAYS = 30


def pending_nodes(conn, table: str, cutoff: pd.Timestamp) -> pd.Series:
    """Nodes whose history stops short of `cutoff` -> their last hour before it (NaT when none).
    Hours after `cutoff` are left out because the daily syncs may already have them."""
    cur = conn.cursor()
    cur.execute(
        f"select pnode_id, (select max(time) from {table} t where t.pnode_id = n.pnode_id and t.time < %s) "
        "from raw_lmp.pnode_topology_nodes n order by pnode_id",
        (cutoff,),
    )
    last = pd.Series(dict(cur.fetchall()), dtype="datetime64[ns, UTC]")
    return last[last.isna() | (last < cutoff - pd.Timedelta(days=1))]


def chunks(pnode_ids: list[int]) -> list[list[int]]:
    out, cur = [], []
    for pid in pnode_ids:
        if cur and sum(len(str(x)) + 3 for x in cur + [pid]) - 3 > MAX_ID_PARAM_CHARS:
            out.append(cur)
            cur = []
        cur.append(pid)
    return out + [cur]


def pull(pjm: gs.PJM, market: str, ids: list[int], lo: pd.Timestamp, hi: pd.Timestamp) -> pd.DataFrame:
    sfx = f"_{market}"
    try:
        raw = pjm._get_pjm_json(
            f"{market}_hrl_lmps",
            start=lo,
            end=hi - pd.Timedelta(minutes=1),
            params={
                "pnode_id": ";".join(map(str, ids)),
                "fields": f"datetime_beginning_utc,pnode_id,system_energy_price{sfx},total_lmp{sfx},congestion_price{sfx},marginal_loss_price{sfx},row_is_current",
            },
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
        )
    except NoDataFoundException:  # window before any of these nodes took effect, or not yet posted
        return pd.DataFrame(columns=COLUMNS)
    raw = raw[raw["row_is_current"]]
    return pd.DataFrame({
        "time": raw["Interval End"].dt.tz_convert("UTC"),
        "pnode_id": raw["pnode_id"].astype("int64"),
        "system_energy_price": raw[f"system_energy_price{sfx}"],
        "lmp": raw[f"total_lmp{sfx}"],
        "congestion_price": raw[f"congestion_price{sfx}"],
        "marginal_loss_price": raw[f"marginal_loss_price{sfx}"],
    })


def backfill(conn, pjm: gs.PJM, market: str, table: str) -> None:
    today = pd.Timestamp.now(tz="US/Eastern").normalize()
    start, end = today - pd.Timedelta(days=ARCHIVE_DAYS - 1), today + pd.Timedelta(days=2)
    pending = pending_nodes(conn, table, today - pd.Timedelta(days=RECENT_DAYS))
    logger.info(f"{market}: {len(pending)} nodes with missing history, {start.date()} -> {end.date()}")
    cur = conn.cursor()
    for ids in chunks(pending.index.tolist()):
        window = pd.Timedelta(days=ROWS_PER_PAGE // (24 * len(ids)))
        lo = start if pending[ids].isna().any() else max(start, pending[ids].min().tz_convert("US/Eastern").normalize())
        total = 0
        while lo < end:
            hi = min(lo + window, end)
            df = pull(pjm, market, ids, lo, hi).drop_duplicates(["time", "pnode_id"])
            rows = list(df[COLUMNS].astype(object).itertuples(index=False, name=None))
            if rows:
                psycopg2.extras.execute_values(
                    cur,
                    f"insert into {table} ({', '.join(COLUMNS)}) values %s on conflict (time, pnode_id) do nothing",
                    rows,
                    page_size=10_000,
                )
                conn.commit()
            total += len(rows)
            logger.info(f"{market} {lo.date()} -> {hi.date()}: {len(rows)} rows ({total:,} total)")
            lo = hi
            time.sleep(REQUEST_SPACING_S)


def main() -> None:
    conn = get_connection()
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    for market, table in MARKETS.items():
        backfill(conn, pjm, market, table)
    conn.close()


if __name__ == "__main__":
    main()
