"""One-off historical backfill for raw_lmp.lmp_rt_unverified_fivemin.

Unlike the other 10 raw_lmp tables, this one was never backfilled - it only ever gets
data via the Kafka producer/consumer pair (src/producers/consumers/
raw_lmp_lmp_rt_unverified_fivemin_*.py), which polls a fixed 6-hour trailing window from
"now" every run with no resume-from-table-max logic. So turning that deployment on alone
leaves a permanent gap between whenever it's first unpaused and the feed's true history.

RETENTION_FLOOR was found empirically (2026-09-19, binary search directly against PJM's
API): rt_unverified_fivemin_lmps only retains ~15 days, not the ~30 initially guessed -
data starts around 2026-09-04 20:10 UTC and nothing before that returns totalRows > 0.
This is a pre-settlement/unverified feed, unlike the settled history the other 10
raw_lmp sources pull, so it doesn't share their longer retention.

CHUNK_MINUTES matches the producer's own POLL_WINDOW_MINUTES (360) - this specific feed
returns "No data found" on shorter windows (confirmed 2026-09-05, see the producer's
comment) - and pulls straight into an upsert, bypassing Kafka, same reasoning as
gapfill_raw_lmp.py: this window is small enough not to need a CSV-snapshot resumability
step. Direct upsert also keeps this script table-max-resumable across reruns, unlike the
producer's fixed trailing window.
"""

from datetime import UTC, datetime, timedelta
import os
import time

from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd
import psycopg2.extras
import requests

from gridcast.config import PROCESSED_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

RETENTION_FLOOR = datetime(2026, 9, 4, 20, 10, tzinfo=UTC)
CHUNK_MINUTES = 360
NOW = datetime.now(UTC)

# Each chunk pages through the feed's full unfiltered response (every pnode type, not just
# ZONE - confirmed hundreds of thousands of rows per 6-hour window) via many sequential
# 50k-row requests. The 2nd chunk of this backfill's first run exhausted gridstatus's own
# retry/backoff (6 tries, capping at 32s) under sustained PJM rate limiting and crashed the
# whole script - retrying the whole chunk after a longer cooldown gives PJM's rate limit
# window more room to reset than gridstatus's own backoff does. gridstatus wraps repeated
# rate-limit (429) responses in RuntimeError, but a raw connection timeout (confirmed on
# this backfill's 2nd run, chunk 9) bubbles up as requests.exceptions.RequestException
# instead - both are caught here.
CHUNK_RETRIES = 4
CHUNK_RETRY_COOLDOWN_SECONDS = 90

COLUMNS = ["time", "zone", "lmp", "congestion_price", "marginal_loss_price"]

UPSERT_SQL = """
    INSERT INTO raw_lmp.lmp_rt_unverified_fivemin (time, zone, lmp, congestion_price, marginal_loss_price)
    VALUES %s
    ON CONFLICT (time, zone) DO UPDATE SET
        lmp = EXCLUDED.lmp,
        congestion_price = EXCLUDED.congestion_price,
        marginal_loss_price = EXCLUDED.marginal_loss_price
"""


def _is_zone_row(item: dict, location_to_zone: dict[str, str]) -> bool:
    return item.get("type") == "ZONE" and item.get("pnode_name") in location_to_zone


def poll_chunk(pjm: gs.PJM, start: datetime, end: datetime, location_to_zone: dict[str, str]) -> pd.DataFrame:
    # gridstatus's own pjm._get_pjm_json (used by the producer this mirrors) pages through
    # the feed's full unfiltered response - every pnode type, not just ZONE, since type=ZONE
    # isn't accepted as a query param - and concatenates all ~20 pages of 50k rows (~1M rows
    # total) into one DataFrame before any filtering happens. That repeatedly OOM-killed this
    # backfill (confirmed across 3 runs, 2026-09-19/20) despite only ~1440 ZONE rows actually
    # being needed per chunk. Paginating by hand here and filtering each page immediately -
    # reusing pjm._make_api_call for the same auth/retry handling gridstatus itself uses -
    # keeps peak memory to one page's raw JSON instead of the whole chunk's.
    headers = {"Ocp-Apim-Subscription-Key": pjm.api_key}
    params = {
        "startRow": 1,
        "rowCount": 50000,
        "datetime_beginning_ept": f"{start.strftime('%m/%d/%Y %H:%M')}to{end.strftime('%m/%d/%Y %H:%M')}",
    }
    r = pjm._make_api_call("https://api.pjm.com/api/v1/rt_unverified_fivemin_lmps", params=params, headers=headers)
    if "errors" in r:
        raise RuntimeError(r["errors"])

    zone_rows = [item for item in r.get("items", []) if _is_zone_row(item, location_to_zone)]
    while next_links := [link["href"] for link in r.get("links", []) if link["rel"] == "next"]:
        r = pjm._make_api_call(next_links[0], headers=headers)
        zone_rows.extend(item for item in r.get("items", []) if _is_zone_row(item, location_to_zone))

    if not zone_rows:
        return pd.DataFrame(columns=COLUMNS)

    raw = pd.DataFrame(zone_rows)
    raw["zone"] = raw["pnode_name"].map(location_to_zone)
    raw["time"] = pd.to_datetime(raw["datetime_beginning_utc"], format="ISO8601").dt.tz_localize("UTC") + pd.Timedelta(minutes=5)
    raw = raw.rename(
        columns={"total_lmp_rt": "lmp", "congestion_price_rt": "congestion_price", "marginal_loss_price_rt": "marginal_loss_price"}
    )
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["time", "zone"])
    return raw[COLUMNS]


def main():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")

    # Same mapping as the producer / public.lmp's producer (src/producers/lmp_producer.py).
    zone_to_location = {
        "AE": "AECO", "AEP": "AEP", "AP": "APS", "ATSI": "ATSI", "BC": "BGE", "CE": "COMED",
        "DAY": "DAY", "DEOK": "DEOK", "DOM": "DOM", "DPL": "DPL", "DUQ": "DUQ", "EKPC": "EKPC",
        "JC": "JCPL", "ME": "METED", "PE": "PECO", "PEP": "PEPCO", "PL": "PPL", "PN": "PENELEC",
        "PS": "PSEG", "RECO": "RECO",
    }
    assert set(zone_to_location) == set(zones["zone_id"]), "zone_to_location is out of sync with pjm_weather_zones.csv"
    location_to_zone = {location: zone for zone, location in zone_to_location.items()}

    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    conn = get_connection()

    cur = conn.cursor()
    cur.execute("select max(time) from raw_lmp.lmp_rt_unverified_fivemin")
    table_max = cur.fetchone()[0]

    start = max(table_max, RETENTION_FLOOR) if table_max else RETENTION_FLOOR
    chunk_start = start

    while chunk_start < NOW:
        chunk_end = min(chunk_start + timedelta(minutes=CHUNK_MINUTES), NOW)

        for attempt in range(1, CHUNK_RETRIES + 1):
            try:
                chunk = poll_chunk(pjm, chunk_start, chunk_end, location_to_zone)
                break
            except (RuntimeError, requests.exceptions.RequestException) as e:
                if attempt == CHUNK_RETRIES:
                    raise
                logger.warning(
                    f"chunk {chunk_start} to {chunk_end} failed (attempt {attempt}/{CHUNK_RETRIES}): {e} - "
                    f"cooling down {CHUNK_RETRY_COOLDOWN_SECONDS}s before retrying"
                )
                time.sleep(CHUNK_RETRY_COOLDOWN_SECONDS)

        rows = list(chunk.itertuples(index=False, name=None))
        if rows:
            cur = conn.cursor()
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=5000)
        logger.success(f"raw_lmp.lmp_rt_unverified_fivemin: upserted {len(rows)} rows for {chunk_start} to {chunk_end}")

        chunk_start = chunk_end

    conn.close()


if __name__ == "__main__":
    main()
