from datetime import UTC, datetime, timedelta
import logging
import os
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import gridstatus as gs
from kafka_client import build_producer, produce_json
from loguru import logger
import pandas as pd

from gridcast.config import PROCESSED_DATA_DIR, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

logging.getLogger("gridstatus").setLevel(logging.WARNING)

POLL_WINDOW_DAYS = 7
# gridstatus formats get_load's date/end args straight into PJM's datetime_beginning_ept
# param with no tz conversion of its own - they need to already be Eastern wall-clock
# values, not UTC, and need minute precision, not just a date (a date-only string parses
# to midnight, which silently caps every poll at Eastern midnight of "today" until the
# calendar date rolls over - found 2026-09-02, see references/decisions.md).
EASTERN = ZoneInfo("America/New_York")

# inst_load labels 3 zones differently than this project's zone_id codes
ZONE_RENAME = {"APS": "AP", "COMED": "CE", "DAYTON": "DAY", "PJM RTO": "RTO"}

# not zones: UG is an "underground asset" category, not a load area; the 3 regional
# aggregates aren't part of this project's zone scheme. RTO is kept (see schema.sql).
DROP_COLUMNS = [
    "Time", "Interval End", "Load", "UG",
    "PJM MID ATLANTIC REGION", "PJM SOUTHERN REGION", "PJM WESTERN REGION",
]

INST_LOAD_COLUMNS = [
    "time",
    "zone",
    "instantaneous_load_mw",
]


def poll_pjm_inst_load(pjm: gs.PJM, start: datetime, end: datetime, zone_ids: list[str]) -> pd.DataFrame:
    # get_load returns one column per zone (wide), not one row per zone like get_load_metered_hourly
    wide = pjm.get_load(
        start.astimezone(EASTERN).strftime("%Y-%m-%d %H:%M"),
        end.astimezone(EASTERN).strftime("%Y-%m-%d %H:%M"),
    )
    wide = wide.rename(columns=ZONE_RENAME).drop(columns=DROP_COLUMNS)

    found = set(wide.columns) - {"Interval Start"}
    assert found == set(zone_ids), f"zone mismatch: {found ^ set(zone_ids)}"

    long = wide.melt(id_vars=["Interval Start"], var_name="zone", value_name="instantaneous_load_mw")
    long["time"] = long["Interval Start"].dt.tz_convert("UTC")
    return long[INST_LOAD_COLUMNS]


@flow(name="inst_load_producer", description="Polls PJM instantaneous load every 10 min.", log_prints=True)
def main():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
    zone_ids = ["RTO"] + zones["zone_id"].unique().tolist()

    end = datetime.now(UTC)
    start = end - timedelta(days=POLL_WINDOW_DAYS)

    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    inst_load = poll_pjm_inst_load(pjm, start, end, zone_ids)

    producer = build_producer()
    for record in inst_load.to_dict(orient="records"):
        produce_json(producer, "inst_load", key=record["zone"], record=record)
    producer.flush()
    logger.success(f"Produced {len(inst_load)} inst_load messages")


if __name__ == "__main__":
    main()
