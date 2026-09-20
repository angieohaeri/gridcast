from datetime import UTC, datetime
import logging
import os
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd

from gridcast.config import PROCESSED_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

logging.getLogger("gridstatus").setLevel(logging.WARNING)

EASTERN = ZoneInfo("America/New_York")

# One-off backfill for the Mac Mini wifi outage: inst_load_producer's 2-hour poll window
# has no room to self-heal a multi-day gap. Raw gap confirmed uniform across all zones:
# 2026-09-17 17:00 UTC to 2026-09-19 22:50 UTC.
START = datetime(2026, 9, 17, 17, 0, tzinfo=UTC)
END = datetime(2026, 9, 19, 22, 50, tzinfo=UTC)

# same rename/drop as inst_load_producer.py
ZONE_RENAME = {"APS": "AP", "COMED": "CE", "DAYTON": "DAY", "PJM RTO": "RTO"}
DROP_COLUMNS = [
    "Time", "Interval End", "Load", "UG",
    "PJM MID ATLANTIC REGION", "PJM SOUTHERN REGION", "PJM WESTERN REGION",
]
INST_LOAD_COLUMNS = ["time", "zone", "instantaneous_load_mw"]

INSERT = """
INSERT INTO instantaneous_load (time, zone, instantaneous_load_mw)
VALUES (%(time)s, %(zone)s, %(instantaneous_load_mw)s);
"""


def fetch_inst_load(pjm: gs.PJM, start: datetime, end: datetime, zone_ids: list[str]) -> pd.DataFrame:
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


def main():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
    zone_ids = ["RTO"] + zones["zone_id"].unique().tolist()

    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    inst_load = fetch_inst_load(pjm, START, END, zone_ids)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT zone, time FROM instantaneous_load WHERE time >= %s AND time < %s",
        (START, END),
    )
    existing = {(zone, t.isoformat()) for zone, t in cur.fetchall()}

    written = 0
    for record in inst_load.to_dict(orient="records"):
        key = (record["zone"], record["time"].isoformat())
        if key in existing:
            continue
        cur.execute(INSERT, record)
        written += 1

    conn.close()
    logger.success(f"Backfilled {written} inst_load rows for {START} to {END}")


if __name__ == "__main__":
    main()
