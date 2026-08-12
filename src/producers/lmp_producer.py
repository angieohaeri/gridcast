from datetime import UTC, datetime, timedelta
import logging
import os

from dotenv import load_dotenv
import gridstatus as gs
from kafka_client import build_producer, produce_json
from loguru import logger
import pandas as pd

from gridcast.config import PROCESSED_DATA_DIR, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

# gridstatus logs the PJM API key in its INFO request lines; WARNING keeps retry warnings
logging.getLogger("gridstatus").setLevel(logging.WARNING)

# rt_hrl_lmps settles over ~2 days - re-poll so revisions upsert
POLL_WINDOW_DAYS = 7

LMP_COLUMNS = [
    "time",
    "pnode_id",
    "pnode_name",
    "zone",
    "lmp",
    "congestion_price",
    "marginal_loss_price",
]


def poll_lmp(pjm: gs.PJM, start: datetime, end: datetime, zone_to_location: dict[str, str]) -> pd.DataFrame:
    lmp = pjm.get_lmp(
        start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        market=gs.Markets.REAL_TIME_HOURLY,
        location_type="ZONE",
    )

    location_to_zone = {location: zone for zone, location in zone_to_location.items()}
    lmp = lmp[lmp["Location Short Name"].isin(location_to_zone)].copy()
    lmp["zone"] = lmp["Location Short Name"].map(location_to_zone)

    # time = Interval End, UTC (gridstatus returns Eastern) - see references/decisions.md
    lmp["Interval End"] = lmp["Interval End"].dt.tz_convert("UTC")

    lmp = lmp.rename(
        columns={
            "Interval End": "time",
            "Location Id": "pnode_id",
            "Location Name": "pnode_name",
            "LMP": "lmp",
            "Congestion": "congestion_price",
            "Loss": "marginal_loss_price",
        }
    )
    lmp["pnode_id"] = lmp["pnode_id"].astype(str)
    return lmp[LMP_COLUMNS]

@flow(name="lmp_producer", description="Polls PJM LMP API every hour.", retries=3, retry_delay_seconds=60, log_prints=True)
def main():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")

    # rt_hrl_lmps labels zones by utility short name, not zone_id. MID-ATL/APS, OVEC and
    # PJM-RTO also come back - aggregate, out of scope, RTO hub - and stay unmapped.
    zone_to_location = {
        "AE": "AECO",
        "AEP": "AEP",
        "AP": "APS",
        "ATSI": "ATSI",
        "BC": "BGE",
        "CE": "COMED",
        "DAY": "DAY",
        "DEOK": "DEOK",
        "DOM": "DOM",
        "DPL": "DPL",
        "DUQ": "DUQ",
        "EKPC": "EKPC",
        "JC": "JCPL",
        "ME": "METED",
        "PE": "PECO",
        "PEP": "PEPCO",
        "PL": "PPL",
        "PN": "PENELEC",
        "PS": "PSEG",
        "RECO": "RECO",
    }
    assert set(zone_to_location) == set(zones["zone_id"]), "zone_to_location is out of sync with pjm_weather_zones.csv"

    end = datetime.now(UTC)
    start = end - timedelta(days=POLL_WINDOW_DAYS)

    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    lmp = poll_lmp(pjm, start, end, zone_to_location)

    producer = build_producer()
    for record in lmp.to_dict(orient="records"):
        produce_json(producer, "lmp", key=record["zone"], record=record)
    producer.flush()
    logger.success(f"Produced {len(lmp)} lmp messages")


if __name__ == "__main__":
    main()
