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

# Genuinely streaming (~5min cadence, ~8min lag, confirmed by direct polling 2026-09-05) -
# this is the model's real-time LMP TARGET (unverified/pre-settlement), not a feature; see
# lmp_model_schema.sql's comment for why the unverified feed, not the settled
# rt_hrl_lmps/marginal_value_rt, is the coherent target. Trailing window re-polls in case
# PJM revises the "unverified" numbers shortly after posting (occ_check/multi-interval-case
# fields in the raw feed suggest this happens occasionally).
#
# 360, not something tighter like 60-180: this specific feed (unlike every other PJM feed
# used in this project) returned "No data found" on repeated direct tests at 90min and
# 180min windows, but succeeded reliably at 360min every time (confirmed 2026-09-05) - a
# real characteristic of this endpoint, not a fluke. Costs ~60-70s per poll (the feed
# returns every pnode type - BUS/AGGREGATE/EHV/etc, not just ZONE - before client-side
# filtering), comfortably inside the 10-minute poll interval.
POLL_WINDOW_MINUTES = 360

COLUMNS = ["time", "zone", "lmp", "congestion_price", "marginal_loss_price"]


def poll_lmp_rt_unverified_fivemin(pjm: gs.PJM, start: datetime, end: datetime, zone_to_location: dict[str, str]) -> pd.DataFrame:
    # No gridstatus wrapper for the 5-min unverified feed (only the hourly one has one) -
    # calls PJM._get_pjm_json() directly against the raw Data Miner 2 feed name, same
    # pattern as the other raw_lmp sources with no wrapper. type=ZONE isn't accepted as a
    # query param on this feed (unlike da_hrl_lmps's location_type) - filtered client-side.
    raw = pjm._get_pjm_json(
        "rt_unverified_fivemin_lmps",
        start=start.strftime("%Y-%m-%d %H:%M"),
        end=end.strftime("%Y-%m-%d %H:%M"),
        params={},
    )
    raw = raw[raw["type"] == "ZONE"].copy()

    location_to_zone = {location: zone for zone, location in zone_to_location.items()}
    raw = raw[raw["pnode_name"].isin(location_to_zone)].copy()
    raw["zone"] = raw["pnode_name"].map(location_to_zone)

    raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True) + pd.Timedelta(minutes=5)
    raw = raw.rename(
        columns={"total_lmp_rt": "lmp", "congestion_price_rt": "congestion_price", "marginal_loss_price_rt": "marginal_loss_price"}
    )
    raw = raw.drop_duplicates(subset=["time", "zone"])
    return raw[COLUMNS]


@flow(
    name="raw_lmp_lmp_rt_unverified_fivemin_producer",
    description="Polls PJM real-time unverified 5-min LMP API every few minutes.",
    retries=3,
    retry_delay_seconds=30,
    log_prints=True,
)
def main():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")

    # Same mapping as public.lmp's producer (src/producers/lmp_producer.py).
    zone_to_location = {
        "AE": "AECO", "AEP": "AEP", "AP": "APS", "ATSI": "ATSI", "BC": "BGE", "CE": "COMED",
        "DAY": "DAY", "DEOK": "DEOK", "DOM": "DOM", "DPL": "DPL", "DUQ": "DUQ", "EKPC": "EKPC",
        "JC": "JCPL", "ME": "METED", "PE": "PECO", "PEP": "PEPCO", "PL": "PPL", "PN": "PENELEC",
        "PS": "PSEG", "RECO": "RECO",
    }
    assert set(zone_to_location) == set(zones["zone_id"]), "zone_to_location is out of sync with pjm_weather_zones.csv"

    end = datetime.now(UTC)
    start = end - timedelta(minutes=POLL_WINDOW_MINUTES)

    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    lmp = poll_lmp_rt_unverified_fivemin(pjm, start, end, zone_to_location)

    producer = build_producer()
    for record in lmp.to_dict(orient="records"):
        produce_json(producer, "raw_lmp_lmp_rt_unverified_fivemin", key=record["zone"], record=record)
    producer.flush()
    logger.success(f"Produced {len(lmp)} raw_lmp_lmp_rt_unverified_fivemin messages")


if __name__ == "__main__":
    main()
