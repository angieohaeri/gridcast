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

# PJM revises for ~3 days, EIA ~1. Re-poll the window so revisions upsert on (time, zone, source).
POLL_WINDOW_DAYS = 7

LOAD_COLUMNS = [
    "time",
    "zone",
    "source",
    "demand_mw",
    "demand_forecast_mw",
    "net_generation_mw",
    "total_interchange_mw",
    "is_verified",
]


def poll_pjm_load(pjm: gs.PJM, start: datetime, end: datetime, zone_ids: list[str]) -> pd.DataFrame:
    load_hourly = pjm.get_load_metered_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    load_hourly = load_hourly[load_hourly["Zone"].isin(zone_ids)]

    # Six zones report multiple Load Areas per hour; sum to one zonal total, and only mark
    # verified if every sub-area is. Interval End = Hour Ending, must match lmp_producer.
    load_hourly = load_hourly.groupby(["Interval End", "Zone"], as_index=False).agg(
        demand_mw=("MW", "sum"),
        is_verified=("Is Verified", "all"),
    )
    load_hourly = load_hourly.rename(columns={"Interval End": "time", "Zone": "zone"})
    load_hourly["source"] = "pjm"
    load_hourly["demand_forecast_mw"] = float("nan")
    load_hourly["net_generation_mw"] = float("nan")
    load_hourly["total_interchange_mw"] = float("nan")
    return load_hourly[LOAD_COLUMNS]


def poll_eia_load(eia: gs.EIA, start: datetime, end: datetime) -> pd.DataFrame:
    # get_dataset, not get_grid_monitor - the latter can't filter by date, so it refetches
    # all history every poll.
    eia_rto = eia.get_dataset(
        "electricity/rto/region-data",
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        facets={"respondent": "PJM"},
    )
    eia_rto = eia_rto.rename(
        columns={
            "Interval End": "time",
            "Load": "demand_mw",
            "Load Forecast": "demand_forecast_mw",
            "Net Generation": "net_generation_mw",
            "Total Interchange": "total_interchange_mw",
        }
    )
    eia_rto["zone"] = "RTO"
    eia_rto["source"] = "eia"
    eia_rto["is_verified"] = None
    return eia_rto[LOAD_COLUMNS]

@flow(name="load_producer", description="Polls PJM and EIA load data every hour.", log_prints=True)
def main():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
    # one row per station, so composite zones repeat
    zone_ids = ["RTO"] + zones["zone_id"].unique().tolist()

    end = datetime.now(UTC)
    start = end - timedelta(days=POLL_WINDOW_DAYS)

    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    eia = gs.EIA(api_key=os.environ["EIA_API_KEY"])

    load = pd.concat(
        [poll_pjm_load(pjm, start, end, zone_ids), poll_eia_load(eia, start, end)],
        ignore_index=True,
    )

    producer = build_producer()
    for record in load.to_dict(orient="records"):
        produce_json(producer, "load", key=record["zone"], record=record)
    producer.flush()
    logger.success(f"Produced {len(load)} load messages")


if __name__ == "__main__":
    main()
