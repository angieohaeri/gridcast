"""Pulls all 10 raw_lmp sources and upserts each to its own table.

Direct pull-and-upsert, no Kafka - these are daily/hourly/monthly batch posts, not
streaming data (see decisions.md's Isolation section, and the per-source cadence notes
below, confirmed against PJM Data Miner where a feed page was found). One `@flow`
function per source so each keeps its own retries/schedule/run history in Prefect
(deployments.py schedules them independently) - running this file directly just runs
all 10 in sequence, same pattern as _archive/scripts/gapfill_raw_lmp.py.
"""

from datetime import UTC, datetime, timedelta
import json
import logging
import os

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import PROCESSED_DATA_DIR, get_connection, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

logging.getLogger("gridstatus").setLevel(logging.WARNING)

POLL_WINDOW_DAYS = 7
# ops_init_commit only refreshes monthly - a longer trailing window comfortably bridges
# one monthly run to the next.
COMMITMENTS_POLL_WINDOW_DAYS = 45
# EIA is monthly with a ~3-month reporting lag - a day-based window would never reach
# back far enough to catch a newly published month.
NATURAL_GAS_POLL_WINDOW_MONTHS = 6

# Same zone_id <-> Location Short Name mapping as src/producers/lmp_producer.py.
ZONE_TO_LOCATION = {
    "AE": "AECO", "AEP": "AEP", "AP": "APS", "ATSI": "ATSI", "BC": "BGE", "CE": "COMED",
    "DAY": "DAY", "DEOK": "DEOK", "DOM": "DOM", "DPL": "DPL", "DUQ": "DUQ", "EKPC": "EKPC",
    "JC": "JCPL", "ME": "METED", "PE": "PECO", "PEP": "PEPCO", "PL": "PPL", "PN": "PENELEC",
    "PS": "PSEG", "RECO": "RECO",
}
LOCATION_TO_ZONE = {location: zone for zone, location in ZONE_TO_LOCATION.items()}

FUEL_COLUMNS = ["Coal", "Gas", "Hydro", "Multiple Fuels", "Nuclear", "Oil", "Other Renewables", "Solar", "Storage", "Wind"]

# EIA census regions, national total, Puerto Rico - not states, dropped.
NON_STATE_LOCATIONS = {"90", "ENC", "ESC", "MAT", "MTN", "NEW", "PCC", "PCN", "SAT", "WNC", "WSC", "PR", "US"}


def _assert_zone_mapping_in_sync():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
    assert set(ZONE_TO_LOCATION) == set(zones["zone_id"]), "ZONE_TO_LOCATION is out of sync with pjm_weather_zones.csv"


def _upsert(upsert_sql: str, rows: list[tuple]) -> None:
    conn = get_connection()
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, upsert_sql, rows, page_size=5000)
    conn.close()


# --- marginal_value_rt - confirmed daily, 11am-12pm ET (shadow price is 5-min-native, ------
# but the feed itself only refreshes once a day). ------------------------------------------

MARGINAL_VALUE_RT_UPSERT = """
INSERT INTO raw_lmp.marginal_value_rt
    (datetime_beginning_utc, datetime_ending_utc, monitored_facility, contingency_facility,
     transmission_constraint_penalty_factor, limit_control_percentage, shadow_price)
VALUES %s
ON CONFLICT (datetime_beginning_utc, monitored_facility, contingency_facility) DO UPDATE SET
    datetime_ending_utc = EXCLUDED.datetime_ending_utc,
    transmission_constraint_penalty_factor = EXCLUDED.transmission_constraint_penalty_factor,
    limit_control_percentage = EXCLUDED.limit_control_percentage,
    shadow_price = EXCLUDED.shadow_price
"""


def poll_marginal_value_rt(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_marginal_value_real_time_5_min(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw["datetime_ending_utc"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])


@flow(name="raw_lmp_marginal_value_rt_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_marginal_value_rt():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_marginal_value_rt(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end)
    rows = list(raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Monitored Facility", "Contingency Facility",
        "Transmission Constraint Penalty Factor", "Limit Control Percentage", "Shadow Price",
    ]].itertuples(index=False, name=None))
    _upsert(MARGINAL_VALUE_RT_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.marginal_value_rt rows")


# --- marginal_value_da - cadence unconfirmed, treated as daily (same settlement family --
# as marginal_value_rt). No penalty factor / limit control percentage - RT-only fields. --

MARGINAL_VALUE_DA_UPSERT = """
INSERT INTO raw_lmp.marginal_value_da
    (datetime_beginning_utc, datetime_ending_utc, monitored_facility, contingency_facility, shadow_price)
VALUES %s
ON CONFLICT (datetime_beginning_utc, monitored_facility, contingency_facility) DO UPDATE SET
    datetime_ending_utc = EXCLUDED.datetime_ending_utc,
    shadow_price = EXCLUDED.shadow_price
"""


def poll_marginal_value_da(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_marginal_value_day_ahead_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw["datetime_ending_utc"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])


@flow(name="raw_lmp_marginal_value_da_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_marginal_value_da():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_marginal_value_da(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end)
    rows = list(raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Monitored Facility", "Contingency Facility", "Shadow Price",
    ]].itertuples(index=False, name=None))
    _upsert(MARGINAL_VALUE_DA_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.marginal_value_da rows")


# --- lmp_da_hourly - cadence unconfirmed, treated as daily (DA market clears once/day). --

LMP_DA_HOURLY_UPSERT = """
INSERT INTO raw_lmp.lmp_da_hourly (time, zone, lmp, congestion_price, marginal_loss_price)
VALUES %s
ON CONFLICT (time, zone) DO UPDATE SET
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price
"""


def poll_lmp_da_hourly(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_lmp(
        start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), market=gs.Markets.DAY_AHEAD_HOURLY, location_type="ZONE"
    )
    raw = raw[raw["Location Short Name"].isin(LOCATION_TO_ZONE)].copy()
    raw["zone"] = raw["Location Short Name"].map(LOCATION_TO_ZONE)
    raw["time"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["time", "zone"])


@flow(name="raw_lmp_lmp_da_hourly_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_lmp_da_hourly():
    _assert_zone_mapping_in_sync()
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_lmp_da_hourly(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end)
    rows = list(raw[["time", "zone", "LMP", "Congestion", "Loss"]].itertuples(index=False, name=None))
    _upsert(LMP_DA_HOURLY_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.lmp_da_hourly rows")


# --- generation_by_fuel - confirmed hourly, :15 past the hour. Still a periodic batch ---
# post, not a push feed, so this stays direct-upsert, just on an hourly schedule. --------

GENERATION_BY_FUEL_UPSERT = """
INSERT INTO raw_lmp.generation_by_fuel (time, fuel_type, generation_mw)
VALUES %s
ON CONFLICT (time, fuel_type) DO UPDATE SET
    generation_mw = EXCLUDED.generation_mw
"""


def poll_generation_by_fuel(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_fuel_mix(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["time"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw = raw.drop_duplicates(subset=["time"])
    long = raw.melt(id_vars=["time"], value_vars=FUEL_COLUMNS, var_name="fuel_type", value_name="generation_mw")
    return long.where(long.notna(), None)


@flow(name="raw_lmp_generation_by_fuel_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_generation_by_fuel():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    long = poll_generation_by_fuel(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end)
    rows = list(long[["time", "fuel_type", "generation_mw"]].itertuples(index=False, name=None))
    _upsert(GENERATION_BY_FUEL_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.generation_by_fuel rows")


# --- transmission_constraints_da - confirmed daily, 12-2pm ET. --------------------------

TRANSMISSION_CONSTRAINTS_DA_UPSERT = """
INSERT INTO raw_lmp.transmission_constraints_da
    (datetime_beginning_utc, datetime_ending_utc, duration_hours, monitored_facility, contingency_facility)
VALUES %s
ON CONFLICT (datetime_beginning_utc, monitored_facility, contingency_facility) DO UPDATE SET
    datetime_ending_utc = EXCLUDED.datetime_ending_utc,
    duration_hours = EXCLUDED.duration_hours
"""


def poll_transmission_constraints_da(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_transmission_constraints_day_ahead_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw["datetime_ending_utc"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])


@flow(name="raw_lmp_transmission_constraints_da_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_transmission_constraints_da():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_transmission_constraints_da(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end)
    rows = list(raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Duration", "Monitored Facility", "Contingency Facility",
    ]].itertuples(index=False, name=None))
    _upsert(TRANSMISSION_CONSTRAINTS_DA_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.transmission_constraints_da rows")


# --- operator_initiated_commitments - confirmed monthly, posts on the 20th. No stable ---
# per-row id in the feed - duplicates on the full natural key collapse here, same -------
# reasoning as the historical backfill (see lmp_model_schema.sql). ----------------------

OPERATOR_INITIATED_COMMITMENTS_UPSERT = """
INSERT INTO raw_lmp.operator_initiated_commitments (datetime_beginning_utc, zone, economic_max_mw, reason)
VALUES %s
ON CONFLICT (datetime_beginning_utc, zone, reason, economic_max_mw) DO NOTHING
"""


def poll_operator_initiated_commitments(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    try:
        raw = pjm._get_pjm_json(
            "ops_init_commit", start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), params={}
        )
    except gs.NoDataFoundException:
        return pd.DataFrame(columns=["datetime_beginning_utc", "zone", "economic_max_mw", "reason"])

    raw = raw[raw["zone"].isin(LOCATION_TO_ZONE)].copy()
    raw["zone"] = raw["zone"].map(LOCATION_TO_ZONE)
    raw["datetime_beginning_utc"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["datetime_beginning_utc", "zone", "reason", "economic_max_mw"])


@flow(name="raw_lmp_operator_initiated_commitments_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_operator_initiated_commitments():
    _assert_zone_mapping_in_sync()
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_operator_initiated_commitments(pjm, end - timedelta(days=COMMITMENTS_POLL_WINDOW_DAYS), end)
    rows = list(raw[["datetime_beginning_utc", "zone", "economic_max_mw", "reason"]].itertuples(index=False, name=None))
    _upsert(OPERATOR_INITIATED_COMMITMENTS_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.operator_initiated_commitments rows")


# --- scheduled_generation - cadence unconfirmed, treated as daily. conf_disclaimer -------
# dropped - static explanatory text for why rt_ecomax is null, not a data column. --------

SCHEDULED_GENERATION_UPSERT = """
INSERT INTO raw_lmp.scheduled_generation (time, rt_ecomax, self_ecomax)
VALUES %s
ON CONFLICT (time) DO UPDATE SET
    rt_ecomax = EXCLUDED.rt_ecomax,
    self_ecomax = EXCLUDED.self_ecomax
"""


def poll_scheduled_generation(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm._get_pjm_json("rt_and_self_ecomax", start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), params={})
    raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["time"])


@flow(name="raw_lmp_scheduled_generation_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_scheduled_generation():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_scheduled_generation(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end)
    rows = list(raw[["time", "rt_ecomax", "self_ecomax"]].itertuples(index=False, name=None))
    _upsert(SCHEDULED_GENERATION_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.scheduled_generation rows")


# --- generation_ehv_losses - cadence unconfirmed, treated as daily. ---------------------

GENERATION_EHV_LOSSES_UPSERT = """
INSERT INTO raw_lmp.generation_ehv_losses (time, total_gen, total_losses)
VALUES %s
ON CONFLICT (time) DO UPDATE SET
    total_gen = EXCLUDED.total_gen,
    total_losses = EXCLUDED.total_losses
"""


def poll_generation_ehv_losses(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm._get_pjm_json("gen_ehv_losses", start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), params={})
    raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["time"])


@flow(name="raw_lmp_generation_ehv_losses_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_generation_ehv_losses():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_generation_ehv_losses(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end)
    rows = list(raw[["time", "total_gen", "total_losses"]].itertuples(index=False, name=None))
    _upsert(GENERATION_EHV_LOSSES_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.generation_ehv_losses rows")


# --- forecasted_generation_outages - confirmed daily, 4:00am ET. -----------------------

FORECASTED_GENERATION_OUTAGES_UPSERT = """
INSERT INTO raw_lmp.forecasted_generation_outages
    (forecast_execution_date, forecast_date, outage_mw_rto, outage_mw_west, outage_mw_other)
VALUES %s
ON CONFLICT (forecast_execution_date, forecast_date) DO UPDATE SET
    outage_mw_rto = EXCLUDED.outage_mw_rto,
    outage_mw_west = EXCLUDED.outage_mw_west,
    outage_mw_other = EXCLUDED.outage_mw_other
"""


def poll_forecasted_generation_outages(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_forecasted_generation_outages(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["forecast_execution_date"] = raw["Publish Time"].dt.tz_convert("UTC")
    raw["forecast_date"] = pd.to_datetime(raw["Interval Start"], utc=True).dt.date
    return raw.drop_duplicates(subset=["forecast_execution_date", "forecast_date"])


@flow(name="raw_lmp_forecasted_generation_outages_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_forecasted_generation_outages():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_forecasted_generation_outages(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end)
    rows = list(
        raw[["forecast_execution_date", "forecast_date", "RTO MW", "West MW", "Other MW"]]
        .itertuples(index=False, name=None)
    )
    _upsert(FORECASTED_GENERATION_OUTAGES_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.forecasted_generation_outages rows")


# --- natural_gas_fuel_cost - EIA (not PJM), monthly with a ~3-month reporting lag. ------

NATURAL_GAS_FUEL_COST_UPSERT = """
INSERT INTO raw_lmp.natural_gas_fuel_cost (period, location, cost_per_mmbtu)
VALUES %s
ON CONFLICT (period, location) DO UPDATE SET
    cost_per_mmbtu = EXCLUDED.cost_per_mmbtu
"""


def poll_natural_gas_fuel_cost(eia: gs.EIA, start: datetime, end: datetime) -> pd.DataFrame:
    url = f"{eia.BASE_URL}electricity/electric-power-operational-data/data/"
    params = {
        "start": start.strftime("%Y-%m"),
        "end": end.strftime("%Y-%m"),
        "frequency": "monthly",
        "data": ["cost-per-btu"],
        "facets": {"fueltypeid": ["NG"], "sectorid": ["98"]},
        "offset": 0,
        "length": 5000,
        "sort": [{"column": "period", "direction": "asc"}],
    }

    frames = []
    total = None
    while total is None or params["offset"] < total:
        headers = {"X-Api-Key": eia.api_key, "X-Params": json.dumps(params)}
        chunk, total = eia._fetch_page(url, headers)
        frames.append(chunk)
        params["offset"] += params["length"]

    raw = pd.concat(frames, ignore_index=True)
    raw = raw[~raw["location"].isin(NON_STATE_LOCATIONS)].copy()
    raw["period"] = pd.to_datetime(raw["period"], format="%Y-%m").dt.date
    raw = raw.rename(columns={"cost-per-btu": "cost_per_mmbtu"})
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["period", "location"])


@flow(name="raw_lmp_natural_gas_fuel_cost_sync", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_natural_gas_fuel_cost():
    end = datetime.now(UTC)
    eia = gs.EIA(api_key=os.environ["EIA_API_KEY"])
    raw = poll_natural_gas_fuel_cost(eia, end - relativedelta(months=NATURAL_GAS_POLL_WINDOW_MONTHS), end)
    rows = list(raw[["period", "location", "cost_per_mmbtu"]].itertuples(index=False, name=None))
    _upsert(NATURAL_GAS_FUEL_COST_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.natural_gas_fuel_cost rows")


if __name__ == "__main__":
    sync_marginal_value_rt()
    sync_marginal_value_da()
    sync_lmp_da_hourly()
    sync_generation_by_fuel()
    sync_transmission_constraints_da()
    sync_operator_initiated_commitments()
    sync_scheduled_generation()
    sync_generation_ehv_losses()
    sync_forecasted_generation_outages()
    sync_natural_gas_fuel_cost()
