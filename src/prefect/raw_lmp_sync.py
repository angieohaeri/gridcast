"""Pulls all 18 raw_lmp sources and upserts each to its own table.

Direct pull-and-upsert, no Kafka - these are daily/hourly/monthly batch posts, not
streaming data (see decisions.md's Isolation section, and the per-source cadence notes
below, confirmed against PJM Data Miner where a feed page was found). One `@flow`
function per source so each keeps its own retries/schedule/run history in Prefect
(deployments.py schedules them independently) - running this file directly just runs
all 18 in sequence.
"""

from datetime import UTC, datetime, timedelta
from functools import partial
from itertools import pairwise
import json
import logging
import os
import time

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd
from prefect.cache_policies import NO_CACHE
import psycopg2.extras
import requests

from gridcast.config import PROCESSED_DATA_DIR, get_connection, setup_logging
from prefect import flow, task

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


# every poll is a task so a failed PJM/EIA/Open-Meteo call retries on its own instead of
# re-running the whole flow; NO_CACHE because the client and DataFrames aren't hashable inputs
fetch = partial(task, retries=3, retry_delay_seconds=60, cache_policy=NO_CACHE)


def _pjm() -> gs.PJM:
    return gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)


def _eia() -> gs.EIA:
    return gs.EIA(api_key=os.environ["EIA_API_KEY"])


def _assert_zone_mapping_in_sync():
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
    assert set(ZONE_TO_LOCATION) == set(zones["zone_id"]), "ZONE_TO_LOCATION is out of sync with pjm_weather_zones.csv"


@task(cache_policy=NO_CACHE)
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


@fetch
def poll_marginal_value_rt(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_marginal_value_real_time_5_min(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw["datetime_ending_utc"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])


@flow(name="marginal_value_rt", log_prints=True)
def sync_marginal_value_rt(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_marginal_value_rt(pjm, end - timedelta(days=lookback_days), end + timedelta(days=1))
    rows = list(raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Monitored Facility", "Contingency Facility",
        "Transmission Constraint Penalty Factor", "Limit Control Percentage", "Shadow Price",
    ]].itertuples(index=False, name=None))
    _upsert(MARGINAL_VALUE_RT_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} .marginal_value_rt rows")


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


@fetch
def poll_marginal_value_da(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_marginal_value_day_ahead_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw["datetime_ending_utc"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])


@flow(name="marginal_value_da", log_prints=True)
def sync_marginal_value_da(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_marginal_value_da(pjm, end - timedelta(days=lookback_days), end + timedelta(days=2))
    rows = list(raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Monitored Facility", "Contingency Facility", "Shadow Price",
    ]].itertuples(index=False, name=None))
    _upsert(MARGINAL_VALUE_DA_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.marginal_value_da rows")


# --- lmp_da_zonal - cadence unconfirmed, treated as daily (DA market clears once/day). --

LMP_DA_ZONAL_UPSERT = """
INSERT INTO raw_lmp.lmp_da_zonal (time, zone, lmp, congestion_price, marginal_loss_price)
VALUES %s
ON CONFLICT (time, zone) DO UPDATE SET
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price
"""


@fetch
def poll_lmp_da_zonal(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_lmp(
        start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), market=gs.Markets.DAY_AHEAD_HOURLY, location_type="ZONE"
    )
    raw = raw[raw["Location Short Name"].isin(LOCATION_TO_ZONE)].copy()
    raw["zone"] = raw["Location Short Name"].map(LOCATION_TO_ZONE)
    raw["time"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["time", "zone"])


@flow(name="lmp_da_zonal", log_prints=True)
def sync_lmp_da_zonal(lookback_days: int = POLL_WINDOW_DAYS):
    _assert_zone_mapping_in_sync()
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_lmp_da_zonal(pjm, end - timedelta(days=lookback_days), end + timedelta(days=2))
    rows = list(raw[["time", "zone", "LMP", "Congestion", "Loss"]].itertuples(index=False, name=None))
    _upsert(LMP_DA_ZONAL_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.lmp_da_zonal rows")


# --- generation_by_fuel - confirmed hourly, :15 past the hour. Still a periodic batch ---
# post, not a push feed, so this stays direct-upsert, just on an hourly schedule. --------

GENERATION_BY_FUEL_UPSERT = """
INSERT INTO raw_lmp.generation_by_fuel (time, fuel_type, generation_mw)
VALUES %s
ON CONFLICT (time, fuel_type) DO UPDATE SET
    generation_mw = EXCLUDED.generation_mw
"""


@fetch
def poll_generation_by_fuel(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_fuel_mix(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["time"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw = raw.drop_duplicates(subset=["time"])
    long = raw.melt(id_vars=["time"], value_vars=FUEL_COLUMNS, var_name="fuel_type", value_name="generation_mw")
    return long.where(long.notna(), None)


@flow(name="generation_by_fuel", log_prints=True)
def sync_generation_by_fuel(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    long = poll_generation_by_fuel(pjm, end - timedelta(days=lookback_days), end + timedelta(days=1))
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


@fetch
def poll_transmission_constraints_da(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_transmission_constraints_day_ahead_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw["datetime_ending_utc"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])


@flow(name="transmission_constraints_da", log_prints=True)
def sync_transmission_constraints_da(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_transmission_constraints_da(pjm, end - timedelta(days=lookback_days), end + timedelta(days=2))
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


@fetch
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


@flow(name="operator_initiated_commitments", log_prints=True)
def sync_operator_initiated_commitments(lookback_days: int = COMMITMENTS_POLL_WINDOW_DAYS):
    _assert_zone_mapping_in_sync()
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_operator_initiated_commitments(pjm, end - timedelta(days=lookback_days), end)
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


@fetch
def poll_scheduled_generation(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm._get_pjm_json("rt_and_self_ecomax", start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), params={})
    raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["time"])


@flow(name="scheduled_generation", log_prints=True)
def sync_scheduled_generation(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_scheduled_generation(pjm, end - timedelta(days=lookback_days), end + timedelta(days=1))
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


@fetch
def poll_generation_ehv_losses(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm._get_pjm_json("gen_ehv_losses", start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), params={})
    raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["time"])


@flow(name="generation_ehv_losses", log_prints=True)
def sync_generation_ehv_losses(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_generation_ehv_losses(pjm, end - timedelta(days=lookback_days), end + timedelta(days=1))
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


@fetch
def poll_forecasted_generation_outages(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_forecasted_generation_outages(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["forecast_execution_date"] = raw["Publish Time"].dt.tz_convert("UTC")
    raw["forecast_date"] = pd.to_datetime(raw["Interval Start"], utc=True).dt.date
    return raw.drop_duplicates(subset=["forecast_execution_date", "forecast_date"])


@flow(name="forecasted_generation_outages", log_prints=True)
def sync_forecasted_generation_outages(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_forecasted_generation_outages(pjm, end - timedelta(days=lookback_days), end + timedelta(days=1))
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


@fetch
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


@flow(name="natural_gas_fuel_cost", log_prints=True)
def sync_natural_gas_fuel_cost(lookback_months: int = NATURAL_GAS_POLL_WINDOW_MONTHS):
    end = datetime.now(UTC)
    eia = _eia()
    raw = poll_natural_gas_fuel_cost(eia, end - relativedelta(months=lookback_months), end)
    rows = list(raw[["period", "location", "cost_per_mmbtu"]].itertuples(index=False, name=None))
    _upsert(NATURAL_GAS_FUEL_COST_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.natural_gas_fuel_cost rows")


# --- lmp_rt - verified real-time hourly LMP for the GNN node set, treated as ---

NODAL_IDS_PER_CHUNK = 150
NODAL_MAX_ID_PARAM_CHARS = 1705
NODAL_REQUEST_SPACING_S = 10.5

# Verified always wins: upgrades any unverified row for the same hour.
RT_HRL_LMPS_NODAL_UPSERT = """
INSERT INTO raw_lmp.lmp_rt (time, pnode_id, system_energy_price, lmp, congestion_price, marginal_loss_price, is_verified)
VALUES %s
ON CONFLICT (time, pnode_id) DO UPDATE SET
    system_energy_price = EXCLUDED.system_energy_price,
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price,
    is_verified = true
"""


def _nodal_chunks(pnode_ids: list[int]) -> list[list[int]]:
    chunks, cur = [], []
    for pid in pnode_ids:
        nxt = cur + [pid]
        if len(nxt) > NODAL_IDS_PER_CHUNK or sum(len(str(x)) for x in nxt) + 3 * (len(nxt) - 1) > NODAL_MAX_ID_PARAM_CHARS:
            chunks.append(cur)
            nxt = [pid]
        cur = nxt
    chunks.append(cur)
    return chunks


def _nodal_pnode_ids() -> list[int]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("select pnode_id from raw_lmp.pnode_topology_nodes order by pnode_id")
    pnode_ids = [r[0] for r in cur.fetchall()]
    conn.close()
    return pnode_ids


@fetch(retry_delay_seconds=120)
def poll_lmp_nodal(pjm: gs.PJM, market: str, start: datetime, end: datetime, pnode_ids: list[int]) -> pd.DataFrame:
    """market 'rt' -> rt_hrl_lmps (verified), 'da' -> da_hrl_lmps; same fields with that suffix."""
    sfx = f"_{market}"
    parts = []
    for i, ids in enumerate(_nodal_chunks(pnode_ids)):
        if i:
            time.sleep(NODAL_REQUEST_SPACING_S)
        raw = pjm._get_pjm_json(
            f"{market}_hrl_lmps",
            start=pd.Timestamp(start).tz_convert("US/Eastern"),
            end=pd.Timestamp(end).tz_convert("US/Eastern"),
            params={
                "pnode_id": ";".join(map(str, ids)),
                "fields": f"datetime_beginning_utc,pnode_id,system_energy_price{sfx},total_lmp{sfx},congestion_price{sfx},marginal_loss_price{sfx},row_is_current",
            },
            filter_timestamp_name="datetime_beginning",
            interval_duration_min=60,
        )
        parts.append(raw[raw["row_is_current"]])
    raw = pd.concat(parts)
    raw["time"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.rename(columns={
        f"system_energy_price{sfx}": "system_energy_price", f"total_lmp{sfx}": "lmp",
        f"congestion_price{sfx}": "congestion_price", f"marginal_loss_price{sfx}": "marginal_loss_price",
    })
    return raw.drop_duplicates(subset=["time", "pnode_id"])


@flow(name="lmp_rt", log_prints=True)
def sync_lmp_rt(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_lmp_nodal(pjm, "rt", end - timedelta(days=lookback_days), end, _nodal_pnode_ids())
    raw["is_verified"] = True
    rows = list(raw[[
        "time", "pnode_id", "system_energy_price", "lmp", "congestion_price", "marginal_loss_price", "is_verified",
    ]].itertuples(index=False, name=None))
    _upsert(RT_HRL_LMPS_NODAL_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} verified raw_lmp.lmp_rt rows ({raw['pnode_id'].nunique()} pnodes)")


# --- lmp_da - day-ahead hourly LMP for the GNN node set. DA clears once a day -

DA_HRL_LMPS_NODAL_UPSERT = """
INSERT INTO raw_lmp.lmp_da (time, pnode_id, system_energy_price, lmp, congestion_price, marginal_loss_price)
VALUES %s
ON CONFLICT (time, pnode_id) DO UPDATE SET
    system_energy_price = EXCLUDED.system_energy_price,
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price
"""


@flow(name="lmp_da", log_prints=True)
def sync_lmp_da(lookback_days: int = POLL_WINDOW_DAYS):
    today = pd.Timestamp.now(tz="US/Eastern").normalize()
    start, end = today - pd.Timedelta(days=lookback_days), today + pd.Timedelta(days=2)
    pjm = _pjm()
    raw = poll_lmp_nodal(pjm, "da", start, end - pd.Timedelta(minutes=1), _nodal_pnode_ids())
    rows = list(raw[["time", "pnode_id", "system_energy_price", "lmp", "congestion_price", "marginal_loss_price"]].itertuples(index=False, name=None))
    _upsert(DA_HRL_LMPS_NODAL_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.lmp_da rows ({raw['pnode_id'].nunique()} pnodes, latest {raw['time'].max()})")


# --- rt_unverified_hrl_lmps -> lmp_rt, is_verified = false - confirmed hourly ---

UNVERIFIED_POLL_WINDOW_HOURS = 3

RT_UNVERIFIED_HRL_LMPS_NODAL_UPSERT = """
INSERT INTO raw_lmp.lmp_rt (time, pnode_id, system_energy_price, lmp, congestion_price, marginal_loss_price, is_verified)
VALUES %s
ON CONFLICT (time, pnode_id) DO UPDATE SET
    system_energy_price = EXCLUDED.system_energy_price,
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price
WHERE NOT raw_lmp.lmp_rt.is_verified
"""


@fetch(retry_delay_seconds=120)
def poll_lmp_rt_unverified(pjm: gs.PJM, start: datetime, end: datetime, name_to_id: dict[str, int]) -> pd.DataFrame:
    raw = pjm._get_pjm_json(
        "rt_unverified_hrl_lmps",
        start=pd.Timestamp(start).tz_convert("US/Eastern"),
        end=pd.Timestamp(end).tz_convert("US/Eastern"),
        params={},
        filter_timestamp_name="datetime_beginning",
        interval_duration_min=60,
    )
    raw = raw[raw["pnode_name"].isin(name_to_id)].copy()
    raw["pnode_id"] = raw["pnode_name"].map(name_to_id)
    raw["time"] = raw["Interval End"].dt.tz_convert("UTC")
    raw["system_energy_price"] = raw["total_lmp_rt"] - raw["congestion_price_rt"] - raw["marginal_loss_price_rt"]
    raw["is_verified"] = False
    return raw.drop_duplicates(subset=["time", "pnode_id"])


@flow(name="lmp_rt_unverified", log_prints=True)
def sync_lmp_rt_unverified(lookback_hours: int = UNVERIFIED_POLL_WINDOW_HOURS):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("select pnode_name, pnode_id from raw_lmp.pnode_topology_nodes")
    name_to_id = dict(cur.fetchall())
    conn.close()

    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_lmp_rt_unverified(pjm, end - timedelta(hours=lookback_hours), end, name_to_id)
    rows = list(raw[[
        "time", "pnode_id", "system_energy_price", "total_lmp_rt", "congestion_price_rt", "marginal_loss_price_rt", "is_verified",
    ]].itertuples(index=False, name=None))
    _upsert(RT_UNVERIFIED_HRL_LMPS_NODAL_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} unverified raw_lmp.lmp_rt rows ({raw['pnode_id'].nunique()} pnodes, latest {raw['time'].max()})")


# --- load_forecast - PJM's own load forecast by area, 5 evaluations a day (~05:45, 09:45, ---
# 11:45, 17:45, 23:45 ET). The hist feed keeps every evaluation, so the table is point in ---
# time: no row is ever revised, hence DO NOTHING on conflict. ---------------------------

LOAD_FORECAST_UPSERT = """
INSERT INTO raw_lmp.load_forecast (evaluated_at, time, forecast_area, forecast_load_mw)
VALUES %s
ON CONFLICT (evaluated_at, time, forecast_area) DO NOTHING
"""
LOAD_FORECAST_BACKFILL_CHUNK_DAYS = 20


@fetch
def poll_load_forecast(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm._get_pjm_json(
        "load_frcstd_hist",
        start=pd.Timestamp(start).tz_convert("US/Eastern"),
        end=pd.Timestamp(end).tz_convert("US/Eastern"),
        params={},
        filter_timestamp_name="evaluated_at",
    )
    raw["evaluated_at"] = pd.to_datetime(raw["evaluated_at_utc"]).dt.tz_localize("UTC")
    raw["time"] = pd.to_datetime(raw["forecast_hour_beginning_utc"]).dt.tz_localize("UTC") + pd.Timedelta(hours=1)
    return raw.drop_duplicates(subset=["evaluated_at", "time", "forecast_area"])


@flow(name="load_forecast", log_prints=True)
def sync_load_forecast(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_load_forecast(pjm, end - timedelta(days=lookback_days), end)
    rows = list(raw[["evaluated_at", "time", "forecast_area", "forecast_load_mw"]].itertuples(index=False, name=None))
    _upsert(LOAD_FORECAST_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.load_forecast rows (latest evaluation {raw['evaluated_at'].max()})")


# The hist feed lags about a day (checked 2026-10-25: newest evaluation was 23:45 ET the --
# night before), so at the 14:30 ET cutoff it has nothing covering tomorrow. The live ------
# 7-day feed has only its latest evaluation (refreshed twice an hour), so it is snapshotted -
# into the same table, renamed to the hist feed's 11 coarse areas (live MIDATL is one ------
# MID_ATLANTIC_REGION row, equal to hist MIDATL). ---------------------------------------

LIVE_TO_HIST_AREA = {
    "AEP": "AEP", "AP": "APS", "ATSI": "ATSI", "COMED": "COMED", "DAYTON": "DAY", "DEOK": "DEOK",
    "DOMINION": "DOM", "DUQUESNE": "DUQ", "EKPC": "EKPC", "MID_ATLANTIC_REGION": "MIDATL",
    "RTO_COMBINED": "RTO",
}


@fetch
def poll_load_forecast_live(pjm: gs.PJM) -> pd.DataFrame:
    raw = pjm._get_pjm_json("load_frcstd_7_day", start=None, params={})
    raw = raw[raw["forecast_area"].isin(LIVE_TO_HIST_AREA)].copy()
    raw["forecast_area"] = raw["forecast_area"].map(LIVE_TO_HIST_AREA)
    raw["evaluated_at"] = pd.to_datetime(raw["evaluated_at_datetime_utc"]).dt.tz_localize("UTC")
    raw["time"] = pd.to_datetime(raw["forecast_datetime_ending_utc"]).dt.tz_localize("UTC")
    return raw.drop_duplicates(subset=["evaluated_at", "time", "forecast_area"])


@flow(name="load_forecast_live", log_prints=True)
def sync_load_forecast_live():
    pjm = _pjm()
    raw = poll_load_forecast_live(pjm)
    rows = list(raw[["evaluated_at", "time", "forecast_area", "forecast_load_mw"]].itertuples(index=False, name=None))
    _upsert(LOAD_FORECAST_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.load_forecast rows (latest evaluation {raw['evaluated_at'].max()})")


# --- weather_forecast - Open-Meteo, zone-level hourly forecast. No auth, no PJM rate limit. ---
# `previous_day1` (previous-runs API) is what the model forecast a day before each hour and
# is what training sees; `live` is a snapshot of the current forecast, what serving sees.
# Both are collapsed from the 30 stations to their zone by mean, as the weather producer does.

WEATHER_FORECAST_UPSERT = """
INSERT INTO raw_lmp.weather_forecast
    (time, zone, source, issued_at, temperature, precipitation, wind_speed, cloud_cover)
VALUES %s
ON CONFLICT (time, zone, source, issued_at) DO NOTHING
"""
PREVIOUS_RUNS_API = "https://previous-runs-api.open-meteo.com/v1/forecast"
LIVE_FORECAST_API = "https://api.open-meteo.com/v1/forecast"
WEATHER_VARIABLES = ["temperature_2m", "precipitation", "wind_speed_10m", "cloud_cover"]
WEATHER_FORECAST_COLUMNS = ["time", "zone", "source", "issued_at", "temperature", "precipitation", "wind_speed", "cloud_cover"]
WEATHER_BACKFILL_CHUNK_DAYS = 30


def _weather_forecast_request(url: str, **params) -> list[dict]:
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
    response = requests.get(
        url,
        params={
            "latitude": ",".join(str(v) for v in zones["lat"]),
            "longitude": ",".join(str(v) for v in zones["lon"]),
            "timezone": "UTC",
            **params,
        },
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def _collapse_stations_to_zones(responses: list[dict], suffix: str) -> pd.DataFrame:
    zones = pd.read_csv(PROCESSED_DATA_DIR / "pjm_weather_zones.csv")
    stations = []
    for zone_id, response in zip(zones["zone_id"], responses):
        hourly = response["hourly"]
        stations.append(
            pd.DataFrame({
                "time": pd.to_datetime(hourly["time"], utc=True) + pd.Timedelta(hours=1),
                "zone": zone_id,
                "temperature": hourly[f"temperature_2m{suffix}"],
                "precipitation": hourly[f"precipitation{suffix}"],
                "wind_speed": hourly[f"wind_speed_10m{suffix}"],
                "cloud_cover": hourly[f"cloud_cover{suffix}"],
            })
        )
    return pd.concat(stations).groupby(["time", "zone"], as_index=False).mean()


@fetch
def poll_weather_forecast_previous_day1(start: datetime, end: datetime) -> pd.DataFrame:
    responses = _weather_forecast_request(
        PREVIOUS_RUNS_API,
        hourly=",".join(f"{v}_previous_day1" for v in WEATHER_VARIABLES),
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
    )
    forecast = _collapse_stations_to_zones(responses, "_previous_day1")
    forecast["source"] = "previous_day1"
    forecast["issued_at"] = forecast["time"] - pd.Timedelta(hours=24)
    return forecast.dropna(subset=["temperature"])


@fetch
def poll_weather_forecast_live(issued_at: datetime) -> pd.DataFrame:
    responses = _weather_forecast_request(LIVE_FORECAST_API, hourly=",".join(WEATHER_VARIABLES), forecast_days=3)
    forecast = _collapse_stations_to_zones(responses, "")
    forecast["source"] = "live"
    forecast["issued_at"] = pd.Timestamp(issued_at).floor("s")
    return forecast


@flow(name="weather_forecast", log_prints=True)
def sync_weather_forecast(lookback_days: int = POLL_WINDOW_DAYS):
    now = datetime.now(UTC)
    previous_day1 = poll_weather_forecast_previous_day1(now - timedelta(days=lookback_days), now)
    live = poll_weather_forecast_live(now)
    for forecast in (previous_day1, live):
        rows = list(
            forecast[WEATHER_FORECAST_COLUMNS].astype(object).where(forecast[WEATHER_FORECAST_COLUMNS].notna(), None)
            .itertuples(index=False, name=None)
        )
        _upsert(WEATHER_FORECAST_UPSERT, rows)
        logger.success(f"Upserted {len(rows)} raw_lmp.weather_forecast rows ({forecast['source'].iloc[0]}, latest {forecast['time'].max()})")


# PJM rejects a date range over a year, so long lookbacks (the backfill from 2024-10-23 is
# lookback_days ~740) are split into chunks, spaced for the shared 6 requests/min.
REQUEST_MAX_DAYS = 180


def _chunks(start: datetime, end: datetime) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    edges = list(pd.date_range(pd.Timestamp(start).normalize(), end, freq=f"{REQUEST_MAX_DAYS}D")) + [pd.Timestamp(end)]
    return list(pairwise(edges))


# --- interface_flows_da - confirmed daily, 12:00-3:15pm ET (2026-10-28 posted 12:33), next ---
# day's DA flow and limit per interface. Same DA-results posting as marginal_value_da. ------

INTERFACE_FLOWS_DA_UPSERT = """
INSERT INTO raw_lmp.interface_flows_da (time, interface, flow_mw, limit_mw)
VALUES %s
ON CONFLICT (time, interface) DO UPDATE SET
    flow_mw = EXCLUDED.flow_mw,
    limit_mw = EXCLUDED.limit_mw
"""


@fetch
def poll_interface_flows_da(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    parts = []
    for i, (chunk_start, chunk_end) in enumerate(_chunks(start, end)):
        if i:
            time.sleep(NODAL_REQUEST_SPACING_S)
        parts.append(pjm.get_interface_flows_and_limits_day_ahead(chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
    raw = pd.concat(parts)
    raw["time"] = raw["Interval End"].dt.tz_convert("UTC")
    return raw.drop_duplicates(subset=["time", "Interface Limit Name"])


@flow(name="interface_flows_da", log_prints=True)
def sync_interface_flows_da(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_interface_flows_da(pjm, end - timedelta(days=lookback_days), end + timedelta(days=2))
    rows = list(raw[["time", "Interface Limit Name", "Flow", "Limit"]].itertuples(index=False, name=None))
    _upsert(INTERFACE_FLOWS_DA_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.interface_flows_da rows (latest {raw['time'].max()})")


# --- scheduled_interchange - confirmed daily, 07:30am ET, for the previous day. No ----------
# gridstatus wrapper. This endpoint rate-limits hard, so requests are spaced like the nodal ones.

SCHEDULED_INTERCHANGE_UPSERT = """
INSERT INTO raw_lmp.scheduled_interchange (time, tie_line, net_schedule_mw)
VALUES %s
ON CONFLICT (time, tie_line) DO UPDATE SET
    net_schedule_mw = EXCLUDED.net_schedule_mw
"""


@fetch(retry_delay_seconds=120)
def poll_scheduled_interchange(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    parts = []
    for i, (chunk_start, chunk_end) in enumerate(_chunks(start, end)):
        if i:
            time.sleep(NODAL_REQUEST_SPACING_S)
        parts.append(pjm._get_pjm_json(
            "rt_scheduled_interchange",
            start=chunk_start.strftime("%Y-%m-%d"),
            end=chunk_end.strftime("%Y-%m-%d"),
            params={"fields": "datetime_beginning_utc,tie_line,hrly_net_tie_sched"},
            interval_duration_min=60,
        ))
    raw = pd.concat(parts)
    raw["time"] = raw["Interval End"].dt.tz_convert("UTC")
    raw["tie_line"] = raw["tie_line"].str.removesuffix("-PJM-E")
    raw["hrly_net_tie_sched"] = raw["hrly_net_tie_sched"].astype(object).where(raw["hrly_net_tie_sched"].notna(), None)
    return raw.drop_duplicates(subset=["time", "tie_line"])


@flow(name="scheduled_interchange", log_prints=True)
def sync_scheduled_interchange(lookback_days: int = POLL_WINDOW_DAYS):
    end = datetime.now(UTC)
    pjm = _pjm()
    raw = poll_scheduled_interchange(pjm, end - timedelta(days=lookback_days), end)
    rows = list(raw[["time", "tie_line", "hrly_net_tie_sched"]].itertuples(index=False, name=None))
    _upsert(SCHEDULED_INTERCHANGE_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.scheduled_interchange rows ({raw['tie_line'].nunique()} tie lines, latest {raw['time'].max()})")


if __name__ == "__main__":
    sync_marginal_value_rt()
    sync_marginal_value_da()
    sync_lmp_da_zonal()
    sync_generation_by_fuel()
    sync_transmission_constraints_da()
    sync_operator_initiated_commitments()
    sync_scheduled_generation()
    sync_generation_ehv_losses()
    sync_forecasted_generation_outages()
    sync_natural_gas_fuel_cost()
    sync_lmp_rt()
    sync_lmp_rt_unverified()
    sync_lmp_da()
    sync_load_forecast()
    sync_load_forecast_live()
    sync_weather_forecast()
    sync_interface_flows_da()
    sync_scheduled_interchange()
