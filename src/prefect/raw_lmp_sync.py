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
import time

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd
import psycopg2.extras
import requests

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


@flow(name="marginal_value_rt", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_marginal_value_rt():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_marginal_value_rt(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end + timedelta(days=1))
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


def poll_marginal_value_da(pjm: gs.PJM, start: datetime, end: datetime) -> pd.DataFrame:
    raw = pjm.get_marginal_value_day_ahead_hourly(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = raw["Interval Start"].dt.tz_convert("UTC")
    raw["datetime_ending_utc"] = raw["Interval End"].dt.tz_convert("UTC")
    raw = raw.where(raw.notna(), None)
    return raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])


@flow(name="marginal_value_da", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_marginal_value_da():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_marginal_value_da(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end + timedelta(days=2))
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


@flow(name="lmp_da_hourly", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_lmp_da_hourly():
    _assert_zone_mapping_in_sync()
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_lmp_da_hourly(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end + timedelta(days=2))
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


@flow(name="generation_by_fuel", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_generation_by_fuel():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    long = poll_generation_by_fuel(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end + timedelta(days=1))
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


@flow(name="transmission_constraints_da", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_transmission_constraints_da():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_transmission_constraints_da(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end + timedelta(days=2))
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


@flow(name="operator_initiated_commitments", retries=3, retry_delay_seconds=60, log_prints=True)
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


@flow(name="scheduled_generation", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_scheduled_generation():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_scheduled_generation(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end + timedelta(days=1))
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


@flow(name="generation_ehv_losses", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_generation_ehv_losses():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_generation_ehv_losses(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end + timedelta(days=1))
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


@flow(name="forecasted_generation_outages", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_forecasted_generation_outages():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_forecasted_generation_outages(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end + timedelta(days=1))
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


@flow(name="natural_gas_fuel_cost", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_natural_gas_fuel_cost():
    end = datetime.now(UTC)
    eia = gs.EIA(api_key=os.environ["EIA_API_KEY"])
    raw = poll_natural_gas_fuel_cost(eia, end - relativedelta(months=NATURAL_GAS_POLL_WINDOW_MONTHS), end)
    rows = list(raw[["period", "location", "cost_per_mmbtu"]].itertuples(index=False, name=None))
    _upsert(NATURAL_GAS_FUEL_COST_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.natural_gas_fuel_cost rows")


# --- rt_hrl_lmps_nodal - verified real-time hourly LMP for the GNN node set, treated as ---

NODAL_IDS_PER_CHUNK = 150
NODAL_MAX_ID_PARAM_CHARS = 1705
NODAL_REQUEST_SPACING_S = 10.5

# Verified always wins: upgrades any unverified row for the same hour.
RT_HRL_LMPS_NODAL_UPSERT = """
INSERT INTO raw_lmp.rt_hrl_lmps_nodal (time, pnode_id, system_energy_price, lmp, congestion_price, marginal_loss_price, is_verified)
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


def poll_hrl_lmps_nodal(pjm: gs.PJM, market: str, start: datetime, end: datetime, pnode_ids: list[int]) -> pd.DataFrame:
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


@flow(name="rt_hrl_lmps_nodal", retries=3, retry_delay_seconds=120, log_prints=True)
def sync_rt_hrl_lmps_nodal():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_hrl_lmps_nodal(pjm, "rt", end - timedelta(days=POLL_WINDOW_DAYS), end, _nodal_pnode_ids())
    raw["is_verified"] = True
    rows = list(raw[[
        "time", "pnode_id", "system_energy_price", "lmp", "congestion_price", "marginal_loss_price", "is_verified",
    ]].itertuples(index=False, name=None))
    _upsert(RT_HRL_LMPS_NODAL_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} verified raw_lmp.rt_hrl_lmps_nodal rows ({raw['pnode_id'].nunique()} pnodes)")


# --- da_hrl_lmps_nodal - day-ahead hourly LMP for the GNN node set. DA clears once a day -

DA_HRL_LMPS_NODAL_UPSERT = """
INSERT INTO raw_lmp.da_hrl_lmps_nodal (time, pnode_id, system_energy_price, lmp, congestion_price, marginal_loss_price)
VALUES %s
ON CONFLICT (time, pnode_id) DO UPDATE SET
    system_energy_price = EXCLUDED.system_energy_price,
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price
"""


@flow(name="da_hrl_lmps_nodal", retries=3, retry_delay_seconds=120, log_prints=True)
def sync_da_hrl_lmps_nodal():
    today = pd.Timestamp.now(tz="US/Eastern").normalize()
    start, end = today - pd.Timedelta(days=POLL_WINDOW_DAYS), today + pd.Timedelta(days=2)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_hrl_lmps_nodal(pjm, "da", start, end - pd.Timedelta(minutes=1), _nodal_pnode_ids())
    rows = list(raw[["time", "pnode_id", "system_energy_price", "lmp", "congestion_price", "marginal_loss_price"]].itertuples(index=False, name=None))
    _upsert(DA_HRL_LMPS_NODAL_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.da_hrl_lmps_nodal rows ({raw['pnode_id'].nunique()} pnodes, latest {raw['time'].max()})")


# --- rt_unverified_hrl_lmps -> rt_hrl_lmps_nodal, is_verified = false - confirmed hourly ---

UNVERIFIED_POLL_WINDOW_HOURS = 3

RT_UNVERIFIED_HRL_LMPS_NODAL_UPSERT = """
INSERT INTO raw_lmp.rt_hrl_lmps_nodal (time, pnode_id, system_energy_price, lmp, congestion_price, marginal_loss_price, is_verified)
VALUES %s
ON CONFLICT (time, pnode_id) DO UPDATE SET
    system_energy_price = EXCLUDED.system_energy_price,
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price
WHERE NOT raw_lmp.rt_hrl_lmps_nodal.is_verified
"""


def poll_rt_unverified_hrl_lmps_nodal(pjm: gs.PJM, start: datetime, end: datetime, name_to_id: dict[str, int]) -> pd.DataFrame:
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


@flow(name="rt_unverified_hrl_lmps_nodal", retries=3, retry_delay_seconds=120, log_prints=True)
def sync_rt_unverified_hrl_lmps_nodal():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("select pnode_name, pnode_id from raw_lmp.pnode_topology_nodes")
    name_to_id = dict(cur.fetchall())
    conn.close()

    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    raw = poll_rt_unverified_hrl_lmps_nodal(pjm, end - timedelta(hours=UNVERIFIED_POLL_WINDOW_HOURS), end, name_to_id)
    rows = list(raw[[
        "time", "pnode_id", "system_energy_price", "total_lmp_rt", "congestion_price_rt", "marginal_loss_price_rt", "is_verified",
    ]].itertuples(index=False, name=None))
    _upsert(RT_UNVERIFIED_HRL_LMPS_NODAL_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} unverified raw_lmp.rt_hrl_lmps_nodal rows ({raw['pnode_id'].nunique()} pnodes, latest {raw['time'].max()})")


# --- load_forecast - PJM's own load forecast by area, 5 evaluations a day (~05:45, 09:45, ---
# 11:45, 17:45, 23:45 ET). The hist feed keeps every evaluation, so the table is point in ---
# time: no row is ever revised, hence DO NOTHING on conflict. ---------------------------

LOAD_FORECAST_UPSERT = """
INSERT INTO raw_lmp.load_forecast (evaluated_at, time, forecast_area, forecast_load_mw)
VALUES %s
ON CONFLICT (evaluated_at, time, forecast_area) DO NOTHING
"""
LOAD_FORECAST_BACKFILL_CHUNK_DAYS = 20


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


def _upsert_load_forecast(raw: pd.DataFrame) -> None:
    rows = list(raw[["evaluated_at", "time", "forecast_area", "forecast_load_mw"]].itertuples(index=False, name=None))
    _upsert(LOAD_FORECAST_UPSERT, rows)
    logger.success(f"Upserted {len(rows)} raw_lmp.load_forecast rows (latest evaluation {raw['evaluated_at'].max()})")


def backfill_load_forecast(start: datetime, end: datetime) -> None:
    """History in chunks, spaced for PJM's shared 6 requests/min. Safe to re-run."""
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(days=LOAD_FORECAST_BACKFILL_CHUNK_DAYS), end)
        _upsert_load_forecast(poll_load_forecast(pjm, chunk_start, chunk_end))
        chunk_start = chunk_end
        time.sleep(10.5)


@flow(name="load_forecast", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_load_forecast():
    end = datetime.now(UTC)
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    _upsert_load_forecast(poll_load_forecast(pjm, end - timedelta(days=POLL_WINDOW_DAYS), end))


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


def poll_load_forecast_live(pjm: gs.PJM) -> pd.DataFrame:
    raw = pjm._get_pjm_json("load_frcstd_7_day", start=None, params={})
    raw = raw[raw["forecast_area"].isin(LIVE_TO_HIST_AREA)].copy()
    raw["forecast_area"] = raw["forecast_area"].map(LIVE_TO_HIST_AREA)
    raw["evaluated_at"] = pd.to_datetime(raw["evaluated_at_datetime_utc"]).dt.tz_localize("UTC")
    raw["time"] = pd.to_datetime(raw["forecast_datetime_ending_utc"]).dt.tz_localize("UTC")
    return raw.drop_duplicates(subset=["evaluated_at", "time", "forecast_area"])


@flow(name="load_forecast_live", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_load_forecast_live():
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    _upsert_load_forecast(poll_load_forecast_live(pjm))


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


def poll_weather_forecast_live(issued_at: datetime) -> pd.DataFrame:
    responses = _weather_forecast_request(LIVE_FORECAST_API, hourly=",".join(WEATHER_VARIABLES), forecast_days=3)
    forecast = _collapse_stations_to_zones(responses, "")
    forecast["source"] = "live"
    forecast["issued_at"] = pd.Timestamp(issued_at).floor("s")
    return forecast


def _upsert_weather_forecast(forecast: pd.DataFrame) -> None:
    forecast = forecast[WEATHER_FORECAST_COLUMNS].astype(object).where(forecast[WEATHER_FORECAST_COLUMNS].notna(), None)
    _upsert(WEATHER_FORECAST_UPSERT, list(forecast.itertuples(index=False, name=None)))
    logger.success(f"Upserted {len(forecast)} raw_lmp.weather_forecast rows ({forecast['source'].iloc[0]}, latest {forecast['time'].max()})")


def backfill_weather_forecast(start: datetime, end: datetime) -> None:
    """previous_day1 history in chunks. Safe to re-run."""
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(days=WEATHER_BACKFILL_CHUNK_DAYS), end)
        _upsert_weather_forecast(poll_weather_forecast_previous_day1(chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
        time.sleep(1)


@flow(name="weather_forecast", retries=3, retry_delay_seconds=60, log_prints=True)
def sync_weather_forecast():
    now = datetime.now(UTC)
    _upsert_weather_forecast(poll_weather_forecast_previous_day1(now - timedelta(days=POLL_WINDOW_DAYS), now))
    _upsert_weather_forecast(poll_weather_forecast_live(now))


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
    sync_rt_hrl_lmps_nodal()
    sync_rt_unverified_hrl_lmps_nodal()
    sync_da_hrl_lmps_nodal()
    sync_load_forecast()
    sync_load_forecast_live()
    sync_weather_forecast()
