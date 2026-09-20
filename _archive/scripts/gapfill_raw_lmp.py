"""Gap-fill all 10 raw_lmp tables from each table's current max timestamp through today.

One-off catch-up before wiring live Kafka producers for the LMP pricing model
(references/lmp-pricing-model/). The original 2023-01-01 backfill scripts already ran
and were removed (git history: commits 82707ce, ac59a91) - no ingestion has touched
raw_lmp since, so a ~2 week gap opened between that backfill and now. Pulls straight
from each API into an upsert, no CSV snapshot step - this gap window is far smaller
than the original multi-year pull and doesn't need the resumability that motivated
splitting pull/backfill into two phases there.

REVISION_LOOKBACK re-pulls a couple days before each table's current max, same reason
lmp_producer.py re-polls a trailing window: PJM settlement revisions land after the
row's first post (see project_pjm_eia_data_gotchas memory). ON CONFLICT DO UPDATE (or
DO NOTHING where rows have no stable id) makes the overlap safe.
"""

from datetime import UTC, datetime, timedelta
import json
import os

from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import get_connection, setup_logging

load_dotenv()
setup_logging()

REVISION_LOOKBACK = timedelta(days=2)
NOW = datetime.now(UTC)

# Same zone_id <-> Location Short Name mapping as src/producers/lmp_producer.py.
ZONE_TO_LOCATION = {
    "AE": "AECO", "AEP": "AEP", "AP": "APS", "ATSI": "ATSI", "BC": "BGE", "CE": "COMED",
    "DAY": "DAY", "DEOK": "DEOK", "DOM": "DOM", "DPL": "DPL", "DUQ": "DUQ", "EKPC": "EKPC",
    "JC": "JCPL", "ME": "METED", "PE": "PECO", "PEP": "PEPCO", "PL": "PPL", "PN": "PENELEC",
    "PS": "PSEG", "RECO": "RECO",
}
LOCATION_TO_ZONE = {location: zone for zone, location in ZONE_TO_LOCATION.items()}

FUEL_COLUMNS = [
    "Coal", "Gas", "Hydro", "Multiple Fuels", "Nuclear", "Oil",
    "Other Renewables", "Solar", "Storage", "Wind",
]


def table_max(conn, table: str, column: str):
    cur = conn.cursor()
    cur.execute(f"select max({column}) from {table}")
    return cur.fetchone()[0]


def gapfill_marginal_value_rt(pjm, conn):
    start = table_max(conn, "raw_lmp.marginal_value_rt", "datetime_beginning_utc") - REVISION_LOOKBACK
    raw = pjm.get_marginal_value_real_time_5_min(start.strftime("%Y-%m-%d"), NOW.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw["datetime_ending_utc"] = pd.to_datetime(raw["Interval End"], utc=True)
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])

    rows = list(raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Monitored Facility", "Contingency Facility",
        "Transmission Constraint Penalty Factor", "Limit Control Percentage", "Shadow Price",
    ]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.marginal_value_rt
            (datetime_beginning_utc, datetime_ending_utc, monitored_facility, contingency_facility,
             transmission_constraint_penalty_factor, limit_control_percentage, shadow_price)
        VALUES %s
        ON CONFLICT (datetime_beginning_utc, monitored_facility, contingency_facility) DO UPDATE SET
            datetime_ending_utc = EXCLUDED.datetime_ending_utc,
            transmission_constraint_penalty_factor = EXCLUDED.transmission_constraint_penalty_factor,
            limit_control_percentage = EXCLUDED.limit_control_percentage,
            shadow_price = EXCLUDED.shadow_price
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.marginal_value_rt: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_marginal_value_da(pjm, conn):
    start = table_max(conn, "raw_lmp.marginal_value_da", "datetime_beginning_utc") - REVISION_LOOKBACK
    raw = pjm.get_marginal_value_day_ahead_hourly(start.strftime("%Y-%m-%d"), NOW.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw["datetime_ending_utc"] = pd.to_datetime(raw["Interval End"], utc=True)
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])

    rows = list(raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Monitored Facility", "Contingency Facility",
        "Shadow Price",
    ]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.marginal_value_da
            (datetime_beginning_utc, datetime_ending_utc, monitored_facility, contingency_facility, shadow_price)
        VALUES %s
        ON CONFLICT (datetime_beginning_utc, monitored_facility, contingency_facility) DO UPDATE SET
            datetime_ending_utc = EXCLUDED.datetime_ending_utc,
            shadow_price = EXCLUDED.shadow_price
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.marginal_value_da: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_lmp_da_hourly(pjm, conn):
    start = table_max(conn, "raw_lmp.lmp_da_hourly", "time") - REVISION_LOOKBACK
    raw = pjm.get_lmp(
        start.strftime("%Y-%m-%d"), end=NOW.strftime("%Y-%m-%d"),
        market=gs.Markets.DAY_AHEAD_HOURLY, location_type="ZONE",
    )
    raw = raw[raw["Location Short Name"].isin(LOCATION_TO_ZONE)].copy()
    raw["zone"] = raw["Location Short Name"].map(LOCATION_TO_ZONE)
    raw["time"] = pd.to_datetime(raw["Interval End"], utc=True)
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["time", "zone"])

    rows = list(raw[["time", "zone", "LMP", "Congestion", "Loss"]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.lmp_da_hourly (time, zone, lmp, congestion_price, marginal_loss_price)
        VALUES %s
        ON CONFLICT (time, zone) DO UPDATE SET
            lmp = EXCLUDED.lmp,
            congestion_price = EXCLUDED.congestion_price,
            marginal_loss_price = EXCLUDED.marginal_loss_price
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.lmp_da_hourly: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_generation_by_fuel(pjm, conn):
    start = table_max(conn, "raw_lmp.generation_by_fuel", "time") - REVISION_LOOKBACK
    raw = pjm.get_fuel_mix(start.strftime("%Y-%m-%d"), NOW.strftime("%Y-%m-%d"))
    raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.drop_duplicates(subset=["time"])

    long = raw.melt(id_vars=["time"], value_vars=FUEL_COLUMNS, var_name="fuel_type", value_name="generation_mw")
    long = long.where(long.notna(), None)
    rows = list(long[["time", "fuel_type", "generation_mw"]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.generation_by_fuel (time, fuel_type, generation_mw)
        VALUES %s
        ON CONFLICT (time, fuel_type) DO UPDATE SET
            generation_mw = EXCLUDED.generation_mw
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.generation_by_fuel: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_transmission_constraints_da(pjm, conn):
    start = table_max(conn, "raw_lmp.transmission_constraints_da", "datetime_beginning_utc") - REVISION_LOOKBACK
    raw = pjm.get_transmission_constraints_day_ahead_hourly(start.strftime("%Y-%m-%d"), NOW.strftime("%Y-%m-%d"))
    raw["datetime_beginning_utc"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw["datetime_ending_utc"] = pd.to_datetime(raw["Interval End"], utc=True)
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["datetime_beginning_utc", "Monitored Facility", "Contingency Facility"])

    rows = list(raw[[
        "datetime_beginning_utc", "datetime_ending_utc", "Duration", "Monitored Facility", "Contingency Facility",
    ]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.transmission_constraints_da
            (datetime_beginning_utc, datetime_ending_utc, duration_hours, monitored_facility, contingency_facility)
        VALUES %s
        ON CONFLICT (datetime_beginning_utc, monitored_facility, contingency_facility) DO UPDATE SET
            datetime_ending_utc = EXCLUDED.datetime_ending_utc,
            duration_hours = EXCLUDED.duration_hours
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.transmission_constraints_da: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_operator_initiated_commitments(pjm, conn):
    start = table_max(conn, "raw_lmp.operator_initiated_commitments", "datetime_beginning_utc") - REVISION_LOOKBACK
    try:
        raw = pjm._get_pjm_json(
            "ops_init_commit", start=start.strftime("%Y-%m-%d"), end=NOW.strftime("%Y-%m-%d"), params={}
        )
    except gs.NoDataFoundException:
        logger.info(f"raw_lmp.operator_initiated_commitments: 0 rows from {start:%Y-%m-%d} (no data)")
        return

    raw = raw[raw["zone"].isin(LOCATION_TO_ZONE)].copy()
    raw["zone"] = raw["zone"].map(LOCATION_TO_ZONE)
    raw["datetime_beginning_utc"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["datetime_beginning_utc", "zone", "reason", "economic_max_mw"])

    rows = list(raw[["datetime_beginning_utc", "zone", "economic_max_mw", "reason"]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.operator_initiated_commitments (datetime_beginning_utc, zone, economic_max_mw, reason)
        VALUES %s
        ON CONFLICT (datetime_beginning_utc, zone, reason, economic_max_mw) DO NOTHING
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.operator_initiated_commitments: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_scheduled_generation(pjm, conn):
    start = table_max(conn, "raw_lmp.scheduled_generation", "time") - REVISION_LOOKBACK
    raw = pjm._get_pjm_json(
        "rt_and_self_ecomax", start=start.strftime("%Y-%m-%d"), end=NOW.strftime("%Y-%m-%d"), params={}
    )
    raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["time"])

    rows = list(raw[["time", "rt_ecomax", "self_ecomax"]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.scheduled_generation (time, rt_ecomax, self_ecomax)
        VALUES %s
        ON CONFLICT (time) DO UPDATE SET
            rt_ecomax = EXCLUDED.rt_ecomax,
            self_ecomax = EXCLUDED.self_ecomax
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.scheduled_generation: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_generation_ehv_losses(pjm, conn):
    start = table_max(conn, "raw_lmp.generation_ehv_losses", "time") - REVISION_LOOKBACK
    raw = pjm._get_pjm_json("gen_ehv_losses", start=start.strftime("%Y-%m-%d"), end=NOW.strftime("%Y-%m-%d"), params={})
    raw["time"] = pd.to_datetime(raw["Interval Start"], utc=True)
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["time"])

    rows = list(raw[["time", "total_gen", "total_losses"]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.generation_ehv_losses (time, total_gen, total_losses)
        VALUES %s
        ON CONFLICT (time) DO UPDATE SET
            total_gen = EXCLUDED.total_gen,
            total_losses = EXCLUDED.total_losses
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.generation_ehv_losses: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_forecasted_generation_outages(pjm, conn):
    start = table_max(conn, "raw_lmp.forecasted_generation_outages", "forecast_execution_date") - REVISION_LOOKBACK
    raw = pjm.get_forecasted_generation_outages(start.strftime("%Y-%m-%d"), NOW.strftime("%Y-%m-%d"))
    raw["forecast_execution_date"] = pd.to_datetime(raw["Publish Time"], utc=True)
    raw["forecast_date"] = pd.to_datetime(raw["Interval Start"], utc=True).dt.date
    raw = raw.drop_duplicates(subset=["forecast_execution_date", "forecast_date"])

    rows = list(
        raw[["forecast_execution_date", "forecast_date", "RTO MW", "West MW", "Other MW"]]
        .itertuples(index=False, name=None)
    )

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.forecasted_generation_outages
            (forecast_execution_date, forecast_date, outage_mw_rto, outage_mw_west, outage_mw_other)
        VALUES %s
        ON CONFLICT (forecast_execution_date, forecast_date) DO UPDATE SET
            outage_mw_rto = EXCLUDED.outage_mw_rto,
            outage_mw_west = EXCLUDED.outage_mw_west,
            outage_mw_other = EXCLUDED.outage_mw_other
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.forecasted_generation_outages: upserted {len(rows)} rows from {start:%Y-%m-%d}")


def gapfill_natural_gas_fuel_cost(eia, conn):
    # Non-state location codes returned alongside real states: census regions, national
    # total, Puerto Rico - dropped, same as the original backfill.
    non_state_locations = {"90", "ENC", "ESC", "MAT", "MTN", "NEW", "PCC", "PCN", "SAT", "WNC", "WSC", "PR", "US"}

    start_period = table_max(conn, "raw_lmp.natural_gas_fuel_cost", "period")
    url = f"{eia.BASE_URL}electricity/electric-power-operational-data/data/"
    params = {
        "start": start_period.strftime("%Y-%m"),
        "end": NOW.strftime("%Y-%m"),
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
    raw = raw[~raw["location"].isin(non_state_locations)].copy()
    raw["period"] = pd.to_datetime(raw["period"], format="%Y-%m").dt.date
    raw = raw.rename(columns={"cost-per-btu": "cost_per_mmbtu"})
    raw = raw.where(raw.notna(), None)
    raw = raw.drop_duplicates(subset=["period", "location"])

    rows = list(raw[["period", "location", "cost_per_mmbtu"]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO raw_lmp.natural_gas_fuel_cost (period, location, cost_per_mmbtu)
        VALUES %s
        ON CONFLICT (period, location) DO UPDATE SET
            cost_per_mmbtu = EXCLUDED.cost_per_mmbtu
    """, rows, page_size=5000)
    logger.success(f"raw_lmp.natural_gas_fuel_cost: upserted {len(rows)} rows from {start_period:%Y-%m}")


def main():
    pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
    eia = gs.EIA(api_key=os.environ["EIA_API_KEY"])
    conn = get_connection()

    gapfill_marginal_value_rt(pjm, conn)
    gapfill_marginal_value_da(pjm, conn)
    gapfill_lmp_da_hourly(pjm, conn)
    gapfill_generation_by_fuel(pjm, conn)
    gapfill_transmission_constraints_da(pjm, conn)
    gapfill_operator_initiated_commitments(pjm, conn)
    gapfill_scheduled_generation(pjm, conn)
    gapfill_generation_ehv_losses(pjm, conn)
    gapfill_forecasted_generation_outages(pjm, conn)
    gapfill_natural_gas_fuel_cost(eia, conn)

    conn.close()


if __name__ == "__main__":
    main()
