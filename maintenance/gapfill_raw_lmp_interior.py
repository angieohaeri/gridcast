"""Fill interior gaps in the raw_lmp batch tables - stretches inside a table's history
where the Prefect flows were off (2026-10-09 → 10-15 hit every table) plus older small holes.

The daily syncs only reach back their own lookback window, so they can't reach these. Holes are found from the data (same rules as the gap scan): dense hourly tables -
any missing hour; event tables (shadow prices, DA constraints) - days with zero rows, since
PJM binds constraints essentially every day; outage forecasts - missing execution days.
Missing days are merged into windows padded a day each side, then pulled with
raw_lmp_sync.py's own poll functions / upsert SQL / column lists, so rows match the live
flows exactly. Some holes may be missing at PJM too - those stay empty.
"""

from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import sys
import time

from dotenv import load_dotenv
import gridstatus as gs
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "prefect"))
import raw_lmp_sync as s

from gridcast.config import get_connection, setup_logging

load_dotenv()
setup_logging()

REQUEST_SPACING_S = 10.5
COMMITMENTS_WINDOW = (datetime(2026, 8, 25, tzinfo=UTC), datetime.now(UTC))

# table: (time column, hole rule, poll fn, upsert sql, row columns)
SOURCES = {
    "generation_by_fuel": ("time", "hourly", s.poll_generation_by_fuel, s.GENERATION_BY_FUEL_UPSERT,
                           ["time", "fuel_type", "generation_mw"]),
    "generation_ehv_losses": ("time", "hourly", s.poll_generation_ehv_losses, s.GENERATION_EHV_LOSSES_UPSERT,
                              ["time", "total_gen", "total_losses"]),
    "scheduled_generation": ("time", "hourly", s.poll_scheduled_generation, s.SCHEDULED_GENERATION_UPSERT,
                             ["time", "rt_ecomax", "self_ecomax"]),
    "lmp_da_hourly": ("time", "hourly", s.poll_lmp_da_zonal, s.LMP_DA_ZONAL_UPSERT,
                      ["time", "zone", "LMP", "Congestion", "Loss"]),
    "marginal_value_rt": ("datetime_beginning_utc", "daily", s.poll_marginal_value_rt, s.MARGINAL_VALUE_RT_UPSERT,
                          ["datetime_beginning_utc", "datetime_ending_utc", "Monitored Facility", "Contingency Facility",
                           "Transmission Constraint Penalty Factor", "Limit Control Percentage", "Shadow Price"]),
    "marginal_value_da": ("datetime_beginning_utc", "daily", s.poll_marginal_value_da, s.MARGINAL_VALUE_DA_UPSERT,
                          ["datetime_beginning_utc", "datetime_ending_utc", "Monitored Facility", "Contingency Facility", "Shadow Price"]),
    "transmission_constraints_da": ("datetime_beginning_utc", "daily", s.poll_transmission_constraints_da,
                                    s.TRANSMISSION_CONSTRAINTS_DA_UPSERT,
                                    ["datetime_beginning_utc", "datetime_ending_utc", "Duration", "Monitored Facility", "Contingency Facility"]),
    "forecasted_generation_outages": ("forecast_execution_date", "daily", s.poll_forecasted_generation_outages,
                                      s.FORECASTED_GENERATION_OUTAGES_UPSERT,
                                      ["forecast_execution_date", "forecast_date", "RTO MW", "West MW", "Other MW"]),
    "operator_initiated_commitments": ("datetime_beginning_utc", "fixed", s.poll_operator_initiated_commitments,
                                       s.OPERATOR_INITIATED_COMMITMENTS_UPSERT,
                                       ["datetime_beginning_utc", "zone", "economic_max_mw", "reason"]),
}


def missing_days(cur, table: str, tc: str, rule: str) -> list:
    """Dates (ET) with at least one missing hour (hourly) or no rows at all (daily)."""
    if rule == "hourly":
        cur.execute(f"""
            with h as (select generate_series(min(date_trunc('hour', {tc})), max({tc}), interval '1 hour') t from raw_lmp.{table})
            select distinct (h.t at time zone 'America/New_York')::date from h
            where not exists (select 1 from raw_lmp.{table} x where date_trunc('hour', x.{tc}) = h.t)
        """)
    else:
        cur.execute(f"""
            with d as (select generate_series(min(({tc} at time zone 'America/New_York')::date),
                                              max(({tc} at time zone 'America/New_York')::date), interval '1 day')::date t
                       from raw_lmp.{table})
            select d.t from d
            where not exists (select 1 from raw_lmp.{table} x where ({tc} at time zone 'America/New_York')::date = d.t)
        """)
    return sorted(r[0] for r in cur.fetchall())


def windows(days: list) -> list[tuple[datetime, datetime]]:
    out = []
    for d in days:
        lo = datetime(d.year, d.month, d.day, tzinfo=UTC) - timedelta(days=1)
        hi = lo + timedelta(days=3)
        if out and lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


conn = get_connection()
cur = conn.cursor()
pjm = gs.PJM(api_key=os.environ["PJM_API_KEY"], retries=6)
s._assert_zone_mapping_in_sync()

for table, (tc, rule, poll, upsert, cols) in SOURCES.items():
    if rule == "fixed":
        wins = [COMMITMENTS_WINDOW]
    else:
        days = missing_days(cur, table, tc, rule)
        wins = windows(days)
        logger.info(f"{table}: {len(days)} day(s) with holes -> {len(wins)} window(s)")
    for lo, hi in wins:
        time.sleep(REQUEST_SPACING_S)
        try:
            raw = poll(pjm, lo, hi)
        except gs.NoDataFoundException:
            logger.warning(f"{table} {lo:%Y-%m-%d} -> {hi:%Y-%m-%d}: no data at PJM")
            continue
        rows = list(raw[cols].itertuples(index=False, name=None))
        s._upsert(upsert, rows)
        logger.info(f"{table} {lo:%Y-%m-%d} -> {hi:%Y-%m-%d}: upserted {len(rows)} rows")

conn.close()
logger.success("interior gap fill done")
