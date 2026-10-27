"""Prefect deployments for the LMP pricing model (references/lmp-pricing-model/).

Kept separate from deployments.py - isolated from the working load-forecasting stack's
schedules, same as its own branch/schema/dbt target/MLflow naming (see decisions.md's
"Isolation" section). Run as its own `serve()` process, alongside (not instead of)
deployments.py's.

Every raw_lmp source lives in raw_lmp_sync.py as one @flow function per source - direct
pull-and-upsert, no Kafka. (The one Kafka pair, the zonal 5-min unverified LMP, was removed
2026-10-24; see decisions.md.)
"""

from dbt_build import main as dbt_build_flow
from lmp_daily import lmp_afternoon, lmp_morning
from prefect.schedules import Cron
from raw_lmp_sync import (
    sync_generation_by_fuel,
    sync_natural_gas_fuel_cost,
    sync_operator_initiated_commitments,
    sync_rt_unverified_hrl_lmps_nodal,
)

from prefect import serve

if __name__ == "__main__":
    serve(
        # The daily syncs and dbt builds run in order inside these two flows (lmp_daily.py),
        # not on a cron each. Morning starts once verified RT prices are out; afternoon starts
        # after DA results post (~13:30 ET) and has to finish before the 14:30 ET forecast.
        lmp_morning.to_deployment(name="lmp_morning", schedule=Cron("15 12 * * *", timezone="America/New_York")),
        lmp_afternoon.to_deployment(name="lmp_afternoon", schedule=Cron("50 13 * * *", timezone="America/New_York")),
        # Unscheduled, for running one dbt selector by hand.
        dbt_build_flow.to_deployment(name="dbt_build_lmp_history", parameters={"selector": "lmp_history"}),
        dbt_build_flow.to_deployment(name="dbt_build_lmp_serving", parameters={"selector": "lmp_serving"}),
        # Confirmed hourly, :15 past the hour - still a periodic batch post, not a push
        # feed, so still direct pull-and-upsert, just on an hourly schedule.
        sync_generation_by_fuel.to_deployment(
            name="generation_by_fuel", schedule=Cron("20 * * * *", timezone="America/New_York"),
        ),
        # Unverified hourly LMP, same table with is_verified = false - live inputs until the
        # daily verified run overwrites them. Confirmed hourly, posts ~20 min after the
        # hour; one request per run (every location, no pnode filter on this feed).
        sync_rt_unverified_hrl_lmps_nodal.to_deployment(
            name="rt_unverified_hrl_lmps_nodal", schedule=Cron("25 * * * *", timezone="America/New_York"),
        ),
        # Confirmed monthly, posts on the 20th.
        sync_operator_initiated_commitments.to_deployment(
            name="operator_initiated_commitments", schedule=Cron("0 5 21 * *", timezone="America/New_York"),
        ),
        # EIA (not PJM), monthly data with a ~3-month reporting lag - weekly is plenty.
        sync_natural_gas_fuel_cost.to_deployment(
            name="natural_gas_fuel_cost", schedule=Cron("0 3 * * 0", timezone="America/New_York"),
        ),
    )
