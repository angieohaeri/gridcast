"""Prefect deployments for the LMP pricing model (references/lmp-pricing-model/).

Kept separate from deployments.py - isolated from the working load-forecasting stack's
schedules, same as its own branch/schema/dbt target/MLflow naming (see decisions.md's
"Isolation" section). Run as its own `serve()` process, alongside (not instead of)
deployments.py's.

Every raw_lmp source lives in raw_lmp_sync.py as one @flow function per source - direct
pull-and-upsert, no Kafka. (The one Kafka pair, the zonal 5-min unverified LMP, was removed
2026-10-24; see decisions.md.)
"""

from prefect.schedules import Cron
from raw_lmp_sync import (
    sync_da_hrl_lmps_nodal,
    sync_forecasted_generation_outages,
    sync_generation_by_fuel,
    sync_generation_ehv_losses,
    sync_load_forecast,
    sync_load_forecast_live,
    sync_lmp_da_hourly,
    sync_marginal_value_da,
    sync_marginal_value_rt,
    sync_natural_gas_fuel_cost,
    sync_operator_initiated_commitments,
    sync_rt_hrl_lmps_nodal,
    sync_rt_unverified_hrl_lmps_nodal,
    sync_scheduled_generation,
    sync_transmission_constraints_da,
)

from prefect import serve

if __name__ == "__main__":
    serve(
        # Confirmed daily, 11am-12pm ET - shadow price data is 5-min-native but the feed
        # itself only refreshes once a day.
        sync_marginal_value_rt.to_deployment(
            name="marginal_value_rt", schedule=Cron("15 12 * * *", timezone="America/New_York"),
        ),
        # DA results for tomorrow post ~1:30pm ET and the forecast is issued at 14:30 ET, so
        # the four DA flows run 13:50-14:05, staggered for PJM's shared 6 requests/min.
        sync_marginal_value_da.to_deployment(
            name="marginal_value_da", schedule=Cron("50 13 * * *", timezone="America/New_York"),
        ),
        sync_lmp_da_hourly.to_deployment(
            name="lmp_da_hourly", schedule=Cron("55 13 * * *", timezone="America/New_York"),
        ),
        # Confirmed hourly, :15 past the hour - still a periodic batch post, not a push
        # feed, so still direct pull-and-upsert, just on an hourly schedule.
        sync_generation_by_fuel.to_deployment(
            name="generation_by_fuel", schedule=Cron("20 * * * *", timezone="America/New_York"),
        ),
        # Confirmed daily, 12-2pm ET.
        sync_transmission_constraints_da.to_deployment(
            name="transmission_constraints_da", schedule=Cron("0 14 * * *", timezone="America/New_York"),
        ),
        # Confirmed monthly, posts on the 20th.
        sync_operator_initiated_commitments.to_deployment(
            name="operator_initiated_commitments", schedule=Cron("0 5 21 * *", timezone="America/New_York"),
        ),
        # Unconfirmed - treated as daily, same as its rt_hrl_lmps-family siblings.
        sync_scheduled_generation.to_deployment(
            name="scheduled_generation", schedule=Cron("30 12 * * *", timezone="America/New_York"),
        ),
        # Unconfirmed - treated as daily.
        sync_generation_ehv_losses.to_deployment(
            name="generation_ehv_losses", schedule=Cron("35 12 * * *", timezone="America/New_York"),
        ),
        # Confirmed daily, 4:00am ET.
        sync_forecasted_generation_outages.to_deployment(
            name="forecasted_generation_outages", schedule=Cron("15 4 * * *", timezone="America/New_York"),
        ),
        # EIA (not PJM), monthly data with a ~3-month reporting lag - weekly is plenty.
        sync_natural_gas_fuel_cost.to_deployment(
            name="natural_gas_fuel_cost", schedule=Cron("0 3 * * 0", timezone="America/New_York"),
        ),

        # Verified real-time hourly LMP for the GNN node set - the real-time target since
        # 2026-10-22. Treated as daily, after the rest of the rt_hrl_lmps family; ~11
        # spaced requests, 7-day trailing window catches settlement revisions.
        sync_rt_hrl_lmps_nodal.to_deployment(
            name="rt_hrl_lmps_nodal", schedule=Cron("40 12 * * *", timezone="America/New_York"),
        ),
        # Unverified hourly LMP, same table with is_verified = false - live inputs until the
        # daily verified run above overwrites them. Confirmed hourly, posts ~20 min after the
        # hour; one request per run (every location, no pnode filter on this feed).
        sync_rt_unverified_hrl_lmps_nodal.to_deployment(
            name="rt_unverified_hrl_lmps_nodal", schedule=Cron("25 * * * *", timezone="America/New_York"),
        ),
        # Day-ahead hourly LMP for the GNN node set - DA results for tomorrow post ~1:30pm
        # ET, so this runs after, ahead of the 14:30 ET forecast; window runs through tomorrow.
        sync_da_hrl_lmps_nodal.to_deployment(
            name="da_hrl_lmps_nodal", schedule=Cron("5 14 * * *", timezone="America/New_York"),
        ),
        # PJM load forecast by area, point in time. The hist feed trails by about a day, so it
        # fills training history; the live 7-day feed is snapshotted before the 14:30 ET
        # forecast for the day-ahead hours. Staggered after the DA flows for the shared rate limit.
        sync_load_forecast.to_deployment(
            name="load_forecast", schedule=Cron("15 14 * * *", timezone="America/New_York"),
        ),
        sync_load_forecast_live.to_deployment(
            name="load_forecast_live", schedule=Cron("20 14 * * *", timezone="America/New_York"),
        ),
    )
