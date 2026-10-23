"""Prefect deployments for the LMP pricing model (references/lmp-pricing-model/).

Kept separate from deployments.py - isolated from the working load-forecasting stack's
schedules, same as its own branch/schema/dbt target/MLflow naming (see decisions.md's
"Isolation" section). Run as its own `serve()` process, alongside (not instead of)
deployments.py's.

The raw_lmp batch sources live together in raw_lmp_sync.py as one @flow function per
source. lmp_rt_unverified_fivemin is the exception - a Kafka producer/consumer pair
(src/producers/, src/consumers/) - and is paused since 2026-10-22: the real-time target
moved to hourly LMP at pnode level (rt_hrl_lmps_nodal - unverified hourly for live
inputs, overwritten by verified daily), see decisions.md. Kept, not deleted.
"""

from datetime import timedelta
from pathlib import Path
import sys

from prefect.schedules import Cron
from raw_lmp_sync import (
    sync_forecasted_generation_outages,
    sync_generation_by_fuel,
    sync_generation_ehv_losses,
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

# raw_lmp_lmp_rt_unverified_fivemin_producer/consumer do a bare sibling import (e.g.
# `from kafka_client import ...`) resolved via their own directory on sys.path - add
# both here, same as deployments.py does for the working pipeline's producers/consumers.
SRC = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SRC / "producers"))
sys.path.insert(0, str(SRC / "consumers"))

from raw_lmp_lmp_rt_unverified_fivemin_consumer import (
    main as raw_lmp_lmp_rt_unverified_fivemin_consumer_flow,
)
from raw_lmp_lmp_rt_unverified_fivemin_producer import (
    main as raw_lmp_lmp_rt_unverified_fivemin_producer_flow,
)

if __name__ == "__main__":
    serve(
        # Confirmed daily, 11am-12pm ET - shadow price data is 5-min-native but the feed
        # itself only refreshes once a day.
        sync_marginal_value_rt.to_deployment(
            name="marginal_value_rt", schedule=Cron("15 12 * * *", timezone="America/New_York"), 
        ),
        # Unconfirmed on PJM Data Miner - same settlement family as marginal_value_rt,
        # treated as daily.
        sync_marginal_value_da.to_deployment(
            name="marginal_value_da", schedule=Cron("20 12 * * *", timezone="America/New_York"), 
        ),
        # Unconfirmed - DA market clears once/day; treated as daily.
        sync_lmp_da_hourly.to_deployment(
            name="lmp_da_hourly", schedule=Cron("25 12 * * *", timezone="America/New_York"), 
        ),
        # Confirmed hourly, :15 past the hour - still a periodic batch post, not a push
        # feed, so still direct pull-and-upsert, just on an hourly schedule.
        sync_generation_by_fuel.to_deployment(
            name="generation_by_fuel", schedule=Cron("20 * * * *", timezone="America/New_York"), 
        ),
        # Confirmed daily, 12-2pm ET.
        sync_transmission_constraints_da.to_deployment(
            name="transmission_constraints_da", schedule=Cron("15 14 * * *", timezone="America/New_York"), 
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

        # Paused 2026-10-22 (decisions.md, "Real-time target moved to verified hourly,
        # nodal"): zonal-only, so it doesn't feed the GNN, and its full-feed pages every
        # 10 min took ~1/3 of the API key's 6 requests/min. Interval-based, not Cron -
        # streaming feed (~5min cadence, ~8min lag).
        raw_lmp_lmp_rt_unverified_fivemin_producer_flow.to_deployment(
            name="lmp_rt_5min_producer_unverified", interval=timedelta(minutes=10), paused=True,
        ),
        raw_lmp_lmp_rt_unverified_fivemin_consumer_flow.to_deployment(
            name="lmp_rt_5min_consumer_unverified", interval=timedelta(minutes=10), paused=True,
        ),
    )
