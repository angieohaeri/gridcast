"""Prefect deployments for the LMP pricing model (references/lmp-pricing-model/).

Kept separate from deployments.py - isolated from the working load-forecasting stack's
schedules, same as its own branch/schema/dbt target/MLflow naming (see decisions.md's
"Isolation" section). Run as its own `serve()` process, alongside (not instead of)
deployments.py's.

10 of the 11 raw_lmp sources are batch pulls, living together in raw_lmp_sync.py as one
@flow function per source. The 11th - lmp_rt_unverified_fivemin, the model's actual
real-time LMP target - is genuinely streaming, so it's a real Kafka producer/consumer
pair instead (src/producers/, src/consumers/), same shape as lmp_producer.py/
lmp_consumer.py.  on every deployment per decisions.md, until each has a
validated run.
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

        # Genuinely streaming (~5min cadence, ~8min lag, confirmed 2026-09-05) - the
        # model's real-time LMP target. Interval-based like weather_producer/consumer,
        # not Cron, same reasoning: this needs to keep up with a continuously-updating
        # feed, not hit a fixed daily post time.
        raw_lmp_lmp_rt_unverified_fivemin_producer_flow.to_deployment(
            name="lmp_rt_5min_producer_unverified", interval=timedelta(minutes=10), 
        ),
        raw_lmp_lmp_rt_unverified_fivemin_consumer_flow.to_deployment(
            name="lmp_rt_5min_consumer_unverified", interval=timedelta(minutes=10), 
        ),
    )
