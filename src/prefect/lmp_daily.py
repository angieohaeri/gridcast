"""Daily LMP pipeline as two sequential flows, replacing a cron entry per sync and dbt build.

lmp_morning: verified RT prices -> dbt lmp_history, then the remaining RT-side syncs.
lmp_afternoon: DA prices, load and weather forecasts -> dbt lmp_serving before the 14:30 ET
forecast, then the remaining DA-side syncs.

Critical syncs are called plainly, so a failure stops the flow before the dbt build that needs
them. Optional syncs run with return_state=True: a failure is logged and the flow continues.
Running them one after another also keeps PJM's shared 6 requests/min.
"""

from dbt_build import main as dbt_build
from loguru import logger
from raw_lmp_sync import (
    sync_forecasted_generation_outages,
    sync_generation_ehv_losses,
    sync_interface_flows_da,
    sync_lmp_da,
    sync_lmp_da_zonal,
    sync_lmp_rt,
    sync_load_forecast,
    sync_load_forecast_live,
    sync_marginal_value_da,
    sync_marginal_value_rt,
    sync_scheduled_generation,
    sync_scheduled_interchange,
    sync_transmission_constraints_da,
    sync_weather_forecast,
)

from prefect import flow


def _run_optional(*syncs) -> None:
    for sync in syncs:
        if sync(return_state=True).is_failed():
            logger.warning(f"{sync.name} failed, continuing")


@flow(name="lmp_morning", log_prints=True)
def lmp_morning():
    sync_lmp_rt()
    dbt_build(selector="lmp_history")
    _run_optional(
        sync_marginal_value_rt,
        sync_scheduled_generation,
        sync_generation_ehv_losses,
        sync_forecasted_generation_outages,
        sync_scheduled_interchange,
    )


@flow(name="lmp_afternoon", log_prints=True)
def lmp_afternoon():
    sync_lmp_da()
    sync_load_forecast()
    sync_load_forecast_live()
    _run_optional(sync_weather_forecast, sync_marginal_value_da, sync_interface_flows_da)
    dbt_build(selector="lmp_serving")
    _run_optional(sync_lmp_da_zonal, sync_transmission_constraints_da)
