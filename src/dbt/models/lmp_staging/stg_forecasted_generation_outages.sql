select
    forecast_execution_date,
    forecast_date,
    outage_mw_rto,
    outage_mw_west,
    outage_mw_other
from {{ source('raw_lmp', 'forecasted_generation_outages') }}
