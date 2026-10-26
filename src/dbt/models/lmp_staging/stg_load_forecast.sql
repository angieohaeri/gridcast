select
    evaluated_at,
    time,
    forecast_area,
    forecast_load_mw
from {{ source('raw_lmp', 'load_forecast') }}
