select
    time,
    zone,
    source,
    issued_at,
    temperature,
    precipitation,
    wind_speed,
    cloud_cover
from {{ source('raw_lmp', 'weather_forecast') }}
