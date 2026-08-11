{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

-- Open-Meteo is polled ~3x/hour and reports instantaneous state, so observations are
-- collapsed to hourly. `precipitation` is the preceding hour's total on every poll, so
-- the three readings overlap - averaged, not summed.
--
-- time_bucket labels an hour by its start; load/lmp use Hour Ending, so +1h aligns them:
-- readings from 14:00-14:59 describe the hour labelled 15:00 in `stg_load`.

select
    time_bucket('1 hour', time) + interval '1 hour' as time,
    zone,
    avg(temperature) as temperature,
    avg(precipitation) as precipitation,
    avg(wind_speed) as wind_speed,
    avg(cloud_cover) as cloud_cover,
    count(*) as observation_count
from {{ ref('stg_weather') }}

{% if is_incremental() %}
where time >= (select max(time) - interval '1 day' from {{ this }})
{% endif %}

group by 1, 2
