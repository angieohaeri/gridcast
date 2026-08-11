{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

select
    time,
    zone,
    temperature,
    precipitation,
    wind_speed,
    cloud_cover
from {{ source('raw', 'weather') }}
where zone in ('{{ var("in_scope_zones") | join("','") }}')

{% if is_incremental() %}
and time >= (select max(time) - interval '1 day' from {{ this }})
{% endif %}
