{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

with base as (
    select time, zone, demand_mw
    from {{ ref('stg_load') }}
    where source = 'pjm'

    {% if is_incremental() %}
    -- 8 days (168 hrs) of history being re-aquired
    and time >= (select max(time) - interval '8 days' from {{ this }})
    {% endif %}
)

select
    time,
    zone,
    demand_mw,
    lag(demand_mw, 1)   over w as demand_lag_1h,
    lag(demand_mw, 3)   over w as demand_lag_3h,
    lag(demand_mw, 24)  over w as demand_lag_24h,
    lag(demand_mw, 168) over w as demand_lag_168h,
    avg(demand_mw) over (
        partition by zone order by time
        range between interval '6 hours' preceding and interval '1 hour' preceding
    ) as demand_roll_6h,
    avg(demand_mw) over (
        partition by zone order by time
        range between interval '24 hours' preceding and and interval '1 hour' preceding
    ) as demand_roll_24h
from base
window w as (partition by zone order by time)
