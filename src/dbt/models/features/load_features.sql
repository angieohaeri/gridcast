{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

with base as (
    -- zone filter lives here, not in stg_load: every EIA row is zone='RTO', so
    -- filtering at staging would drop that source entirely. RTO is a system total,
    -- a different grain from the zonal rows - it doesn't belong in a global model
    -- that treats zone as a categorical feature.
    select time, zone, demand_mw
    from {{ ref('stg_load') }}
    where source = 'pjm'
      and zone in ('{{ var("in_scope_zones") | join("','") }}')

    {% if is_incremental() %}
    -- 15 days pulled: 7 days of buffer so demand_lag_168h has full history for
    -- every row in the 8-day reprocessing window below, plus the 8 days themselves
    and time >= (select max(time) - interval '15 days' from {{ this }})
    {% endif %}
),

lagged as (
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
            range between interval '24 hours' preceding and interval '1 hour' preceding
        ) as demand_roll_24h
    from base
    window w as (partition by zone order by time)
)

select *
from lagged
{% if is_incremental() %}
-- drop the 7-day lookback buffer from the output - it only exists to give the
-- reprocessing window's earliest rows full history, not to be written itself
where time >= (select max(time) - interval '8 days' from {{ this }})
{% endif %}
