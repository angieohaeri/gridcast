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
    -- demand_lag_1h/3h/24h dropped: PJM zonal load settles ~2-3 days after the
    -- fact (see references/decisions.md), so anything newer than 72h is not
    -- knowable at "now" regardless of forecast horizon - staleness depends on
    -- now vs. settlement lag, not on how far ahead the target is. 72h floor is
    -- conservative (measured lag looks closer to 48h).
    select
        time,
        zone,
        demand_mw,
        lag(demand_mw, 72)  over w as demand_lag_72h,
        lag(demand_mw, 168) over w as demand_lag_168h,
        avg(demand_mw) over (
            partition by zone order by time
            range between interval '78 hours' preceding and interval '72 hours' preceding
        ) as demand_roll_6h,
        avg(demand_mw) over (
            partition by zone order by time
            range between interval '96 hours' preceding and interval '72 hours' preceding
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
