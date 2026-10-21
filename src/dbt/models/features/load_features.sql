{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

with demand as (
    -- settled pjm zonal demand (hour-ending). zone filter lives here, not in stg_load:
    -- every EIA row is zone='RTO', a system total of a different grain that doesn't
    -- belong in a global model treating zone as a categorical feature.
    select time, zone, demand_mw
    from {{ ref('stg_load') }}
    where source = 'pjm'
      and zone in ('{{ var("in_scope_zones") | join("','") }}')

    {% if is_incremental() %}
    -- 15 days: 7-day buffer so demand_lag_168h and the trailing rolls have full
    -- history for every row in the 8-day reprocessing window below, plus the 8 days
    and time >= (select max(time) - interval '15 days' from {{ this }})
    {% endif %}
),

demand_asof as (
    -- demand value + trailing means AS OF each demand hour, with no lag applied yet;
    -- the 72h settlement lag is applied by the offset joins in the final select.
    select
        time,
        zone,
        demand_mw,
        avg(demand_mw) over (
            partition by zone order by time
            range between interval '6 hours' preceding and current row
        ) as roll_6h,
        avg(demand_mw) over (
            partition by zone order by time
            range between interval '24 hours' preceding and current row
        ) as roll_24h
    from demand
),

spine as (
    -- one row per zone-hour up to now, independent of whether demand has settled - this
    -- is what lets the model forecast from "now" rather than the settlement edge.
    -- demand_mw is NULL for hours past the edge (it is the target, never an input).
    -- demand_lag_1h/3h/24h are intentionally absent: pjm load settles ~2-3 days late, so
    -- nothing newer than 72h is knowable at the anchor (72h floor is conservative).
    -- seeded from a real demand hour so the series stays on the UTC :00 grid.
    select s.time, z.zone
    from generate_series(
        (select min(time) from demand),
        now(),
        interval '1 hour'
    ) as s(time)
    cross join (select distinct zone from demand) z

    {% if is_incremental() %}
    where s.time >= (select max(time) - interval '8 days' from {{ this }})
    {% endif %}
)

-- lags are offset joins on the hour, not lag(n) over rows, so a gap in settled demand
-- reads NULL instead of shifting onto the wrong hour (same pattern as the lmp offset
-- join in features.sql). lag_72h and both rolls come from the T-72h row, since the
-- rolls are trailing means as of T-72h ([T-78h, T-72h] and [T-96h, T-72h]).
select
    sp.time,
    sp.zone,
    d.demand_mw,
    l72.demand_mw  as demand_lag_72h,
    l168.demand_mw as demand_lag_168h,
    l72.roll_6h    as demand_roll_6h,
    l72.roll_24h   as demand_roll_24h
from spine sp
left join demand_asof d    on d.zone    = sp.zone and d.time    = sp.time
left join demand_asof l72  on l72.zone  = sp.zone and l72.time  = sp.time - interval '72 hours'
left join demand_asof l168 on l168.zone = sp.zone and l168.time = sp.time - interval '168 hours'
