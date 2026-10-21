{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

-- inst_load carries no settlement lag (unlike demand_mw's ~2-3 days), so it can supply
-- near-term demand-proxy lags that load_features.sql deliberately lacks at 1h/3h/24h.
-- This is a proxy feature, not a substitute for demand_mw - see references/decisions.md.

with hourly as (
    -- time_bucket labels an hour by its start; load/lmp use Hour Ending, so +1h aligns
    -- them: readings from 14:00-14:59 describe the hour labelled 15:00 in stg_load -
    -- same convention as weather_features.sql.
    select
        time_bucket('1 hour', time) + interval '1 hour' as time,
        zone,
        avg(instantaneous_load_mw) as inst_load_mw
    from {{ ref('stg_inst_load') }}
    where zone in ('{{ var("in_scope_zones") | join("','") }}')

    {% if is_incremental() %}
    -- 2 day pull: 1 day buffer so inst_load_lag_24h has full history for every row in
    -- the 1-day reprocessing window below, plus the day itself
    and time >= (select max(time) - interval '2 days' from {{ this }})
    {% endif %}

    group by 1, 2
)

-- lags are offset joins on the hour, not lag(n) over rows: inst_load and weather drop
-- together on a poller outage, and a positional lag would then slide onto the wrong
-- hour across the gap. an offset join reads NULL for a missing hour instead.
select
    h.time,
    h.zone,
    h.inst_load_mw,
    l1.inst_load_mw  as inst_load_lag_1h,
    l3.inst_load_mw  as inst_load_lag_3h,
    l24.inst_load_mw as inst_load_lag_24h
from hourly h
left join hourly l1  on l1.zone = h.zone and l1.time = h.time - interval '1 hour'
left join hourly l3  on l3.zone = h.zone and l3.time = h.time - interval '3 hours'
left join hourly l24 on l24.zone = h.zone and l24.time = h.time - interval '24 hours'
{% if is_incremental() %}
-- drop the 1-day lookback buffer from the output - it only exists to give the
-- reprocessing window's earliest rows full history, not to be written itself
where h.time >= (select max(time) - interval '1 day' from {{ this }})
{% endif %}
