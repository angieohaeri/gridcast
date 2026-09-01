{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

-- left join from load_features (the target variable) so a missing lmp/weather
-- 3 day reprocessing window (like lmp_features) to catch

-- lmp is joined 48h behind the target hour, not contemporaneously: rt_hrl_lmps
-- settles over ~2 days, so the price for hour t is not knowable at forecast time.
-- offset join rather than lag() so missing zone-hours shift nothing.

select
    l.time,
    l.zone,
    l.demand_mw,
    l.demand_lag_72h,
    l.demand_lag_168h,
    l.demand_roll_6h,
    l.demand_roll_24h,
    m.lmp_total as lmp_total_lag_48h,
    m.congestion_price as congestion_price_lag_48h,
    m.marginal_loss_price as marginal_loss_price_lag_48h,
    w.temperature,
    w.precipitation,
    w.wind_speed,
    w.cloud_cover,
    w.observation_count,
    i.inst_load_mw,
    i.inst_load_lag_1h,
    i.inst_load_lag_3h,
    i.inst_load_lag_24h
from {{ ref('load_features') }} l
left join {{ ref('lmp_features') }} m
    on m.time = l.time - interval '48 hours' and l.zone = m.zone
left join {{ ref('weather_features') }} w
    on l.time = w.time and l.zone = w.zone
left join {{ ref('inst_load_features') }} i
    on l.time = i.time and l.zone = i.zone

{% if is_incremental() %}
where l.time >= (select max(time) - interval '3 days' from {{ this }})
{% endif %}