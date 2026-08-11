{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

-- left join from load_features (the target variable) so a missing lmp/weather
-- 3 day reprocessing window (like lmp_features) to catch 

select
    l.time,
    l.zone,
    l.demand_mw,
    l.demand_lag_1h,
    l.demand_lag_3h,
    l.demand_lag_24h,
    l.demand_lag_168h,
    l.demand_roll_6h,
    l.demand_roll_24h,
    m.lmp_total,
    m.congestion_price,
    m.marginal_loss_price,
    w.temperature,
    w.precipitation,
    w.wind_speed,
    w.cloud_cover,
    w.observation_count
from {{ ref('load_features') }} l
left join {{ ref('lmp_features') }} m
    on l.time = m.time and l.zone = m.zone
left join {{ ref('weather_features') }} w
    on l.time = w.time and l.zone = w.zone

{% if is_incremental() %}
where l.time >= (select max(time) - interval '3 days' from {{ this }})
{% endif %}