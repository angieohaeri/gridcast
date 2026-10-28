{{ config(materialized='view') }}

-- flat (time, pnode_id) read of every model input for consumers that want one frame (the
-- per-node reference model, serving). A view, so nothing is stored: each area's forecast and
-- each zone's weather are joined onto nodes at query time. Calendar features are not here,
-- they come from gridcast.features.

select
    b.*,
    na.forecast_area,
    lf.forecast_load_mw,
    w.temperature,
    w.precipitation,
    w.wind_speed,
    w.cloud_cover,
    w.hdd,
    w.cdd,
    sp.da_constraint_count,
    sp.da_shadow_total,
    sp.da_shadow_min,
    sp.rt_constraint_count_last_day,
    sp.rt_shadow_total_last_day,
    sp.rt_shadow_min_last_day,
    sp.rt_constraint_count_mean_7d,
    sp.rt_shadow_total_mean_7d
from {{ ref('baseline_features') }} b
join {{ ref('node_areas') }} na on na.pnode_id = b.pnode_id
join {{ ref('stg_pnode_topology') }} n on n.pnode_id = b.pnode_id
left join {{ ref('load_forecast_features') }} lf
    on lf.time = b.time and lf.forecast_area = na.forecast_area
left join {{ ref('weather_forecast_features') }} w
    on w.time = b.time and w.zone = n.zone_id
left join {{ ref('shadow_price_features') }} sp on sp.time = b.time
