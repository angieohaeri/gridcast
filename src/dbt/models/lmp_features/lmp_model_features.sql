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
    w.cdd
from {{ ref('lmp_baseline_features') }} b
join {{ ref('lmp_node_areas') }} na on na.pnode_id = b.pnode_id
join {{ ref('stg_pnode_topology') }} n on n.pnode_id = b.pnode_id
left join {{ ref('lmp_load_forecast_features') }} lf
    on lf.time = b.time and lf.forecast_area = na.forecast_area
left join {{ ref('lmp_weather_forecast_features') }} w
    on w.time = b.time and w.zone = n.zone_id
