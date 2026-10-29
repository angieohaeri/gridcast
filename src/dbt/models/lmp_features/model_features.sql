{{ config(materialized='view') }}

-- flat (time, pnode_id) read of every model input for consumers that want one frame (the
-- per-node reference model, serving). A view, so nothing is stored: each area's load forecast,
-- each zone's features and the system features are joined onto nodes at query time. Calendar
-- features are not here, they come from gridcast.features.

select
    b.*,
    n.forecast_area,
    lf.forecast_load_mw,
    z.temperature,
    z.precipitation,
    z.wind_speed,
    z.cloud_cover,
    z.hdd,
    z.cdd,
    z.commitment_count,
    z.commitment_mw,
    z.constraint_commitment_count,
    z.constraint_commitment_mw,
    z.border_interchange_prev_day,
    z.interface_util,
    s.da_constraint_count,
    s.da_shadow_total,
    s.da_shadow_min,
    s.rt_constraint_count_prev_day,
    s.rt_shadow_total_prev_day,
    s.rt_shadow_min_prev_day,
    s.rt_constraint_count_mean_7d,
    s.rt_shadow_total_mean_7d,
    s.da_constraint_count_prev_day,
    s.da_constraint_hours_prev_day,
    s.generation_mw_prev_day,
    s.gas_share_prev_day,
    s.coal_share_prev_day,
    s.nuclear_share_prev_day,
    s.wind_share_prev_day,
    s.solar_share_prev_day,
    s.self_scheduled_mw_prev_day,
    s.loss_ratio_prev_day,
    s.outage_mw_rto,
    s.outage_mw_west,
    s.outage_mw_other,
    s.gas_cost_per_mmbtu,
    s.apsouth_util,
    s.bcpep_util,
    s.bed_bla_util,
    s.central_util,
    s.east_util,
    s.west_util,
    s.net_interchange_prev_day
from {{ ref('baseline_features') }} b
join {{ ref('nodes') }} n on n.pnode_id = b.pnode_id
left join {{ ref('load_forecast_features') }} lf
    on lf.time = b.time and lf.forecast_area = n.forecast_area
left join {{ ref('zone_features') }} z
    on z.time = b.time and z.zone = n.zone_id
left join {{ ref('system_features') }} s on s.time = b.time
