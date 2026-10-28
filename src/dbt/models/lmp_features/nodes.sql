-- the GNN node set and every per-node attribute the model reads: zone_id is the weather and
-- commitments zone, forecast_area the load forecast area. aggregates without a zone (hubs,
-- interfaces, PJM-RTO, OVEC) take the RTO total.

select
    n.pnode_id,
    n.pnode_name,
    n.pnode_subtype,
    n.role,
    n.zone,
    n.zone_id,
    n.substation,
    n.sub_key,
    n.voltage_kv,
    n.lat,
    n.lon,
    n.conflict,
    coalesce(a.forecast_area, 'RTO') as forecast_area
from {{ source('raw_lmp', 'pnode_topology_nodes') }} n
left join {{ ref('load_forecast_areas') }} a on a.zone_id = n.zone_id
