-- which load forecast area each pnode takes. aggregates without a zone (hubs, interfaces,
-- PJM-RTO, OVEC) take the RTO total.

select
    n.pnode_id,
    coalesce(a.forecast_area, 'RTO') as forecast_area
from {{ ref('stg_pnode_topology') }} n
left join {{ ref('load_forecast_areas') }} a on a.zone_id = n.zone_id
