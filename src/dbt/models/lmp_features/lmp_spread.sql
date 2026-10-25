{{ config(
    materialized='incremental',
    unique_key=['time', 'pnode_id'],
    incremental_strategy='delete+insert'
) }}

-- the GNN target: verified RT minus DA at the same node-hour. verified only (is_verified),
-- re-read over 7 days because sync_rt_hrl_lmps_nodal overwrites a 7-day trailing window.

select
    r.time,
    r.pnode_id,
    r.lmp_total as rt_lmp,
    d.lmp_total as da_lmp,
    r.lmp_total - d.lmp_total as spread
from {{ ref('stg_rt_lmp_nodal') }} r
join {{ ref('stg_da_lmp_nodal') }} d
    on r.time = d.time and r.pnode_id = d.pnode_id
where r.is_verified
{% if is_incremental() %}
and r.time >= (select max(time) - interval '7 days' from {{ this }})
{% endif %}
