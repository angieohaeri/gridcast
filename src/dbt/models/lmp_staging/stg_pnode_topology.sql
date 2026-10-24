select
    pnode_id,
    pnode_name,
    pnode_subtype,
    role,
    zone,
    zone_id,
    substation,
    sub_key,
    voltage_kv,
    lat,
    lon,
    conflict
from {{ source('raw_lmp', 'pnode_topology_nodes') }}
