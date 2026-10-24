select
    time,
    pnode_id,
    system_energy_price,
    lmp as lmp_total,
    congestion_price,
    marginal_loss_price,
    is_verified
from {{ source('raw_lmp', 'rt_hrl_lmps_nodal') }}
