select
    time,
    zone,
    lmp as lmp_total,
    congestion_price,
    marginal_loss_price
from {{ source('raw_lmp', 'lmp_da_zonal') }}
