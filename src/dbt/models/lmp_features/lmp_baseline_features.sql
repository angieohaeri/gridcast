{{ config(
    materialized='incremental',
    unique_key=['time', 'pnode_id'],
    incremental_strategy='delete+insert'
) }}

-- spine is DA: it exists for every target hour through D+1, while RT does not yet, so the
-- rows being forecast are present with spread null (like load_features' nullable target).
--
-- the baseline lag is derived from the availability var, not hardcoded: at the 14:30 ET
-- cutoff on D the latest verified RT day is D + rt_lmp_verified, and the target day is D+1.
-- for -2 that is 72h back, i.e. the same hour on the most recent complete verified day.

{% set lag_hours = (1 - var('feature_availability_days')['rt_lmp_verified']) * 24 %}

select
    d.time,
    d.pnode_id,
    d.lmp_total as da_lmp,
    d.congestion_price as da_congestion_price,
    d.marginal_loss_price as da_marginal_loss_price,
    l.spread as spread_last_verified_day,
    t.spread
from {{ ref('stg_da_lmp_nodal') }} d
left join {{ ref('lmp_spread') }} l
    on l.time = d.time - interval '{{ lag_hours }} hours' and l.pnode_id = d.pnode_id
left join {{ ref('lmp_spread') }} t
    on t.time = d.time and t.pnode_id = d.pnode_id
{% if is_incremental() %}
where d.time >= (select max(time) - interval '7 days' from {{ this }})
{% endif %}
