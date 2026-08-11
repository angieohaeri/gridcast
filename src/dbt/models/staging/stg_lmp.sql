{{ config(
    materialized='incremental',
    unique_key=['time', 'pnode_id'],
    incremental_strategy='delete+insert'
) }}

select
    time,
    pnode_id,
    pnode_name,
    zone,
    lmp as lmp_total,
    congestion_price,
    marginal_loss_price
from {{ source('raw', 'lmp') }}
where zone in ('{{ var("in_scope_zones") | join("','") }}')

{% if is_incremental() %}
and time >= (select max(time) - interval '2 days' from {{ this }})
{% endif %}
