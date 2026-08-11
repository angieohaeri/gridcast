{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

-- rt_hrl_lmps is already hourly and the producer tracks one representative node per zone,
-- so this is a grain declaration (zone-hour) rather than a reduction. Aggregating keeps
-- the model correct if PJM ever returns more than one node for a zone.

select
    time,
    zone,
    avg(lmp_total) as lmp_total,
    avg(congestion_price) as congestion_price,
    avg(marginal_loss_price) as marginal_loss_price
from {{ ref('stg_lmp') }}

{% if is_incremental() %}
where time >= (select max(time) - interval '3 days' from {{ this }})
{% endif %}

group by 1, 2
