{{ config(
    materialized='incremental',
    unique_key=['time', 'pnode_id'],
    incremental_strategy='delete+insert',
    post_hook="{{ ensure_time_pnode_index() }}"
) }}

-- the GNN target (verified RT minus DA at the same node-hour) and the trailing 7-day stats of
-- that spread per node, as of each hour (inclusive). baseline_features reads the stats at the
-- last verified day's hour, so no stat here looks past what is known at the 14:30 ET cutoff.
-- verified only (is_verified); range (not rows) so a missing hour shortens the window instead
-- of silently stretching it.
-- incremental: sync_lmp_rt overwrites a 7-day trailing window, so rows are
-- re-emitted for 7 days, computed over 14 so every emitted row has its full window.

with joined as (
    select
        r.time,
        r.pnode_id,
        r.lmp_total as rt_lmp,
        d.lmp_total as da_lmp,
        r.lmp_total - d.lmp_total as spread,
        r.congestion_price - d.congestion_price as congestion_spread
    from {{ ref('stg_lmp_rt') }} r
    join {{ ref('stg_lmp_da') }} d
        on r.time = d.time and r.pnode_id = d.pnode_id
    where r.is_verified
    {% if is_incremental() %}
    and r.time >= (select max(time) - interval '14 days' from {{ this }})
    {% endif %}
),

rolled as (
    select
        *,
        avg(spread) over w as spread_mean_7d,
        stddev_samp(spread) over w as spread_std_7d,
        avg((abs(spread) >= {{ var('lmp_spike_threshold') }})::int) over w as spike_share_7d,
        avg(congestion_spread) over w as congestion_spread_mean_7d
    from joined
    window w as (
        partition by pnode_id order by time
        range between interval '167 hours' preceding and current row
    )
)

select *
from rolled
{% if is_incremental() %}
where time >= (select max(time) - interval '7 days' from {{ this }})
{% endif %}
