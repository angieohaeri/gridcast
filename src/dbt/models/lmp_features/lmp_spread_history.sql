{{ config(
    materialized='incremental',
    unique_key=['time', 'pnode_id'],
    incremental_strategy='delete+insert',
    post_hook="{{ ensure_time_pnode_index() }}"
) }}

-- trailing 7-day stats of the verified spread per node, as of each hour (inclusive).
-- lmp_baseline_features joins this at the last verified day's hour, so no stat here
-- looks past what is known at the 14:30 ET cutoff. range (not rows) so a missing hour
-- shortens the window instead of silently stretching it.
-- incremental: rows are re-emitted for 7 days, computed over 14 so every emitted row has
-- its full window.

with rolled as (
    select
        time,
        pnode_id,
        spread,
        congestion_spread,
        avg(spread) over w as spread_mean_7d,
        stddev_samp(spread) over w as spread_std_7d,
        avg((abs(spread) >= {{ var('lmp_spike_threshold') }})::int) over w as spike_share_7d,
        avg(congestion_spread) over w as congestion_spread_mean_7d
    from {{ ref('lmp_spread') }}
    {% if is_incremental() %}
    where time >= (select max(time) - interval '14 days' from {{ this }})
    {% endif %}
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
