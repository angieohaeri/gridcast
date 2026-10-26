{{ config(
    materialized='incremental',
    unique_key=['time', 'pnode_id'],
    incremental_strategy='delete+insert',
    post_hook="{{ ensure_time_pnode_index() }}"
) }}

-- spine is DA: it exists for every target hour through D+1, while RT does not yet, so the
-- rows being forecast are present with spread null (like load_features' nullable target).
--
-- the baseline lag is derived from the availability var, not hardcoded: at the 14:30 ET
-- cutoff on D the latest verified RT day is D + rt_lmp_verified, and the target day is D+1.
-- for -2 that is 72h back, i.e. the same hour on the most recent complete verified day;
-- the *_7d columns are trailing windows ending at that same hour.

{% set lag_hours = (1 - var('feature_availability_days')['rt_lmp_verified']) * 24 %}

-- incremental window as a literal: with a subquery the planner can't see it is ~7 days of
-- 26M rows, guesses millions, and merge-joins (sorting) the whole history table.
{% if is_incremental() and execute %}
{% set since = run_query("select max(time) - interval '7 days' from " ~ this).columns[0].values()[0] %}
{% endif %}

select
    d.time,
    d.pnode_id,
    d.lmp_total as da_lmp,
    d.congestion_price as da_congestion_price,
    d.marginal_loss_price as da_marginal_loss_price,
    h.spread as spread_last_verified_day,
    h.congestion_spread as congestion_spread_last_verified_day,
    h.spread_mean_7d,
    h.spread_std_7d,
    h.spike_share_7d,
    h.congestion_spread_mean_7d,
    t.spread
from {{ ref('stg_da_lmp_nodal') }} d
left join {{ ref('lmp_spread_history') }} h
    on h.time = d.time - interval '{{ lag_hours }} hours' and h.pnode_id = d.pnode_id
    {% if is_incremental() %}
    and h.time >= timestamptz '{{ since }}' - interval '{{ lag_hours }} hours'
    {% endif %}
left join {{ ref('lmp_spread') }} t
    on t.time = d.time and t.pnode_id = d.pnode_id
    {% if is_incremental() %}
    and t.time >= timestamptz '{{ since }}'
    {% endif %}
{% if is_incremental() %}
where d.time >= timestamptz '{{ since }}'
{% endif %}
