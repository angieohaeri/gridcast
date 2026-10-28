{{ config(materialized='table') }}

-- system-wide congestion state per hour, the same for every node: how many transmission
-- constraints bound and how hard (shadow price, negative; the strongest is the min).
-- da_* are the target hour's own day-ahead constraints, known at the cutoff once the DA sync has
-- run. rt_* are the real-time constraints at the last verified day's hour, plus a trailing 7-day
-- mean ending there; the lag comes from the availability var like baseline_features, so for -1
-- (RT shadow prices through D-1) it is 48h.
-- hours with no binding constraint count as 0, but only up to the last day each feed has loaded:
-- later hours are null, so a late sync shows up as nulls instead of silent zeros.
-- rt shadow_price_sum adds 12 five-minute intervals, so it is divided by 12 to be an hourly mean.

{% set lag_hours = (1 - var('feature_availability_days')['marginal_value_rt']) * 24 %}

with hours as (
    select distinct time from {{ ref('stg_lmp_da_zonal') }}
),

loaded as (
    select
        (date_trunc('day', ((select max(time) from {{ ref('stg_marginal_value_rt') }}) - interval '1 hour') at time zone 'America/New_York') + interval '1 day') at time zone 'America/New_York' as rt_through,
        (date_trunc('day', ((select max(time) from {{ ref('stg_marginal_value_da') }}) - interval '1 hour') at time zone 'America/New_York') + interval '1 day') at time zone 'America/New_York' as da_through
),

da_hourly as (
    select
        h.time,
        case when h.time <= l.da_through then count(d.time) end as da_constraint_count,
        case when h.time <= l.da_through then coalesce(sum(d.shadow_price), 0) end as da_shadow_total,
        case when h.time <= l.da_through then coalesce(min(d.shadow_price), 0) end as da_shadow_min
    from hours h
    cross join loaded l
    left join {{ ref('stg_marginal_value_da') }} d on d.time = h.time
    group by h.time, l.da_through
),

rt_hourly as (
    select
        h.time,
        case when h.time <= l.rt_through then count(r.time) end as rt_constraint_count,
        case when h.time <= l.rt_through then coalesce(sum(r.shadow_price_sum), 0) / 12.0 end as rt_shadow_total,
        case when h.time <= l.rt_through then coalesce(min(r.shadow_price_min), 0) end as rt_shadow_min
    from hours h
    cross join loaded l
    left join {{ ref('stg_marginal_value_rt') }} r on r.time = h.time
    group by h.time, l.rt_through
),

rt_rolled as (
    select
        time,
        rt_constraint_count,
        rt_shadow_total,
        rt_shadow_min,
        avg(rt_constraint_count) over w as rt_constraint_count_mean_7d,
        avg(rt_shadow_total) over w as rt_shadow_total_mean_7d
    from rt_hourly
    window w as (order by time range between interval '167 hours' preceding and current row)
)

select
    h.time,
    d.da_constraint_count,
    d.da_shadow_total,
    d.da_shadow_min,
    r.rt_constraint_count as rt_constraint_count_last_day,
    r.rt_shadow_total as rt_shadow_total_last_day,
    r.rt_shadow_min as rt_shadow_min_last_day,
    r.rt_constraint_count_mean_7d,
    r.rt_shadow_total_mean_7d
from hours h
left join da_hourly d on d.time = h.time
left join rt_rolled r on r.time = h.time - interval '{{ lag_hours }} hours'
