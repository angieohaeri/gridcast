{{ config(materialized='table') }}

-- system-wide state per target hour, the same for every node. Each source is read as of the
-- 14:30 ET cutoff: hourly sources at the same hour on their last available day (the lag comes
-- from feature_availability_days like baseline_features, so -1 is 48h back), monthly sources by
-- the month of the cutoff day.
-- shadow prices: how many transmission constraints bound and how hard (negative; the strongest
-- is the min). da_* are the target hour's own day-ahead constraints; rt_* are the real-time ones
-- at the lagged hour plus a trailing 7-day mean ending there. rt shadow_price_sum adds 12
-- five-minute intervals, so it is divided by 12 to be an hourly mean.
-- counts are 0 when nothing bound, but only up to the last day each feed has loaded: later hours
-- are null, so a late sync shows up as nulls instead of silent zeros.

{% set availability = var('feature_availability_days') %}
{% set rt_lag = (1 - availability['marginal_value_rt']) * 24 %}
{% set constraints_lag = (1 - availability['transmission_constraints_da']) * 24 %}
{% set fuel_lag = (1 - availability['generation_by_fuel']) * 24 %}
{% set scheduled_lag = (1 - availability['scheduled_generation']) * 24 %}
{% set losses_lag = (1 - availability['generation_ehv_losses']) * 24 %}

with hours as (
    select distinct time from {{ source('raw_lmp', 'lmp_da_zonal') }}
),

loaded as (
    select
        (date_trunc('day', ((select max(time) from {{ ref('stg_marginal_value_rt') }}) - interval '1 hour') at time zone 'America/New_York') + interval '1 day') at time zone 'America/New_York' as rt_through,
        (date_trunc('day', ((select max(time) from {{ ref('stg_marginal_value_da') }}) - interval '1 hour') at time zone 'America/New_York') + interval '1 day') at time zone 'America/New_York' as da_through,
        (select max(datetime_ending_utc) from {{ source('raw_lmp', 'transmission_constraints_da') }}) as constraints_through
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
),

-- day-ahead constraints binding in each hour ending (an interval covers the hours ending after
-- its start through its end), with how long each one bound that day
constraints_by_hour as (
    select hour_ending as time, count(*) as constraint_count, avg(c.duration_hours) as constraint_hours
    from {{ source('raw_lmp', 'transmission_constraints_da') }} c
    cross join lateral generate_series(c.datetime_beginning_utc + interval '1 hour', c.datetime_ending_utc, interval '1 hour') as hour_ending
    group by hour_ending
),

constraints_hourly as (
    select
        h.time,
        case when h.time <= l.constraints_through then coalesce(ch.constraint_count, 0) end as constraint_count,
        case when h.time <= l.constraints_through then coalesce(ch.constraint_hours, 0) end as constraint_hours
    from hours h
    cross join loaded l
    left join constraints_by_hour ch on ch.time = h.time
),

fuel as (
    select
        time,
        sum(generation_mw) as generation_mw,
        sum(generation_mw) filter (where fuel_type = 'Gas') / nullif(sum(generation_mw), 0) as gas_share,
        sum(generation_mw) filter (where fuel_type = 'Coal') / nullif(sum(generation_mw), 0) as coal_share,
        sum(generation_mw) filter (where fuel_type = 'Nuclear') / nullif(sum(generation_mw), 0) as nuclear_share,
        sum(generation_mw) filter (where fuel_type = 'Wind') / nullif(sum(generation_mw), 0) as wind_share,
        sum(generation_mw) filter (where fuel_type = 'Solar') / nullif(sum(generation_mw), 0) as solar_share
    from {{ ref('stg_generation_by_fuel') }}
    group by time
),

-- outage forecast for the target day, from the latest forecast executed by the cutoff
outages as (
    select distinct on (h.time)
        h.time,
        o.outage_mw_rto,
        o.outage_mw_west,
        o.outage_mw_other
    from hours h
    join {{ source('raw_lmp', 'forecasted_generation_outages') }} o
        on o.forecast_date = ((h.time - interval '1 hour') at time zone 'America/New_York')::date
        and o.forecast_execution_date <= {{ forecast_cutoff('h.time') }}
    order by h.time, o.forecast_execution_date desc
),

gas_cost as (
    select period, avg(cost_per_mmbtu) as gas_cost_per_mmbtu
    from {{ source('raw_lmp', 'natural_gas_fuel_cost') }}
    where location in ('{{ var("pjm_states") | join("', '") }}') and cost_per_mmbtu is not null
    group by period
)

select
    h.time,
    d.da_constraint_count,
    d.da_shadow_total,
    d.da_shadow_min,
    r.rt_constraint_count as rt_constraint_count_prev_day,
    r.rt_shadow_total as rt_shadow_total_prev_day,
    r.rt_shadow_min as rt_shadow_min_prev_day,
    r.rt_constraint_count_mean_7d,
    r.rt_shadow_total_mean_7d,
    c.constraint_count as da_constraint_count_prev_day,
    c.constraint_hours as da_constraint_hours_prev_day,
    f.generation_mw as generation_mw_prev_day,
    f.gas_share as gas_share_prev_day,
    f.coal_share as coal_share_prev_day,
    f.nuclear_share as nuclear_share_prev_day,
    f.wind_share as wind_share_prev_day,
    f.solar_share as solar_share_prev_day,
    s.self_ecomax as self_scheduled_mw_prev_day,
    ls.total_losses / nullif(ls.total_gen, 0) as loss_ratio_prev_day,
    o.outage_mw_rto,
    o.outage_mw_west,
    o.outage_mw_other,
    g.gas_cost_per_mmbtu
from hours h
left join da_hourly d on d.time = h.time
left join rt_rolled r on r.time = h.time - interval '{{ rt_lag }} hours'
left join constraints_hourly c on c.time = h.time - interval '{{ constraints_lag }} hours'
left join fuel f on f.time = h.time - interval '{{ fuel_lag }} hours'
left join {{ ref('stg_scheduled_generation') }} s on s.time = h.time - interval '{{ scheduled_lag }} hours'
left join {{ ref('stg_generation_ehv_losses') }} ls on ls.time = h.time - interval '{{ losses_lag }} hours'
left join outages o on o.time = h.time
left join gas_cost g
    on g.period = date_trunc('month', {{ forecast_cutoff('h.time') }} at time zone 'America/New_York')::date
        - interval '{{ var("natural_gas_fuel_cost_lag_months") }} months'
