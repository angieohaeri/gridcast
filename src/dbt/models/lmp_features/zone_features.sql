-- per target hour and zone (zone_id code), as of the 14:30 ET cutoff on the day before.
-- weather: a live snapshot taken at or before the cutoff wins (what serving sees); otherwise the
-- previous_day1 forecast stands in (history, back to 2024). previous_day1 is the forecast ~24h
-- before the hour; at the cutoff the true lead is 10-34h, so evening hours of the target day are
-- a little fresher in training than in serving.
-- commitments: operator-initiated commitments in the zone over the latest month posted by the
-- cutoff (month M posts on commitments_posting_day of M+1). 0 when the zone had none; null for
-- months not loaded yet, so a late sync shows up as nulls instead of silent zeros.
-- border interchange: net scheduled flow into PJM over the tie lines landing in the zone
-- (tie_line_zones; a tie touching several zones counts in full in each) at the last available
-- day's hour. 0 for zones with no tie line; null outside the loaded hours.
-- interface_util: the target hour's highest day-ahead flow / limit among the transfer interfaces
-- whose lines end in the zone (interface_zones), 1 = at its limit. Max, not mean: the interface
-- closest to binding is what moves prices, and APSOUTH and BED-BLA move together. 0 for zones
-- with no interface; null outside the loaded hours.

{% set interchange_lag = (1 - var('feature_availability_days')['scheduled_interchange']) * 24 %}

with sources as (
    select
        time,
        zone,
        temperature,
        precipitation,
        wind_speed,
        cloud_cover,
        source,
        issued_at,
        {{ forecast_cutoff() }} as cutoff
    from {{ source('raw_lmp', 'weather_forecast') }}
),

eligible as (
    select *, case source when 'live' then 1 else 2 end as priority
    from sources
    where source = 'previous_day1' or issued_at <= cutoff
),

chosen as (
    select distinct on (time, zone) *
    from eligible
    order by time, zone, priority, issued_at desc
),

-- the latest commitments month posted by each row's cutoff
posted as (
    select
        *,
        (date_trunc('month', cutoff at time zone 'America/New_York')
            - case when extract(day from cutoff at time zone 'America/New_York') >= {{ var('commitments_posting_day') }}
                then interval '1 month' else interval '2 months' end)::date as commitment_month
    from chosen
),

commitments as (
    select
        zone,
        date_trunc('month', datetime_beginning_utc at time zone 'America/New_York')::date as month,
        count(*) as commitment_count,
        sum(economic_max_mw) as commitment_mw,
        count(*) filter (where reason = 'Constraint Management') as constraint_commitment_count,
        coalesce(sum(economic_max_mw) filter (where reason = 'Constraint Management'), 0) as constraint_commitment_mw
    from {{ source('raw_lmp', 'operator_initiated_commitments') }}
    group by 1, 2
),

border_interchange as (
    select s.time, t.zone_id as zone, sum(s.net_schedule_mw) as border_interchange
    from {{ source('raw_lmp', 'scheduled_interchange') }} s
    join {{ ref('tie_line_zones') }} t on t.tie_line = s.tie_line
    group by 1, 2
),

interchange_loaded as (
    select min(time) as first_time, max(time) as last_time
    from {{ source('raw_lmp', 'scheduled_interchange') }}
),

interface_util as (
    select f.time, iz.zone_id as zone, max(f.flow_mw / f.limit_mw) as interface_util
    from {{ source('raw_lmp', 'interface_flows_da') }} f
    join {{ ref('interface_zones') }} iz on iz.interface = f.interface
    group by 1, 2
),

interfaces_loaded as (
    select min(time) as first_time, max(time) as last_time
    from {{ source('raw_lmp', 'interface_flows_da') }}
),

commitments_through as (
    select date_trunc('month', max(datetime_beginning_utc) at time zone 'America/New_York')::date as month
    from {{ source('raw_lmp', 'operator_initiated_commitments') }}
)

-- base 18.33C (65F) is the utility HDD/CDD reference, same as gridcast.features
select
    p.time,
    p.zone,
    p.temperature,
    p.precipitation,
    p.wind_speed,
    p.cloud_cover,
    greatest(18.33 - p.temperature, 0) as hdd,
    greatest(p.temperature - 18.33, 0) as cdd,
    p.source as weather_source,
    case when p.commitment_month <= t.month then coalesce(c.commitment_count, 0) end as commitment_count,
    case when p.commitment_month <= t.month then coalesce(c.commitment_mw, 0) end as commitment_mw,
    case when p.commitment_month <= t.month then coalesce(c.constraint_commitment_count, 0) end as constraint_commitment_count,
    case when p.commitment_month <= t.month then coalesce(c.constraint_commitment_mw, 0) end as constraint_commitment_mw,
    case when p.time - interval '{{ interchange_lag }} hours' between il.first_time and il.last_time
        then coalesce(b.border_interchange, 0) end as border_interchange_prev_day,
    case when p.time between fl.first_time and fl.last_time then coalesce(u.interface_util, 0) end as interface_util
from posted p
cross join commitments_through t
cross join interchange_loaded il
cross join interfaces_loaded fl
left join commitments c on c.zone = p.zone and c.month = p.commitment_month
left join border_interchange b on b.zone = p.zone and b.time = p.time - interval '{{ interchange_lag }} hours'
left join interface_util u on u.zone = p.zone and u.time = p.time
