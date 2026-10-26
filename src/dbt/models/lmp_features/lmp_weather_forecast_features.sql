-- the weather forecast available at the cutoff, per target hour and zone. a live snapshot
-- taken at or before forecast_cutoff_et on the ET day before the target day wins (what
-- serving sees); otherwise the previous_day1 forecast stands in (history, back to 2024).
-- previous_day1 is the forecast ~24h before the hour; at the cutoff the true lead is 10-34h,
-- so evening hours of the target day are a little fresher in training than in serving.

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
        (date_trunc('day', (time - interval '1 hour') at time zone 'America/New_York')
            - interval '1 day' + interval '{{ var("forecast_cutoff_et") }}')
            at time zone 'America/New_York' as cutoff
    from {{ ref('stg_weather_forecast') }}
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
)

-- base 18.33C (65F) is the utility HDD/CDD reference, same as gridcast.features
select
    time,
    zone,
    temperature,
    precipitation,
    wind_speed,
    cloud_cover,
    greatest(18.33 - temperature, 0) as hdd,
    greatest(temperature - 18.33, 0) as cdd,
    source
from chosen
