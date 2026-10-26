-- the forecast PJM had issued by the cutoff, per target hour and area. the cutoff is
-- forecast_cutoff_et on the ET day before the target hour's day (hour-ending 00:00 belongs
-- to the previous day), and distinct on keeps the latest evaluation at or before it.
-- point-in-time on purpose: a later evaluation of the same hour would be a revision the
-- model could not have seen.

with cutoffs as (
    select
        evaluated_at,
        time,
        forecast_area,
        forecast_load_mw,
        (date_trunc('day', (time - interval '1 hour') at time zone 'America/New_York')
            - interval '1 day' + interval '{{ var("forecast_cutoff_et") }}')
            at time zone 'America/New_York' as cutoff
    from {{ ref('stg_load_forecast') }}
)

select distinct on (time, forecast_area)
    time,
    forecast_area,
    forecast_load_mw,
    evaluated_at
from cutoffs
where evaluated_at <= cutoff
order by time, forecast_area, evaluated_at desc
