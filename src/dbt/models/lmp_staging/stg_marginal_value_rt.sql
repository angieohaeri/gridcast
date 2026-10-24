-- 5-min intervals rolled up to Hour Ending UTC (interval beginning 10:00-10:55 -> 11:00),
-- matching stg_rt_lmp_nodal.time. A constraint absent from an interval is not binding, so
-- binding_intervals is kept next to the sum/min rather than averaging over present rows.
-- shadow_price is always negative in this feed (checked: min -5000, max -0.01), so the
-- strongest constraint in the hour is the min, not the max.

select
    date_trunc('hour', datetime_beginning_utc) + interval '1 hour' as time,
    monitored_facility,
    contingency_facility,
    sum(shadow_price) as shadow_price_sum,
    min(shadow_price) as shadow_price_min,
    count(*) as binding_intervals
from {{ source('raw_lmp', 'marginal_value_rt') }}
group by 1, 2, 3
