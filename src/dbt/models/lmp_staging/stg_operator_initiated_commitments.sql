-- event-level, irregular timestamps; hourly alignment happens in the feature models
select
    datetime_beginning_utc as event_time,
    zone,
    economic_max_mw,
    reason
from {{ source('raw_lmp', 'operator_initiated_commitments') }}
