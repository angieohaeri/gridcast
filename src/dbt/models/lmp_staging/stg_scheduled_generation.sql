-- raw time is Hour Beginning (checked against public.load); shifted to Hour Ending to match the LMP tables
select
    time + interval '1 hour' as time,
    rt_ecomax,
    self_ecomax
from {{ source('raw_lmp', 'scheduled_generation') }}
