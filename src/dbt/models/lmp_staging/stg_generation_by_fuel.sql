-- raw time is Hour Beginning (checked against public.load); shifted to Hour Ending to match the LMP tables
select
    time + interval '1 hour' as time,
    fuel_type,
    generation_mw
from {{ source('raw_lmp', 'generation_by_fuel') }}
