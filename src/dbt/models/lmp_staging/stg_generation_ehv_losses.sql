-- raw time is Hour Beginning (checked against public.load); shifted to Hour Ending to match the LMP tables
select
    time + interval '1 hour' as time,
    total_gen,
    total_losses
from {{ source('raw_lmp', 'generation_ehv_losses') }}
