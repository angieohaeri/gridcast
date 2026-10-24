-- already hourly; datetime_ending_utc is the Hour Ending, matching stg_da_lmp_nodal.time
select
    datetime_ending_utc as time,
    monitored_facility,
    contingency_facility,
    shadow_price
from {{ source('raw_lmp', 'marginal_value_da') }}
