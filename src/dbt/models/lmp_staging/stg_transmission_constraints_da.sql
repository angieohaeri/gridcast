select
    datetime_beginning_utc,
    datetime_ending_utc,
    duration_hours,
    monitored_facility,
    contingency_facility
from {{ source('raw_lmp', 'transmission_constraints_da') }}
