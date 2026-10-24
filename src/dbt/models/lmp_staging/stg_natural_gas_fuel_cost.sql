select
    period,
    location as state,
    cost_per_mmbtu
from {{ source('raw_lmp', 'natural_gas_fuel_cost') }}
