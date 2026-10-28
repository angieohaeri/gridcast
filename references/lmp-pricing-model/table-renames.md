# Table rename worksheet

**Title: Current table names with a column for new names, Author: Angie Ohaeri (assisted), Date: October 27th Time: 1:30pm CT**

Built from `references/schema.md`, `references/lmp-pricing-model/schema.md`, `src/consumers/schema.sql` and the dbt
models, not from the live DB (it was unreachable when this was written). Fill in **New name** for any table to
rename; leave it blank to keep the current name. `public` is the default schema where the consumers' DDL has no
schema prefix.

## Load stack

| Schema | Current name | Kind | New name |
|---|---|---|---|
| `public` | `load` | hypertable | |
| `public` | `lmp` | hypertable | |
| `public` | `weather` | hypertable | |
| `public` | `instantaneous_load` | hypertable | |
| `public` | `datacenters` | table | |
| `public` | `prediction_log` | table | |
| `analytics` | `features` | dbt model | |
| `analytics` | `load_features` | dbt model | |
| `analytics` | `inst_load_features` | dbt model | |
| `analytics` | `weather_features` | dbt model | |
| `analytics` | `stg_load` | dbt staging view | |
| `analytics` | `stg_lmp` | dbt staging view | |
| `analytics` | `stg_weather` | dbt staging view | |
| `analytics` | `stg_inst_load` | dbt staging view | |

## LMP model: raw (`raw_lmp`)

| Schema | Current name | Kind | New name |
|---|---|---|---|
| `raw_lmp` | `rt_hrl_lmps_nodal` | raw | lmp_rt|
| `raw_lmp` | `da_hrl_lmps_nodal` | raw | lmp_da|
| `raw_lmp` | `lmp_da_hourly` | raw | lmp_da_zonal|
| `raw_lmp` | `marginal_value_rt` | raw | |
| `raw_lmp` | `marginal_value_da` | raw | |
| `raw_lmp` | `transmission_constraints_da` | raw | |
| `raw_lmp` | `generation_by_fuel` | raw | |
| `raw_lmp` | `scheduled_generation` | raw | |
| `raw_lmp` | `generation_ehv_losses` | raw | |
| `raw_lmp` | `forecasted_generation_outages` | raw | |
| `raw_lmp` | `operator_initiated_commitments` | raw | |
| `raw_lmp` | `natural_gas_fuel_cost` | raw | |
| `raw_lmp` | `load_forecast` | raw | |
| `raw_lmp` | `weather_forecast` | raw | |
| `raw_lmp` | `transmission_nodes` | raw | |
| `raw_lmp` | `transmission_edges` | raw | |
| `raw_lmp` | `facilities` | raw | |
| `raw_lmp` | `pnode_topology_nodes` | raw | |
| `raw_lmp` | `pnode_topology_edges` | raw | |

## LMP model: dbt (`analytics_lmp`)

| Schema | Current name | Kind | New name |
|---|---|---|---|
| `analytics_lmp` | `lmp_spread` | dbt model | |
| `analytics_lmp` | `lmp_spread_history` | dbt model | |
| `analytics_lmp` | `lmp_baseline_features` | dbt model | |
| `analytics_lmp` | `lmp_model_features` | dbt view | |
| `analytics_lmp` | `lmp_load_forecast_features` | dbt model | |
| `analytics_lmp` | `lmp_weather_forecast_features` | dbt model | |
| `analytics_lmp` | `lmp_node_areas` | dbt model | |
| `analytics_lmp` | `lmp_load_forecast_areas` | dbt seed | |
| `analytics_lmp` | `stg_rt_lmp_nodal` | dbt staging view | stg_lmp_rt |
| `analytics_lmp` | `stg_da_lmp_nodal` | dbt staging view | stg_lmp_da |
| `analytics_lmp` | `stg_lmp_da_zonal` | dbt staging view | |
| `analytics_lmp` | `stg_marginal_value_rt` | dbt staging view | |
| `analytics_lmp` | `stg_marginal_value_da` | dbt staging view | |
| `analytics_lmp` | `stg_transmission_constraints_da` | dbt staging view | |
| `analytics_lmp` | `stg_generation_by_fuel` | dbt staging view | |
| `analytics_lmp` | `stg_scheduled_generation` | dbt staging view | |
| `analytics_lmp` | `stg_generation_ehv_losses` | dbt staging view | |
| `analytics_lmp` | `stg_forecasted_generation_outages` | dbt staging view | |
| `analytics_lmp` | `stg_operator_initiated_commitments` | dbt staging view | |
| `analytics_lmp` | `stg_natural_gas_fuel_cost` | dbt staging view | |
| `analytics_lmp` | `stg_load_forecast` | dbt staging view | |
| `analytics_lmp` | `stg_weather_forecast` | dbt staging view | |
| `analytics_lmp` | `stg_pnode_topology` | dbt staging view | |
