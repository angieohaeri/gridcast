# LMP Pricing Model — Schema

TimescaleDB table definitions for the `raw_lmp` schema — isolated from `public` so the
whole schema is droppable via `DROP SCHEMA raw_lmp CASCADE` (see `decisions.md` — Isolation).
DDL: `src/consumers/lmp_model_schema.sql`, applied manually (not on container init).

Extracted from `lmp-pricing-model.md`, **Author:** Angie Ohaeri (assisted), **Date:**
August 23rd Time: 3:31pm.

---

### `marginal_value_rt`

Real-Time Marginal Value feed: shadow price µ per binding transmission constraint —
the congestion term of `πᵢ = λ + ΣₖAᵢₖµₖ`. Keyed by `Monitored Facility`/`Contingency
Facility` (a substation or line name), not pnode/zone. 5-min native since 2018, posts
daily 11am-12pm ET. Backfilled 2026-08-21: 755,530 rows, 1,048 distinct facilities.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_marginal_value_rt.py`.

| column | type | notes |
|---|---|---|
| `datetime_beginning_utc` | timestamptz, not null | hypertable partitioning column |
| `datetime_ending_utc` | timestamptz, not null | |
| `monitored_facility` | text, not null | substation or line name |
| `contingency_facility` | text | |
| `transmission_constraint_penalty_factor` | numeric | RT-only field |
| `limit_control_percentage` | numeric | RT-only field |
| `shadow_price` | numeric, not null | µ, the congestion term |

Unique constraint: `(datetime_beginning_utc, monitored_facility, contingency_facility)`.
Index: `(monitored_facility, datetime_beginning_utc DESC)`.

---

### `marginal_value_da`

Same congestion term as `marginal_value_rt`, for the day-ahead target. No penalty
factor / limit control percentage fields — those are RT-only (see the feed's own field
definitions on Data Miner). Backfilled 2026-08-01: 278,986 rows.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_marginal_value_da.py`.

| column | type | notes |
|---|---|---|
| `datetime_beginning_utc` | timestamptz, not null | hypertable partitioning column |
| `datetime_ending_utc` | timestamptz, not null | |
| `monitored_facility` | text, not null | |
| `contingency_facility` | text | |
| `shadow_price` | numeric, not null | µ, the congestion term |

Unique constraint: `(datetime_beginning_utc, monitored_facility, contingency_facility)`.
Index: `(monitored_facility, datetime_beginning_utc DESC)`.

---

### `forecasted_generation_outages`

Daily 90-day-horizon forecast of generation outages, RTO/West/Other only — not zonal.
Weaker than hoped (no zonal breakdown) but still useful as a system-stress signal.
Backfilled 2026-08-21 (+90d): 120,666 rows.

**Not a hypertable** — ~90 rows per daily forecast execution (one per `forecast_date`
in the 90-day horizon), not a high-volume time series.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_forecasted_outages.py`.

| column | type | notes |
|---|---|---|
| `forecast_execution_date` | timestamptz, not null | when this forecast was posted |
| `forecast_date` | date, not null | the date being forecast |
| `outage_mw_rto` | numeric | |
| `outage_mw_west` | numeric | |
| `outage_mw_other` | numeric | |

Unique constraint: `(forecast_execution_date, forecast_date)`.

---

### `generation_by_fuel`

`gen_by_fuel` (via gridstatus `get_fuel_mix`): hourly actual generation by fuel type,
RTO-wide — informs λ (system-wide energy price), not the congestion term. Long format
(one row per fuel per hour), not the wide shape gridstatus returns, to match this
project's other categorical time series (`load`, `instantaneous_load`). Backfilled
2026-08-23: 319,100 rows, 2023-01-01 → 2026-08-23, 10 fuel types.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_generation_by_fuel.py`.

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null | hypertable partitioning column |
| `fuel_type` | text, not null | Coal, Gas, Hydro, Multiple Fuels, Nuclear, Oil, Other Renewables, Solar, Storage, Wind |
| `generation_mw` | numeric, not null | |

Unique constraint: `(time, fuel_type)`. Index: `(fuel_type, time DESC)`.

---

### `transmission_constraints_da`

`da_transconstraints` (via gridstatus `get_transmission_constraints_day_ahead_hourly`):
which facility/contingency pairs bound in the day-ahead market and for how long — a
duration signal, no price magnitude (pairs with `marginal_value_da`'s shadow price for
the same facility). The raw feed's `Day Ahead Congestion Event` field is dropped —
confirmed always identical to `Monitored Facility`, redundant. Backfilled 2026-08-23:
67,087 rows, 2023-01-01 → 2026-08-23, 2,384 distinct monitored facilities.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_transmission_constraints_da.py`.

| column | type | notes |
|---|---|---|
| `datetime_beginning_utc` | timestamptz, not null | hypertable partitioning column |
| `datetime_ending_utc` | timestamptz, not null | |
| `duration_hours` | integer, not null | hours the constraint bound |
| `monitored_facility` | text, not null | |
| `contingency_facility` | text | |

Unique constraint: `(datetime_beginning_utc, monitored_facility, contingency_facility)`.
Index: `(monitored_facility, datetime_beginning_utc DESC)`.

---

### `lmp_da_hourly`

`da_hrl_lmps`, `location_type=ZONE` (via gridstatus `get_lmp`): day-ahead zone-level
hourly LMP — same grain as `public.lmp` (zone × hour) but the DA market instead of RT,
enabling a DA-RT basis feature. `zone` uses this project's zone_id codes via the same
mapping as `public.lmp`'s producer (`src/producers/lmp_producer.py`); `MID-ATL/APS`
(aggregate), `OVEC` (out of scope), and `PJM-RTO` (hub) are dropped at ingestion, same
as `public.lmp`. `time` = Interval End (Hour Ending), matching `public.lmp`'s
convention. Backfilled 2026-08-23: 638,380 rows, 2023-01-01 → 2026-08-23, 20 zones.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_lmp_da_hourly.py`.

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null | Hour Ending, UTC; hypertable partitioning column |
| `zone` | text, not null | project zone_id code |
| `lmp` | numeric, not null | total day-ahead LMP ($/MWh) |
| `congestion_price` | numeric | |
| `marginal_loss_price` | numeric | |

Unique constraint: `(time, zone)`. Index: `(zone, time DESC)`.

---

### `transmission_nodes` / `transmission_edges`

Substation graph built from `US_Electric_Power_Transmission_Lines.gpkg` (HIFLD/EIA) —
plumbing for the shift-factor proxy (see `lmp-pricing-model.md` — "How the transmission
line data actually feeds into the model"), not features by themselves. Built from each
line's own endpoint coordinates directly, not by name-joining `SUB_1`/`SUB_2` against a
substations file (see `decisions.md`). Not hypertables — static reference graph, rebuilt
wholesale (`DROP`/recreate) rather than incrementally updated.

Script: `_archive/scripts/build_transmission_graph.py`. 9,445 nodes, 12,434 edges.

**`transmission_nodes`**

| column | type | notes |
|---|---|---|
| `node_id` | integer, primary key | |
| `lon` | double precision, not null | |
| `lat` | double precision, not null | |
| `zone_id` | text | project zone_id code, null if unattributed |

**`transmission_edges`**

| column | type | notes |
|---|---|---|
| `line_id` | text | |
| `node1_id` | integer, references `transmission_nodes(node_id)` | |
| `node2_id` | integer, references `transmission_nodes(node_id)` | |
| `voltage_kv` | numeric | null where source `VOLTAGE` was `-999999` (missing) |
| `volt_class` | text | |
| `inferred` | text | `INFERRED = Y` on 62% of rows nationally — most edges are inferred, not confirmed; weight/filter before trusting the graph |
| `length_m` | numeric | |
