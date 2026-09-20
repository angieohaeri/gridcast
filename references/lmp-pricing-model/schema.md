# LMP Pricing Model — Schema

TimescaleDB table definitions for the `raw_lmp` schema — isolated from `public` so the
whole schema is droppable via `DROP SCHEMA raw_lmp CASCADE` (see `decisions.md` — Isolation).
DDL: `src/consumers/lmp_model_schema.sql`, applied manually (not on container init).

Extracted from `lmp-pricing-model.md`, **Author:** Angie Ohaeri (assisted), **Date:**
August 23rd Time: 3:31pm.

**Title: Added `operator_initiated_commitments`, `scheduled_generation`, `generation_ehv_losses`, Author: Angie Ohaeri (assisted), Date: August 23rd Time: 4:51pm**

These 3 are sourced differently from every other table here: no gridstatus wrapper
method exists for their Data Miner 2 feeds, so they're pulled by calling gridstatus's
`PJM._get_pjm_json()` directly against the raw feed name (`ops_init_commit`,
`rt_and_self_ecomax`, `gen_ehv_losses`) instead of a public `get_*` method — see
`decisions.md` ("Backfilled the 3 remaining Tier 1 sources...") for why that was
sufficient and how the feed names were found.

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

---

### `facilities`

**Title: Facility-name-to-location lookup for Marginal Value's monitored_facility/contingency_facility, Author: Angie Ohaeri (assisted), Date: August 23rd Time: (session)**

Resolves `monitored_facility`/`contingency_facility` (4,551 distinct, pooled — the two
columns never share an exact string, but the same substation code appears in both) to a
name and, where available, coordinates. Matched by truncated substation-code
prefix/substring against PJM's pnode list, HIFLD substations (PJM-footprint filtered),
and `lmp-aggregate-definitions.xlsx` — only HIFLD carries coordinates.
(`_archive/scripts/build_facilities_table.py`)

3,940/4,551 (86.6%) matched by name, but only **23.8% of actual constraint-hours** have
real coordinates once frequency-weighted — errors concentrate in the highest-frequency
facilities (same-name collisions within the PJM footprint, e.g. Greentown OH vs. IN).
Real coordinate data beyond this is CEII-restricted by PJM — `decisions.md`. Treat this
table as a partial, lower-confidence lookup, not a complete crosswalk.

Reference table, rebuilt wholesale (`DROP`/recreate), not incrementally updated.

| column | type | notes |
|---|---|---|
| `facility` | text, primary key | raw string, from either `monitored_facility` or `contingency_facility` |
| `matched_name` | text | `null` if nothing matched (611 of 4,551) |
| `pnode_id` | bigint | `null` if only a name/HIFLD match, no pnode |
| `lat` / `lon` | double precision | HIFLD only; `null` otherwise |
| `voltage_match` | boolean | `null` = unchecked, not disagreement |
| `source` | text | `'pnode'` \| `'hifld_substation'` \| `'aggregate_def'` |

---

### `operator_initiated_commitments`

`ops_init_commit` (via `PJM._get_pjm_json()` directly — no gridstatus wrapper): zonal(!)
out-of-merit unit commitments with a `reason` field — "Constraint Management" ties
directly to congestion. Irregular event-level timestamps (sub-minute granularity, not a
regular hourly/monthly grid). No stable per-row id in the raw feed — duplicates on the
full natural key collapse on upsert (`ON CONFLICT ... DO NOTHING`), confirmed
empirically that rows sharing `(time, zone, reason, economic_max_mw)` are re-posts of
the same event, not distinct simultaneous commitments (those differ in
`economic_max_mw`). `zone` uses this project's zone_id codes via the same mapping as
`public.lmp`'s producer; `OVEC` (out of scope) dropped at ingestion. Backfilled
2026-08-23: 14,674 rows, 2023-01-02 → 2026-07-31, 19 zones (RECO had no commitments in
the window).

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_operator_initiated_commitments.py`.

| column | type | notes |
|---|---|---|
| `datetime_beginning_utc` | timestamptz, not null | hypertable partitioning column; irregular event timestamps |
| `zone` | text, not null | project zone_id code |
| `economic_max_mw` | numeric | |
| `reason` | text | e.g. "System Wide Capacity", "Constraint Management", "Voltage Support" |

Unique constraint: `(datetime_beginning_utc, zone, reason, economic_max_mw)`.
Index: `(zone, datetime_beginning_utc DESC)`.

---

### `scheduled_generation`

`rt_and_self_ecomax` (via `PJM._get_pjm_json()` directly — no gridstatus wrapper):
hourly, RTO-wide self-scheduled generation (`self_ecomax`) — runs regardless of price
(must-run/contractual, not operator-directed), distorts normal dispatch, causes uplift
charges. No zone field, so it can only inform λ, not the congestion term. `rt_ecomax` is
null whenever PJM applies confidentiality suppression (~55% of rows) — a real flag from
the source, not missing data; kept as-is rather than imputed. The raw feed's
`conf_disclaimer` field (the static explanatory text for that suppression) is dropped —
not a data column. Backfilled 2026-08-23: 31,727 rows, 2023-01-01 → 2026-08-23.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_scheduled_generation.py`.

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null | hypertable partitioning column |
| `rt_ecomax` | numeric | null under PJM confidentiality suppression (~55% of rows) |
| `self_ecomax` | numeric | |

Unique constraint: `(time)`.

---

### `generation_ehv_losses`

`gen_ehv_losses` (via `PJM._get_pjm_json()` directly — no gridstatus wrapper): hourly,
RTO-wide — the losses term of `πᵢ = λ + ΣₖAᵢₖµₖ`, smallest of the three LMP components
and, until this backfill, unsourced. Backfilled 2026-08-23: 31,919 rows,
2023-01-01 → 2026-08-23.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_generation_ehv_losses.py`.

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null | hypertable partitioning column |
| `total_gen` | numeric | RTO-wide total generation, MW |
| `total_losses` | numeric | RTO-wide EHV losses, MW |

Unique constraint: `(time)`.

---

### `natural_gas_fuel_cost`

`electricity/electric-power-operational-data`, `cost-per-btu` metric, `fueltypeid=NG`,
`sectorid=98` (Electric Power) — **EIA, not PJM.** Monthly average natural gas cost per
BTU by state; pairs with `generation_by_fuel` to approximate λ (the "spark spread"
signal). No gridstatus wrapper — `gridstatus.EIA.get_dataset()` only supports 5
hardcoded EIA routes, none of which is this one — pulled by calling `EIA._fetch_page()`
directly with a manually built request instead of a new HTTP client (reuses its
auth/pagination handling, same pattern as the PJM `_get_pjm_json()` sources above).
Census-region/national/territory rows (`ENC`, `PCC`, `US`, `PR`, etc.) are dropped,
keeping only real states — DC and HI never appear in this facet combination (negligible
NG-fired generation). Null cost means no NG generation was reported for that
state/month, not suppressed/missing data. **Reporting lag**: EIA's Form EIA-923 monthly
data lags ~3 months behind real time (unlike the PJM feeds, which are near-real-time or
next-day) — as of this backfill, data only reaches 2026-05 despite pulling through
2026-08. Backfilled 2026-08-23: 2,009 rows, 2023-01-01 → 2026-05-01, 49 states.

**Not a hypertable** — ~2,500 rows total (49 states × ~40 months), same low-volume
reasoning as `forecasted_generation_outages`.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/{pull,backfill}_natural_gas_fuel_cost.py`.

| column | type | notes |
|---|---|---|
| `period` | date, not null | first of month |
| `location` | text, not null | 2-letter state postal code |
| `cost_per_mmbtu` | numeric | dollars per million BTU (fixed unit — EIA's `cost-per-btu-units` field, always this value, dropped); null = no NG generation reported |

Unique constraint: `(period, location)`.
