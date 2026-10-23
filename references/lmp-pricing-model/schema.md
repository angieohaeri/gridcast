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

### `pnode_topology_nodes` / `pnode_topology_edges`

**Title: GNN node set and substation edges for pnode-level LMP modeling, Author: Angie Ohaeri (assisted), Date: October 22nd Time: 3:34am**

Node set and graph for a pnode-level GNN over real-time hourly LMPs. Plain tables, not
hypertables (no time axis); rebuilt wholesale (`DROP`/recreate) like `transmission_nodes`/
`transmission_edges`, with `built_at` marking the build - PJM's pnode list changes
quarterly, so the topology goes stale slowly rather than per row.

Built in stages, all in `_archive/scripts/`: `pull_pnode_metadata.py` (PJM `pnode` feed →
`data/external/pjm_pnodes.parquet`) → `join_pnode_hifld.py` (HIFLD attach) →
`build_pnode_topology.py` (GridSFM attach + merge + edges) → `build_backfill_node_list.py`
(node selection) → `load_pnode_topology.py` (these tables). Intermediate parquet lives in
`data/interim/`.

How pnodes get edges:
- **pnode → substation**: first 8 chars of `pnode_name`, which is fixed-width (substation
  code + voltage + equipment). Agrees 100% with the Substation column of PJM's 2008-2019 LMP
  Bus Model Update files (`data/external/2008-2019/`, 4,528 overlapping buses) - those files
  are change logs with no line/branch records, so they add no edges themselves.
- **substation → graph**, name match within zone (exact, then 8-char truncation, voltage
  tie-break) against two sources:
  - **HIFLD** substations (`electric_substation_hifld_v4.gpkg`), snapped to the nearest
    `transmission_nodes` endpoint within 1 km. Limited by naming: only ~2,700 HIFLD
    substations in the PJM footprint have real names (the rest are `UNKNOWN…`).
  - **GridSFM** (`data/external/gridsfm_pjm_network.json` - Microsoft's OSM-derived
    bus/branch network, `huggingface.co/datasets/microsoft/GridSFM_US_power_grid`,
    `16h/pjm_model.json`, MIT). A power-network model (buses, branches, impedances), not an
    ML model. One bus per voltage yard - same-name yards within 2 km are merged into one
    physical substation, which also resolved 133 of 143 apparently ambiguous names.
- **Validation**: where both sources attach the same pnode substation (346), their locations
  agree to a median ~35 m. 4 disagree by > 5 km (name collisions) and are flagged
  `conflict`, kept out of the edges.
- **Edges**: two attached substations are linked if a path of ≤ 4 hops runs between them
  through unattached graph nodes, computed in each graph separately and unioned. Uncapped,
  this connects nearly everything (median degree ~387); 4 hops gives median degree 4.

Coverage: 1,037 of 6,293 pnode substations attached (3,032 BUS pnodes, 22%; 42% of ≥ 230 kV
buses). Weak by zone - ComEd 0% (numbered station codes like `107 DIXO` never match a name),
EKPC/Dayton ~1%, DEOK ~4% - so those zones get `fill_in` nodes: price history now, edges
later (learned adjacency or a ComEd-specific name fix) without re-pulling LMPs.

Other edge sources checked: PJM-format constraint names (`CONASTON-PEACHBOT 5012 B 500 KV`)
are real substation-pair lines in pnode naming, but only binding lines - 598 edges, largest
component 29 on their own; a raw OSM Overpass pull was dropped as redundant with GridSFM.
PJM's own network model is CEII.

DDL: in `load_pnode_topology.py` (same pattern as `build_transmission_graph.py`).

**`pnode_topology_nodes`** - 1,505 rows: 1,037 `attached`, 426 `fill_in`, 42 `aggregate`

| column | type | notes |
|---|---|---|
| `pnode_id` | bigint, primary key | |
| `pnode_name` | text, not null | |
| `pnode_subtype` | text, not null | LOAD / GEN / EXT for buses; ZONE / HUB / INTERFACE for aggregates |
| `role` | text, not null | `aggregate` (all ZONE/HUB/INTERFACE pnodes), `attached` (one representative bus per attached substation), `fill_in` (one per ≥ 138 kV substation in ComEd/EKPC/Dayton/DEOK not already attached) |
| `zone` | text | PJM zone name (`COMED`, `PSEG`, …); ZONE aggregates use their own name |
| `zone_id` | text | project zone_id code; null for hubs, interfaces, `PJM-RTO`, `MID-ATL/APS`, `OVEC` |
| `substation` | text | 8-char substation code; null for aggregates |
| `sub_key` | text | `zone_id\|substation` - join key to the edges; null for aggregates. Indexed |
| `voltage_kv` | numeric | |
| `lat` / `lon` | double precision | attached only (GridSFM location, else HIFLD) |
| `topo_source` | text | `gridsfm` \| `hifld` \| `both`; null if not attached |
| `conflict` | boolean | GridSFM and HIFLD locations > 5 km apart; excluded from edges |
| `built_at` | timestamptz, not null | |

Representative bus per substation: highest voltage, LOAD before GEN (gen-bus prices can
carry unit-local constraints), then lowest `pnode_id`.

**`pnode_topology_edges`** - 2,921 rows: 1,628 `gridsfm`, 953 `hifld`, 340 `gridsfm+hifld`

| column | type | notes |
|---|---|---|
| `sub_a` / `sub_b` | text, not null | `sub_key` of each end, `sub_a < sub_b`; primary key `(sub_a, sub_b)`, index on `sub_b` |
| `source` | text, not null | which graph(s) produced the edge |
| `built_at` | timestamptz, not null | |

Largest connected component: 995 of 1,037 attached substations; 27 isolated.

---

### `rt_hrl_lmps_nodal`

**Title: Verified real-time hourly LMPs at pnode level for the GNN node set, Author: Angie Ohaeri (assisted), Date: October 22nd Time: 7:38am**

`rt_hrl_lmps` (verified real-time hourly LMP - the hourly average of the 5-min settlement
prices) for every pnode in `pnode_topology_nodes` (1,505). The only pnode-level LMP table;
every other LMP table here is zonal. Hypertable, 30-day chunks. Loaded 2026-10-22:
25,737,322 rows, 1,505 pnodes, 2024-10-22 01:00 ET → 2026-10-19 00:00 ET (hour ending).
4.11 GB uncompressed (1.55 GB table + 2.55 GB indexes, ~160 B/row).

**Title: Compression enabled on rt_hrl_lmps_nodal, Author: Angie Ohaeri (assisted), Date: October 22nd Time: 7:50am**

The only compressed `raw_lmp` hypertable - a deliberate exception to the 2026-08-23
revert (note at the end of `lmp_model_schema.sql`: a compressed chunk breaks Postico's row
browser for the whole table). Accepted for this table because of its size; query it with
SQL rather than the row browser. `compress_segmentby = 'pnode_id'`,
`compress_orderby = 'time desc'` - matches the main access pattern (one pnode's history).
Chunks older than 30 days compress via policy (job 1008); the newest chunk stays
uncompressed so partial-month refreshes stay cheap. Upserts into compressed chunks still
work (slower). Result: 4.11 GB → 0.68 GB with 23 of 25 chunks compressed (whole DB
5.7 GB → 2.5 GB); row count unchanged; one pnode's full 2-year history queries in ~0.07 s.

DDL: `src/consumers/lmp_model_schema.sql`. Backfill: `_archive/scripts/backfill_rt_hrl_lmps_nodal.py`
(→ `data/interim/rt_hrl_lmps_nodal/`, 275 parquet checkpoints, DVC-tracked) →
`_archive/scripts/load_rt_hrl_lmps_nodal.py`.

Pull constraints found while backfilling (all free-tier Data Miner 2):
- **History**: the `pnode_id` filter only works on non-archived data - the last 731 days.
  Archived requests return 400 ("invalid attribute(s) for archived data - Pnode_Id"), so
  anything older means pulling every pnode (~14k) and filtering client-side. The cutoff
  rolls daily: it moved past 2024-10-21 mid-backfill and silently dropped that day for later
  requests, hence the common 2024-10-22 start.
- **URL length, not id count**: ~1,700 characters of URL-encoded `pnode_id` list is the
  most that works per request (150 mostly-10-digit ids = 1,888 chars → 404). Chunks are
  capped on both.
- **Rate limit**: 6 requests/min, shared with the live producers on the same API key.
  Requests are sized to one 50,000-row page and spaced 10.5 s apart; gridstatus's own
  backoff alone doesn't keep up. ~2.5 h for the full pull.
- **Late pnodes**: 60 of the 1,505 became effective after 2024-10-22 (up to 2026-09-16),
  so their history starts at their effective date - a GNN needs to mask them before that.

Validated on load: no duplicate `(time, pnode_id)`, no nulls, no missing hours for any
pnode from max(2024-10-22, effective date); exactly one `system_energy_price` per hour;
`system_energy_price + congestion_price + marginal_loss_price = lmp` to $0.01. Heavy-tailed:
LMP −$2,328 to $4,673/MWh, congestion −$2,942 to $3,706; 99.8% of LMPs within −$59 to $1,275.

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null | Hour Ending, UTC (feed's Interval Start + 1h), matching `public.lmp` / `lmp_da_hourly`; hypertable partitioning column |
| `pnode_id` | bigint, not null | join to `pnode_topology_nodes` for name/zone/substation/role |
| `system_energy_price` | real, not null | λ - system-wide, identical for every pnode in an hour |
| `lmp` | real, not null | total LMP ($/MWh) |
| `congestion_price` | real, not null | |
| `marginal_loss_price` | real, not null | |
| `is_verified` | boolean, not null, default true | `true` = verified `rt_hrl_lmps`; `false` = unverified `rt_unverified_hrl_lmps`, live, not settlement quality. Filter to `true` for training/backtests |

`real` (float4), not `numeric` like the zonal tables: the feed's values fit float4 to the
cent, and at ~26M rows the width matters. Unique constraint: `(time, pnode_id)`. Index:
`(pnode_id, time DESC)`.

**Title: Unverified hourly rows added alongside verified (is_verified), Author: Angie Ohaeri (assisted), Date: October 23rd Time: 6:30pm CT**

Two syncs write this table, both in `src/prefect/raw_lmp_sync.py`:

- `sync_rt_unverified_hrl_lmps_nodal` - **hourly** (:25 ET). PJM's `rt_unverified_hrl_lmps`
  posts each hour ~20 min after it ends, retains 30 days. Inserts `is_verified = false` and
  never overwrites a verified row. Feed quirks: no `pnode_id` (mapped from `pnode_name`,
  unique across PJM's active pnodes - covers 1,504 of 1,505 nodes, `MID-ATL/APS` isn't
  published), no `system_energy_price` (derived as `lmp - congestion_price -
  marginal_loss_price`; identical at all 14,504 locations in an hour, checked
  2026-10-23), and no pnode filter - each run pulls every location for a 3-hour window
  (~43k rows, one request).
- `sync_rt_hrl_lmps_nodal` - **daily** (12:40 ET), verified, 7-day trailing window. Writes
  `is_verified = true` and overwrites unverified rows for the same hours. Verification does
  revise prices: 20 of 3,008 re-pulled pnode-hours differed by more than $0.01.

So at any moment: history is verified; the last ~1-2 days are unverified until the daily run
catches up. Added 2026-10-23 with all 25.9M existing rows `true`, plus a one-off unverified
fill of 2026-10-23 01:00-19:00 ET (the gap between the last verified hour and the first
hourly run).

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
