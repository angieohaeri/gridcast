# TimescaleDB Schema

TimescaleDB table definitions.

---

### `load`

**Title:** Dropped ba_code (constant across all rows, single-BA project), renamed subregion to zone for consistency with lmp/weather, **Author:** Angie Ohaeri, 
**Date: August 8th Time: (session)**

**Title:** Added source and is_verified columns, made zone NOT NULL (EIA's RTO-level row now uses zone='RTO' instead of NULL, distinguished from PJM's own zone='RTO' row via source), added UNIQUE(time, zone, source) for upsert-safe consumer writes, 
**Author:** Angie Ohaeri, 
**Date: August 8th Time: (session)**

Raw landing table for EIA-930 hourly demand and PJM zonal load (Kafka topic `load`). One row per (zone, source) per hour. PJM's metered feed (`source='pjm'`) and EIA's grid monitor (`source='eia'`) are independent measurements — both use `zone='RTO'` for their system totals, kept as separate rows (never merged) since they're different measurements, not duplicates.

DDL: `src/consumers/schema.sql`

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null, default now() | Hour Ending, UTC; hypertable partitioning column |
| `zone` | text, not null | PJM zone, or `RTO` for system-wide totals (from either source) |
| `source` | text, not null | `pjm` or `eia` - which feed this row came from |
| `demand_mw` | double precision, not null | actual demand |
| `demand_forecast_mw` | double precision | EIA-930's own day-ahead forecast, when present |
| `net_generation_mw` | double precision | |
| `total_interchange_mw` | double precision | |
| `is_verified` | boolean | only meaningful for `source='pjm'`; PJM revises a given (time, zone) from unverified to verified over ~3 days after publish. Null for `source='eia'` (EIA exposes no equivalent flag) |

Unique constraint: `(time, zone, source)` - the upsert key consumers write against, since
both sources republish the same hour as values get revised.

Index: `(zone, time DESC)` for per-zone lookups.

---

### `lmp`

**Title: Added lmp hypertable, Author: Angie Ohaeri, Date: August 4th Time: (session)**

**Title: Normalized `zone` to project zone_id codes across the whole table, Author: Angie
Ohaeri, Date: August 12th Time: 11:15am**

Raw landing table for the PJM Data Miner 2 `rt_hrl_lmps` real-time hourly LMP feed (Kafka topic `lmp`). One row per pricing node per hour. Kept at native resolution — not resampled to match `load`'s grain at ingestion; alignment happens explicitly in a named dbt model.

`zone` had been inconsistent: the original 4 in-scope zones stored project zone_ids while the rest carried PJM's raw Location Short Name from the backfill. A one-off UPDATE renamed the nine that differed (eleven already matched). `MID-ATL/APS` (aggregate), `OVEC` (out of scope), and `PJM-RTO` (hub) still use raw names and are excluded by `in_scope_zones` in `stg_lmp`. Mapping recorded in `data/external/pjm_eia930_subregions.csv`.

DDL: `src/consumers/schema.sql`

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null, default now() | Hour Ending, UTC; hypertable partitioning column |
| `pnode_id` | text, not null | PJM pricing node id |
| `pnode_name` | text | |
| `zone` | text, not null | PJM zone the node belongs to |
| `lmp` | double precision, not null | total locational marginal price ($/MWh) |
| `congestion_price` | double precision | |
| `marginal_loss_price` | double precision | |

Index: `(zone, time DESC)` for per-zone lookups.

**Title: Documented LMP zone-label mismatch discovered writing the producer, Author: Angie Ohaeri, Date: August 8th Time: 11:30pm**

`rt_hrl_lmps` labels zones by utility short name, not this project's zone_id codes; the producer (`src/producers/lmp_producer.py`) maps explicitly. *(Scope was 4 zones at the time — superseded by the August 12th entry above.)*

**Title: Added UNIQUE(time, pnode_id) and migrated existing rows to zone_id codes, Author: Angie Ohaeri, Date: August 9th Time: (session)**

Added `lmp_time_pnode_uidx` so `lmp_consumer.py` can upsert on re-delivery (mirrors `load`'s design; confirmed no duplicate `(time, pnode_id)` rows first). Applied directly against the live DB since `schema.sql` only runs on first container init. *(Migrated only the 4 in-scope zones then; reversed August 12th — all 20 now normalized.)*

---

### `weather`

**Title:** Added weather hypertable, Author: Angie Ohaeri, Date: August 4th Time: (session)

**Title:** Expanded to all 20 zones; composite zones averaged before write, **Author:** Angie
Ohaeri, **Date: August 12th Time: 11:20am**

Raw landing table for Open-Meteo observations (Kafka topic `weather`), one row per zone per poll. All 20 in-scope zones covered, drawn from 30 stations — seven multi-climate zones carry 2-3 stations, which `weather_producer.py` averages before publishing, so the grain stays one row per (time, zone). Station list: `pjm_weather_zones.csv`; selection method in `decisions.md`.

DDL: `src/consumers/schema.sql`

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null, default now() | ingestion time; hypertable partitioning column |
| `zone` | text, not null | PJM zone this weather observation represents |
| `temperature` | double precision, not null | |
| `precipitation` | double precision, not null | |
| `wind_speed` | double precision, not null | |
| `cloud_cover` | double precision, not null | |

Index: `(zone, time DESC)` for per-zone lookups.

---

### `instantaneous_load`

**Title:** Added instantaneous_load hypertable + one-off backfill, **Author:** Angie Ohaeri (assisted), **Date: August 22nd Time: 12:07pm**

Raw landing table for PJM Data Miner 2's `inst_load` feed (Kafka topic: TBD, `src/producers/inst_load_producer.py` in progress). Point-in-time telemetry at ~5-minute native resolution, distinct from `load`'s settled hourly-integrated `demand_mw` — kept as its own table rather than a new `source` value in `load`, since `load`'s `is_verified` semantics assume settlement and this feed has none. See `references/decisions.md` for why this is being added (near-term demand lags `load` can't support due to settlement lag).

**`inst_load` only retains a trailing ~30 days** (confirmed empirically 2026-08-22: 29 days back returns data, 31+ raises `NoDataFoundException`) — unlike `hrl_load_metered`/`rt_hrl_lmps`, there's no multi-year history available for this feed. The one-off backfill (`_archive/scripts/backfill_instantaneous_load.py`) pulled whatever was left of that window at run time, not a fixed start date.

`zone` uses this project's zone_id codes; `inst_load`'s own labels differ for 3 zones (`APS`→`AP`, `COMED`→`CE`, `DAYTON`→`DAY`), mapped at ingestion. `'RTO'` is kept (mirrors `load`'s ingested-but-unmodelled `RTO` row) from `inst_load`'s `PJM RTO` column; its 3 other regional aggregates (`PJM MID ATLANTIC REGION`/`PJM SOUTHERN REGION`/`PJM WESTERN REGION`) and `'UG'` (not a zone — an "underground asset" category) are dropped at ingestion.

DDL: `src/consumers/schema.sql`

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null | UTC, native ~5-minute posting interval — not Hour Ending (that convention is for settled hourly quantities, not point-in-time telemetry); hypertable partitioning column |
| `zone` | text, not null | PJM zone, or `RTO` for the system-wide total |
| `instantaneous_load_mw` | double precision, not null | raw telemetry snapshot, not settled/verified energy |

Unique constraint: `(time, zone)`. Index: `(zone, time DESC)` for per-zone lookups.

---

### `datacenters`

**Title:** Added datacenters reference table, **Author:** Angie Ohaeri, **Date: August 21st Time: (session)**

**Title:** Switched from truncate-and-reload to append-only daily snapshots, **Author:** Angie Ohaeri, **Date: August 21st Time: (session)**

Reference table of built/planned/proposed U.S. data centers, synced daily from FracTracker Alliance's public tracker (a Google Sheet, `src/prefect/data_center_sync.py`) - candidate treatment/control data for a diff-in-diff analysis of data center buildout on PJM zonal load and LMPs. Not a hypertable; `synced_at` is ingestion time, not an event time. Each sync appends a full snapshot rather than truncating, so a facility's `status`/`mw`/etc over time is recoverable - the sheet itself carries no per-field history, only a single `date_updated`. No upsert key: the sheet has no stable per-facility id and `facility_name` isn't unique (e.g. "DC Blox" appears at multiple addresses). FracTracker's terms require citing them as the source in any public-facing use and restrict redistribution of the raw compilation - don't commit their raw export to the repo, pull it live via the flow.

DDL: `src/consumers/schema.sql`

| column | type | notes |
|---|---|---|
| `facility_name` | text, not null | |
| `address`, `city`, `state`, `zip`, `county` | text | for mapping to PJM zone |
| `lat`, `long` | double precision | for mapping to PJM zone/pnode |
| `location_confidence` | text | High/Medium/Low - many rows are approximate |
| `status` | text | Proposed/Operating/Cancelled/etc - the treatment variable |
| `expected_date_online` | text | treatment timing, but free text ("Full buildout by 2037", "2027-28"), not a clean year |
| `mw` | text | treatment intensity; sometimes a range ("100-200"), not always numeric |
| `sizerank` | text | categorical treatment intensity fallback when `mw` is blank |
| `operator_name` | text | |
| `facility_size_sqft`, `property_size_acres`, `project_cost` | text | kept on hand, not for the DiD itself; inconsistent formats ("1.43 Million", "$14.5 billion") |
| `date_created`, `date_updated` | date | FracTracker's own record timestamps, not a status-change history |

Appended, not truncated, on each daily sync - see the switch note above. Indexes: `(synced_at DESC)` for pulling the latest full snapshot, `(facility_name, synced_at DESC)` for a given facility's history over time.
