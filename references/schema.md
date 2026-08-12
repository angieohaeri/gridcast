# TimescaleDB Schema

TimescaleDB table definitions.

---

### `load`

**Title:** Dropped ba_code (constant across all rows, single-BA project), renamed subregion to zone for consistency with lmp/weather, **Author:** Angie Ohaeri, 
**Date: August 8th Time: (session)**

**Title:** Added source and is_verified columns, made zone NOT NULL (EIA's RTO-level row now uses zone='RTO' instead of NULL, distinguished from PJM's own zone='RTO' row via source), added UNIQUE(time, zone, source) for upsert-safe consumer writes, 
**Author:** Angie Ohaeri, 
**Date: August 8th Time: (session)**

Raw landing table for EIA-930 hourly demand data and PJM zonal load (Kafka topic `load`). One row per (zone, source) per hour. PJM's own metered feed (`source='pjm’`) and EIA's grid monitor (`source='eia'`) are independent measurements - PJM's feed includes its own `zone='RTO'` system total, and EIA's RTO-level row also uses `zone='RTO'`; the two are kept as separate rows (never merged) since they’re different measurements of the same quantity, not duplicates.

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

Raw landing table for the PJM Data Miner 2 `rt_hrl_lmps` real-time hourly LMP feed (Kafka topic `lmp`). One row per pricing node per hour. Kept at native resolution — not resampled to match `load`'s grain at ingestion time; any alignment happens explicitly in a named dbt model.

`zone` had been inconsistent: the original 4 in-scope zones were stored as project zone_ids
while the rest carried PJM's raw Location Short Name from the backfill. A one-off UPDATE
renamed the nine that differed (`AECO`→`AE`, `APS`→`AP`, `JCPL`→`JC`, `METED`→`ME`,
`PECO`→`PE`, `PENELEC`→`PN`, `PEPCO`→`PEP`, `PPL`→`PL`, `PSEG`→`PS`); the other eleven
already matched. `MID-ATL/APS` (an aggregate), `OVEC` (out of scope) and `PJM-RTO` (the RTO
hub) are still present under their raw names and are excluded by `in_scope_zones` in
`stg_lmp`. The mapping is recorded in `data/external/pjm_eia930_subregions.csv`.

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

`zone` is written using this project's zone_id codes (`data/processed/pjm_weather_zones.csv`), not PJM's raw `Location Short Name` from `rt_hrl_lmps` - the two disagree for 2 of the 4 in-scope zones (`CE` → `COMED`, `BC` → `BGE`; `DOM` and `AEP` happen to match). The producer (`src/producers/lmp_producer.py`) maps explicitly; scope is the same 4 zones as `load`/`weather` (LMP zone scope was previously undecided, now resolved).

**Title: Added UNIQUE(time, pnode_id) and migrated existing rows to zone_id codes, Author: Angie Ohaeri, Date: August 9th Time: (session)**

Added `lmp_time_pnode_uidx` so `src/consumers/lmp_consumer.py` can upsert on re-delivery (mirrors `load`'s upsert design; confirmed no existing duplicate `(time, pnode_id)` rows before adding). Also migrated the ~126K in-scope historical rows already in the live table from PJM's raw names to zone_id codes (`COMED`→`CE`, `BGE`→`BC`; `AEP`/`DOM` unchanged) so `zone` is consistent with `load`/`weather` going forward. The other 19 zones' historical rows (outside current live scope) were left as PJM's raw names - not actively maintained, not worth relabeling. Applied directly against the live DB, since `schema.sql` only runs via `docker-entrypoint-initdb.d` on first container init and this table already had data.

---

### `weather`

**Title:** Added weather hypertable, Author: Angie Ohaeri, Date: August 4th Time: (session)

Raw landing table for Open-Meteo observations (Kafka topic `weather`), one row per representative zone city per poll. Not every PJM zone gets its own weather feed — scoped to a small set of representative cities to start.

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
