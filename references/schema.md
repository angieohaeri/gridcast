# TimescaleDB Schema

Load-stack tables (`public`). LMP model tables: `lmp-pricing-model/schema.md`. All DDL: `src/consumers/schema.sql`. Tightened by Angie Ohaeri (assisted), October 29th 2:35am CT.

---

### `load`

**Title:** Dropped ba_code (constant, single-BA project); renamed subregion to zone; added source and is_verified; zone NOT NULL (EIA's RTO row uses zone='RTO', told apart from PJM's by source); UNIQUE(time, zone, source) for upserts, **Author:** Angie Ohaeri, **Date: August 8th Time: (session)**

EIA-930 hourly demand and PJM zonal load (topic `load`), one row per (zone, source) per hour. PJM metered (`source='pjm'`) and EIA (`source='eia'`) are independent measurements; both have a `zone='RTO'` total, kept as separate rows.

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

Unique `(time, zone, source)`: the upsert key, since both sources republish revised hours. Index `(zone, time DESC)`.

---

### `lmp`

**Title: Added lmp hypertable (Aug 4); UNIQUE(time, pnode_id) for consumer upserts (Aug 9); zone normalized to project zone_ids (Aug 12 11:15am), Author: Angie Ohaeri**

PJM `rt_hrl_lmps` real-time hourly LMPs (topic `lmp`), one row per pricing node per hour, joined to `load` in a named dbt model.

`rt_hrl_lmps` labels zones by utility short name (`COMED`, `APS`); the producer maps them to zone_ids. Backfilled rows had kept the raw names, so a one-off UPDATE renamed the nine that differed. `MID-ATL/APS` (aggregate), `OVEC` (out of scope) and `PJM-RTO` (hub) keep raw names and are excluded by `in_scope_zones` in `stg_lmp`. Mapping: `data/external/pjm_eia930_subregions.csv`.

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null, default now() | Hour Ending, UTC; hypertable partitioning column |
| `pnode_id` | text, not null | PJM pricing node id |
| `pnode_name` | text | |
| `zone` | text, not null | PJM zone the node belongs to |
| `lmp` | double precision, not null | total locational marginal price ($/MWh) |
| `congestion_price` | double precision | |
| `marginal_loss_price` | double precision | |

Unique `lmp_time_pnode_uidx` `(time, pnode_id)`, added on the live DB (`schema.sql` only runs on first container init). Index `(zone, time DESC)`.

---

### `weather`

**Title:** Added weather hypertable (Aug 4); all 20 zones, composite zones averaged before write (Aug 12 11:20am), **Author:** Angie Ohaeri

Open-Meteo observations (topic `weather`), one row per zone per poll. 30 stations; the seven multi-climate zones' 2-3 stations are averaged in `weather_producer.py`. Stations: `pjm_weather_zones.csv`; selection: `decisions.md`.

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null, default now() | ingestion time; hypertable partitioning column |
| `zone` | text, not null | PJM zone this weather observation represents |
| `temperature` | double precision, not null | |
| `precipitation` | double precision, not null | |
| `wind_speed` | double precision, not null | |
| `cloud_cover` | double precision, not null | |

Index `(zone, time DESC)`.

---

### `instantaneous_load`

**Title:** Added instantaneous_load hypertable + one-off backfill, **Author:** Angie Ohaeri (assisted), **Date: August 22nd Time: 12:07pm**

PJM `inst_load` (topic `inst_load`, `inst_load_producer.py` every 10 min): point-in-time telemetry at ~5-minute resolution, unlike `load`'s settled hourly `demand_mw`. Its own table because `load`'s `is_verified` assumes settlement. Why: near-term demand lags that `load`'s settlement lag can't support (`decisions.md`).

**Retains only ~30 days** (2026-08-22: 29 days back works, 31+ raises `NoDataFoundException`), so the one-off backfill (`_archive/scripts/backfill_instantaneous_load.py`) took whatever was left of that window.

Zones mapped to zone_ids at ingestion (`APS`→`AP`, `COMED`→`CE`, `DAYTON`→`DAY`). `PJM RTO` kept as `'RTO'`; the 3 regional aggregates and `'UG'` (an underground-asset category, not a zone) dropped.

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null | UTC, native ~5-minute posting interval — not Hour Ending (that convention is for settled hourly quantities, not point-in-time telemetry); hypertable partitioning column |
| `zone` | text, not null | PJM zone, or `RTO` for the system-wide total |
| `instantaneous_load_mw` | double precision, not null | raw telemetry snapshot, not settled/verified energy |

Unique `(time, zone)`. Index `(zone, time DESC)`.

---

### `datacenters`

**Title:** Added datacenters reference table; switched to append-only daily snapshots, **Author:** Angie Ohaeri, **Date: August 21st Time: (session)**

U.S. data centers (built/planned/proposed), synced daily from FracTracker Alliance's public Google Sheet (`src/prefect/data_center_sync.py`): treatment/control data for a diff-in-diff of data center buildout on PJM zonal load and LMPs. Not a hypertable; `synced_at` is ingestion time. Each sync appends a full snapshot, so a facility's history is recoverable (the sheet only has one `date_updated`). No upsert key: no stable id and `facility_name` repeats (e.g. "DC Blox"). FracTracker's terms require citing them in public use and restrict redistribution: never commit their export; pull it live via the flow.

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

Indexes: `(synced_at DESC)` for the latest snapshot, `(facility_name, synced_at DESC)` for one facility's history.
