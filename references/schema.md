# TimescaleDB Schema

TimescaleDB table definitions.

---

### `load`

Title: Added load hypertable, Author: Angie Ohaeri, Date: August 4th Time: (session)

Raw landing table for EIA-930 hourly demand data (Kafka topic `load`). One row per
balancing authority/subregion per hour.

DDL: `src/consumers/schema.sql`

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null, default now() | EIA-930 reporting hour; hypertable partitioning column |
| `ba_code` | text, not null | balancing authority code (e.g. `PJM`) |
| `subregion` | text | subregion code, when reported; null for BA-level totals |
| `demand_mw` | double precision, not null | actual demand |
| `demand_forecast_mw` | double precision | EIA-930's own day-ahead forecast, when present |
| `net_generation_mw` | double precision | |
| `total_interchange_mw` | double precision | |

Index: `(ba_code, time DESC)` for per-zone lookups.

---

### `lmp`

Title: Added lmp hypertable, Author: Angie Ohaeri, Date: August 4th Time: (session)

Raw landing table for the PJM Data Miner 2 `rt_hrl_lmps` real-time hourly LMP feed
(Kafka topic `lmp`). One row per pricing node per hour. Kept at native resolution —
not resampled to match `load`'s grain at ingestion time; any alignment happens
explicitly in a named dbt model.

DDL: `src/consumers/schema.sql`

| column | type | notes |
|---|---|---|
| `time` | timestamptz, not null, default now() | LMP interval start; hypertable partitioning column |
| `pnode_id` | text, not null | PJM pricing node id |
| `pnode_name` | text | |
| `zone` | text, not null | PJM zone the node belongs to |
| `lmp` | double precision, not null | total locational marginal price ($/MWh) |
| `congestion_price` | double precision | |
| `marginal_loss_price` | double precision | |

Index: `(zone, time DESC)` for per-zone lookups.

---

### `weather`

Title: Added weather hypertable, Author: Angie Ohaeri, Date: August 4th Time: (session)

Raw landing table for Open-Meteo observations (Kafka topic `weather`), one row per
representative zone city per poll. Not every PJM zone gets its own weather feed —
scoped to a small set of representative cities to start.

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
