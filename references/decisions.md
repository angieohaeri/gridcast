# Decisions

Why I made certain decisions, for future reference.

## Project Pivot

**Title: Pivoted from bike-share availability to PJM electricity demand forecasting, Author: Angie Ohaeri, Date: August 4th Time: (session)**

Bike-share demand forecasting is a saturated portfolio project category. Switched to
short-term electricity load forecasting instead, using the same architecture
(Kafka → TimescaleDB → dbt → LightGBM → FastAPI → Streamlit).

Picked **PJM** over NYISO and MISO: NYISO would have kept geographic continuity with the
old Citi Bike framing and required no ISO account, but PJM's larger, more heterogeneous
market (more zones, more topology) makes for a more interesting engineering story. The
tradeoff is a free PJM Data Miner account and per-zone weather features instead of one
metro's weather feed. Explicit fallback if that scope proves too costly: cut down to a
handful of zones rather than modeling the full PJM footprint — the interesting
engineering is in the prediction-log and scoring layer, not breadth of coverage.

## Data Exploration


## Database Design

**Title: `time` is Interval End (Hour Ending), in UTC, for `load` and `lmp`, Author: Angie Ohaeri, Date: August 10th Time: (session)**

Switched from Interval Start to Interval End (PJM's "HE" convention) for industry
consistency; both producers must agree since the `load`/`lmp` dbt join depends on it.
`lmp_producer.py` also now converts from US/Eastern to UTC (`load` was already UTC).

Migrated existing rows (+1h shift, 189,164 `load` / 725,305 `lmp` rows) so old and new
rows don't silently mix conventions. Gotcha: a plain `UPDATE ... SET time = time +
INTERVAL '1 hour'` fails partway through on TimescaleDB hypertables - chunks enforce
their range via CHECK constraint, and UPDATE doesn't re-route rows across chunks like
INSERT does. Fixed by copying to a staging table, truncating, and re-inserting.

**Title: `lmp` retains all 23 PJM zones historically; `load` only ever has the 4 in-scope zones, Author: Angie Ohaeri, Date: August 10th Time: (session)**

`load`'s scope (4 zones + `RTO`) was fixed at the query level from the start
(`load_producer.py:80`), so nothing else was ever stored. `lmp`'s 4-zone scope was
decided later (`lmp_producer.py:40`) - the historical bulk import predates that decision
and pulled all 23 zones, which were never pruned after. Confirmed via `SELECT zone,
count(*) FROM lmp GROUP BY zone` (same for `load`), not assumed.

## dbt

**Title: Initial staging + features layer for load/lmp/weather, Author: Angie Ohaeri, Date: August 11th Time: (session)**

dbt writes to its own `analytics` schema (`~/.dbt/profiles.yml`), separate from the
`public` schema the Kafka consumers write raw tables into - avoids a `--full-refresh`
ever touching a raw table by name collision. Layer naming is `staging` (1:1 with raw
tables, `+materialized: view`) and `features` (joins/window functions, `+materialized:
table`) - project uses "features" over dbt's usual "marts" since the consumer is a
model trainer, not a BI analyst.

`stg_load`, `stg_lmp`, `stg_weather` are incremental (`delete+insert` on natural keys)
with lookback windows on the incremental filter, not a plain `time > max(time)`:
4 days for `load` (PJM revises `is_verified` for ~3 days after publish), 2 days for
`lmp`, 1 day for `weather`. A plain "newer than" filter would freeze revised rows at
their first-seen value.

`lmp`/`weather` staging models filter to `var("in_scope_zones")` (`dbt_project.yml`) to
drop the 19 legacy zones sitting in raw `lmp` (see prior entry). Zone scope is
therefore declared twice - the var (used in `WHERE` filters) and the `pjm_weather_zones`
seed (used as the `relationships` test target) - accepted as redundant for now rather
than one driving the other.

`feat_weather_hourly`: Open-Meteo is polled ~3x/hour and each poll's `precipitation` is
the *preceding hour's* total, so the 3 readings overlap - **averaged, not summed**, or
rainfall would read ~3x too high. Also shifts `time` by +1h (`time_bucket` labels an
hour by its start; `load`/`lmp` use Hour Ending) to align join keys with
`stg_load`/`stg_lmp` - unverified against real data, worth spot-checking before training.

`feat_lmp_hourly` aggregates (`avg`) rather than passes through, even though there's
currently exactly one pnode per zone - keeps the model correct if PJM ever returns
multiple nodes per zone without changing downstream grain.

`pjm_weather_zones.csv` moved from `data/processed/` (gitignored, was untracked) into
`src/dbt/seeds/` - it's hand-maintained config (the `CE`/`BC` zone-code mapping), not a
model-ready feature table, and needs to be in git to be a valid dbt seed at all.
