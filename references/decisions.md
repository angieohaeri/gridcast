# Decisions

Why I made certain decisions, for future reference.

## Project Pivot

**Title: Pivoted from bike-share availability to PJM electricity demand forecasting, Author: Angie Ohaeri, Date: August 4th Time: (session)**

- Bike-share demand forecasting is a saturated portfolio project category; switched to short-term electricity load forecasting, same architecture (Kafka → TimescaleDB → dbt → LightGBM → FastAPI → Streamlit)
- Picked **PJM** over NYISO and MISO:
  - NYISO would've kept geographic continuity with the old Citi Bike framing and needed no ISO account
  - PJM's larger, more heterogeneous market (more zones, more topology) makes for a more interesting engineering story
  - Tradeoff: a free PJM Data Miner account, and per-zone weather features instead of one metro's weather feed
- Fallback if PJM's scope proves too costly: cut down to a handful of zones rather than modeling the full footprint — the interesting engineering is in the prediction-log and scoring layer, not breadth of coverage *(never needed; went to the full 20 on August 12th)*

## Data Exploration

**Title: Expanded from 4 zones to 20, and chose weather stations by degree-hour regression
rather than by map, Author: Angie Ohaeri, Date: August 12th Time: 11:10am**

Scope went from `CE`, `DOM`, `AEP`, `BC` to every PJM zone except `OVEC` (54 MW average,
and absent from EIA's subba list). `RTO` stays ingested but unmodelled — different grain.

**Why not pick weather cities off a map.** Zones are legacy utility territories, not
metros. `AP` (Allegheny Power) sprawls across southwestern PA, western MD and most of WV,
and wraps around Pittsburgh without serving it — Pittsburgh is `DUQ`, a separate zone. A
nearest-city-on-the-map approach lands `AP` on a metro it doesn't cover.

**How they were chosen instead.** 54 candidate stations, 2-4 per zone. For each, hourly
temperature 2023→present was converted to degree hours against the 65°F utility baseline
(`cdh = max(tempF-65, 0)`, `hdh = max(65-tempF, 0)`) — raw temperature correlates near
zero with load because load is U-shaped in it, climbing in both heat and cold. Each
candidate was then scored by *partial* R²: how much variance it explains beyond hour-of-day
and day-of-week, which otherwise dominate and are identical across candidates for a given
zone. Full results in `data/interim/zone_city_scoring.csv`.

`AP` came out on Hagerstown MD (0.784); Morgantown WV, the intuitive pick, ranked last of
four (0.713). Every zone cleared 0.55, so no zone needed splitting into a finer grain.

**Why seven zones use composite stations.** Averaging 2-3 stations beat the best single
station by ≥0.02 partial R² for `PN` (+0.052), `AEP` (+0.045), `ATSI` (+0.034), `DOM`
(+0.032), `AE` (+0.032), `CE` (+0.026) and `DPL` (+0.023) — all zones spanning multiple
climates. It made four compact single-metro zones *worse* (`PEP` −0.014, `PE` −0.009,
`DEOK` −0.002, `RECO` −0.001), which is a useful sign the metric isn't just rewarding more
inputs. Those 13 stay single-station.

This is why `pjm_weather_zones.csv` is one row per station rather than per zone. The
producer averages a zone's stations before publishing, so the `weather` hypertable grain is
unchanged at one row per (time, zone), and `zone_id` is no longer unique in the seed.

**`CE` = Chicago + Joliet specifically.** Joliet alone scored higher than Chicago (0.734 vs
0.694), and its edge has widened every year — 0.000 in 2023, 0.069 in 2026 — consistent
with new temperature-sensitive load in the area. Chicago's lakefront station understates
the temperature swing driving HVAC load across ComEd's inland collar counties. Keeping both
holds the metro population signal while picking up the inland swing, and beat either alone
in every individual year.

## Database Design

**Title: `time` is Interval End (Hour Ending), in UTC, for `load` and `lmp`, Author: Angie Ohaeri, Date: August 10th Time: (session)**

- Switched from Interval Start to Interval End (PJM's "HE" convention) for industry consistency; both producers must agree since the `load`/`lmp` dbt join depends on it
- `lmp_producer.py` now converts US/Eastern → UTC (`load` was already UTC)
- Migrated existing rows (+1h shift, 189,164 `load` / 725,305 `lmp` rows) so old and new rows don't silently mix conventions
- Gotcha: a plain `UPDATE ... SET time = time + INTERVAL '1 hour'` fails partway through on TimescaleDB hypertables — chunks enforce their range via CHECK constraint, and UPDATE doesn't re-route rows across chunks like INSERT does
- Fixed by copying to a staging table, truncating, and re-inserting

**Title: `lmp` retains all 23 PJM zones historically; `load` only ever has the 4 in-scope zones, Author: Angie Ohaeri, Date: August 10th Time: (session)**

*Mostly superseded August 12th — `load` was backfilled to all 20 zones and `lmp`'s codes
normalized. The durable part: producers scope at the query level, so `load` only ever stores
what the producer asked for, while `lmp`'s historical bulk import predated any scoping and
pulled everything. That asymmetry is why the two tables needed different remediation —
`load` a backfill, `lmp` only a rename. Verified by `SELECT zone, count(*) ... GROUP BY
zone`, not assumed.*

## dbt

**Title: Initial staging + features layer for load/lmp/weather, Author: Angie Ohaeri, Date: August 11th Time: (session)**

- dbt writes to its own `analytics` schema (`~/.dbt/profiles.yml`), separate from `public` where the Kafka consumers write raw tables — avoids a `--full-refresh` ever touching a raw table by name collision
- Layer naming: `staging` (1:1 with raw tables, `+materialized: view`) and `features` (joins/window functions, `+materialized: table`) — "features" chosen over dbt's usual "marts" since the consumer is a model trainer, not a BI analyst
- `stg_load`, `stg_lmp`, `stg_weather` are incremental (`delete+insert` on natural keys) with lookback windows on the incremental filter, not a plain `time > max(time)`:
  - 4 days for `load` (PJM revises `is_verified` for ~3 days after publish)
  - 2 days for `lmp`
  - 1 day for `weather`
  - a plain "newer than" filter would freeze revised rows at their first-seen value
- `stg_lmp`/`stg_weather` filter to `var("in_scope_zones")` (`dbt_project.yml`) to drop the 19 legacy zones sitting in raw `lmp` (see prior entry)
  - zone scope is declared twice — the var (used in `WHERE` filters) and the `pjm_weather_zones` seed (used as the `relationships` test target) — accepted as redundant for now rather than one driving the other
- `weather_features.sql`:
  - Open-Meteo is polled ~3x/hour and each poll's `precipitation` is the *preceding hour's* total, so the 3 readings overlap — **averaged, not summed**, or rainfall would read ~3x too high
  - shifts `time` by +1h (`time_bucket` labels an hour by its start; `load`/`lmp` use Hour Ending) to align join keys with `load_features`/`lmp_features` — unverified against real data, worth spot-checking before training
- `lmp_features.sql` aggregates (`avg`) rather than passes through, even though there's currently exactly one pnode per zone — keeps the model correct if PJM ever returns multiple nodes per zone, without changing downstream grain
- `pjm_weather_zones.csv` moved from `data/processed/` (gitignored, was untracked) into `src/dbt/seeds/` — it's hand-maintained config (the `CE`/`BC` zone-code mapping), not a model-ready feature table, and needs to be in git to be a valid dbt seed at all
