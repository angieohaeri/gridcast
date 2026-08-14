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

## Dashboard

**Title: Dashboard framework switched from Streamlit to Shiny for Python, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- Original architecture (see Project Pivot entry above) specced Streamlit; the actual dashboard scaffold (`src/dashboard/app.py`) and `pyproject.toml` were already built on Shiny for Python by the time this was caught — docs (`CLAUDE.md`, `architecture.md`, `README.md`, `data-flow.md`, `best-practices.md`) updated to match
- See `references/dashboard-design.md` for the full dashboard design (map, drill-down, accuracy panel, new `/history` endpoint)

**Title: PJM zone polygons rebuilt from real HIFLD utility-territory data, not inferred from substations, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- No public PJM zone shapefile exists. First attempt matched `lmp-bus-model.xlsx` substation codes against `electric_substation_hifld_v4.gpkg` points by name, then built zone shapes via Voronoi tessellation over the matched points — looked wrong at every iteration (ballooned into the ocean past a convex hull, filled entire host states edge-to-edge, then looked like overlapping flowers once bounded to a buffered-point coverage mask)
- Replaced entirely: HIFLD also publishes "Electric Retail Service Territories" — real drawn utility boundaries (EIA/ORNL sourced) with a `CNTRL_AREA` field tagging PJM membership directly (`CNTRL_AREA LIKE '%PJM%'`, since some entries like Rockland Electric are dual-tagged `"PJM, NYIS"`). Pulled from the ArcGIS FeatureServer at `services3.arcgis.com/OYP7N6mAJJCyH6hd/.../Electric_Retail_Service_Territories_HIFLD`, saved to `data/external/hifld_electric_retail_service_territories_pjm.geojson`
- Of the 312 PJM-tagged polygons, ~28 are the major Transmission Owners gridcast tracks as zones (matched by canonical company name in `MAJOR_UTILITY_TO_ZONE`); the rest are small municipal utilities/co-ops. EKPC has no HIFLD entity of its own (it's a wholesale G&T co-op, not a retailer) — mapped instead via its 16 known owner-member distribution co-ops, which match HIFLD's KY entries exactly
- Minor entries are folded into whichever major zone they overlap most, with a *tightly capped* (3km) nearest-neighbor fallback for near-misses — learned the hard way that (a) an uncapped fallback reattaches genuinely orphaned enclaves to whatever tracked zone is merely closest, and (b) minor entries above ~1,000 km² are often large independent co-ops (e.g. Midwest Energy Cooperative, MI, 17,835 km²) rather than small gap-filling enclaves, and folding those in wholesale reads as a geometry bug (a long sliver hugging the Michigan shoreline up to Muskegon) rather than the real, odd-shaped territory it is
- Final zones are clipped to Natural Earth's 10m land polygon (`data/external/ne_10m_land/`) — HIFLD's polygons follow jurisdictional/county lines, which sometimes run out into a bay or lake past the real shoreline
- Script: `_archive/scripts/build_pjm_zone_geometry.py`. Output: `data/external/pjm_zones.geojson` (20 zones, matches every `zone_id` in `pjm_weather_zones.csv` except `CE`/ComEd needed a separate join path — its bus-model codes use a numeric-prefixed truncated scheme unrelated to any of this, but HIFLD's `COMMONWEALTH EDISON CO` entry covers it directly now)
- `electric_substation_hifld_v4.gpkg` and `lmp-bus-model.xlsx` are no longer used by any pipeline step, left in `data/external/` in case they're useful for something else
- Two follow-on fixes found by visual review against PJM's own published zone map:
  - **Overlapping zones ("double-filled" patches):** HIFLD's individual utility polygons aren't mutually exclusive - a big IOU's recorded territory often isn't clipped around a co-op enclave nested inside it (VA co-ops folded into DOM sit mostly inside `VIRGINIA ELECTRIC & POWER CO`'s own polygon, but also spill into `APPALACHIAN POWER CO`/AEP by several hundred km2), so the same ground gets claimed by two zones and renders as a darker stacked-alpha patch. Fixed with a smallest-first sequential carve-out over all 257 territories before the final dissolve: each entity's geometry has whatever's already claimed by a smaller entity subtracted first, so granular co-op boundaries win over a big utility's broad-brush outline. Cut the worst pairwise overlap from 8,421 km2 (AEP/DOM) to under 2.5 km2 everywhere (topology noise at shared edges, invisible at map scale)
  - **Real gaps between zones:** central/western VA (Shenandoah Valley/Piedmont, between DOM/AEP/AP) and the Southern MD peninsula along the Potomac were both blank - large rural co-ops (Rappahannock, Shenandoah Valley, Mecklenburg, Southside, Craig-Botetourt, Central Virginia, Community, BARC, Prince George, Northern Neck, A&N, Northern Virginia, Southern Maryland, Choptank) with no PJM zone of their own, dropped by the same >1,000 km2 minor-entity threshold that fixed the Michigan sliver. Unlike Midwest Energy Cooperative in MI, these read as part of their neighboring zone on PJM's own map, so they're mapped directly in `MAJOR_UTILITY_TO_ZONE` (DOM for the VA ones and Southern Maryland, DPL for Choptank per its empirical overlap) rather than left to the size-threshold/fold-in logic

## Infra

**Title: Nightly DB backup flow, synced to Google Drive via rclone, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- No backup existed before this - single TimescaleDB instance on the Mac Mini was the only copy of ~3.6 years of load/lmp/weather history (~880MB total, growing ~250MB/year)
- `src/prefect/db_backup.py`: nightly (`03:00`) `pg_dump -Fc` of the whole `gridcast` database to `backups/` (a named Docker volume, `backup_data`, mounted into `prefect-deployments`), pruned in-flow to the last 14 dumps
- Chose Prefect over a bare host cron job for consistency - every other scheduled job in this project (producers, consumers, dbt build) already goes through Prefect, so this gets the same retry/observability path for free rather than a second, invisible scheduling mechanism
- Local pruning alone still leaves the Mac Mini as a single point of failure (a disk failure takes out primary and backup together) - `rclone sync` ships the same `backups/` dir to Google Drive (`RCLONE_REMOTE` env var, e.g. `gdrive:gridcast-backups`) right after the local prune, so the remote copy mirrors local retention automatically rather than tracking it twice
- rclone's Google Drive OAuth token is set up once on the host (`rclone config`, headless flow via `rclone authorize` from a machine with a browser) and bind-mounted read-only into the container (`~/.config/rclone` → `/root/.config/rclone`) - never passed through `.env`/committed, since it's a live credential rather than project config
- `postgresql-client` (`pg_dump`) and `rclone` added to the `python:3.12-slim`-based image for this; not present in the base image

**Title: docker-compose filled out with mlflow, api, dashboard, cloudflared; train.py wired into Prefect, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- `mlflow` service reuses the same `gridcast:latest` image as every other service (already has `mlflow`/`psycopg2-binary` via `pyproject.toml`) rather than the official `ghcr.io/mlflow/mlflow` image - one fewer image to track, consistent with how producers/consumers/prefect-deployments all just vary `command:` on the same build
  - backend-store is Postgres on the shared timescaledb instance (`mlflow` db, created manually - not via an init script, since docker-entrypoint-initdb.d only runs against an empty volume and this instance already has data)
  - artifact-store is a plain named volume (`mlflow_artifacts`) - no S3/GCS-style object storage needed at this scale
  - the `mlflow` db is now covered by the nightly `db_backup` flow too (see below) - the artifact volume (`mlflow_artifacts`) still isn't, since that's files, not a database
- `api` (FastAPI) and `dashboard` (Shiny) added as their own services, also on `gridcast:latest` - `dashboard` talks to `api` over the internal `http://api:8000` hostname (`API_URL` env var), matching the host-vs-container-hostname split already used for `TIMESCALEDB_HOST`/`KAFKA_BOOTSTRAP_SERVERS`
- `cloudflared` uncommented and fixed: previously referenced an undefined `web-app` service and an undefined `tunnel-network` - now depends on `dashboard`/`api` and sits on the same flat `kafka-net` as everything else rather than a separate network
- `train.py` (`gridcast/modeling/train.py`) stayed a bare typer command with no way to run on a schedule - added `@flow` on top of `@app.command()` (same pattern as every producer/consumer) and registered it in `src/prefect/deployments.py` as a weekly (`Sun 04:00`) deployment; cadence is a placeholder pending an actual read on how much the model drifts week to week
- `src/prefect/db_backup.py` extended to also `pg_dump` the `mlflow` db (same timescaledb instance/credentials, `mlflow` hardcoded rather than a new env var since docker-compose already hardcodes that db name for the mlflow service) - dumps and retention (14) are tracked per-database by filename prefix (`gridcast_*.dump` / `mlflow_*.dump`) so the two don't share a retention count, then both get synced to the same rclone remote in one pass

**Title: db_backup deployment disabled pending RCLONE_REMOTE/rclone setup; train.py auto-promotes to Production, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- `db_backup` isn't wired into `src/prefect/deployments.py` right now (import + `.to_deployment()` call both commented out, `backup_data`/rclone volume mounts on `prefect-deployments` commented out too) - local `.env` is missing `RCLONE_REMOTE`, so the flow would `KeyError` the first time it fired. Getting the rest of the stack running first; re-enable all three spots together once rclone is actually configured on the host
- `train.py`: added `promote_if_better()`, called right after the existing Staging transition - compares the new version's test RMSE against whatever's currently in Production (via `MlflowClient.get_latest_versions(stages=["Production"])`) and promotes only if it's lower, or if nothing's in Production yet (the very first run). A worse model stays in Staging instead of silently regressing what `api`/`predict.py` serve
- `train.py`: `evaluate()`'s test-split metrics were logged under bare `rmse`/`mae`/`r2` right next to `train_r2`/`val_r2` - ambiguous once those exist side by side, so they're now prefixed `test_rmse`/`test_mae`/`test_r2` (moved the prefixing to the call site, not `evaluate()` itself, since it's also called for train/val where the split name differs)
- `train.py`: also now logs `row_count` (param) and a `source_table: analytics.features` tag on the parent run - `dataset()` does a bare unbounded `SELECT *`, so previously there was no record of how much data a given run actually trained on, just the date span
- `train.py`: per-zone MAE/MAPE (~20 zones × 2 = 40 entries) pulled out of the flat `metrics` dict and into its own `per_zone_metrics()` function, logged via `mlflow.log_table(..., artifact_file="per_zone_metrics.json")` instead of `log_metrics()` - keeps the metrics tab to the handful of real aggregate metrics, per-zone breakdown lives in its own sortable table artifact
- considered logging `data.dvc`'s hash for dataset provenance - skipped: `data.dvc` versions the `data/` directory (external/raw/processed files), but `dataset()` pulls from `analytics.features` in TimescaleDB directly, which isn't DVC-tracked and changes continuously via the Kafka→dbt pipeline. The hash wouldn't reflect what a given run actually trained on, so it'd be a false reproducibility signal - `row_count`/date-range tags are the meaningful stand-in given this project's live-DB training source
