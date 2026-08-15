# Decisions

Why I made certain decisions, for future reference.

## Project Pivot

**Title: Pivoted from bike-share availability to PJM electricity demand forecasting, Author: Angie Ohaeri, Date: August 4th Time: (session)**

- Bike-share is a saturated portfolio category; switched to short-term electricity load forecasting, same architecture (Kafka → TimescaleDB → dbt → LightGBM → FastAPI → Streamlit)
- Picked **PJM** over NYISO/MISO: NYISO needed no ISO account and kept the old Citi Bike geography, but PJM's larger, more heterogeneous market (more zones, more topology) makes a better engineering story. Tradeoff: a free PJM Data Miner account, and per-zone weather instead of one metro's feed
- Fallback if PJM proved too costly: cut down to a handful of zones rather than the full footprint — never needed, went to all 20 on August 12th

## Data Exploration

**Title: Expanded from 4 zones to 20, and chose weather stations by degree-hour regression rather than by map, Author: Angie Ohaeri, Date: August 12th Time: 11:10am**

Scope went from `CE`, `DOM`, `AEP`, `BC` to every PJM zone except `OVEC` (54 MW average, absent from EIA's subba list). `RTO` stays ingested but unmodelled — different grain.

**Not picked off a map.** Zones are legacy utility territories, not metros — e.g. `AP` (Allegheny Power) wraps around Pittsburgh without serving it (Pittsburgh is `DUQ`). Nearest-city-on-a-map would misassign it.

**Method.** 54 candidate stations, 2-4 per zone. Hourly temp 2023→present converted to degree-hours against a 65°F baseline (`cdh = max(tempF-65, 0)`, `hdh = max(65-tempF, 0)`) — raw temperature correlates near zero with load since load is U-shaped in it. Each candidate scored by *partial* R²: variance explained beyond hour-of-day/day-of-week. Full results in `data/interim/zone_city_scoring.csv`.

`AP` → Hagerstown MD (0.784) beat the intuitive pick, Morgantown WV (0.713). Every zone cleared 0.55, so none needed a finer grain.

**Composite stations (7 zones).** Averaging 2-3 stations beat the best single station by ≥0.02 partial R² for `PN`, `AEP`, `ATSI`, `DOM`, `AE`, `CE`, `DPL` — all multi-climate zones. It made compact single-metro zones (`PEP`, `PE`, `DEOK`, `RECO`) worse, confirming the metric isn't just rewarding more inputs. Those 13 stay single-station. This is why `pjm_weather_zones.csv` is one row per station, not per zone — the producer averages a zone's stations before publishing, so the `weather` hypertable grain stays one row per (time, zone).

**`CE` = Chicago + Joliet.** Joliet alone scored higher (0.734 vs 0.694) and its edge has widened every year (0.000 in 2023 → 0.069 in 2026), consistent with new temperature-sensitive load there. Chicago's lakefront station understates the HVAC swing inland. Keeping both beat either alone in every individual year.

## Database Design

**Title: `time` is Interval End (Hour Ending), in UTC, for `load` and `lmp`, Author: Angie Ohaeri, Date: August 10th Time: (session)**

- Switched from Interval Start to Interval End (PJM's "HE" convention) for industry consistency; both producers must agree since the `load`/`lmp` dbt join depends on it
- `lmp_producer.py` now converts US/Eastern → UTC (`load` was already UTC)
- Migrated existing rows (+1h shift, 189,164 `load` / 725,305 `lmp` rows) so old and new rows don't silently mix conventions
- Gotcha: a plain `UPDATE ... SET time = time + INTERVAL '1 hour'` fails partway through on TimescaleDB hypertables — chunks enforce their range via CHECK constraint, and UPDATE doesn't re-route rows across chunks like INSERT does. Fixed by copying to a staging table, truncating, and re-inserting

**Title: `lmp` retains all 23 PJM zones historically; `load` only ever has the 4 in-scope zones, Author: Angie Ohaeri, Date: August 10th Time: (session)**

*Superseded August 12th — `load` was backfilled to all 20 zones and `lmp`'s codes normalized. Durable part: producers scope at the query level, so `load` only ever stores what was asked for, while `lmp`'s historical bulk import predated scoping and pulled everything — hence `load` needed a backfill and `lmp` only a rename. Verified via `SELECT zone, count(*) ... GROUP BY zone`, not assumed.*

## dbt

**Title: Initial staging + features layer for load/lmp/weather, Author: Angie Ohaeri, Date: August 11th Time: (session)**

- dbt writes to its own `analytics` schema (`~/.dbt/profiles.yml`), separate from `public` (Kafka consumers) — avoids `--full-refresh` ever touching a raw table by name collision
- Layers: `staging` (1:1 with raw tables, views) and `features` (joins/window functions, tables) — "features" over dbt's usual "marts" since the consumer is a model trainer, not a BI analyst
- `stg_load`, `stg_lmp`, `stg_weather` are incremental (`delete+insert` on natural keys) with lookback windows rather than a plain `time > max(time)`, since PJM revises `is_verified` for ~3 days after publish and a plain filter would freeze revised rows at their first-seen value: 4 days for `load`, 2 for `lmp`, 1 for `weather`
- `stg_lmp`/`stg_weather` filter to `var("in_scope_zones")` (`dbt_project.yml`) to drop the 19 legacy zones in raw `lmp`. Zone scope is declared twice (the var, and the `pjm_weather_zones` seed used for the `relationships` test) — accepted as redundant for now
- `weather_features.sql`: Open-Meteo is polled ~3x/hour and each poll's `precipitation` is the *preceding hour's* total, so the 3 readings overlap — averaged, not summed. Also shifts `time` by +1h to align `time_bucket` (labels an hour by its start) with `load`/`lmp`'s Hour Ending — unverified against real data, worth spot-checking
- `lmp_features.sql` aggregates (`avg`) rather than passes through, even though there's currently one pnode per zone — keeps it correct if PJM returns multiple nodes per zone later
- `pjm_weather_zones.csv` moved from `data/processed/` (gitignored) into `src/dbt/seeds/` — it's hand-maintained config (the `CE`/`BC` zone-code mapping), not a model-ready feature table, and needs to be in git to be a valid dbt seed

## Dashboard

**Title: Dashboard framework switched from Streamlit to Shiny for Python, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- Original architecture specced Streamlit, but the actual scaffold (`src/dashboard/app.py`) and `pyproject.toml` were already built on Shiny for Python by the time this was caught — docs updated to match
- See `references/dashboard-design.md` for the full design (map, drill-down, accuracy panel, `/history` endpoint)

**Title: PJM zone polygons rebuilt from real HIFLD utility-territory data, not inferred from substations, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- No public PJM zone shapefile exists. First attempt matched substation codes to HIFLD substation points by name, then built zones via Voronoi tessellation — looked wrong at every iteration (ballooned into the ocean, filled whole states, or looked like flowers once bounded)
- Replaced entirely: HIFLD's "Electric Retail Service Territories" (real drawn utility boundaries, EIA/ORNL sourced) tags PJM membership via `CNTRL_AREA LIKE '%PJM%'`. Pulled from ArcGIS FeatureServer, saved to `data/external/hifld_electric_retail_service_territories_pjm.geojson`
- Of 312 PJM-tagged polygons, ~28 major Transmission Owners are matched to zones via `MAJOR_UTILITY_TO_ZONE`; the rest are small municipal utilities/co-ops. EKPC (a wholesale co-op with no HIFLD entity of its own) is mapped via its 16 member distribution co-ops
- Minor entries fold into whichever major zone they overlap most, with a tightly capped (3km) nearest-neighbor fallback — an uncapped fallback wrongly reattached orphaned enclaves, and minor entries above ~1,000 km² are often large independent co-ops (not gap-filling slivers) that read as geometry bugs if folded in wholesale
- Final zones clipped to Natural Earth's 10m land polygon, since HIFLD boundaries follow jurisdictional lines that sometimes run past the real shoreline
- Script: `_archive/scripts/build_pjm_zone_geometry.py`. Output: `data/external/pjm_zones.geojson` (20 zones). `CE`/ComEd needed a separate join path (bus-model codes use an unrelated numeric scheme) but HIFLD's `COMMONWEALTH EDISON CO` entry covers it directly
- `electric_substation_hifld_v4.gpkg` and `lmp-bus-model.xlsx` are no longer used by any pipeline step, kept in `data/external/` in case useful later
- Two follow-on fixes from visual review against PJM's published zone map:
  - **Overlapping zones:** HIFLD polygons aren't mutually exclusive (e.g. VA co-ops folded into DOM also spill ~hundreds of km² into AEP's polygon), rendering as darker stacked patches. Fixed with a smallest-first sequential carve-out over all 257 territories before dissolve — cut the worst overlap (AEP/DOM) from 8,421 km² to under 2.5 km²
  - **Real gaps:** central/western VA and the Southern MD peninsula were blank — large rural co-ops with no PJM zone of their own, dropped by the same >1,000 km² threshold that fixed a Michigan sliver elsewhere. Unlike that sliver, these read as part of a neighboring zone on PJM's map, so mapped directly in `MAJOR_UTILITY_TO_ZONE` (DOM for the VA ones + Southern MD, DPL for Choptank per its empirical overlap)

## Infra

**Title: Nightly DB backup flow, synced to Google Drive via rclone, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- No backup existed before — single TimescaleDB instance on the Mac Mini was the only copy of ~3.6 years of history (~880MB, growing ~250MB/year)
- `src/prefect/db_backup.py`: nightly (03:00) `pg_dump -Fc` of `gridcast` to `backups/` (named Docker volume `backup_data`), pruned in-flow to the last 14 dumps
- Prefect over a bare cron job for consistency — every other scheduled job in this project already goes through Prefect
- `rclone sync` ships `backups/` to Google Drive (`RCLONE_REMOTE` env var) right after the local prune, so remote retention mirrors local automatically
- rclone's Google Drive OAuth token is set up once on the host (`rclone config`) and bind-mounted read-only into the container — never passed through `.env`/committed, since it's a live credential
- `postgresql-client` and `rclone` added to the `python:3.12-slim`-based image for this

**Title: docker-compose filled out with mlflow, api, dashboard, cloudflared; train.py wired into Prefect, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- `mlflow` service reuses `gridcast:latest` (already has `mlflow`/`psycopg2-binary`) rather than the official image — one fewer image to track
  - backend-store: Postgres on the shared timescaledb instance (`mlflow` db, created manually since docker-entrypoint-initdb.d only runs against an empty volume)
  - artifact-store: plain named volume (`mlflow_artifacts`) — no S3/GCS needed at this scale
  - `mlflow` db is now covered by the nightly `db_backup` flow; the artifact volume still isn't (files, not a database)
- `api` (FastAPI) and `dashboard` (Shiny) added as their own services; `dashboard` talks to `api` over `http://api:8000` (`API_URL`), matching the existing host-vs-container-hostname split
- `cloudflared` fixed: previously referenced an undefined service/network — now depends on `dashboard`/`api` on the same `kafka-net`
- `train.py` got `@flow` on top of `@app.command()` (same pattern as producers/consumers) and a weekly (Sun 04:00) Prefect deployment — cadence is a placeholder pending a real read on drift
- `db_backup.py` extended to also dump the `mlflow` db (same instance/credentials); dumps/retention tracked per-database by filename prefix so they don't share a retention count

**Title: db_backup deployment disabled pending RCLONE_REMOTE/rclone setup; train.py auto-promotes to Production, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- `db_backup` isn't wired into `deployments.py` yet (import + `.to_deployment()` commented out) — local `.env` is missing `RCLONE_REMOTE`, so it would `KeyError` on first run. Re-enable once rclone is configured on the host
- `train.py`: added `promote_if_better()` — compares new test RMSE against current Production version, promotes only if lower (or nothing's in Production yet). A worse model stays in Staging instead of silently regressing what's served
- `train.py`: test-split metrics now prefixed `test_rmse`/`test_mae`/`test_r2` (were bare `rmse`/`mae`/`r2`, ambiguous next to `train_r2`/`val_r2`)
- `train.py`: now logs `row_count` and a `source_table: analytics.features` tag, since `dataset()` does an unbounded `SELECT *` with no other record of how much data a run trained on
- `train.py`: per-zone MAE/MAPE (~40 entries) moved out of the flat `metrics` dict into `mlflow.log_table(..., artifact_file="per_zone_metrics.json")` — keeps the metrics tab to real aggregates
- Considered logging `data.dvc`'s hash for provenance — skipped, since `dataset()` pulls live from `analytics.features` (not DVC-tracked, changes continuously). `row_count`/date-range tags are the meaningful stand-in given a live-DB training source

**Title: mlflow server needs --allowed-hosts or every non-localhost request 400s, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- mlflow ≥2.14 validates the `Host` header by default and rejects anything not explicitly allowed — hitting the UI via the Tailscale hostname returned "Invalid Host header" even though the container was healthy
- Fixed with `--allowed-hosts "localhost:*,127.0.0.1:*,mlflow:*,${KAFKA_TAILSCALE_HOST}:*"` — needs both the container-internal hostname (`mlflow`, used by `api`/`train.py`) and the Tailscale hostname, or fixing browser access would've broken in-cluster clients

**Title: mlflow container looked deadlocked on startup for ~2 days - it was slow bytecode compilation, not a hang, Author: Angie Ohaeri, Date: August 14th Time: (session)**

- Right after the `--allowed-hosts` fix, `mlflow` went "unhealthy" for 4-8+ minutes on every restart. Ruled out with evidence: DB/network (connections succeeded instantly, no blocking locks), workers, the new Host-header middleware, resource starvation (`docker stats` showed 0.05% CPU), uvloop (not installed)
- Same command/version against a real local Postgres started clean in ~2s outside the container, narrowing it to something about this Linux/Docker environment
- Root cause via `py-spy dump --pid 1` (needed temporary `cap_add: SYS_PTRACE`): the main thread was mid-`import pandas`, inside CPython's `.pyc` bytecode-cache step. Every container recreate starts from a fresh writable layer, so `.pyc` caches never persist — every restart recompiled pandas + mlflow's submodules from scratch, and competing for disk I/O with ~10 other containers stretched a normally sub-second cost into minutes
- Fixed at the source: `Dockerfile`'s `uv sync --frozen --no-dev` → `--compile-bytecode`, baking `.pyc` files into the image layer at build time. Applies to every service on `gridcast:latest`, not just `mlflow`
- Diagnostic-only flags (`--disable-security-middleware`, `cap_add: SYS_PTRACE`) reverted once root-caused
