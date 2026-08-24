# LMP Pricing Model — Decisions

Why I made certain decisions, for future reference. Extracted from `lmp-pricing-model.md`,
**Author:** Angie Ohaeri (assisted), **Date:** August 23rd Time: 3:31pm.

## Isolation

**Title: Isolate the LMP pricing model from the working load-forecasting stack at every layer, Author: Angie Ohaeri, Date: August 22nd Time: (session)**

- Branch: `lmp-pricing-model` (created).
- Raw tables → schema `raw_lmp` (created), never `public`. Droppable via `DROP SCHEMA ... CASCADE`.
- dbt: `models/lmp_features/` → `+schema: lmp` → lands in `analytics_lmp`, separate from
  `analytics.features` (scaffolded in `dbt_project.yml`).
- MLflow: new experiment/model names — never `gridcast-lgbm-{h}h` or its `Production` stage.
- Prefect: new deployments, `paused=True` until validated.
- Kafka: skip it for daily-batch feeds (most of these) — use the `data_center_sync.py`
  direct-upsert pattern instead. Reserve Kafka for genuinely streaming data.

## Targets

**Title: Two separate targets (day-ahead hourly, real-time 5-min), not one mixed grain, Author: Angie Ohaeri, Date: August 22nd Time: (session)**

Matches how PJM's markets actually clear — DA only clears hourly, RT settles 5-min since
April 2018, so blending them into one grain would misrepresent both.

- **Day-ahead LMP**: hourly. Reuses existing hourly lag/rolling/join architecture almost
  as-is. The easy one.
- **Real-time LMP**: 5-min. Bigger lift — new 5-min ingestion (~12x the row volume of
  hourly `lmp`), hourly-only features (load, weather) need explicit forward-fill onto
  the 5-min grid (not silent resampling), and instantaneous load / unverified 5-min LMP
  become load-bearing feature sources here rather than nice-to-haves. 5-min spikes are
  sharper and noisier than hourly averages — exactly where congestion-state features
  matter most (see `lmp-pricing-model.md` — Mechanism).

**Title: Unverified → verified reconciliation for real-time LMP, decided 2026-08-22**

Ingest Real-Time Unverified Five Minute LMPs continuously for freshness (updates ~every
5 min, vs. the verified feed's once-daily batch post — same `is_verified`-style upsert
pattern `load` already uses). Serve live predictions from unverified data — there's no
choice, verified data for "now" doesn't exist yet — but **train/backtest against the
verified value once it lands**, not the unverified value the model saw at inference
time, since unverified values can revise.

## Zone attribution / shift-factor proxy

**Title: Build the transmission graph from each line's own endpoint coordinates, not by name-joining substations, Author: Angie Ohaeri, Date: August 22nd Time: (session)**

`US_Electric_Power_Transmission_Lines.gpkg`'s `SUB_1`/`SUB_2` fields are never null,
which makes name-joining against the substations file tempting — but the graph is built
from each line's own endpoint coordinates directly instead, since that's the more
reliable source of truth for node identity.

**Title: Name-match fallback for the 511 point-in-polygon zone-attribution misses: ruled out, Author: Angie Ohaeri, Date: August 22nd Time: (session)**

Only 8.6% coverage, and it mis-assigns EKPC (Kentucky) substations to AEP via name
collisions. Nearest-zone stays the fallback instead.

## Data source scope

**Title: Backfilled Generation by Fuel Type, Day-Ahead Transmission Constraints, and Day-Ahead Hourly LMPs via gridstatus only; deferred the rest of Tier 1, Author: Angie Ohaeri (assisted), Date: August 23rd Time: 3:31pm**

Of the six remaining Tier 1 sources considered, three had direct gridstatus support
(`get_fuel_mix`, `get_transmission_constraints_day_ahead_hourly`,
`get_lmp(market="DAY_AHEAD_HOURLY", location_type="ZONE")`) — pulled and backfilled
into `raw_lmp` in this pass (see `schema.md` for row counts/date ranges).

Deferred:
- **Operator Initiated Commitments, Scheduled Generation, Generation and EHV Losses** —
  no gridstatus method for any of these. Every source ingested so far (including
  `inst_load`) has gone through gridstatus; pulling these would require a new direct
  PJM Data Miner 2 REST-API pattern this repo hasn't built yet. Held until that's worth
  building.
- **EIA natural gas fuel cost** — not a PJM source at all (EIA API, monthly by state,
  different auth/pull mechanism entirely). Deliberately held out of this PJM-focused
  backfill pass.
