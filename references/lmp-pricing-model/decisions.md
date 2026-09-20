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

**Title: Structural shift-factor proxy deprioritized — CEII caps it at ~24%; empirical regression is now primary, Author: Angie Ohaeri (assisted), Date: August 23rd Time: (session)**

`raw_lmp.facilities` geocodes monitored/contingency facilities (schema.md), but only
23.8% of constraint-hours land real coordinates, errors concentrated in the
highest-frequency facilities. PJM's system map would fill the gap but is CEII-restricted
by PJM's own published policy — confirmed via web search, not distributable.

**Decision:** empirical regression (zone `congestion_price` on facility shadow prices)
is now primary, not fallback — no coordinates needed, and it's the exact relationship
(`πᵢ = λ + ΣₖAᵢₖµₖ`), not just a proxy. Watch for: dimensionality (needs Lasso/Ridge),
long-tail sparsity, topology drift (rolling refit), leakage (walk-forward only).

## Data source scope

**Title: Backfilled Generation by Fuel Type, Day-Ahead Transmission Constraints, and Day-Ahead Hourly LMPs via gridstatus only; deferred the rest of Tier 1, Author: Angie Ohaeri (assisted), Date: August 23rd Time: 3:31pm**

Of the six remaining Tier 1 sources considered, three had direct gridstatus support
(`get_fuel_mix`, `get_transmission_constraints_day_ahead_hourly`,
`get_lmp(market="DAY_AHEAD_HOURLY", location_type="ZONE")`) — pulled and backfilled
into `raw_lmp` in this pass (see `schema.md` for row counts/date ranges).

Deferred:
- **Operator Initiated Commitments, Scheduled Generation, Generation and EHV Losses** —
  no gridstatus method for any of these. Held until it's worth building a new direct
  PJM Data Miner 2 REST-API pattern. *(Superseded same-day — see the entry below: a new
  pattern turned out unnecessary.)*
- **EIA natural gas fuel cost** — not a PJM source at all (EIA API, monthly by state,
  different auth/pull mechanism entirely). Deliberately held out of this PJM-focused
  backfill pass. Still deferred.

**Title: Backfilled the 3 remaining Tier 1 sources via gridstatus's private `_get_pjm_json`, not new raw-API plumbing, Author: Angie Ohaeri (assisted), Date: August 23rd Time: 4:51pm**

The "new direct Data Miner 2 REST-API pattern" flagged as needed above turned out
unnecessary. `gridstatus.PJM._get_pjm_json()` — the internal method every public
gridstatus PJM wrapper method already calls — takes an arbitrary Data Miner 2 feed name
and handles auth headers, rate-limit retries, and pagination generically. Calling it
directly with a feed name gridstatus hasn't wrapped in a public method works exactly
like the wrapped feeds, so there was no need to write a new HTTP client.

The missing piece was the internal feed names (Data Miner 2's UI shows human names like
"Operator Initiated Commitments", not the snake_case identifier the API expects). Found
via web search against PJM's public feed-definition pages
(`dataminer2.pjm.com/feed/<name>/definition`) — those pages are JS SPAs that don't
render their field lists to a fetch, so the actual columns were confirmed empirically
instead (a live pull with no `fields` filter, inspecting what came back), same as every
other source in this project. Feed names: `ops_init_commit`, `rt_and_self_ecomax`,
`gen_ehv_losses`.

This completes every Tier 1 source identified in this project except EIA natural gas
fuel cost (still deferred — not a PJM source).

**Title: Backfilled EIA natural gas fuel cost, also via a private client method rather than new plumbing, Author: Angie Ohaeri (assisted), Date: August 23rd Time: 5:59pm**

The last deferred Tier 1 source. Same shape of problem as the 3 PJM sources above:
`gridstatus.EIA.get_dataset()` only supports 5 hardcoded EIA v2 routes (all under
`electricity/rto/*` plus Henry Hub spot prices) — not `electric-power-operational-data`,
the route with the `cost-per-btu` metric this needed. Its `data` parameter is also
hardcoded to `["value"]`, which is wrong for this route (the metric name itself,
`cost-per-btu`, has to go in `data`). Rather than write a new HTTP client, called
`EIA._fetch_page()` directly — the actual paginated-GET helper every `get_dataset` call
already goes through — with a manually built `params`/`headers` pair matching what
`get_dataset` would have built for a supported route.

Discovered the route/facets by walking gridstatus's `EIA.list_routes()` /
`list_facets()` helpers interactively (`electricity` → `electric-power-operational-data`
→ facets `location`/`sectorid`/`fueltypeid`), not by reading EIA API docs externally —
those helpers hit EIA's own metadata endpoints, so they're authoritative and don't go
stale. `fueltypeid=NG` (natural gas, not `NGO` "natural gas & other gases"),
`sectorid=98` (Electric Power sector, broader than "1" Electric Utility — covers
independent power producers too, matching PJM's competitive generation mix).

This completes **every** Tier 1 source identified in this project, including the one
explicitly out-of-scope PJM source.
