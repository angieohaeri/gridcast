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

## Nodal LMP granularity

**Title: Investigated whether richer-than-zone LMP/load data is available and unused; added two new sources, deliberately left paused and unbackfilled, Author: Angie Ohaeri (assisted), Date: October 4th Time: (session)**

Trigger: Data Miner 2's web UI for "Day-Ahead Hourly LMPs" shows full nodal detail —
thousands of individual pricing nodes (`ALDENE`, `ATHENIA`, `BELLEVIL`, ... in PSEG),
each with its own `Pricing Node ID`/`Voltage`/`Equipment`/`Pricing Node Type`
(LOAD/GEN/ZONE/etc) — while every LMP table in this project (`public.lmp`,
`raw_lmp.lmp_da_hourly`, `raw_lmp.lmp_rt_unverified_fivemin`) only has 20-23 distinct
`zone` values. Checked whether the recent raw_lmp/lmp_model work already changed this
before assuming the gap still existed — it didn't:

- `lmp_producer.py` → `rt_hrl_lmps`, `location_type="ZONE"`.
- `raw_lmp_sync.py`'s `poll_lmp_da_hourly` → `da_hrl_lmps`, `location_type="ZONE"`.
- `raw_lmp_lmp_rt_unverified_fivemin_producer.py` → pulls `rt_unverified_fivemin_lmps`
  fully unfiltered (its own comment: "the feed returns every pnode type - BUS/
  AGGREGATE/EHV/etc, not just ZONE"), then filters to `type == "ZONE"` client-side.

So the nodal detail isn't missing because it's inaccessible — every one of these three
pulls already receives it (or could with one param change) and throws it away. This was
never an explicit decision in this file; it's just `lmp_producer.py`'s original
zone-only pattern, inherited by every source added after it, including everything built
on this branch.

**Confirmed via gridstatus + a live test pull**, not assumed:
`PJM.location_types` includes `LOAD`, `GEN`, `AGGREGATE`, `ZONE`, `HUB`, `EHV`, `TIE`,
`INTERFACE`, `EXT`, `RESIDUAL_METERED_EDC`. `da_hrl_lmps`/`rt_hrl_lmps` accept
server-side `type=*LOAD*` filtering; `rt_fivemin_hrl_lmps` (verified) and
`rt_unverified_fivemin_lmps` only filter client-side (confirmed in gridstatus's own
`get_lmp` source — a documented PJM API limitation, not a gridstatus gap). A one-hour
test pull of `da_hrl_lmps`, `type=*LOAD*` returned exactly the nodes the UI showed
(`ALDENE`/`ATHENIA`/`BELLEVIL`, zone `PSEG`, `voltage`/`equipment` intact) and
**11,751 distinct LOAD-type pnodes** — matches gridstatus's own `get_lmp` docstring
("over 10,000 unique pnodes") exactly, not a surprise in retrospect but not something
this project had verified first-hand before.

Auth: no new credentials needed. Every nodal pull above goes through the same
`PJM_API_KEY` / `gs.PJM(api_key=...)` every other raw_lmp source already uses — nodal
granularity on Data Miner 2 is not member-gated differently from zone-level, unlike
PJM's large-load interconnection queue (checked per-feed, not assumed from that one
known exception).

**Load's equivalent**: PJM has no nodal load feed — load is metered per utility
sub-area, not per pricing node, so there's no load analog to LMP's pricing nodes.
But `hrl_load_metered` (the feed `load_producer.py` already pulls) carries a `Load Area`
field that `load_producer.py` sums away into one zonal total before it ever reaches
`public.load` — same shape of problem, a feed already being pulled that's coarser than
necessary at the point it lands, not a missing feed.

**Decision: added two new raw_lmp sources, both deliberately left `paused=True` and
unbackfilled** (full detail: `schema.md`; code: `raw_lmp_sync.py`, `lmp_deployments.py`,
`lmp_model_schema.sql`):

1. **`raw_lmp.lmp_da_hourly_nodal`** — `da_hrl_lmps`, `type=*LOAD*`. Day-ahead only, not
   real-time, as the lower-risk starting point (no Kafka, no settlement-target
   implications). Pulled via `PJM._get_pjm_json()` directly rather than gridstatus's
   `get_lmp(location_type="LOAD")` — `get_lmp()` drops the `equipment` field and does a
   second API call merging in pnode metadata, which hit PJM's rate limiter on its own
   during a single day's test pull of this feed; calling the feed directly (same pattern
   already used for `ops_init_commit`/`rt_and_self_ecomax`/`gen_ehv_losses`) avoids both
   and keeps `equipment`/`voltage`.

   **Not backfilled, deployment paused — not a smaller version of the other Tier 1
   sources.** 11,751 LOAD pnodes × 24h ≈ 282k rows/day just for day-ahead. A test pull of
   one day alone (6 pages of 50k rows) hit PJM's rate limiter on the last page even with
   `retries=3`; this project's standard `POLL_WINDOW_DAYS=7` trailing-window pull would
   be ~2M rows (~40 pages) per run — two orders of magnitude more than any existing
   raw_lmp source, and in the same territory that forced `lmp_rt_unverified_fivemin`'s
   backfill script to hand-roll chunked pagination with cooldown retries instead of
   trusting gridstatus's default pagination. Backfilling to 2023-01-01 (matching the
   other Tier 1 sources) extrapolates to **~350-400M rows**. Did not make that call
   unilaterally — needs an explicit decision on scope (all ~11,750 nodes vs. a specific
   zone or voltage-class subset; how far back) before either backfilling or unpausing.
   No backfill script written yet, on purpose — don't want one sitting in the repo
   inviting an accidental 400M-row pull before that decision is made.

2. **`raw_lmp.load_metered_by_area`** — `hrl_load_metered`, keeping `Load Area` instead
   of summing it away. Confirmed empirically: 672 rows/day, 28 distinct `(zone,
   load_area)` pairs across the 20 zones (e.g. `ATSI` reports `PAPWR` and `OE`
   separately). Low volume, no scope decision needed, no Kafka implications — just not
   backfilled yet because the deployment isn't validated, same as any other new source
   before its first unpause.

**Also surfaced, not acted on this pass**:
- `rt_fivemin_hrl_lmps` (verified real-time 5-min LMP) — gridstatus-wrapped
  (`get_lmp(market=REAL_TIME_5_MIN)`), currently unused anywhere in this project. Would
  match the RT target's actual 5-min grain, unlike the hourly-verified (`rt_hrl_lmps`)
  value the unverified→verified reconciliation step (above) implicitly relies on today.
  Worth a closer look before that reconciliation step is finalized, not added here —
  out of scope for a nodal-LMP investigation.
- PJM load forecast feeds (`get_load_forecast`, `get_load_forecast_historical`,
  `get_load_forecast_5_min`) — day-ahead (6-day hourly) and 5-min-ahead forecasts,
  gridstatus-wrapped, zone/forecast-area grain (no finer — forecasts don't exceed the
  metering grain they forecast). Not ingested in any form today; a leading indicator
  neither `load` nor `lmp` currently has. Flagged, not built — outside this
  investigation's LMP/load-granularity scope.
- Confirmed `get_instantaneous_dispatch_rates(date, end=...)` supports a real date-range
  pull, not point-sampling — resolves this file's own Tier 2 "unverified, check first"
  note on Instantaneous Dispatch Rates (lmp-pricing-model.md). Still not built, but the
  open question blocking it is now closed.
- Scanned gridstatus's full set of wrapped PJM feed names for anything LMP/load-adjacent
  beyond the above: five-minute/hourly solar and wind generation + forecasts (Tier 2,
  "planned" above — confirmed gridstatus already wraps all of them, lowering that row's
  remaining lift to "just build it"), interchange/reserve/ancillary-market feeds (Tier 3,
  lower priority, informs λ at best), transfer-interface/transmission-limit feeds
  (flagged in the table above as likely redundant with Marginal Value — still
  unconfirmed). None changed this pass's priority call: nodal LMP and load-area
  granularity were the two gaps directly responsive to what prompted this investigation.

**Open questions for whoever reviews this**, not decided here:
- `lmp_da_hourly_nodal` backfill scope (node set, history length) — blocks both
  backfill and unpause.
- Whether nodal granularity should eventually extend to `rt_hrl_lmps`/
  `rt_unverified_fivemin_lmps` too (same `type=*LOAD*`/client-side-filter mechanism
  would work for both) once the DA case proves out, or whether day-ahead nodal alone is
  enough for the shift-factor/congestion work this was originally motivated by.
- Storage/compute cost of ~350-400M additional rows at full nodal-DA-backfill scope —
  not estimated here (depends on the DB host's current headroom, not something this
  investigation had visibility into).
