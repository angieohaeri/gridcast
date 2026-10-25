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

**Title: Real-time target moved to verified hourly, nodal; 5-min unverified pipeline paused, Author: Angie Ohaeri (assisted), Date: October 22nd Time: 8:49pm**

Supersedes the real-time half of the two entries above (5-min target, served from the
unverified feed). The day-ahead hourly target is unchanged.

- **New target**: verified real-time hourly LMP (`rt_hrl_lmps`) at pnode level, for a GNN
  over the 1,505-pnode node set in `raw_lmp.pnode_topology_nodes` - stored in
  `raw_lmp.rt_hrl_lmps_nodal` (see `schema.md`). Real time, not day-ahead, because
  congestion spikes happen in real time; that propagation is what a graph model adds.
  Hourly, not 5-min: 2 years × 1,505 pnodes is already 25.7M rows, and verified is what
  training/backtesting should use anyway (the reconciliation entry above).
- **Kept current by** `sync_rt_hrl_lmps_nodal` in `raw_lmp_sync.py` - daily Prefect
  deployment, 7-day trailing upsert, ~11 spaced requests.
- **Paused, not deleted**: the `lmp_rt_unverified_fivemin` Kafka producer/consumer -
  `paused=True` on both deployments in `lmp_deployments.py`, and both containers behind
  the `fivemin` compose profile. Reasons: it's zonal only, so it doesn't feed a nodal
  model; and it pages the full all-pnode feed every 10 min (~60-70 s of requests), about
  a third of the API key's shared 6 requests/min - the likely cause of rate-limit retries
  in the other pulls. `raw_lmp.lmp_rt_unverified_fivemin` keeps what it already holds.
- **Cost of the reversal**: verified hourly posts ~1-2 days late, so the model can't see
  the last few hours' prices at prediction time. That makes it a backtest model, or one
  forecasting 2+ days out, rather than a live intraday one. Undo path for live serving:
  unpause the 5-min pipeline and repurpose its client-side filter from `type == ZONE` to
  the GNN node set - it already downloads every pnode each poll.

**Title: Live inputs from unverified hourly LMPs, verified still the training truth, Author: Angie Ohaeri (assisted), Date: October 23rd Time: 6:30pm CT**

Amends the entry above, whose cost was that verified hourly posts 1-2 days late, so the
model couldn't see recent prices at prediction time. Verified-only isn't actually
real-time: at 6 PM the newest row was the previous midnight.

- **Decision**: also collect PJM's `rt_unverified_hrl_lmps` (hourly, ~20 min lag) into the
  same `raw_lmp.rt_hrl_lmps_nodal` table with `is_verified = false`. The daily verified sync
  keeps running and overwrites those rows with settled values; unverified never overwrites
  verified. The 2-year verified backfill is kept as-is - not re-pulled.
- **Why not unverified-only**: unverified values revise (20 of 3,008 re-pulled pnode-hours
  moved by more than $0.01), and the feed keeps only 30 days. This is the original
  reconciliation rule (serve from unverified, train/backtest on verified), applied at pnode
  level and hourly instead of zonal and 5-min.
- **Why the same table, not a separate one**: live queries need "latest value per
  pnode-hour", which one table with a flag gives directly; training filters
  `is_verified`. Same pattern as `load`.
- **Cost**: one request per hourly run (every location, ~14.5k rows/hour - the feed has no
  pnode filter), ~24/day - versus the paused 5-min pipeline's ~2/min. The 5-min pipeline
  stays paused: this replaces it for live inputs.
- **Watch**: a sync outage longer than 30 days loses those hours from the unverified feed;
  they'd still arrive as verified a day or two later, so only live freshness is lost.

**Title: 5-min unverified pipeline removed, Author: Angie Ohaeri (assisted), Date: October 24th Time: 11:19am CT**

Follows the two entries above: the hourly unverified feed replaced it for live inputs, so
the paused pipeline was deleted rather than kept. Removed: the producer/consumer scripts,
both Prefect deployments, both compose services and Kafka topics (+ DLQ), the table
`raw_lmp.lmp_rt_unverified_fivemin` (86,040 zonal rows, 2026-09-05 → 09-20) and its DDL,
and `_archive/scripts/backfill_lmp_rt_unverified_fivemin.py`. The code is in git history
if live 5-min data is ever needed again; the data is not recoverable from PJM past its
~15-day retention.

**Title: First GNN: next-day 24-hour horizon, RT minus DA spread target, Author: Angie Ohaeri (assisted), Date: October 24th Time: 11:06am CT**

Settles the open "forecast horizon N" question for the pnode-level GNN.

- **Horizon**: one forecast per day, issued at 14:30 ET on day D (after DA results for D+1
  post ~1:30pm ET and the `da_hrl_lmps_nodal` sync runs), covering every hour of D+1 at
  every node - 11 to 35 hours ahead. Mirrors the actual market decision (bid DA knowing
  tomorrow's DA prices; profit is the RT-DA spread), and puts the DA backfill to work: all
  DA prices for the target day are known at issue time. Chosen over intraday 1-6h, where
  last-hour persistence is a hard baseline to beat and every forecast depends on the hourly
  unverified feed.
- **Target**: per node, `rt_lmp - da_lmp` for the same hour; the RT forecast is reported as
  DA + predicted spread. Centered near zero (DA already explains the price level), and it is
  the trader-relevant quantity. Train on verified RT only (`is_verified`). Spikes remain as
  large spreads, so use a robust loss (e.g. Huber) rather than plain MSE.
- **Feature cutoff (as-of 14:30 ET on D)**: RT unverified through hour ending ~14:00 ET on
  D; RT verified through D-1 at best; DA LMP for D and D+1; RT shadow prices only through
  D-1 (posts daily for the prior day); hourly fuel mix through ~13:00 ET. DA constraints /
  DA shadow prices for D+1 post around the same time as DA LMPs - confirm they are in by
  14:30 before using them for D+1. Every feature model filters on this cutoff.
- **Evaluation**: walk-forward splits; metrics on RT LMP in $/MWh (MAE overall, plus error
  on spike hours, e.g. top 1% of |spread|) so any target choice stays comparable.
  Baselines to beat: spread = 0 (RT = DA), and same-hour spread from the most recent
  complete day.
- **Revisit later**: intraday horizons as a second model once this one works; the
  decomposed target (λ + per-node congestion/loss) if the spread target struggles.

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

## Baselines for the next-day RT-DA spread

**Title: Baseline MAE on the per-pnode spread: last day's spread is worse than predicting zero, Author: Angie Ohaeri (assisted), Date: October 25th Time: 2:42am UTC**

Source: `analytics_lmp.lmp_baseline_features`, verified rows with a non-null
`spread_last_verified_day`, test window 2026-04-01 onward (7,379,876 pnode-hours). Neither
baseline is fit, so no train split is needed. Spike = top 1% of |spread| (≥ $342/MWh).

| | spread = 0 | last verified day's spread |
|---|---|---|
| MAE ($/MWh) | 25.0 | 40.1 |
| median AE | 7.8 | 11.6 |
| spike MAE | 686 | 672 |

- Last-day spread loses to zero in every month since 2024-10 (e.g. 2026-01: 54 vs 75),
  so the spread has no day-to-day persistence; copying it adds the previous day's noise.
- Spike hours are 1% of rows but 28% of total absolute error, so spike MAE matters as much
  as overall MAE. Both baselines are equally bad there (~$680), the bar for the GNN.
- The GNN's bar is spread = 0 on overall MAE and either baseline on spike MAE.
