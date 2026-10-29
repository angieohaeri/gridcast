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
  (The shadow-price and fuel-mix lags above were measured on a sync that stopped at D-1; see
  "The one-day lag in the PJM feeds" below.)
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

## GNN edge set

**Title: Correlation edges for nodes the topology data cannot connect, Author: Angie Ohaeri (assisted), Date: October 25, 4:49PM UTC**

`pnode_topology_edges` links only the 1,037 attached substations. The 426 fill-ins (ComEd:
277 fill-ins, 0 attached; DEOK 90/9; DAY 30/1; EKPC 29/5) and 42 aggregates had no edges, plus
23 attached substations with no geographic edge.

Check on hourly spread with the cross-node hourly median removed (so the system-wide λ
component doesn't make every pair look correlated), last year of data:
- Geographic edges are real: connected pairs median corr 0.96 vs 0.49 for random same-zone
  non-edges.
- Learned edges hold out of sample: top-3 correlated same-zone nodes picked on the first half
  of the year have median corr 0.98-0.99 on the second half (zone median 0.78-0.92); 92% of
  fill-ins stay above 0.8.

Decision: keep the geographic edges; every node without one is linked to its top-3 most
correlated real nodes in its zone (any zone when its zone has no real nodes: hubs, interfaces,
PJM-RTO, OVEC). Built by `lmp_model/graph.py:build_edges` → `data/processed/pnode_edges_<train_end>.parquet`
(undirected, `src < dst`, `kind` = geographic | correlation). Correlations use only
`[train_start, train_end)`, so the graph never sees the evaluation period; rebuild when the
train window changes. First build, train 2024-10-23 → 2026-04-01: 2,921 geographic + 1,091
correlation edges, no isolated nodes.

Caveat: top neighbors correlate ~0.99, so many fill-ins carry nearly the same price as a
neighbor and add little independent signal.

## PJM load forecast feed

**Title: Point-in-time PJM load forecast by area, history plus a live snapshot, Author: Angie Ohaeri (assisted), Date: October 25, 11:47PM UTC**

Replaces the EIA `demand_forecast_mw` idea: EIA publishes a day-ahead forecast for the RTO
total only (`analytics.stg_load`: 4.5% of rows, all `zone = RTO`). PJM's own feeds are zonal.

- **History**: `load_frcstd_hist` (Data Miner 2), back to 2011, one row per forecast PJM
  issued with its `evaluated_at`. 5 evaluations a day (~05:45, 09:45, 11:45, 17:45, 23:45
  ET), each covering now through the end of tomorrow. 11 areas: AEP, APS, ATSI, COMED, DAY,
  DEOK, DOM, DUQ, EKPC, MIDATL (all eleven mid-Atlantic zones combined), RTO.
- **Live**: `load_frcstd_7_day` has 25 areas (each mid-Atlantic zone separately) but only its
  latest evaluation, refreshed twice an hour. History lags about a day (checked 2026-10-25:
  newest evaluation 23:45 ET the night before), so at the 14:30 ET cutoff it has nothing
  covering tomorrow. `sync_load_forecast_live` (14:20 ET) snapshots it into the same table,
  mapped to the hist feed's 11 areas (`MID_ATLANTIC_REGION` = MIDATL). Checked against the
  hist feed on overlapping hours despite a 19h gap between evaluations: median ratio 1.00-1.03
  per area. Coarse areas are used for training and serving so they match.
- **Table**: `raw_lmp.load_forecast` (`evaluated_at`, `time` Hour Ending UTC, `forecast_area`,
  `forecast_load_mw`), a hypertable, never revised so DO NOTHING on conflict. Backfilled from
  2024-10-22: 1,450,647 rows (one-time load; the backfill code was removed from `raw_lmp_sync.py`).
- **dbt**: `lmp_load_forecast_features` keeps, per target hour and area, the latest evaluation
  issued at or before `forecast_cutoff_et` (14:30) on the ET day before the target day.
  `lmp_node_areas` maps each pnode to an area through the `lmp_load_forecast_areas` seed
  (hubs, interfaces and other zoneless aggregates take RTO). The loader indexes the area
  forecast by node rather than repeating it across 1,505 nodes.
- **Coverage**: every training hour has all 11 areas except 47 of ~17.6k (0.27%), left as NaN:
  the target hours of the DST spring-forward day in 2025 (03-09) and 2026 (03-08), 23 hours
  each, plus 1 hour on 2024-11-04. Checked at the source for 2025: PJM's history has only the
  23:45 ET evaluation for 2025-03-08, none of the daytime ones, so nothing issued by the
  cutoff covers the next day. The newest hours are covered only once the live snapshot has run.
- **Train/serve difference**: training uses the 11:45 ET evaluation (last hist one before the
  cutoff); serving uses the ~14:20 ET snapshot, slightly fresher.

## Weather forecast feed

**Title: Open-Meteo zone forecasts, previous-day history plus a live snapshot, Author: Angie Ohaeri (assisted), Date: October 26, 6:41PM UTC**

`raw.weather` holds polled observations, not forecasts, so the weather at a target hour is not
knowable at the 14:30 ET cutoff (using it would leak the future). Open-Meteo has the forecast:

- **History (training)**: the [Previous Model Runs API](https://open-meteo.com/en/docs/previous-runs-api),
  `*_previous_day1` variables: what the model forecast a day before each hour. Real values from
  at least 2024-06, so it covers the whole LMP window. Checked on 5 days of 2025-07 against
  observed temperature: MAE 1.0 C, bias +0.4 C.
- **Live (serving)**: the forecast API's hourly forecast, snapshotted daily at 14:25 ET (before
  the cutoff). Same default model selection as the history, same variables and units.
- **Zones**: the 30 stations of `pjm_weather_zones` collapse to their 20 zones by mean, as the
  weather producer does. `time` is Hour Ending UTC, the reading's clock time +1h, the same
  alignment `weather_features` uses for observations (precipitation is the preceding hour's
  total, so it is a little loose either way).
- **Table**: `raw_lmp.weather_forecast` (`time`, `zone`, `source`, `issued_at`, 4 variables), a
  hypertable. `issued_at` is the snapshot time for `live` and the nominal time (target - 24h)
  for `previous_day1`. Backfilled from 2024-10-22: 352,800 rows. `sync_weather_forecast` also
  refreshes the trailing 7 days of history. The backfill code was removed from `raw_lmp_sync.py`.
- **dbt**: `lmp_weather_forecast_features` takes, per target hour and zone, the live snapshot
  issued by the cutoff when there is one, else `previous_day1`, and adds HDD/CDD (base 18.33 C).
  Every training hour has all 20 zones. Zoneless nodes (hubs, interfaces, PJM-RTO, OVEC) get a
  weather index of -1 in the loader.
- **Train/serve difference**: at the cutoff the real lead for D+1 hours is 10-34h and
  `previous_day1` is ~24h, so evening hours are slightly fresher in training than in serving.

## Walk-forward folds

**Title: Four expanding-window folds with a validation block, Author: Angie Ohaeri (assisted), Date: October 26, 7:36PM UTC**

Chosen so that as the model is retrained it always trains on newer data: an expanding window per fold,
each test block following the last. `lmp_model/splits.py`, windows half-open: train
[train_start, val_start), validation [val_start, test_start), test [test_start, test_end).
Train starts 2024-10-23; test blocks are 3 months starting 2025-12-01 (moved from
2025-11-01 by Angie), 2026-03-01, 2026-06-01 and 2026-09-01 (partial, data ends
~2026-10-25); validation is the 2 months before each test block, held out of training for early stopping.
Correlation edges are rebuilt per fold from the training span only (`build_edges(train_start,
val_start)`), so neither validation nor test data shapes the graph. The edge file built for the
first draft (train end 2026-04-01) matches fold 3's window exactly and leaks into folds 1 and 2.

## Feature set for the first pnode model

**Title: What a node-hour sees at the 14:30 ET cutoff, Author: Angie Ohaeri (assisted), Date: October 26, 7:36PM UTC**

Four price columns were too thin and all from one source, so the first model gets: the DA LMP and its
congestion and loss parts at the target hour; the spread and congestion spread 72h earlier (the last
verified day's same hour); trailing 7-day mean, std and spike share of the verified spread (a spike is
|spread| ≥ $100, dbt var `lmp_spike_threshold`) and mean congestion spread, all ending at that same
hour; calendar (hour and month sin/cos, weekend, holiday; `holiday_flag` re-enabled in
`gridcast/features.py`); the PJM load forecast for the node's area; the weather forecast for the node's
zone. Plan the model as an ablation ladder (core price features, then history and calendar, then the
forecasts) with a per-node model on identical inputs beside the GNN, so the graph's contribution is
measurable. Still deferred, each a new dbt feature model: lagged binding-constraint count and shadow
prices, fuel mix, forecasted outages, gas price.

## dbt operations on the 26M-row tables

**Title: Index macro, literal incremental window, profiler exclusions, Author: Angie Ohaeri (assisted), Date: October 26, 7:36PM UTC**

- **Index survives `--full-refresh`**: dbt swaps a rebuilt table in by renaming the old one to a backup,
  which keeps the index name, so `create index if not exists` was skipped and the index went with the
  backup. `ensure_time_pnode_index()` (`src/dbt/macros/`) checks the table for the index and clears the
  name first. The index barely helps big window loads (the database returns a month in ~3s; pulling it
  into Python is ~15s) but makes single-day and single-node queries fast.
- **Incremental window as a literal**: `lmp_baseline_features` once ran 23+ minutes spilling a 26M-row sort
  to disk, because the planner cannot see how small a window taken from a subquery is, guessed 3.6M rows
  and merge-joined all of `lmp_spread_history`. The window is now resolved with `run_query` into a
  literal timestamp, so the planner uses the indexes (135s on the next run).
- **OpenMetadata profiler**: it includes the `analytics_lmp` schema and excluded only hypertables, so it
  ran percentile and uniqueness queries over every column of `lmp_spread`, `lmp_spread_history` and
  `lmp_baseline_features` (10-17 min each), starving the database. Those three tables and the new
  hypertables (`load_forecast`, `weather_forecast`) are now excluded in the profiler config. The
  image bakes `src/` in (`COPY src/ src/`), so config changes need an image rebuild; containers built
  before 2026-10-25 00:55 UTC had none of the exclusions.
- **Tooling**: `pytest` is in the `dev` dependency group (it was only in the pip `dev` extra, which
  `uv sync` ignores); ruff's config now covers `lmp_model` and treats it as first-party for import
  sorting. The OpenMetadata scripts moved to `src/prefect/openmetadata/`; `deployments.py` adds that
  folder to `sys.path` (same pattern as producers and consumers) so its bare sibling imports resolve.

## dbt schedule for the LMP model

**Title: Split dbt builds, one for the load stack and two for the LMP model, Author: Angie Ohaeri (assisted), Date: October 26, 7:53PM UTC**

The single daily `dbt build` (12:55 ET, whole project) had two problems once the LMP models existed:
it ran before the afternoon inputs for tomorrow (DA prices 14:05, load forecast 14:15/14:20, weather
snapshot 14:25), so the tables read at the 14:30 ET cutoff would lack them, and it dragged in the
slow uniqueness tests on the 26M-row tables. `src/dbt/selectors.yml` now defines three selectors,
used by both the Prefect flow (`dbt_build`, parameter `selector`) and `make`:

| selector | models | runs (ET) | why then |
|---|---|---|---|
| `load` | the original staging and features + `pjm_weather_zones` | 12:55 (`deployments.py`) | unchanged |
| `lmp_history` | `lmp_staging` views, `lmp_spread`, `lmp_spread_history` | `lmp_morning` flow, after the RT sync (12:15 start) | needs only verified RT/DA LMPs (RT sync 12:40); carries the slow tests |
| `lmp_serving` | `lmp_baseline_features`, load and weather forecast features, `lmp_node_areas`, areas seed | `lmp_afternoon` flow, after its syncs (13:50 start) | needs the afternoon syncs, must finish before 14:30 |

**Unproven timing**: run by hand on 2026-10-26 the forecast and weather feature models took ~30s each
and their tests passed, but `lmp_baseline_features` took 3,674s (61 min) for 290k rows, most of it in the
delete step, while the host was under heavy load (a Docker image build, load average ~11). The same model
ran in 135s on a quiet host earlier that day. Not table bloat (2% dead tuples, normal sizes), so
contention is the likely cause but is not proven; re-time on a quiet host. A 14:26 start leaves four
minutes, so it will miss 14:30 whenever the host is busy or the model is slow. Options when the serving
path exists: start the afternoon syncs earlier, run `lmp_serving` as soon as the last sync finishes
(chain it instead of using a clock time), or move the cutoff later.

## Cells with missing input features

**Title: Cells with any missing input feature are dropped from the loss, Author: Angie Ohaeri (assisted), Date: October 26, 7:17PM CT**

A (hour, node) cell is excluded from the loss, and from evaluation, when `exists` is false, `y` is NaN, or
any of the 9 raw input features is NaN (`loss_mask` in `prepare`, `models/model.ipynb`). Fold 0 coverage:
train 96.1%, validation 97.9%, test 98.0%. The cells stay in the arrays and still pass messages to
neighbours in the GNN; their NaN inputs are filled with 0 (the train mean after standardizing). Both
baselines (spread = 0, last verified day) are scored on the same mask, with no fill-in on the baseline:
filling `spread_last_verified_day` with 0 made that baseline partly copy the zero baseline.

Why drop: the missing history is mostly the first ~10 days of the window (no 7-day history yet, never
happens at serving time) and a new node's first ~10 days. Those cells still have a real `y`, so dropping
them loses about 0.9% of labelled train rows. A missing-value flag was considered and rejected: it would be
an input the model sees in under 1% of rows. The spread does not persist day to day (zero beats last day),
so the history features probably carry little signal anyway.

Not measured: how the 0.9% splits between window-start cells and new-node cells.

Revisit if new nodes matter at serving time: a node in its first ~10 days gets 0-filled history the model
never trained on, so its forecast should be flagged unreliable or given a missing-value flag.

## Source lag in the raw_lmp feeds

**Title: The one-day lag in the PJM feeds came from our sync window, not PJM, Author: Angie Ohaeri (assisted), Date: October 26, 10:55PM ET**

Every raw_lmp flow built its request window with a date-only end string (`end = now()`). gridstatus and
PJM treat that end date as exclusive, midnight at the start of the day, so today's rows were never
requested and each feed stopped at D-1. Pulling the same windows from PJM directly at 18:55 ET showed the
data exists: with a later end date, day-ahead shadow prices reached the end of D+1 (613 rows vs 210) and
fuel mix reached 18:00 ET the same day. The Prefect runs all showed COMPLETED because they succeeded on
the window they asked for, so nothing flagged it.

Fix, commit `7b66260` (2026-10-24): day-ahead flows poll through today + 2 days, near-real-time and RT flows
through today + 1; the day-ahead flows moved to 13:50-14:05 ET (after DA results post ~13:30 ET, before the
14:30 ET cutoff) and `da_hrl_lmps_nodal` from 14:30 to 14:05.

What is stale and what to do:
- `feature_availability_days` in `src/dbt/dbt_project.yml` was recorded from the DB on 2026-10-24 18:47 ET,
  before the fix. Its day-ahead values (-1) and the fuel-mix lag describe the lagging sync, not PJM. Not
  changed yet.
- Before building a feature on any source, check the newest timestamp per source in the DB at ~14:30 ET on a
  day after the fix has run, and set the var from that. Do not use PJM's nominal posting times or an earlier
  reading of the DB.
- Checked in the DB at 22:50 ET on 2026-10-26 (day D): `marginal_value_da`, `transmission_constraints_da` and
  `da_hrl_lmps_nodal` reach the end of D+1; `marginal_value_rt` reaches the end of D-1 (not D-2);
  `rt_hrl_lmps_nodal` reaches 22:00 ET on D. So the fix works and the day-ahead sources would be +1 and RT
  shadow prices -1. That is the state 8 hours after the cutoff and the raw tables have no load timestamp, so
  it does not prove the data was in by 14:30 ET: confirm with the completion times of that day's Prefect runs
  (day-ahead flows are scheduled 13:50-14:05 ET) before changing `feature_availability_days`.
- Prefect runs on 2026-10-26 (all Completed, one run each): `marginal_value_rt` 12:15:00-12:15:07,
  `marginal_value_da` 13:50:01-13:50:05, `lmp_da_hourly` 13:55:01-13:55:19, `transmission_constraints_da`
  14:00:00-14:00:12, `da_hrl_lmps_nodal` 14:05:00-14:09:15 ET, all before the 14:30 cutoff. The run logs have
  no row counts, but with one run each and D+1 rows in the DB, those runs loaded them. One day of
  evidence: the first full day of the new schedule is 2026-10-27, since `load_forecast`, `load_forecast_live`,
  `weather_forecast` and `dbt_build_lmp_serving` did not run on 10-26 (their deployments were added later
  that day).
- `marginal_value_rt`: its ~2-day settlement lag is not borne out by the check above; recheck at 14:30 ET.
- Schedules live in code (`src/prefect/lmp_deployments.py`, run by `serve()`). Restart the
  `lmp-prefect-deployments` process on the Mac Mini after pulling to apply them. Do not edit them in the
  Prefect UI: a restart re-registers from the file.
- The PJM API key was printed in plaintext by gridstatus in the session that found this; rotate it.

## Daily orchestration

**Title: Daily LMP syncs and dbt builds run as two sequential flows; dbt runs through prefect-dbt, Author: Angie Ohaeri (assisted), Date: October 27, 5:14PM**

- `lmp_morning` (12:15 ET): `rt_hrl_lmps_nodal` -> dbt `lmp_history` -> optional RT syncs (marginal value RT,
  scheduled generation, EHV losses, forecasted outages). `lmp_afternoon` (13:50 ET): `da_hrl_lmps_nodal`,
  `load_forecast`, `load_forecast_live` -> `weather_forecast` (optional) -> dbt `lmp_serving` -> optional DA
  syncs (marginal value DA, `lmp_da_hourly`, transmission constraints). Code: `src/prefect/lmp_daily.py`.
- A failed critical sync stops the flow before the dbt build that needs it. Optional syncs run with
  `return_state=True`: a failure logs a warning and the flow continues. Hourly, weekly and monthly syncs keep
  their own Cron. The two dbt deployments stay as unscheduled manual triggers.
- Why: cron times were the only ordering. A manual run of both dbt deployments overlapped, `lmp_spread`'s
  index post-hook blocked on `lmp_baseline_features`, and three runs had to be cancelled (cancelling a Prefect
  run does not end the Postgres query; it was ended with `pg_terminate_backend`).
- `dbt-build` global concurrency limit of 1 (`strict=True`, so a missing limit fails the run). It also covers the
  load build, which shares `target/`. Create once on the server: `prefect gcl create dbt-build --limit 1
  --slot-decay-per-second 0`.
- Uniqueness and `not_null` tests on `lmp_spread`, `lmp_baseline_features` (10 days) and `lmp_spread_history`
  (15 days) are limited to `time >= now() - interval 'N days'`, wider than each model's incremental lookback.
  Old history is no longer checked: run the tests unfiltered after any `--full-refresh`.
- `dbt_build.py` uses `PrefectDbtRunner` (in-process, per-node tasks) instead of a subprocess; `prefect-dbt`
  added to `pyproject.toml`. The cutoff expression is the `forecast_cutoff()` macro.
- `raw_lmp_sync.py`: each `poll_*` and `_upsert` is a task (retries on the call, not the whole flow); flows take
  `lookback_*` parameters (a backfill is a manual run with a larger value); `_pjm()` and `_eia()` build the
  clients. The backfill functions were removed.
- Dockerfile installs dependencies before copying `gridcast/`; `docker-compose.override.yml` mounts `src/prefect`
  and `src/dbt` from the checkout.

Not verified: none of this has run on its schedule yet, and the new test filters, `prefect-dbt` and the macro
have not been through a full `dbt build`. `lmp_baseline_features` has not been re-timed on a quiet host with
nothing else running. The live load-forecast and weather snapshots now happen about 13:55 instead of
14:20-14:25: earlier, still before the cutoff.

Declined for now: `prefect.yaml` with a worker (little gain on one box), Prefect failure notifications
(`prefect-email` would need installing on the server), a self-hosted GitHub runner for deploys.

**Title: Merged lmp_spread and lmp_spread_history into spread; dropped the lmp_ prefix from analytics_lmp models, Author: Angie Ohaeri (assisted), Date: October 27th Time: 2:10pm CT**

- `lmp_spread` and `lmp_spread_history` were both 26M rows at the same grain, and history repeated the target's `spread` and `congestion_spread`. They are now one incremental model, `spread` (target columns plus the trailing 7-day stats; stats computed over 14 days, 7 emitted, same as before).
- `baseline_features` joins `spread` twice: at the lagged hour for the history columns and at the target hour for `spread`.
- Renamed (drop `lmp_`, the schema is already `analytics_lmp`): `baseline_features`, `model_features`, `load_forecast_features`, `weather_forecast_features`, `node_areas`, seed `load_forecast_areas`. `stg_*` names unchanged.
- Not built yet: the 10 staging views with no feature model (marginal values, constraints, fuel mix, outages, ...) are still unused, so `model_features` does not show them.
- Live DB: the old tables (`lmp_spread`, `lmp_spread_history`, and the renamed ones) stay until dropped by hand after the first build of the new names. Raw table renames in the worksheet (`rt_hrl_lmps_nodal` -> `lmp_rt`, `da_hrl_lmps_nodal` -> `lmp_da`, `lmp_da_hourly` -> `lmp_da_zonal`) are not done.

**Title: Raw table renames and slimmer daily dbt builds, Author: Angie Ohaeri (assisted), Date: October 27th Time: 3:30pm CT**

- Raw renames (code and docs updated, live DB not yet altered): `raw_lmp.rt_hrl_lmps_nodal` -> `lmp_rt`, `raw_lmp.da_hrl_lmps_nodal` -> `lmp_da`, `raw_lmp.lmp_da_hourly` -> `lmp_da_zonal`. Prefect flows, functions and indexes follow the table names: `sync_rt_hrl_lmps_nodal` -> `sync_lmp_rt` (flow `lmp_rt`), `sync_da_hrl_lmps_nodal` -> `sync_lmp_da` (`lmp_da`), `sync_lmp_da_hourly` -> `sync_lmp_da_zonal` (`lmp_da_zonal`), `sync_rt_unverified_hrl_lmps_nodal` -> `sync_lmp_rt_unverified` (flow and deployment `lmp_rt_unverified`), `poll_hrl_lmps_nodal` -> `poll_lmp_nodal`. The live indexes and unique constraints are renamed with the tables (`ALTER TABLE ... RENAME` leaves them on the old names). After deploy, delete the old `rt_unverified_hrl_lmps_nodal` deployment from the Prefect server, or its hourly runs pile up as Late.
- Why the morning run took 22 min (1,361s): the RT sync 302s, dbt 1,041s. Most of dbt was whole-table uniqueness tests on `stg_rt_lmp_nodal` (362s) and `stg_da_lmp_nodal` (388s), plus tests on staging views no feature model reads, and 4-6 min for each of the two 254k-row incremental inserts.
- Fix: the RT and DA staging tests now filter to the last 10 days; selector `lmp_history` is `+spread` plus `stg_pnode_topology` (not the whole staging folder); `lmp_serving` also builds `stg_load_forecast` and `stg_weather_forecast`; new unscheduled selector `lmp_staging` runs every staging view and test.

**Title: Data-driven facility-to-node links from shadow prices (experiment), Author: Angie Ohaeri (assisted), Date: October 27th Time: 3:55pm CT**

Why: name and substation matching puts only 32.5% of RT shadow price weight on a GNN node (35.5% with a substation join), because most constrained substations have no node in the 1,505-node set. Top 25 facilities carry 50% of the weight, top 100 carry 83%.

Method: ridge regression of each node's verified RT `congestion_price` on the hourly RT shadow prices (monitored facility, summed over contingencies, 5-min sum / 12) of the top 100 facilities. Last 365 days, fit on the first 275, scored on the last 90 (same-hour, so an upper bound for lagged use). Scratch script, not in the repo yet.

Result (held-out 90 days, 1,501 nodes):
- Top-100 facilities explain 59.9% of node congestion-price variance (median node R2 0.587; 92% of nodes R2 > 0.3; 68% > 0.5).
- Tier-1 system totals (summed shadow price, constraint count) explain 1.5% (median node R2 0.019). A broadcast signal says little about any one node.
- Check against the 35 known facility-to-GNN-pnode links in the top 100: the learned coefficients rank the linked node #1 at the median, in the top 5 for 66% and the top 20 for 74%.
- The learned matrix is dense (1,493 of 1,505 nodes have a coefficient above 10% of the largest), so the link is a weight per (facility, node), not a hard edge.

Caveats: congestion price, not the RT-DA spread; DA congestion is already in `da_congestion_price`, so the useful signal is how RT shadow prices differ from what DA priced. Lift on the spread target is untested.
Next: per-fold weights, exposure features per node-hour (weights times last verified day's RT shadow prices, and times the target day's DA shadow prices), then the per-node reference model with and without them.

**Title: 75 constraint nodes and facility-line edges added to the GNN, Author: Angie Ohaeri (assisted), Date: October 27th Time: 9:38pm CT**

Why: after station grouping (`network_reference/build.py`), 42% of RT shadow-price weight reached a GNN node directly and another 34% sat at stations that have pnodes but none in the 1,505-node set. The top 100 of those facilities carry 32 of the 34 points.

- `network_reference/constraint_nodes.py` adds 75 nodes to `raw_lmp.pnode_topology_nodes` with role `constraint`: one station per facility (the end with the higher-voltage bus), one pnode per station (highest voltage, LOAD before GEN, lowest id, same as the fill-ins). The list is saved to `network_reference/constraint_nodes.csv`. Many are 13-69 kV buses because that is the only priced bus at those stations.
- `network_reference/backfill_node_prices.py` backfills verified RT and DA hourly LMPs into `raw_lmp.lmp_rt` / `lmp_da` for nodes whose history stops short, resuming where each stopped (731-day pnode-filter limit, so the history starts 2024-10-27, five days after the original nodes). Loaded: 1.27M RT and 1.27M DA rows for the 75 nodes; a few took effect recently and have only ~41 days. The daily syncs read the node table, so they pick the new nodes up on their own.
- `network_reference/line_edges.py` adds physical edges (`source = 'facility_line'`) from lines with both ends linked: 791 lines -> 119 node pairs, 59 already map edges (an independent check that agrees), 60 new. 32 constraint and 14 fill-in nodes get a physical edge for the first time.
- `lmp_model/graph.py` now maps edge substations to any non-aggregate node, not only `attached`, so the new edges are used.
- Still to do: `lmp_history` dbt full refresh so the incremental feature models include the new nodes' history; deploy (graph.py change and the new node count reach the Mac Mini only via git).

**Title: Pass-through LMP staging views removed; feature models read raw_lmp sources, Author: Angie Ohaeri (assisted), Date: October 28th Time: 12:33pm CT**

- Removed 10 staging views that only selected or renamed columns: `stg_pnode_topology`, `stg_load_forecast`, `stg_weather_forecast`, `stg_forecasted_generation_outages`, `stg_transmission_constraints_da`, `stg_lmp_rt`, `stg_lmp_da`, `stg_lmp_da_zonal`, `stg_natural_gas_fuel_cost`, `stg_operator_initiated_commitments`. Feature models use `source('raw_lmp', ...)` and rename inline (`lmp` -> `rt_lmp`/`da_lmp`).
- Kept the 5 views with logic: `stg_marginal_value_rt` (5-min to hourly roll-up) and the four that shift hour beginning to hour ending (`stg_marginal_value_da`, `stg_generation_by_fuel`, `stg_generation_ehv_losses`, `stg_scheduled_generation`). Doing that shift in each feature model would make a one-hour misalignment easy to miss.
- Their tests moved to `sources.yml` (RT/DA still filtered to the last 10 days; `pnode_id` relationships point at the source; `role` accepts `constraint`). Selectors keep the same coverage: `lmp_history` adds `source:raw_lmp.pnode_topology_nodes`, `lmp_serving` adds `source:raw_lmp.lmp_da_zonal`.
- Checked: compiled SQL vs the current tables, row for row: `node_areas`, `load_forecast_features`, `weather_forecast_features`, `shadow_price_features`, `spread` (last 5 days) identical; `baseline_features` (last 3 days) differs only where the target spread for 10/27 ET had arrived after the last build. No full refresh needed.
- The 10 views still exist in `analytics_lmp` until they are dropped (dbt does not drop removed models).

**Title: Feature tables by grain; fuel mix, gas cost, losses, outages, DA constraints and commitments added, Author: Angie Ohaeri (assisted), Date: October 28th Time: 1:15pm CT**

- One feature table per grain, matching the loader's arrays: `nodes` (replaces `node_areas`: node attributes plus `forecast_area`), `system_features` (replaces `shadow_price_features`), `zone_features` (replaces `weather_forecast_features`), plus the existing `baseline_features` (node) and `load_forecast_features` (area). `model_features` joins them all per (time, pnode_id). Calendar features stay in Python (`gridcast.features`).
- `system_features` adds, each as of the 14:30 ET cutoff: `da_constraint_count_prev_day`, `da_constraint_hours_prev_day` (transmission_constraints_da), fuel mix (`generation_mw_prev_day`, gas/coal/nuclear/wind/solar shares), `self_scheduled_mw_prev_day`, `loss_ratio_prev_day`, the target day's outage forecast (`outage_mw_rto/west/other`, latest execution by the cutoff) and `gas_cost_per_mmbtu` (EIA, average of PJM states that report). `_prev_day` = the same hour on the last day the source has available at the cutoff (48h before the target hour for -1 sources, 72h for -2).
- `zone_features` adds operator-initiated commitments per zone over the latest month posted by the cutoff: all reasons (`commitment_count`, `commitment_mw`) and Constraint Management only (`constraint_commitment_count`, `constraint_commitment_mw`).
- New vars: `natural_gas_fuel_cost_lag_months: 3` (latest EIA month on 2026-10-28 was 2026-08), `commitments_posting_day: 20` (latest event on 2026-10-28 was 2026-09-30), `pjm_states`. From 2026-03, EIA reports 8-9 PJM states instead of 13, so the gas average's mix shifts there.
- Loader (`lmp_model/dataset.py`): node attributes from `analytics_lmp.nodes`; `system` = calendar + `system_features` columns (`SYSTEM_FEATURES`), now scaled like the other inputs; the weather array is now `zone_features`/`node_zone` (`ZONE_FEATURES`). The npz cache must be rebuilt. Notebook 3.06 updated to compute its column positions.
- Dropped from the live DB: the 10 pass-through staging views (plus the pre-rename `stg_rt_lmp_nodal`/`stg_da_lmp_nodal`) and `node_areas`, `shadow_price_features`, `weather_forecast_features`. The Mac Mini's flows recreate the old objects until this is deployed.
- Watch: `lmp_serving` must finish before 14:30 ET; `system_features` took 42s alone and 199s while the scheduled flow was also running. Only the da_* shadow-price columns and the outage forecast need the afternoon inputs, so the rest could move to the morning build if it runs long.

**Title: Contingency-list line edges added; mean aggregation weighs indirect edges the same as direct lines, Author: Angie Ohaeri (assisted), Date: October 28th Time: 6:30pm CT**

- `network_reference.line_edges` (commit `cbb589b`) added 1,280 edges to `raw_lmp.pnode_topology_edges`: 282 direct lines in three tiers (`line_single_outage` 187, `line_only_candidate` 15, `line_id_letters` 80), `line_same_circuit` 67, `facility_line` 13, and 918 indirect edges (`line_via_1` 384, `line_via_2` 534) between GNN stations joined through 1-2 stations with no GNN node. Isolated GNN substations 528 -> 218. Details in `line-edge-quirks.md`.
- Problem: `SAGEConv` in `models/model.ipynb` uses mean aggregation, so each neighbour's message gets weight 1/degree whatever the edge is. A `line_via_2` edge (two stations in between, possibly 50+ km of line) counts the same as a direct line or a map edge. Indirect edges are now 72% of the new edges, so for many nodes most of the averaged signal comes from neighbours that are electrically further away. Mean aggregation also drowns a node's one important neighbour as its degree grows.
- `lmp_model/graph.py` `geographic_edges()` reads only `sub_a, sub_b` and labels every physical edge `geographic`, so the model cannot tell edge sources apart today.
- Options, cheapest first: (1) train with and without `line_via_2` / all `line_via_*` and compare val MAE; (2) keep `source` in `geographic_edges()` and give the model edge types, either as an edge feature to an attention layer (`GATv2Conv(edge_dim=...)`, which learns a weight per neighbour and can use the type) or as one convolution per type (`HeteroConv` / R-GCN); (3) a fixed weight per type (e.g. 1 / (1 + via stations)) with a layer that takes `edge_weight`.

**Title: DA interface flows and scheduled tie-line interchange added as features, Author: Angie Ohaeri (assisted), Date: October 28th Time: 7:45pm CT**

- Why: shadow prices only turn on once a constraint binds. Day-ahead flow / limit on PJM's transfer interfaces shows how close each one is before that. Scheduled interchange is the power neighbours plan to push across PJM's borders, which loads the border zones (ComEd, ATSI, PSEG/JCPL, AEP/DOM/EKPC).
- Raw tables (`lmp_model_schema.sql`, backfilled from 2024-10-23 with `sync_*(lookback_days=737)`): `raw_lmp.interface_flows_da` (104,954 rows, 6 interfaces; BED-BLA from 2024-10-26 and missing on 1,294 hours since) and `raw_lmp.scheduled_interchange` (335,616 rows, 19 tie lines, positive = into PJM). `time` is Hour Ending UTC in both. PJM rejects date ranges over a year, so the polls go in 180-day chunks.
- Timing, from Data Miner on 2026-10-28: DA interface flows post daily 12:00-15:15 ET with the DA results (that day at 12:33, next day included), so they are target-day features like `da_shadow_*` (`feature_availability_days.interface_flows_da: 1`). A post after 14:30 leaves them null at serving time. Scheduled interchange posts 07:30 ET for the previous day, so `scheduled_interchange: -1` (48h back, `_prev_day`).
- `system_features`: `apsouth_util`, `bcpep_util`, `bed_bla_util`, `central_util`, `east_util`, `west_util` (target-hour flow / limit; 1 = at the limit; the limit holds one way only, so CENTRAL runs to -6.9 and WEST to -1.2), `net_interchange_prev_day` (all 19 ties). `zone_features`: `border_interchange_prev_day` (ties landing in the zone, 0 for inner zones). All in `model_features` and the loader (`SYSTEM_TABLE_FEATURES`, `ZONE_FEATURES`). The npz cache needs a rebuild to pick them up.
- Tie line -> zone (seed `tie_line_zones`): checked against HIFLD lines (>=100 kV) crossing the PJM border, with the neighbour's owner near the outside end. CE: ALTE, WEC, MEC, AMIL, NIPS. AEP: NIPS, CIN, TVA, DUK, LGEE, CPLW. DEOK: CIN. EKPC: TVA, LGEE. DOM: CPLE. ATSI: MECS. PS: NYIS, HUDS, LIND. JC: NYIS, NEPT (Sayreville). PN and RECO: NYIS. A tie touching several zones counts in full in each. ALTW, CWLP and IPL have no physical tie to PJM, so they are in the total only.
- Not ingested: `act_sch_interchange` (actual, scheduled and inadvertent flow per tie, history to 2014). Actual minus scheduled is large (e.g. MECS about -1,800 MW on 2024-10-22/23, loop flow around Lake Erie) and may be the stronger signal, but it posts weekly (Sundays 08:00 ET), so it is 2-9 days old at the cutoff.
- Scheduling: `sync_interface_flows_da` runs in `lmp_afternoon` with the optional syncs before the `lmp_serving` build (like `sync_marginal_value_da`), `sync_scheduled_interchange` in `lmp_morning`'s optional syncs (posted 07:30 ET, read only by the afternoon build). No standalone deployments: running inside the daily flows keeps them ahead of the build and inside PJM's shared 6 requests/min. A failed sync is logged and the flow continues; the new not_null tests on `apsouth_util`, `net_interchange_prev_day` and `border_interchange_prev_day` then fail `lmp_serving`.

**Title: OSM locations for 227 unlocated GNN stations and 555 HIFLD map edges for them, Author: Angie Ohaeri (assisted), Date: October 28th Time: 9:41pm CT**

- 501 of 1,538 GNN substations had no location (ComEd 279: HIFLD names most ComEd substations `UNKNOWN`/`TAP`, and ComEd codes are number + truncated name). Matched to OpenStreetMap substations (`data/external/osm/substations_<zone>.json`, Overpass): ComEd by OSM `ref` = station number, then name; other zones by name prefix or abbreviation within the zone. Every candidate was scored against its contingency-list line neighbours (distance limit by voltage: 30/60/150/250 km); ambiguous names were resolved by the candidate nearest the neighbours. Reviewed by hand in `comed-osm-locations-review.csv` and `unlocated-osm-locations-review.csv` (`final` column); accepted rows are `network_reference/osm_station_locations.csv` (288 codes, 227 GNN stations, `evidence` per row).
- `network_reference/build.py` `add_osm_locations()` fills `substations.lat/lon` from that file where the topology has none (`location_source = 'osm'`). `network_reference/osm_edges.py` sets `pnode_topology_nodes.lat/lon` (`topo_source = 'osm'`) and adds `hifld_osm` edges with the original map-edge rule (nearest HIFLD line endpoint within 1 km, joined within 4 hops): 190 of 227 attach, 555 new edges (median 36 km, max 273 km, like the existing `hifld` edges). Isolated GNN substations 218 -> 164 (ComEd 149 -> 100). Order: build, constraint_nodes, osm_edges, line_edges.
- Not done: DEOK (81 of 90 still unlocated; only 54 of 444 OSM substations there have names), GridSFM attachment for the new stations (`gridsfm_pjm_network.json` is only on the Mac Mini). `get_connection()` autocommits, so `osm_edges` and `line_edges` now set `autocommit = False` to write all-or-nothing.
- Edge ablation, fold 0, seed 0 (`history` features, 1,580 nodes): test MAE map_only 30.82, direct_lines 30.84, all_lines 30.70, no_graph 30.74; spike MAE 637.5 / 638.3 / 637.1 / 647.7 (zero 33.67 / 693.3). The graph does not move overall MAE; any graph lowers spike MAE by ~10, whichever edges. Single fold and seed: confirm across folds before relying on it. Fold 0 validates on Oct-Nov and tests on Dec-Feb, so early stopping is picked on calmer months than it is tested on.

**Title: DA interface utilization made zone-level (interface_util), Author: Angie Ohaeri (assisted), Date: October 28th Time: 11:17pm CT**

- Why: the 6 PJM-wide `*_util` columns are copied into every node, and tonight's runs showed PJM-wide system features copied into every node made the GNN worse (test MAE 29.28 with 15 inputs, 29.87 with 31, 30.49 with 37, on the original 1,505 nodes). The interface utils track hourly spike size at Spearman 0.32-0.39 PJM-wide, so they are worth keeping as a location-specific signal.
- Mapping (seed `interface_zones`): the lines in each interface from PJM Manual 03 section 3.8 (rev 71, 2026-05-20), each end substation's zone from PJM's LMP bus model (`data/external/lmp-bus-model.xlsx`). EAST: PL, PE, DPL, PS (Hopatcong is PSEG). CENTRAL: PN, PL, BC, PE. WEST: PN, PL, ME, AP, PEP (includes Doubs-Brighton). BED-BLA: AP (both ends). APSOUTH: AP, DOM. BCPEP: BC, PEP, AP, ME, PE, PL, DOM, DPL.
- Not in the bus model, placed by HIFLD (substation and line owner, zone polygon): Breinigsville (PL), Graceton and Five Forks (DPL; Delmarva owns the line between them, and this is the only reason DPL is on BCPEP), Otter Creek and Edwards Ferry (their possible zones are already on BCPEP, so they change nothing).
- `zone_features.interface_util`: the target hour's highest DA flow / limit among the interfaces ending in the zone. Max, not mean: the interface closest to its limit is what moves prices, a mean dilutes it with slack interfaces, and APSOUTH and BED-BLA (0.90 correlated) would count twice. Negative values (flow away from the limit, mostly CENTRAL) drop out under max. 0 for the 10 zones with no interface (AE, AEP, ATSI, CE, DAY, DEOK, DUQ, EKPC, JC, RECO); null outside the loaded hours. Same timing as the system `*_util` columns.
- Limitation: only zones where interface lines end get a value. Zones behind an interface (e.g. AE and JC behind EAST) get 0 even though they likely see its congestion; the graph may carry some of that.
- Added to `model_features` and `ZONE_FEATURES`; the 6 system `*_util` columns are still in `SYSTEM_TABLE_FEATURES`. The npz cache needs a rebuild to pick up `interface_util`.

**Title: Training models/model.ipynb on a Colab GPU over Tailscale, Author: Angie Ohaeri (assisted), Date: October 29th Time: 12:56am CT**

- Why: one GNN run is ~20 min on the Mac Mini's 4 cores, and runs from different sessions compete for its 4 cores and 15 GB. Colab (GPU runtime picked as the kernel in VS Code) does the training; the data stays on the Mac Mini and runs log straight to its MLflow, so there is no export/merge step (chosen over `mlflow-export-import`).
- What Colab needs from `angies-server`: TimescaleDB on 5432 (`load_nodes`, `build_edges`), MLflow on 5000 (experiment `gridcast-lmp-spread`, artifact location `mlflow-artifacts:/3`, so artifacts upload through the server, commit `0dcbc79`), and the cache (`window.npz` ~740 MB, `exposure.npz` ~50 MB) served on 8765. Docker publishes 5432 and 5000 on all interfaces, so both already answer on the tailnet; no Docker or MLflow change.
- Colab joins the tailnet as an ephemeral node tagged `tag:colab`, in userspace mode (Colab has no `/dev/net/tun`). Userspace mode gives no network interface, and psycopg2 can't use a proxy, so the setup cell runs `socat` to forward `localhost:5432/5000/8765` on Colab to `angies-server` through `tailscale nc`. The env vars then point at `localhost`. `tailscale serve` picks what to serve by the request's host name, so the cache download sends `Host: angies-server:8765` (with `Host: localhost` it returns 404).
- Setup is the "Colab setup" cell at the top of `models/model.ipynb` (skip it on the Mac Mini). It installs Tailscale, joins, clones `lmp-pricing-model` from GitHub (public repo, no token), installs `mlflow-skinny==3.14.0` (matches the server) and `torch_geometric`, sets the DB and `MLFLOW_TRACKING_URI` env vars, and downloads the cache. Colab only runs code that is pushed to the branch.
- Secrets: Colab secrets (`userdata.get`) don't work from the VS Code extension (googlecolab/colab-vscode known issues), so the cell asks for the Tailscale auth key and DB login with `getpass`. Nothing is saved in the notebook.
- Cache over the tailnet, not Drive: always the current cache, no re-upload after each rebuild. Cost: 740 MB per Colab session, slower if Tailscale relays the connection (DERP) instead of connecting directly.
- GPU: `device` (cuda if present, else cpu) in the Graph cell; the tensors, edges and model move to it, and runs get a `device` tag. On the Mac Mini nothing changes. GPU results differ slightly from CPU (different kernels; `SAGEConv`'s scatter is not deterministic on GPU), so compare variants run on the same device (filter on the `device` tag). A `model.pt` saved on GPU needs `torch.load(..., map_location="cpu")` on the Mac Mini.

One-time setup (Tailscale admin console, https://login.tailscale.com/admin):

1. Access controls: add the tag owner, the server's address, and a grant that lets `tag:colab` reach only these three ports:

```json
"tagOwners": {
  "tag:colab": ["autogroup:admin"],
},
"hosts": {
  "angies-server": "100.119.203.105",
},
"grants": [
  {"src": ["autogroup:member"], "dst": ["*"], "ip": ["*"]},
  {"src": ["tag:colab"], "dst": ["angies-server"], "ip": ["tcp:5432", "tcp:5000", "tcp:8765"]},
],
"tests": [
  {"src": "tag:colab", "accept": ["angies-server:5432", "angies-server:5000", "angies-server:8765"], "deny": ["angies-server:22", "angies-server:4200", "100.67.156.33:22"]},
],
```

   The default policy's allow-all rule (`"src": ["*"]`) also matches tagged devices, so it must be replaced by the `autogroup:member` rule above (your own devices keep full access; tagged devices are not members). If the file uses `"acls"` instead of `"grants"`, the same rules are `{"action": "accept", "src": ["autogroup:member"], "dst": ["*:*"]}` and `{"action": "accept", "src": ["tag:colab"], "dst": ["angies-server:5432,5000,8765"]}`. The `tests` block makes the console refuse to save a policy that lets Colab reach anything else.
2. Settings > Keys > Generate auth key: Reusable on, Ephemeral on (the node disappears when Colab shuts down), Tags `tag:colab`, expiry 90 days. Keep it in a password manager; paste it when the setup cell asks.
3. On the Mac Mini, serve the cache folder on the tailnet only (persists across reboots; `sudo tailscale serve --http=8765 off` removes it):

```bash
sudo tailscale serve --bg --http=8765 /home/angieohaeri/gridcast/data/processed/train_npz
```

Each Colab session: pick the Colab GPU runtime as the kernel, run the three Colab setup cells (the second only on a new Colab machine), then the notebook as usual.

**Title: Colab smoke test passed; spread table for build_edges built on the Mac Mini, Author: Angie Ohaeri (assisted), Date: October 29th Time: 11:56am CT**

- Smoke test (Tesla T4, free tier): the A/B cell's three runs logged to the Mac Mini's MLflow with `device=cuda`, their three dataset samples, and `model.pt` / `scalers.npz` uploaded through the server (`mlflow-artifacts:/3/...`). An epoch takes 3.8 s on the T4, so a full run (early stop around epoch 8) is under a minute of GPU time.
- GPU vs CPU: the GPU baseline (15 inputs, `history_calendar`) scored 29.28 test MAE on the original 1,505 nodes, the same as the CPU run. Spike MAE 618.9; 29.49 / 626.3 on all 1,580 nodes.
- The slow part is moving data off the Mac Mini, whose internet upload is ~0.35 MB/s (measured against Cloudflare's speed test). `window.npz` took 41 min. `build_edges` pulls the training window's `analytics_lmp.spread` (~325 MB per 30-day chunk over the wire), ~3 hours. Fix: `maintenance/spread_wide_cache.py` builds that table on the Mac Mini (1.2 min, 51 MB, `train_npz/spread_wide_fold0.npz`), Colab downloads it with the cache, and the third Colab setup cell points `lmp_model.graph.load_spread_wide` at it. Fold 0 only; rerun the script after the spread data or fold dates change, and delete Colab's copy to fetch the new one.
- Pitfalls hit: `REPO` must be an absolute path (a relative one split the clone and the cache into two folders); Colab replaces the machine after idle time or a runtime reset (everything re-downloads), while a kernel restart keeps the files and the tunnels but clears variables and env vars; the VS Code extension's link to the kernel dropped once and needed a manual reconnect, which restarted the kernel.
- The first auth key was made without the tag, so Colab joined as an untagged device owned by Angie and the `tag:colab` limits didn't apply. The second key was tagged (`tagged-devices` in `tailscale status`).

**Title: Per-node shadow-price exposure features (no lift) and an hour-level model for the system-wide part of the spread, Author: Angie Ohaeri (assisted), Date: October 29th Time: 1:15am CT**

Exposure features (`lmp_model/exposure.py`, `notebooks/3.07-lmp-exposure-features.ipynb`):
- Ridge weights from the hourly RT shadow prices of the top 100 monitored facilities (by RT shadow-price weight in the training window) to each node's verified RT congestion price, fit per fold on its training hours only, like `graph.py`'s correlation edges. Ridge strength is relative to the mean diagonal of XᵀX; 0.01 was best on held-out hours (0.001-0.1 within 0.03 R²; 1.0 clearly worse).
- `da_exposure` = weights x the target hour's DA shadow prices; `rt_exposure_prev_day` = weights x the RT shadow prices 48h back (`feature_availability_days.marginal_value_rt`). The weights differ per fold, so they are not in `window.npz`: raw inputs go in a separate `train_npz/exposure.npz` (hourly RT/DA shadow price per facility, node RT congestion) and features are computed per fold.
- Weights out of sample (validation hours, same hour): R² 0.38 fold 0 (11 months of training), 0.61 fold 3. Median node R² 0.38 / 0.64.
- Limits: the top 100 RT facilities carry 89% of RT shadow-price weight but 43% of DA weight (56% in fold 0's validation months), since many DA constraints rarely bind in RT. `da_exposure` vs `da_congestion_price`: correlation 0.40. Spike-size rank correlation (|spread|, fold 0 val): |`da_congestion_price`| 0.24, |`rt_exposure_prev_day` - `da_exposure`| 0.12.
- GNN, fold 0, scored on the 1,505 original nodes (folds 1-3 not run, by choice):

| inputs | val MAE | test MAE | test spike MAE | skill vs zero |
|---|---|---|---|---|
| 15 (node + calendar) | 13.17 | 29.27 | 618.0 | 12.6% |
| 15 + exposure | 13.14 | 29.28 | 623.8 | 12.5% |
| predict zero | 15.04 | 33.48 | 689.3 | |

Kept in the repo, not used by the model.

Why the system features hurt, and the hour model (scratch scripts, not in the repo):
- Over Oct 2025-Feb 2026 (1,505 nodes), 59% of RT-DA spread variance is the hour's mean across nodes, mostly the system energy price difference. Predicting zero: node MAE 26.0; knowing each hour's mean exactly: 14.4. DA LMP is 79% between-hour too (mean energy price $73 vs mean |congestion| $14, |loss| $3).
- System-wide features copied into 1,580 nodes repeat ~8,000 training hours 1,580 times, which fits why 15 -> 31 -> 37 inputs got worse.
- Fold 0 hour model (LightGBM, one row per hour, target = hour mean spread, 53 inputs: calendar, system_features, DA energy price and mean DA LMP, area load forecasts, lagged hour-mean spread): hour-level test MAE 25.92 vs zero 27.95 (val 10.42 vs 12.20). Node-level test MAE when every node gets its hour's prediction: 31.38 (zero 33.48, base GNN 29.27, hour mean known 18.42).
- Fresh RT features (verified values standing in for the unverified RT LMPs, which are in the DB through HE14 of the cutoff day): the cutoff day's HE14 hour mean alone scores 26.13 (correlation 0.54 with the target), but adding five fresh features did not improve the hour model (25.8-26.5 across L1, Huber and L2 losses). A linear fit on four fresh features matches it (25.82) with better node spike MAE (637 vs 673).
- Caveat: fold 0's test months swing far more than training (hour-mean std 75 vs 35; validation 27), so one fold is a weak judge.
- Next: GNN on the node part (spread minus hour mean) with node features only, plus the hour model, compared to 29.27; hour model on all 4 folds. Score all 1,580 nodes from here on, with the 1,505 number alongside for one round.
