# LMP Pricing Model — Approach

**Title: Initial draft + target-grain decision, Author: Angie Ohaeri, Date: August 22nd Time: (session)**

**Title: Backfilled Generation by Fuel Type, Day-Ahead Transmission Constraints, and
Day-Ahead Hourly LMPs, Author: Angie Ohaeri (assisted), Date: August 23rd Time: 3:31pm**

Three more Tier 1 sources backfilled into `raw_lmp` — all via gridstatus, no raw Data
Miner 2 API calls needed. See `schema.md` for table structure/row counts and
`decisions.md` ("Data source scope") for what's deferred and why.

**Title: Split into `lmp-pricing-model.md` / `decisions.md` / `schema.md`, Author: Angie Ohaeri (assisted), Date: August 23rd Time: (session)**

This file now covers approach/methodology only. Dated decision entries live in
`decisions.md`; `raw_lmp` table definitions live in `schema.md`.

Stretch extension to the load-forecasting project (see `../architecture.md`).

**Status: in progress.** Branch, `raw_lmp` schema, dbt scaffolding, and five raw tables
exist (two ingested — Real-Time/Day-Ahead Marginal Value — plus three newly backfilled).
No ingestion pipeline, features, or model built yet for the three new tables.

---

## Isolation

Isolated from the working load-forecasting stack at every layer (branch, schema, dbt
target, MLflow naming, Prefect deployments, Kafka usage) — see `decisions.md`.

---

## Targets — two, not one mixed grain

Matches how PJM's markets actually clear (full rationale + the unverified/verified
reconciliation decision: `decisions.md`):

- **Day-ahead LMP**: hourly.
  DA only clears hourly. Reuses existing hourly lag/rolling/join architecture almost as-is.
  The easy one.

- **Real-time LMP**: 5-min.
  RT settles 5-min since April 2018. Bigger lift:
  - New 5-min ingestion, ~12x the row volume of hourly `lmp`.
  - Hourly-only features (load, weather) need explicit forward-fill onto the 5-min grid —
    not silent resampling.
  - **Instantaneous load** / **unverified 5-min LMP** (see `project_instantaneous_load_feature`
    memory) become load-bearing feature sources here, not just nice-to-haves.
  - 5-min spikes are sharper and noisier than hourly averages — exactly where congestion-state
    features matter most (see mechanism below).

---

## Mechanism

Real-time LMP has three components:

- **Energy** (λ) — marginal cost from PJM's security-constrained economic dispatch. One
  system-wide scalar, same for every zone.
- **Congestion** (Σ shadow prices on every congested line) — this is what actually varies LMP
  by zone.
- **Losses** — marginal cost of line losses. Smallest of the three. Ignored by the 2013 paper
  below and not yet sourced here; PJM publishes it as "Generation and Extra High Voltage
  Losses" in Data Miner.

It's an optimization output, not a simple clearing price — a real ceiling on what weather/load-only
features can predict.

Because λ is system-wide, not zonal, system-wide features (fuel mix, gas price, RTO-level outages)
correctly inform λ without needing a zonal breakdown. Broadcasting the same value to every zone's
feature row is the right pattern here, not a limitation. All zone-to-zone LMP variation is the
congestion term's job — that's what the Marginal Value feed + substation-graph proxy work (below)
is for.

**Ji/Kim/Thomas/Tong 2013** ("Forecasting Real-Time LMP: A State Space Approach", PJM 5-bus sim)
found a plain ANN on historical load/price was *worst* (MAPE 20.6%) against a method that models
congestion state as a Markov chain instead (11.75%). ANNs miss unpredictable spikes;
mechanism-aware models anticipate them.

**Implication**: congestion-state features (binding constraints, outages, fuel mix) beat raw
price/load lags for catching spikes — exactly where a lags-only model fails.

Caveat: 2013, toy sim, ignores losses, 6-8hr horizon only. Directionally right, not current SOTA
— newer GAN/spatio-temporal work reportedly closes the gap.

---

## Data sources

**Already ingested**: `load`, `lmp` (hourly `rt_hrl_lmps`), `weather`.

Table structure, row counts, and date ranges for every backfilled `raw_lmp` table:
`schema.md`. Deferred-source rationale: `decisions.md` ("Data source scope").

| Tier | Source | Status | Contributes |
|---|---|---|---|
| 1 — LMP decomposition | Real-Time Marginal Value (`raw_lmp.marginal_value_rt`) | backfilled | shadow price µ per binding constraint (`Monitored Facility`/`Contingency Facility`, not pnode/zone) — the congestion term. 5-min native since 2018, posts daily 11am-12pm ET |
| 1 | Day-Ahead Marginal Value (`raw_lmp.marginal_value_da`) | backfilled | same congestion term, for the day-ahead target — no penalty factor/limit control fields (RT-only) |
| 1 | Forecasted Generation Outages (`raw_lmp.forecasted_generation_outages`) | backfilled | daily, 90-day horizon, RTO/West/Other only (not zonal) — weaker than hoped, still useful |
| 1 | Operator Initiated Commitments | checked, promising, deferred | zonal(!) out-of-merit unit commitments with a `Reason` field — "Constraint Management" reason ties directly to congestion. Monthly, updated the 20th. Narrow: only reliability-driven commitments, not general economic unit commitment |
| 1 | Scheduled Generation | checked, secondary, deferred | self-scheduled generation (runs regardless of price — must-run/contractual, not operator-directed) distorts normal dispatch, causes uplift charges. Weaker than Operator Initiated Commitments though: RTO-wide only (2 aggregate MW numbers, no zone field), so it can only inform λ, not the congestion term — same bucket as fuel mix/gas cost, not a second zonal signal. Daily 5pm |
| 1 | Generation by Fuel Type (`raw_lmp.generation_by_fuel`) | backfilled | what fuel's on the margin; informs λ, RTO-wide by design (see mechanism above, not a limitation) |
| 1 | EIA natural gas fuel cost (`eia.gov/electricity/data.php`, monthly, by state) | planned, deferred | how expensive that margin is; pairs with fuel type to approximate λ (the "spark spread" signal). Not a PJM source — separate EIA API pull |
| 1 | Day-Ahead Transmission Constraints (`raw_lmp.transmission_constraints_da`) | backfilled | the congestion pattern set — which facility/contingency pairs bound and for how long (duration, no price magnitude; pairs with `marginal_value_da`'s shadow price for the same facility) |
| 1 | Day-Ahead Hourly LMPs (`raw_lmp.lmp_da_hourly`) | backfilled | enables a DA-RT basis feature (same zone×hour grain as `public.lmp`, DA market instead of RT) |
| 1 | Generation and Extra High Voltage Losses | planned, lowest priority, deferred | the losses term, smallest of the three components |
| 2 — renewable variability | Five Minute Solar/Wind Generation + forecasts | planned | duck-curve dynamics |
| 3 — investigate first | Energy Market Generation Offers | not a live feature — 4-month posting delay (PJM's stated policy) | not useless though: genuinely rich bid-curve data (masked generator ID, MW/BID pairs, start costs, ECOMAX/ECOMIN). Heat rate is a slow-changing physical property, not something that needs to be fresh — use this (even 4mo stale) to back-calculate typical heat rate/markup per unit or fuel type (the primer's "effective heat rate"), then apply that calibration to **live** fuel cost to estimate a live marginal-cost curve. Freshness requirement moves from the bid (stale) to the heat rate (doesn't need to be fresh) |
| 3 | Daily Cleared INCs, DECs, UTCs | checked, weak — posts daily 4am but only 1 row/day of RTO-wide MW totals (no price/location) | at best a blunt proxy for anticipated DA-RT congestion via UTC volume; low priority |
| 3 | Transfer Interface Information / Transmission Limits | not started | likely redundant with Marginal Value |
| 3 | Off-Cost Operations | checked | out-of-merit ops for voltage/reactive support, not congestion — `Facility`/`Contingency` fields match Marginal Value's structure. Monthly, updated the 4th. Correlates with congestion but isn't the same mechanism |
| skip | Real-Time Default Marginal Value Override | checked | operational fallback-price flag (what PJM substitutes when the normal RT calc doesn't produce one), not a price driver |
| skip | Balancing Transmission Congestion Preliminary Billing Data | checked | settlement/accounting — allocates congestion cost to who pays whom after the fact, not predictive |
| 3 | Day-Ahead Ratings | not started | facility thermal/emergency headroom; needs the same facility-name-to-substation parsing already open for Marginal Value — defer until that's solved |
| 3 | Up-To-Congestion Bid Screening | not started | financial arbitrage bid data, closest thing to revealed trader expectations of congestion; check if published live vs. delayed/aggregated before committing |
| 3 | Operating Reserve Rates Preliminary | not started | separate co-optimized ancillary market; reserve-price spikes can proxy system stress but it's a new mechanism to model, not congestion itself |
| 2 | Instantaneous Dispatch Rates | not started | zonal(!), 15-second native — being zone-keyed skips the facility-name attribution problem entirely (zonal analog to Generation by Fuel Type). Don't point-sample hourly (a single 15s snapshot misses ramps/spikes); batch-pull the time series per date range and aggregate to hourly (mean/max/std/range) in dbt instead, if Data Miner supports a window query for this report — unverified, check first |

### Zone attribution / shift-factor proxy (reference data, not streaming)

- `lmp-bus-model.xlsx`, `lmp-aggregate-definitions.xlsx` (PJM) — substation → zone, direct lookup.

- `electric_substation_hifld_v4.gpkg` (`data/external/`) — substation lat/lon, voltage, line
  count. ~51% named; `-999999` = missing.

- `pjm_zones.geojson` + HIFLD retail-territory geojson (`data/external/`) — point-in-polygon
  zone attribution, cleaner than name-matching.

- `US_Electric_Power_Transmission_Lines.gpkg` (HIFLD/EIA, `data/external/`) — 94,619 lines.
  `SUB_1`/`SUB_2` never null → real substation graph + `VOLTAGE`/`VOLT_CLASS` for a
  graph-shortest-path shift-factor proxy.

  Caveat bigger than expected: `INFERRED = Y` on 62% of rows nationally — most edges are
  inferred, not confirmed. Weight or filter by this before trusting the graph.

  Graph-construction and zone-attribution-fallback decisions: `decisions.md`.


**Shift factor**: πᵢ = λ + ΣₖAᵢₖµₖ. A isn't public and can't be legitimately derived. Two proxies:

1. **Structural** — graph-shortest-path from a constraint's substation to a zone, voltage-weighted.
2. **Empirical** — regress each zone's `congestion_price` against historical facility shadow
   prices ("revealed" shift factor). Better once enough binding-event history exists, noisy early on.

---

## Next steps

- Build producer/loader + dbt models for the raw tables that already exist (Real-Time Marginal
  Value, Forecasted Generation Outages, Generation by Fuel Type, Day-Ahead Transmission
  Constraints, Day-Ahead Hourly LMPs).
- Then decide on Operator Initiated Commitments / Scheduled Generation / EHV Losses — needs a
  new direct Data Miner 2 API pattern (no gridstatus support), and EIA natural gas fuel cost
  (separate EIA API pull).
- Substation graph + zone attribution is built (`raw_lmp.transmission_nodes`/`transmission_edges`)
  — next is matching Marginal Value's `Monitored Facility` to a graph node and computing
  shortest-path distance to each zone, to actually use it per-zone.
- Decide forecast horizon N for both targets (same open question as
  `project_pjm_load_forecast_feature`).


### How the transmission line data actually feeds into the model:

  It's not a feature by itself — it's plumbing for computing a feature, specifically the shift-factor proxy for the congestion term. Recall: πᵢ = λ + Σₖ Aᵢₖµₖ. We have µₖ (shadow price, from the
  Marginal Value tables) but not the real Aᵢₖ (the shift-factor matrix, not public). The transmission graph is how we approximate Aᵢₖ:

  1. **Match:** for each binding constraint in `marginal_value_rt`/`marginal_value_da` (keyed by Monitored Facility, a substation or line name), find its corresponding node(s) in
  `raw_lmp.transmission_nodes`.
  2. **Shortest path:** for that node, compute the shortest path (weighted by `voltage_kv` as a rough impedance proxy — higher voltage ≈ lower impedance ≈ "electrically closer") across
  `raw_lmp.transmission_edges` to each of the 20 zones.
  3. **Feature:** that per-zone distance becomes the weight applied to the constraint's shadow price — closer zones feel more of a given constraint's congestion cost, farther zones feel less.
  Something like `zone_congestion_feature = Σ (shadow_price_k / distance(k, zone))` per hour, summed over all currently-binding constraints.

  So concretely, a row in the eventual `lmp_features` table would have something like `congestion_proximity_score` per zone per hour, derived by joining that hour's binding constraints against the
  graph. That's issue #22 in the project board — matching facilities to nodes and computing the actual distances is the next real step, now that the graph itself exists.
