# LMP Pricing Model — Approach

**Title: Initial draft + target-grain decision, Author: Angie Ohaeri, Date: August 22nd Time: (session)**

Stretch extension to the load-forecasting project (see `architecture.md`).

**Status: in progress.** Branch, `raw_lmp` schema, dbt scaffolding, and two raw tables exist.
No ingestion, features, or model built yet.

---

## Isolation

- Branch: `lmp-pricing-model` (created).
- Raw tables → schema `raw_lmp` (created), never `public`. Droppable via `DROP SCHEMA ... CASCADE`.
- dbt: `models/lmp_features/` → `+schema: lmp` → lands in `analytics_lmp`, separate from
  `analytics.features` (scaffolded in `dbt_project.yml`).
- MLflow: new experiment/model names — never `gridcast-lgbm-{h}h` or its `Production` stage.
- Prefect: new deployments, `paused=True` until validated.
- Kafka: skip it for daily-batch feeds (most of these) — use the `data_center_sync.py`
  direct-upsert pattern instead. Reserve Kafka for genuinely streaming data.

---

## Targets — two, not one mixed grain

Matches how PJM's markets actually clear:

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
  - **Unverified → verified reconciliation** (decided 2026-08-22): ingest Real-Time Unverified
    Five Minute LMPs continuously for freshness (updates ~every 5 min, vs. the verified feed's
    once-daily batch post — same `is_verified`-style upsert pattern `load` already uses).
    Serve live predictions from unverified data — there's no choice, verified data for "now"
    doesn't exist yet — but **train/backtest against the verified value once it lands**, not
    the unverified value the model saw at inference time, since unverified values can revise.

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

| Tier | Source | Status | Contributes |
|---|---|---|---|
| 1 — LMP decomposition | Real-Time Marginal Value (`raw_lmp.marginal_value_rt`) | backfilled — 755,530 rows, 2023-01-02 to 2026-08-21, 1,048 facilities | shadow price µ per binding constraint (`Monitored Facility`/`Contingency Facility`, not pnode/zone) — the congestion term. 5-min native since 2018, posts daily 11am-12pm ET |
| 1 | Day-Ahead Marginal Value (`raw_lmp.marginal_value_da`) | backfilled — 278,986 rows, 2023-01-01 to present | same congestion term, for the day-ahead target — no penalty factor/limit control fields (RT-only) |
| 1 | Forecasted Generation Outages | table built, backfilled — 120,666 rows, 2023-01-02 to present +90d | daily, 90-day horizon, RTO/West/Other only (not zonal) — weaker than hoped, still useful |
| 1 | Operator Initiated Commitments | checked, promising | zonal(!) out-of-merit unit commitments with a `Reason` field — "Constraint Management" reason ties directly to congestion. Monthly, updated the 20th. Narrow: only reliability-driven commitments, not general economic unit commitment |
| 1 | Scheduled Generation | checked, secondary | self-scheduled generation (runs regardless of price — must-run/contractual, not operator-directed) distorts normal dispatch, causes uplift charges. Weaker than Operator Initiated Commitments though: RTO-wide only (2 aggregate MW numbers, no zone field), so it can only inform λ, not the congestion term — same bucket as fuel mix/gas cost, not a second zonal signal. Daily 5pm |
| 1 | Generation by Fuel Type | planned | what fuel's on the margin; informs λ, RTO-wide by design (see mechanism above, not a limitation) |
| 1 | EIA natural gas fuel cost (`eia.gov/electricity/data.php`, monthly, by state) | planned | how expensive that margin is; pairs with fuel type to approximate λ (the "spark spread" signal) |
| 1 | Day-Ahead Transmission Constraints | planned | the congestion pattern set |
| 1 | Day-Ahead Hourly LMPs | planned | enables a DA-RT basis feature |
| 1 | Generation and Extra High Voltage Losses | planned, lowest priority | the losses term, smallest of the three components |
| 2 — renewable variability | Five Minute Solar/Wind Generation + forecasts | planned | duck-curve dynamics |
| 3 — investigate first | Energy Market Generation Offers | not a live feature — 4-month posting delay (PJM's stated policy) | not useless though: genuinely rich bid-curve data (masked generator ID, MW/BID pairs, start costs, ECOMAX/ECOMIN). Heat rate is a slow-changing physical property, not something that needs to be fresh — use this (even 4mo stale) to back-calculate typical heat rate/markup per unit or fuel type (the primer's "effective heat rate"), then apply that calibration to **live** fuel cost to estimate a live marginal-cost curve. Freshness requirement moves from the bid (stale) to the heat rate (doesn't need to be fresh) |
| 3 | Daily Cleared INCs, DECs, UTCs | checked, weak — posts daily 4am but only 1 row/day of RTO-wide MW totals (no price/location) | at best a blunt proxy for anticipated DA-RT congestion via UTC volume; low priority |
| 3 | Transfer Interface Information / Transmission Limits | not started | likely redundant with Marginal Value |
| 3 | Off-Cost Operations | checked | out-of-merit ops for voltage/reactive support, not congestion — `Facility`/`Contingency` fields match Marginal Value's structure. Monthly, updated the 4th. Correlates with congestion but isn't the same mechanism |

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

  **Build the graph from each line's own endpoint coordinates directly, not by name-joining
  `SUB_1`/`SUB_2` against the substations file.**


**Shift factor**: πᵢ = λ + ΣₖAᵢₖµₖ. A isn't public and can't be legitimately derived. Two proxies:

1. **Structural** — graph-shortest-path from a constraint's substation to a zone, voltage-weighted.
2. **Empirical** — regress each zone's `congestion_price` against historical facility shadow
   prices ("revealed" shift factor). Better once enough binding-event history exists, noisy early on.

---

## Next steps

- Build producer/loader + dbt models for the two tables that already exist (Real-Time Marginal
  Value, Forecasted Generation Outages).
- Then: Generation by Fuel Type, Day-Ahead Transmission Constraints.
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
