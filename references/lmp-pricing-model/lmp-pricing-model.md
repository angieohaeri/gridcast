# LMP Pricing Model — Approach

**Title: Initial draft + target-grain decision, Author: Angie Ohaeri, Date: August 22nd Time: (session)**


**Decision: the empirical shift-factor proxy (see below) is now the primary approach,
not a fallback.** It doesn't need coordinates at all, and it's arguably the *more*
accurate quantity to estimate anyway — see `decisions.md` for the tradeoffs.

Stretch extension to the load-forecasting project (see `../architecture.md`).

**Status: in progress.** Branch, `raw_lmp` schema, dbt scaffolding, and 13 raw tables
exist: 10 data-source tables (all of Tier 1, no exceptions — see the table below), the
2 transmission-graph tables (`transmission_nodes`/`transmission_edges`), and the
`facilities` name/geo lookup (partial coverage, see above). No ingestion pipeline,
features, or model built yet for any of it.

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
| 1 | Operator Initiated Commitments (`raw_lmp.operator_initiated_commitments`) | backfilled | zonal(!) out-of-merit unit commitments with a `Reason` field — "Constraint Management" reason ties directly to congestion. Irregular event-level timestamps, not monthly snapshots — narrow: only reliability-driven commitments, not general economic unit commitment |
| 1 | Scheduled Generation (`raw_lmp.scheduled_generation`) | backfilled | self-scheduled generation (runs regardless of price — must-run/contractual, not operator-directed) distorts normal dispatch, causes uplift charges. RTO-wide only (2 MW numbers, no zone field), so it can only inform λ, not the congestion term — same bucket as fuel mix/gas cost, not a second zonal signal |
| 1 | Generation by Fuel Type (`raw_lmp.generation_by_fuel`) | backfilled | what fuel's on the margin; informs λ, RTO-wide by design (see mechanism above, not a limitation) |
| 1 | EIA natural gas fuel cost (`raw_lmp.natural_gas_fuel_cost`) | backfilled | how expensive that margin is; pairs with fuel type to approximate λ (the "spark spread" signal). Not a PJM source — separate EIA API pull, ~3-month reporting lag unlike the PJM feeds |
| 1 | Day-Ahead Transmission Constraints (`raw_lmp.transmission_constraints_da`) | backfilled | the congestion pattern set — which facility/contingency pairs bound and for how long (duration, no price magnitude; pairs with `marginal_value_da`'s shadow price for the same facility) |
| 1 | Day-Ahead Hourly LMPs (`raw_lmp.lmp_da_hourly`) | backfilled | enables a DA-RT basis feature (same zone×hour grain as `public.lmp`, DA market instead of RT) |
| 1 | Generation and Extra High Voltage Losses (`raw_lmp.generation_ehv_losses`) | backfilled | the losses term, smallest of the three components — was the lowest priority, now sourced |
| 2 — renewable variability | Five Minute Solar/Wind Generation + forecasts | planned | duck-curve dynamics |
| 3 — investigate first | Energy Market Generation Offers | not a live feature — 4-month posting delay (PJM's stated policy) | not useless though: genuinely rich bid-curve data (masked generator ID, MW/BID pairs, start costs, ECOMAX/ECOMIN). Heat rate is a slow-changing physical property, not something that needs to be fresh — use this (even 4mo stale) to back-calculate typical heat rate/markup per unit or fuel type (the primer's "effective heat rate"), then apply that calibration to **live** fuel cost to estimate a live marginal-cost curve. Freshness requirement moves from the bid (stale) to the heat rate (doesn't need to be fresh) |
| 3 | Daily Cleared INCs, DECs, UTCs | checked, weak — posts daily 4am but only 1 row/day of RTO-wide MW totals (no price/location) | at best a blunt proxy for anticipated DA-RT congestion via UTC volume; low priority |
| 3 | Transfer Interface Information / Transmission Limits | not started | likely redundant with Marginal Value |
| 3 | Off-Cost Operations | checked | out-of-merit ops for voltage/reactive support, not congestion — `Facility`/`Contingency` fields match Marginal Value's structure. Monthly, updated the 4th. Correlates with congestion but isn't the same mechanism |
| skip | Real-Time Default Marginal Value Override | checked | operational fallback-price flag (what PJM substitutes when the normal RT calc doesn't produce one), not a price driver |
| skip | Balancing Transmission Congestion Preliminary Billing Data | checked | settlement/accounting — allocates congestion cost to who pays whom after the fact, not predictive |
| 3 | Day-Ahead Ratings | not started | facility thermal/emergency headroom; needs the same facility geocoding as Marginal Value (`raw_lmp.facilities`, partial + CEII-capped) — defer |
| 3 | Up-To-Congestion Bid Screening | not started | financial arbitrage bid data, closest thing to revealed trader expectations of congestion; check if published live vs. delayed/aggregated before committing |
| 3 | Operating Reserve Rates Preliminary | not started | separate co-optimized ancillary market; reserve-price spikes can proxy system stress but it's a new mechanism to model, not congestion itself |
| 2 | Instantaneous Dispatch Rates | not started | zonal(!), 15-second native — being zone-keyed skips the facility-name attribution problem entirely (zonal analog to Generation by Fuel Type). Don't point-sample hourly (a single 15s snapshot misses ramps/spikes); batch-pull the time series per date range and aggregate to hourly (mean/max/std/range) in dbt instead, if Data Miner supports a window query for this report — unverified, check first |

### Zone attribution / shift-factor proxy (reference data, not streaming)

- `lmp-bus-model.xlsx`, `lmp-aggregate-definitions.xlsx` (PJM) — substation → zone, direct lookup.

- `electric_substation_hifld_v4.gpkg` (`data/external/`) — substation lat/lon, voltage, line
  count. ~51% named; `-999999` = missing.

- `pjm_zones.geojson` + HIFLD retail-territory geojson (`data/external/`) — point-in-polygon
  zone attribution, cleaner than name-matching.

- `US_Electric_Power_Transmission_Lines.gpkg` (HIFLD/EIA, `data/external/`) — substation
  graph for the structural proxy below. Details/caveats: schema.md (`transmission_nodes`/
  `transmission_edges`).

Graph-construction and zone-attribution-fallback decisions: `decisions.md`.

**Shift factor**: πᵢ = λ + ΣₖAᵢₖµₖ. A isn't public. Two proxies:

1. **Structural** — graph-distance, facility to zone. Capped at ~24% real coverage —
   PJM classifies facility locations as CEII and won't distribute the rest (`decisions.md`).
2. **Empirical** — regress zone `congestion_price` on facility shadow prices. **Now the
   primary approach** (`decisions.md`). Thousands of facilities vs. 20 zones needs
   dimensionality reduction first — plain PCA is variance-driven and will wash out the
   long tail, so pair it with **PLS** (picks components by covariance with the target,
   not just predictor variance) rather than relying on PCA alone.

---

## Next steps

- Build producer/loader + dbt models for all 10 raw data-source tables — all backfilled.
- Ship a simple zonal congestion baseline first (`operator_initiated_commitments` +
  system-wide binding-constraint count), then layer the empirical shift-factor
  regression on top and measure the lift — don't build the complex feature first.
- Decide forecast horizon N for both targets (same open question as
  `project_pjm_load_forecast_feature`).

Graph-distance matching (facility → `transmission_nodes` → shortest path to zone) is on
hold, not deleted — `raw_lmp.facilities` + CEII ceiling means it can only ever cover
~24% of constraint-hours. Full reasoning: `decisions.md`.
