# LMP Pricing Model — Approach

**Title: Initial draft + target-grain decision, Author: Angie Ohaeri, Date: August 22nd Time: (session)**

**Title: Ingestion pipeline built for all 10 raw_lmp sources, Author: Angie Ohaeri, Date: September 5th Time: (session)**

No Kafka: none of the sources stream (checked on Data Miner: `marginal_value_rt` daily 11am-12pm ET though 5-min native, `transmission_constraints_da` 12-2pm ET, `forecasted_generation_outages` 4am ET, `generation_by_fuel` hourly at :15, `operator_initiated_commitments` monthly on the 20th; `marginal_value_da`, `lmp_da_zonal`, `scheduled_generation`, `generation_ehv_losses` had no feed page and are treated as daily). So, per `decisions.md` (Isolation), they use the direct pull-and-upsert pattern: `src/prefect/raw_lmp_sync.py`, one `@flow` per source with a trailing window for settlement revisions. Deployments: `src/prefect/lmp_deployments.py` (its own `serve()` process and compose service `lmp-prefect-deployments`); daily syncs run in order inside `lmp_morning` / `lmp_afternoon` (`lmp_daily.py`), the rest on their own Cron, each `paused=True` until validated. A one-off gap-fill closed the ~2-week gap after the Aug 21-23 backfill (script removed; `maintenance/gapfill_raw_lmp_interior.py` fills later holes).


**Decision: the empirical shift-factor proxy (see below) is now the primary approach,
not a fallback.** It doesn't need coordinates at all, and it's arguably the *more*
accurate quantity to estimate anyway — see `decisions.md` for the tradeoffs.

Stretch extension to the load-forecasting project (see `../architecture.md`).

**Status: modeling in progress (data pipeline done).** Branch, `raw_lmp` schema, dbt
(`analytics_lmp`), the raw tables, their sync flows (`raw_lmp_sync.py`), the feature tables by grain,
the loader (`lmp_model/dataset.py`), walk-forward splits, the edge builder and the GNN
(`models/model.ipynb`) exist. Results: `experiments.md`.

**Status update, Author: Angie Ohaeri (assisted), Date: October 29th Time: 1:55am CT** - stale sections
removed from this file (the 5-min RT target, the zonal-baseline next steps, done next-step lists).

---

## Isolation

Isolated from the working load-forecasting stack at every layer (branch, schema, dbt
target, MLflow naming, Prefect deployments, Kafka usage) — see `decisions.md`.

---

## Targets

**Title: Current target for the first GNN, Author: Angie Ohaeri (assisted), Date: October 24th Time: 11:06am CT**

**Current**: pnode-level **RT minus DA spread**, hourly, forecast **next-day** - issued at
14:30 ET on day D for every hour of D+1 at all 1,580 nodes (the original 1,505 plus 75
`constraint` nodes since Oct 27; 11-35h ahead); RT reported as DA + predicted spread. Data:
`raw_lmp.lmp_rt` (labels: `is_verified` rows) and `raw_lmp.lmp_da`. Feature cutoff, baselines and
metrics: `decisions.md` ("First GNN: next-day 24-hour horizon, RT minus DA spread target"). The
earlier zonal two-target plan with a 5-min RT target was superseded (`decisions.md`, 2026-10-22 and
2026-10-23 entries).

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
inform λ without needing a zonal breakdown. But copying them into every node's input made the GNN
worse (`experiments.md` E04): 59% of spread variance is the hour's mean across nodes (E07), so these
features belong in an hour-level model of that shared part, not in each node's row. All
node-to-node LMP variation is the congestion term's job — that's what the Marginal Value feed +
substation-graph proxy work (below) is for.

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

**Already ingested**: `load`, `lmp` (hourly `rt_hrl_lmps`), `weather` (the load stack), plus the forecast
feeds the pnode model uses: PJM load forecast by area (`raw_lmp.load_forecast`) and Open-Meteo zone
weather forecasts (`raw_lmp.weather_forecast`), both point in time. Nodal RT and DA LMPs for the 1,580
pnodes are `raw_lmp.lmp_rt` / `lmp_da`.

Table structure, row counts, and date ranges for every backfilled `raw_lmp` table:
`schema.md`. Deferred-source rationale: `decisions.md` ("Data source scope").

| Tier | Source | Status | Contributes |
|---|---|---|---|
| 1 — LMP decomposition | Real-Time Marginal Value (`raw_lmp.marginal_value_rt`) | backfilled | shadow price µ per binding constraint (`Monitored Facility`/`Contingency Facility`, not pnode/zone) — the congestion term. 5-min native since 2018, posts daily 11am-12pm ET |
| 1 | Day-Ahead Marginal Value (`raw_lmp.marginal_value_da`) | backfilled | same congestion term, for the day-ahead target — no penalty factor/limit control fields (RT-only), posts daily 12pm-2pm ET |
| 1 | Forecasted Generation Outages (`raw_lmp.forecasted_generation_outages`) | backfilled | daily, 90-day horizon, RTO/West/Other only (not zonal) — weaker than hoped, still useful |
| 1 | Operator Initiated Commitments (`raw_lmp.operator_initiated_commitments`) | backfilled | zonal(!) out-of-merit unit commitments with a `Reason` field — "Constraint Management" reason ties directly to congestion. Irregular event-level timestamps, not monthly snapshots — narrow: only reliability-driven commitments, not general economic unit commitment |
| 1 | Scheduled Generation (`raw_lmp.scheduled_generation`) | backfilled | self-scheduled generation (runs regardless of price — must-run/contractual, not operator-directed) distorts normal dispatch, causes uplift charges. RTO-wide only (2 MW numbers, no zone field), so it can only inform λ, not the congestion term — same bucket as fuel mix/gas cost, not a second zonal signal |
| 1 | Generation by Fuel Type (`raw_lmp.generation_by_fuel`) | backfilled | what fuel's on the margin; informs λ, RTO-wide by design (see mechanism above, not a limitation) |
| 1 | EIA natural gas fuel cost (`raw_lmp.natural_gas_fuel_cost`) | backfilled | how expensive that margin is; pairs with fuel type to approximate λ (the "spark spread" signal). Not a PJM source — separate EIA API pull, ~3-month reporting lag unlike the PJM feeds |
| 1 | Day-Ahead Transmission Constraints (`raw_lmp.transmission_constraints_da`) | backfilled | the congestion pattern set — which facility/contingency pairs bound and for how long (duration, no price magnitude; pairs with `marginal_value_da`'s shadow price for the same facility) |
| 1 | Day-Ahead Hourly LMPs (`raw_lmp.lmp_da_zonal`) | backfilled | enables a DA-RT basis feature (same zone×hour grain as `public.lmp`, DA market instead of RT) |
| 1 | Generation and Extra High Voltage Losses (`raw_lmp.generation_ehv_losses`) | backfilled | the losses term, smallest of the three components — was the lowest priority, now sourced |
| 1 | PJM load forecast by area (`raw_lmp.load_forecast`) | built — history `load_frcstd_hist` + live `load_frcstd_7_day` snapshot | system stress per area; the EIA forecast is RTO-only so was not used |
| 1 | Open-Meteo weather forecast (`raw_lmp.weather_forecast`) | built — `previous_day1` history + live snapshot | temperature, wind, cloud, HDD/CDD per zone as forecast at the cutoff (observations would leak the future) |
| 2 — renewable variability | Five Minute Solar/Wind Generation + forecasts | planned | duck-curve dynamics |
| 3 — investigate first | Energy Market Generation Offers | not a live feature — 4-month posting delay (PJM's stated policy) | not useless though: genuinely rich bid-curve data (masked generator ID, MW/BID pairs, start costs, ECOMAX/ECOMIN). Heat rate is a slow-changing physical property, not something that needs to be fresh — use this (even 4mo stale) to back-calculate typical heat rate/markup per unit or fuel type (the primer's "effective heat rate"), then apply that calibration to **live** fuel cost to estimate a live marginal-cost curve. Freshness requirement moves from the bid (stale) to the heat rate (doesn't need to be fresh) |
| 3 | Daily Cleared INCs, DECs, UTCs | checked, weak — posts daily 4am but only 1 row/day of RTO-wide MW totals (no price/location) | at best a blunt proxy for anticipated DA-RT congestion via UTC volume; low priority |
| 1 | Day-ahead interface flows and limits (`raw_lmp.interface_flows_da`) | backfilled | flow / limit on 6 transfer interfaces for the target day; shows a constraint approaching before it binds |
| 1 | Scheduled interchange (`raw_lmp.scheduled_interchange`) | backfilled | planned flow on 19 tie lines into PJM, previous day |
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

Current open experiments: `experiments.md` ("Open").

Graph-distance matching (facility → `transmission_nodes` → shortest path to zone) is on
hold, not deleted — `raw_lmp.facilities` + CEII ceiling means it can only ever cover
~24% of constraint-hours. Full reasoning: `decisions.md`.
