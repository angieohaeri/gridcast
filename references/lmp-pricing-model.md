# LMP Pricing Model — Approach

**Title: Initial draft, consolidating research session on LMP forecasting, Author: Angie Ohaeri, Date: August 22nd Time: (session)**

Stretch extension to the core load-forecasting project (see `architecture.md`'s "LMP forecasting
is a stretch extension once the load model and LMP-alignment pattern are proven"). Status:
**exploratory / not started** — this document is the plan, not a build log. Nothing below is
implemented yet.

---

## Isolation from the existing project

This adds several new data sources and a second model on top of an already-working load-forecast
pipeline. Keep it fully separable so it can be developed, and abandoned or merged, without risk to
the live load model:

- **Git**: work on a dedicated feature branch (`lmp-pricing-model`), not `main`.
- **Raw tables**: existing landing tables (`load`, `lmp`, `weather`) live in schema `public`. New
  raw feeds (marginal value, outages, fuel mix, transmission lines, substations, bus model, etc.)
  go in their own schema, e.g. `raw_lmp` — never `public`. Makes the whole thing droppable with a
  single `DROP SCHEMA ... CASCADE` if the approach changes.
- **dbt**: existing feature models write to schema `analytics` (`analytics.features`, read by
  `predict.py`/`train.py`). New models go in a new `models/lmp_features/` directory with
  `+schema: lmp` set — dbt's default schema-naming (no custom macro currently overrides it) lands
  these in `analytics_lmp`, isolated from the live model's feature table.
- **MLflow**: register under new experiment/model names, never `gridcast-lgbm-{h}h` — must not
  touch the `Production` stage pointer the live FastAPI serving path reads from.
- **Prefect**: new deployments, `paused=True` until validated (same pattern already used for
  `api_ping`/`dashboard_ping`).
- **Kafka** (if any new feed genuinely needs streaming — most won't, see below): new topic names,
  own consumer group, own `_dlq` topic, isolated from `load`/`lmp`/`weather`.

---

## The mechanism (why this is harder than load forecasting)

Real-time LMP = marginal energy price (λ, the Lagrange multiplier of PJM's system-wide power
balance constraint in security-constrained economic dispatch) **+ the sum of shadow prices on
every currently congested transmission line**. Congestion means a line is at its capacity limit,
forcing costlier local generation onto the margin. This is the output of a constrained
optimization over generator bids and grid topology — not a simple supply/demand clearing price —
which is why plain weather/load-style features have a real ceiling on how well they can predict it.

**Key research finding** (Ji, Kim, Thomas, Tong 2013, "Forecasting Real-Time Locational Marginal
Price: A State Space Approach" — PJM 5-bus simulation): a plain ANN trained on historical
load/price data benchmarked *worst* (MAPE 20.6%) against a method that instead models the
discrete pricing-mechanism state (which generators are marginal-eligible, which lines are
congested) as a Markov chain (MAPE 11.75%). Their explanation: ANNs are very sensitive to
unpredictable price spikes; models that know the pricing mechanism can anticipate spikes
structurally instead of statistically. **Implication**: feature engineering that approximates
congestion state — what's binding, what's forcing costlier units onto the margin — is likely to
matter more than raw price/load lags for catching spikes, which is exactly where a lags-only
model fails. Caveat: 2013 paper, toy simulation, ignores transmission losses, 6-8hr horizon only —
directionally correct, not current state of the art (newer GAN/spatio-temporal approaches
reportedly close the gap).

---

## Data sources and what each contributes

### Already ingested (existing pipeline)
| Source | Contributes |
|---|---|
| `load` (PJM `hrl_load_metered` + EIA `region-data`) | Baseline zonal demand — indirectly informs LMP via the energy component |
| `lmp` (PJM `rt_hrl_lmps`, hourly) | The target variable itself (real-time settled LMP) |
| `weather` (Open-Meteo) | Demand driver — heat/cold swings shift which generators are needed |

### New — Tier 1 (directly maps to the LMP decomposition: λ + Σ shadow prices)
| Source | Contributes |
|---|---|
| Day-Ahead/Real-Time Marginal Value (PJM Data Miner, "Constraints") | Shadow price µ per binding constraint — the direct congestion term. Keyed by `Monitored Facility`/`Contingency Facility` (transmission line/transformer names), not pnode/zone — see zone-attribution plan below |
| Day-Ahead Transmission Constraints | The congestion pattern set itself — which lines are binding |
| Generation by Fuel Type | Which fuel is on the margin; gas-on-margin hours behave very differently from coal/nuclear-on-margin hours |
| Forecasted Generation Outages + Generation Outage for Seven Days by Type | Outages are what force costlier units onto the margin — highest-value addition after load/weather |
| Day-Ahead Hourly LMPs | Enables a DA-RT spread ("basis") feature — one of the most standard, well-documented LMP volatility signals |

### New — Tier 2 (renewable variability / duck-curve dynamics)
| Source | Contributes |
|---|---|
| Five Minute Solar Generation + Power Forecast | Renewable output driving fast ramps |
| Five Minute Wind Generation + Power Forecast | Same, for wind |

### New — Tier 3 (investigate before committing)
| Source | Contributes |
|---|---|
| Energy Market Generation Offers / Daily Cleared INCs, DECs and UTCs | Closest thing to actual bid data — the literal private input SCED optimizes over. Check whether it's published live or with a settlement-style delay/aggregation first |
| Transfer Interface Information / Transmission Limits | Live congestion proxies — likely redundant with Marginal Value data above, worth a look |

### New — near-term load lag capability (separate deferred item, see `project_instantaneous_load_feature` memory)
| Source | Contributes |
|---|---|
| Instantaneous Load (PJM, ~5-min updates) | Unlike settled `hrl_load_metered` (~2-3 day lag), this has no settlement lag — could enable 1h/3h/24h demand lag features the current `load_features.sql` deliberately excludes |
| Unverified real-time 5-min LMP | Same idea, for LMP — low-latency autoregressive lag features for a short-horizon RT LMP model. Caveat: unverified values can revise; evaluate against the verified value once it lands |

### New — zone attribution & shift-factor proxy (reference/static data, not streaming)
| Source | Contributes |
|---|---|
| `lmp-bus-model.xlsx` (PJM, "PJM Bus Model") | `Pnode ID → Transmission Zone → Substation → Voltage → Equipment → Type`. Zone codes match project convention exactly. Direct substation→zone lookup |
| `lmp-aggregate-definitions.xlsx` (PJM, = Data Miner's public "Fixed Weighted Average Aggregate Definitions") | Same substation/zone linkage, organized around aggregate (hub) pnodes |
| `electric_substation_hifld_v4.gpkg` (HIFLD public open release, in `data/external/`) | 75,328 US substations with real lat/lon, voltage range, line-connectivity count. ~51% have real names (rest are `UNKNOWN######` placeholders); `-999999` sentinel for missing numeric fields |
| `pjm_zones.geojson` + `hifld_electric_retail_service_territories_pjm.geojson` (in `data/external/`) | Zone/utility-territory polygons — enables point-in-polygon zone attribution from substation lat/lon directly, cleaner than name-matching |
| US Electric Power Transmission Lines (HIFLD/EIA via FWS ArcGIS Open Data — not yet downloaded, schema confirmed) | `SUB_1`/`SUB_2` per line = a real substation connectivity graph, plus `VOLTAGE`/`VOLT_CLASS`. Public, unrestricted. Enables graph-shortest-path distance (optionally voltage-weighted as an impedance proxy) from a congested facility to a zone — a meaningfully better proxy for shift-factor decay than straight-line geographic distance |

**Explicitly ruled out**: PJM's login-gated GIS system map. Detailed grid topology/bus connectivity
data of that granularity is very likely CEII-protected (Critical Energy/Electric Infrastructure
Information under FERC regulation); the login almost certainly carries terms restricting
systematic extraction, similar to Data Miner's own redistribution restrictions. Not worth the risk
when the public alternatives above cover the same need.

---

## The shift-factor problem

Formally, a bus's LMP = λ + Σₖ Aᵢₖµₖ, where A is PJM's shift-factor matrix (how much a bus's
injection affects flow on each congested line) — not published, and not legitimately derivable
from public data (needs real impedance values + full AC/DC power-flow topology). Two proxy tracks,
neither exact:

1. **Structural/geographic proxy** (available now): build the substation graph from the
   transmission-lines dataset; attribute each substation to a zone via point-in-polygon join
   against the zone polygons (not facility-name string matching); compute shortest weighted path
   (voltage-weighted as a rough impedance stand-in) from a congested facility's substation(s) to
   each zone's substations, as a per-constraint, per-zone "electrical proximity" feature.
2. **Empirically-learned proxy** (more principled, longer-term): once the Marginal Value feed is
   ingested, regress/correlate each zone's `congestion_price` (already computed in
   `lmp_features.sql`) against historical facility shadow-price series — the resulting
   coefficients are a "revealed" shift factor, learned from actual co-movement rather than
   topology. Needs enough historical binding events per facility to be reliable; sparse/noisy for
   rarely-binding constraints early on.

---

## Architecture note: not everything needs Kafka

The existing Kafka producer→consumer→hypertable pattern (`load`/`lmp`/`weather`) exists because
those feeds are genuinely streaming and need upsert-on-revision handling. Several of the new
feeds are naturally daily-batch (outages, fuel mix, day-ahead constraints, marginal value) and fit
the lighter `data_center_sync.py` pattern instead — a single Prefect flow that pulls and
upserts/appends directly, no topic. This project is explicitly built to practice the Kafka
pattern (see `CLAUDE.md`), so that's a deliberate reason to keep using it for the existing
streaming feeds — but there's no reason to default new daily-batch feeds into it too. Start with
2-3 Tier 1 feeds using the lighter pattern, confirm they move a baseline model's spike accuracy,
before scaling out further.

---

## Open questions / next steps

- Pick 2-3 Tier 1 feeds to start with (recommend: Day-Ahead/Real-Time Marginal Value, Forecasted
  Generation Outages, Generation by Fuel Type) and validate they move accuracy before expanding.
- Investigate Tier 3 feeds' actual publish latency before committing.
- Build the substation graph + zone attribution pipeline (structural proxy) as a prerequisite for
  using Marginal Value data at all.
- Decide N (forecast horizon) for the LMP target — same open question as the deferred load-forecast
  feature (`project_pjm_load_forecast_feature` memory).
- Revisit whether instantaneous load / unverified 5-min LMP are worth the added ingestion
  complexity once the Tier 1 congestion features are proven out.
