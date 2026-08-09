# Data Dictionary & Power Industry Glossary

Title: Added data dictionary and power industry glossary, Author: Angie Ohaeri, Date: August 7th Time: (session)

Title: Documented get_dataset("electricity/rto/region-data") as the RTO-level EIA
method for live/producer polling (get_grid_monitor can't filter by date, so it's
backfill-only), Author: Angie Ohaeri, Date: August 9th Time: (session)

Reference doc for someone new to the electricity industry. Two parts: (1) the
vocabulary needed to read PJM/EIA data without guessing, (2) every raw column that
actually comes back from this project's three data sources, so acronyms don't have
to be memorized from scratch each time.

---

## Part 1: Industry Glossary

### Entities

| Term | Meaning |
|---|---|
| **BA (Balancing Authority)** | The entity responsible for keeping electricity supply and demand matched in real time over its footprint. PJM is a BA. |
| **RTO (Regional Transmission Organization)** | A BA that also runs a competitive wholesale electricity *market* across multiple states/utilities, rather than one utility owning generation, transmission, and market all together. PJM is an RTO. |
| **ISO (Independent System Operator)** | Same functional role as an RTO; the two terms are used almost interchangeably (ISO is more common outside PJM's footprint, e.g. NYISO, ISO-NE). |
| **NERC (North American Electric Reliability Corporation)** | The reliability regulator for the North American grid. Shows up as a column (`NERC Region`) in PJM load data, not something this project queries directly. |
| **Zone / Subregion** | A sub-division of PJM's footprint, almost always mapping to a legacy utility's service territory (e.g. ComEd's old footprint = zone `CE`). This is the granularity both EIA-930 and PJM Data Miner report load at, one tier below the RTO-wide total. See the zone table below. |

### Grid & energy concepts

| Term | Meaning |
|---|---|
| **Load / Demand** | Power being consumed right now, in MW. Used interchangeably in this industry; EIA calls its column "Demand," PJM calls its column "Load" — same concept. This project's prediction target. |
| **Net Generation** | Power being produced. Not equal to load at any given instant — the difference is covered by imports/exports (interchange) and transmission losses. |
| **Interchange** | Power flowing between two BAs (e.g. PJM importing from NYISO). Positive/negative sign convention depends on the source; not currently used as a feature in this project. |
| **MW vs MWh** | MW (megawatt) is an instantaneous rate — how fast power is flowing right now. MWh (megawatt-hour) is energy — MW sustained for an hour. Hourly load data reported "in MW" is really the average MW over that hour, which numerically looks like MWh but isn't labeled that way by convention. |
| **Fuel mix / generation mix** | The breakdown of net generation by fuel source (coal, natural gas, nuclear, hydro, solar, wind). EIA-930 reports this; PJM's load feed does not. |
| **Capacity vs. Energy** | Capacity = max MW a resource *can* produce (a nameplate number). Energy = MW actually produced/consumed over time. This project deals entirely in energy (load), not capacity markets. |

### Market concepts (relevant once LMP work starts)

| Term | Meaning |
|---|---|
| **LMP (Locational Marginal Price)** | The $/MWh price of electricity at a specific grid location ("node"), for a specific time. Price, not load/demand — this is the *stretch goal* target per `architecture.md`, not the MVP one. |
| **LMP components** | LMP = **Energy** (system-wide marginal cost of the next MWh) + **Congestion** (extra cost from a transmission bottleneck at that location) + **Loss** (marginal cost of line losses getting power to that location). PJM's API returns all three plus the total. |
| **Pnode (pricing node)** | The specific grid location an LMP is priced at — could be a generator, a load point, a zone aggregate, or a trading **hub** (a standardized aggregate point used for hedging, easier to work with than thousands of individual nodes). |
| **Day-Ahead (DA) market** | PJM clears prices for every hour of the next day, once, the day before. |
| **Real-Time (RT) market** | PJM re-clears prices every 5 minutes (also reported hourly-averaged) to true up actual conditions against the day-ahead plan. |
| **Verified vs. unverified data** | PJM initially publishes real-time data provisionally, then re-publishes a "verified" version after settlement checks. The `Is Verified` flag in load data reflects this — recent rows may be `False` and get corrected later. |

---

## Part 2: PJM Zone / Subregion Code Reference

The 20 codes below are the shared vocabulary between EIA-930's `subba` facet and
PJM Data Miner's `Zone` column — the same 20 IDs come back from both sources.
Canonical machine-readable copy: `data/external/pjm_eia930_subregions.csv`.
`RTO` is not a zone — it's PJM's own code for the whole-footprint total, and only
appears in the PJM load feed, not the EIA subba list.

| id | name |
|---|---|
| CE | Commonwealth Edison (ComEd) |
| PE | PECO Energy |
| BC | Baltimore Gas & Electric |
| DOM | Dominion Virginia Power |
| AEP | American Electric Power |
| PEP | Potomac Electric Power (Pepco) |
| PS | Public Service Electric & Gas |
| PL | Pennsylvania Power & Light |
| JC | Jersey Central Power & Light |
| DUQ | Duquesne Lighting |
| ATSI | American Transmission Systems (Ohio) |
| DAY | Dayton Power & Light |
| DEOK | Duke Energy Ohio/Kentucky |
| AP | Allegheny Power |
| AE | Atlantic Electric |
| ME | Metropolitan Edison |
| DPL | Delmarva Power & Light |
| RECO | Rockland Electric |
| EKPC | East Kentucky Power Cooperative |
| PN | Pennsylvania Electric |
| OVEC | Ohio Valley Electric Corporation *(appears in PJM load data only, not the EIA subba list)* |

This project currently scopes weather + modeling to `RTO` + `CE`, `DOM`, `AEP`, `BC`
(see `data/external/pjm_weather_zones.csv`) — chosen for climate diversity, per
`architecture.md`'s "start small" call on zone count.

---

## Part 3: Raw Feature Dictionary (by source)

Columns as they actually come back from each API/library call used in this
project's notebooks — not aspirational, verified against the installed package
source and (for PJM) a live test pull.

### EIA-930 — RTO-level, historical backfill only (`gridstatus.EIA().get_grid_monitor(area_id="PJM")`)

One row per hour, whole-PJM only (no zone breakdown). Source: EIA's published grid
monitor Excel file per BA. **Cannot filter by date — always fetches full available
history.** This is what `notebooks/0.01-pjm-eia-explore.ipynb` actually used for the
one-time historical backfill (fine there, since a backfill wants full history
anyway), and is why the `load` table's `source='eia'` rows exist. Do **not** use this
for a live/recurring poll — see the `get_dataset` method below for that.

| column | meaning |
|---|---|
| `Interval Start` / `Interval End` | UTC hour boundary |
| `Area Id` / `Area Name` / `Area Type` | `PJM` / "PJM Interconnection, LLC" / `BA` |
| `Demand` | RTO-wide load, MW |
| `Demand Forecast` | EIA's own published day-ahead demand forecast, MW — useful as a baseline to beat |
| `Net Generation` | RTO-wide generation, MW |
| `Total Interchange` | Net power exported (positive) or imported (negative), MW |
| `NG: COL` / `NG: NG` / `NG: NUC` / `NG: OIL` / `NG: WAT` / `NG: SUN` / `NG: WND` / `NG: UNK` / `NG: OTH` | Net generation by fuel type: **Coal, Natural Gas, Nuclear, Oil, Water/hydro, Solar, Wind, Unknown, Other**. Note the acronym collision — `NG` is the *column prefix* for "Net Generation by fuel" here, but `NG` *inside* the prefix (`NG: NG`) specifically means natural gas. |
| `Positive Generation` | Generation only (excludes negative/curtailed values) |
| `Consumed Electricity` | Demand-side equivalent of Positive Generation |
| `CO2 Factor: *` / `CO2 Emissions: *` | Emissions intensity and totals by fuel type — not used by this project currently |

### EIA-930 — RTO-level, live/producer polling (`gridstatus.EIA().get_dataset("electricity/rto/region-data", start=, end=, facets={"respondent": "PJM"})`)

One row per hour, whole-PJM only — same underlying quantities as `get_grid_monitor`
above, but via the EIA v2 REST API's generic dataset endpoint, which **does support
real `start`/`end` date filtering**. This is the method the `load` producer should
actually poll with. Verified live (2026-08-09): a 1-day window returned exactly these
8 columns.

| column | meaning |
|---|---|
| `Interval Start` / `Interval End` | UTC hour boundary |
| `Respondent` / `Respondent Name` | `PJM` / "PJM Interconnection, LLC" |
| `Load` → `demand_mw` | RTO-wide load, MW |
| `Load Forecast` → `demand_forecast_mw` | EIA's own published day-ahead demand forecast, MW |
| `Net Generation` → `net_generation_mw` | RTO-wide generation, MW |
| `Total Interchange` → `total_interchange_mw` | Net power exported (positive) or imported (negative), MW |

No fuel-mix or emissions breakdown here — those only come from `get_grid_monitor` (or
the separate `electricity/rto/fuel-type-data` dataset, not currently used by this
project).

### EIA-930 — zone-level (`gridstatus.EIA().get_dataset("electricity/rto/region-sub-ba-data")`)

One row per hour per PJM subregion — this is the one that matches your zone
granularity and can cross-check the PJM load pull.

| column | meaning |
|---|---|
| `Interval Start` / `Interval End` | UTC hour boundary |
| `BA` / `BA Name` | Always `PJM` / "PJM Interconnection, LLC" here |
| `Subregion` | Zone code — one of the 20 in the table above |
| `Subregion Name` | Human-readable zone name |
| `MW` | Load for that zone/hour |

### PJM Data Miner — load (`gridstatus.PJM().get_load_metered_hourly()`, feed `hrl_load_metered`)

One row per hour per zone. This is the project's actual `load_mw` target source.

| column | meaning |
|---|---|
| `Interval Start` / `Interval End` | Local (Eastern) time boundary |
| `NERC Region` | Reliability region the zone belongs to |
| `Mkt Region` | PJM's internal market sub-region grouping |
| `Zone` | Zone code (matches the table above), or `RTO` for the whole-footprint total |
| `Load Area` | Finer-grained load reporting area within a zone |
| `MW` | Metered load |
| `Is Verified` | `False` for provisional recent data, `True` once PJM settles/corrects it |

### PJM Data Miner — LMP (`gridstatus.PJM().get_lmp()`, feeds `rt_hrl_lmps` / `da_hrl_lmps` / `rt_fivemin_hrl_lmps`)

One row per interval per pricing node. Stretch-goal data, not yet pulled.

| column | meaning |
|---|---|
| `Time` / `Interval Start` / `Interval End` | Interval boundaries |
| `Market` | `REAL_TIME_5_MIN`, `REAL_TIME_HOURLY`, or `DAY_AHEAD_HOURLY` |
| `Location Id` / `Location Name` / `Location Short Name` | The pnode this price applies to |
| `Location Type` | `ZONE`, `LOAD`, `GEN`, `AGGREGATE`, `INTERFACE`, `EXT`, `HUB`, `EHV`, `TIE`, `RESIDUAL_METERED_EDC` |
| `LMP` | Total locational marginal price, $/MWh |
| `Energy` | System-wide energy component of LMP |
| `Congestion` | Transmission-bottleneck component of LMP |
| `Loss` | Marginal line-loss component of LMP |

**Gotcha:** PJM's API only allows filtering by location for recent data — within 731
days (~2 years) for hourly markets, 186 days (~6 months) for the 5-minute market.
Querying further back returns *all* ~10,000+ pnodes for that date range; you filter
down to your locations client-side afterward. A full Jan-2023-forward LMP pull will
hit this path.

### Open-Meteo — historical weather (`historical-forecast-api.open-meteo.com`, hourly)

One row per hour per zone city. Units are Open-Meteo's metric defaults (no unit
override params are set in this project's fetch).

| column | meaning | unit |
|---|---|---|
| `temperature_2m` → `temperature` | Air temp at 2m height | °C |
| `precipitation` | Precipitation | mm |
| `wind_speed_10m` → `wind_speed` | Wind speed at 10m height | km/h |
| `cloud_cover` | Cloud cover | % |

Deliberately pulled from the **historical-forecast** API (what the forecast model
*said* at each past hour) rather than the archive/reanalysis API (what actually
happened, per ERA5). This matters for a forecasting model: at inference time you'll
only ever have forecast weather, never ground-truth reanalysis, so training on
forecast-quality data avoids a train/serve mismatch.

---

## Part 4: Acronym Quick Reference

| Acronym | Expansion | Where it shows up |
|---|---|---|
| BA | Balancing Authority | EIA columns, glossary |
| RTO | Regional Transmission Organization | PJM's own type; also literally the string `"RTO"` used as PJM's whole-footprint zone code in load data — two different meanings, same string |
| ISO | Independent System Operator | Industry-wide term, not PJM-specific |
| LMP | Locational Marginal Price | PJM LMP feed |
| DA / RT | Day-Ahead / Real-Time (market) | PJM LMP `Market` values |
| EPT | Eastern Prevailing Time | PJM API date params (`datetime_beginning_ept`) |
| UTC | Coordinated Universal Time | Used internally for all hypertable storage per `schema.md` |
| NG | **Overloaded**: "Net Generation" (EIA `Type` column) *or* "Natural Gas" (EIA fuel-mix column prefix `NG: NG`) | EIA-930 |
| MW / MWh | Megawatt / Megawatt-hour | Everywhere — MW is a rate, MWh is energy |
| pnode | Pricing node | PJM LMP feed |
| NERC | North American Electric Reliability Corporation | PJM load data column |
| CO2 | Carbon dioxide (emissions columns) | EIA grid monitor, unused currently |

---

## Part 5: Things worth knowing that don't fit a table

- **Same 20 zone codes, two independent sources.** EIA-930 (`region-sub-ba-data`) and
  PJM Data Miner (`hrl_load_metered`) both report zone-level load using the identical
  code set. That's a genuine opportunity to validate one against the other before
  trusting either as ground truth — they're independently collected and published.
- **Frequency mismatch is real, not an edge case.** EIA-930 is hourly. PJM LMPs come
  in 5-minute and hourly flavors. Per `architecture.md`, these are never silently
  resampled — each lands in its own hypertable at native resolution and gets joined
  in a named dbt model.
- **Timezones differ by source.** PJM's raw API timestamps are Eastern Prevailing
  Time (`_ept` fields exist alongside UTC ones); this project's weather pull
  explicitly requests UTC; `schema.md`'s `time` columns are all `timestamptz`. Doesn't
  matter what timezone data arrives in as long as it's tz-aware before it's written —
  Postgres `timestamptz` normalizes to UTC internally regardless.
- **"Verified" data changes after the fact.** Recent PJM load rows can have
  `Is Verified = False` and be revised later. If a backfill and a later re-pull
  disagree slightly for recent dates, this is almost certainly why, not a bug.
- **RTO total ≠ sum of the 4 zones you're using.** RTO is the sum of *all 20* zones,
  not just the 4 in `pjm_weather_zones.csv`. Don't expect `CE + DOM + AEP + BC` to
  reconcile against the `RTO` row.
- **Two RTO totals, from two different EIA methods, only one of which is pollable.**
  `get_grid_monitor` and `get_dataset("electricity/rto/region-data")` report the same
  underlying quantities but `get_grid_monitor` can't filter by date at all (full
  history every call) while `get_dataset` can. Backfill used the former; the producer
  must use the latter, or it'll refetch and reprocess years of data on every poll.
