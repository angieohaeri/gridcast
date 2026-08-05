# Electricity Demand Forecasting (PJM)

## Project Overview
End-to-end ML portfolio project. Predicts short-term zonal electricity load (MW) on the
**PJM Interconnection** using EIA-930 hourly demand data and PJM real-time LMP data.
Built to practice end-to-end ML engineering: ingestion → storage → feature engineering →
modeling → serving → deployment. (Pivoted from an earlier bike-share-availability version
of this project — see `references/decisions.md` for why.)

**Hardware:** Intel Mac Mini running Ubuntu 24.04 Server, self-hosted, exposed via
Cloudflare Tunnel.

---

## Architecture
```
EIA-930 + PJM Data Miner 2 + Open-Meteo → Kafka → TimescaleDB → dbt → LightGBM → FastAPI → Streamlit
```

---

## Stack
- **Python 3.12** — snake_case, type hints throughout, no ORM
- **Kafka** KRaft mode (`confluent-kafka`) — topics: `load`, `lmp`, `weather`
- **TimescaleDB** on PostgreSQL 16 — hypertables + continuous aggregates
- **dbt** — feature engineering SQL models (incremental)
- **Prefect** — orchestration and scheduling
- **MLflow** — experiment tracking + model registry
- **LightGBM** — primary model; Temporal Fusion Transformer planned as extension
- **FastAPI** — prediction serving, loads model from MLflow registry by stage
- **Streamlit + Pydeck** — live map dashboard
- **Docker Compose** — full stack on Mac Mini
- **Cloudflare Tunnel** — public HTTPS URL, no port forwarding

---

## Repo Layout
```
gridcast/           installable Python package (pip install -e .)
  config.py         project-root-relative path constants — import these, never hardcode paths
  dataset.py        data loading functions (TimescaleDB, historical CSVs)
  features.py       Python-side feature engineering (cyclical encoding, zone features)
  modeling/
    train.py        reads processed features → trains → logs to MLflow
    predict.py      loads from MLflow registry → exposes predict()
src/
  producers/          Kafka producer scripts (poll EIA-930, PJM LMP, Open-Meteo, publish messages)
  consumers/          Kafka consumers (read topics, write to TimescaleDB)
  dbt/                feature engineering SQL models
  prefect/            orchestration flows
  training/           MLflow training entry points
  api/                FastAPI app
  dashboard/          Streamlit app
  infra/              Cloudflare config, Docker supporting files
notebooks/          EDA and exploration (naming: 0.NN-description.ipynb)
tests/              pytest — mirrors package structure
references/         architecture, schema, decisions, experiment log, best practices
data/
  raw/              immutable source data — never write here programmatically
  external/         third-party data (Open-Meteo history, PJM zone GeoJSON)
  interim/          partially processed data
  processed/        model-ready feature tables
```

---

## Data Sources
- **gridstatus** — `pip install gridstatus`; open-source wrapper around ISO endpoints, use the `PJM()` class for load, LMP, and fuel mix
- **EIA Open Data API v2** — `https://www.eia.gov/opendata/`; `/v2/electricity/rto/` routes carry EIA-930 hourly demand, forecast demand, net generation, and interchange; free API key required
- **PJM Data Miner 2** — `https://dataminer2.pjm.com/`; real-time hourly LMP feed at `rt_hrl_lmps`; free account, no cost
- **Open-Meteo** — `https://open-meteo.com/` (free, no key required); scoped to a handful of representative PJM zone cities, not every zone

**Note:** EIA-930 is hourly; LMPs are 5- or 15-minute. These land in separate hypertables
and are joined explicitly in a named dbt model — never silently resampled.

---

## Common Commands
```bash
make install        # pip install -e . in active venv
make data           # pull raw EIA-930 / PJM LMP extracts
make features       # run dbt build
make train          # train + log to MLflow
make test           # pytest tests/
make lint           # ruff check . --fix
make up             # docker compose up -d
make down           # docker compose down
```

---

## ML Details
**Target:** zonal `load_mw`, N hours ahead (regression)

**Features:**
- Temporal: hour, day of week, is_weekend, is_holiday, cyclical sin/cos encoding
- Lag: `load_mw` at t-1h, t-3h, t-24h, t-168h (same time last week)
- Rolling: mean over 6hr, 24hr, 7d
- Zonal: zone/BA as a categorical feature (global model, mirrors "station as feature" pattern)
- Weather: current + 1hr forecast per zone (temp, precipitation, wind, cloud cover)

**Models:**
- LightGBM (baseline) — global model with zone as feature
- Temporal Fusion Transformer (planned extension)
- LMP forecasting as a stretch extension once the load model and frequency-alignment pattern are proven
- Load from MLflow registry by stage: `models:/gridcast-lgbm/Production`

---

## Conventions
- Config via environment variables only — never hardcode values (`python-dotenv`)
- Raw SQL for TimescaleDB — no ORM
- MLflow run naming: `{model}_{feature_set}_{YYYYMMDD}`
- Logs via `loguru` module — no `print()` in package code
- Dead letter queue topics: `load_dlq`, `lmp_dlq`, `weather_dlq`
- Manual Kafka offset commits only (`enable.auto.commit: False`)
- dbt incremental models for anything touching raw snapshot tables

---

## Key Reference Files
- `references/architecture.md` — full architecture and starting point
- `references/schema.md` — TimescaleDB table definitions
- `references/best-practices.md` — quality checklist per tool
- `references/ccds-practices.md` — file structure and workflow
- `references/decisions.md` — key technical decisions and rationale
- `references/experiments.md` — dated experiment log

**Note:** When writing to any md file, note the author and the date and time of the entry (ex. Title: Modified x data source, Author: Angie Ohaeri, Date: July 27th Time: 2:49pm)

---

## Notes on working style from Angie

How Angie wants things done, list might change in the future. Meant to maximize	readability and minimize “AI slop”.

- when writing code, especially doing work on tabular or other forms of data (DataFrames, CSVs, etc.), examine the schema/structure first before making changes
- when writing code, do not include unnecessary additions unless requested, this includes but is not limited to:
  - try/except logic for non-existent roadblocks
    - this includes “fallback” logic
  - an excess of comments or notes
  - CLI functionality unless requested
  - any other code that is redundant
- when making edits in a notebook, make small changes to start

---

## Current Focus
<!-- Update this section as you move between project phases -->
PJM electricity demand forecasting — data source integration
