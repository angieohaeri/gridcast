# Bike Share Demand Forecasting

## Project Overview
End-to-end ML portfolio project. Predicts short-term bike availability at Citi Bike (NYC)
stations using live GBFS feeds and weather data. Built to practice end-to-end ML
engineering: ingestion → storage → feature engineering → modeling → serving → deployment.

**Hardware:** Intel Mac Mini running Ubuntu 24.04 Server, self-hosted, exposed via
Cloudflare Tunnel.

---

## Architecture
```
Citi Bike GBFS + Open-Meteo → Kafka → TimescaleDB → dbt → LightGBM → FastAPI → Streamlit
```

---

## Stack
- **Python 3.12** — snake_case, type hints throughout, no ORM
- **Kafka** KRaft mode (`confluent-kafka`) — topics: `station_status`, `weather`
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
bikeshare/          installable Python package (pip install -e .)
  config.py         project-root-relative path constants — import these, never hardcode paths
  dataset.py        data loading functions (TimescaleDB, historical CSVs)
  features.py       Python-side feature engineering (cyclical encoding, neighbor features)
  modeling/
    train.py        reads processed features → trains → logs to MLflow
    predict.py      loads from MLflow registry → exposes predict()
src/
  producers/          Kafka producer scripts (poll GBFS + Open-Meteo, publish messages)
  consumers/          Kafka consumers (read topics, write to TimescaleDB)
  dbt/                feature engineering SQL models
  prefect/            orchestration flows
  training/           MLflow training entry points
  api/                FastAPI app
  dashboard/          Streamlit app
  infra/              Cloudflare config, Docker supporting files
notebooks/          EDA and exploration (naming: 01-description.ipynb)
tests/              pytest — mirrors package structure
references/         architecture, schema, decisions, experiment log, best practices
data/
  raw/              immutable source data — never write here programmatically
  external/         third-party data (Open-Meteo history, borough GeoJSON)
  interim/          partially processed data
  processed/        model-ready feature tables
```

---

## Data Sources
- **Citi Bike GBFS** — station status: `https://gbfs.citibikenyc.com/gbfs/2/en/station_status.json`
- **Citi Bike GBFS** — station info: `https://gbfs.citibikenyc.com/gbfs/2/en/station_information.json`
- **Open-Meteo** — `https://open-meteo.com/` (free, no key required)
- **Historical trips** — `https://citibikenyc.com/system-data` (monthly CSVs, used for training backfill)

---

## Common Commands
```bash
make install        # pip install -e . in active venv
make data           # download raw Citi Bike trip CSVs
make features       # run dbt build
make train          # train + log to MLflow
make test           # pytest tests/
make lint           # ruff check . --fix
make up             # docker compose up -d
make down           # docker compose down
```

---

## ML Details
**Target:** `bikes_available` at each station in 30 minutes (regression)

**Features:**
- Temporal: hour, day of week, is_weekend, is_holiday, cyclical sin/cos encoding
- Lag: `bikes_available` at t-15, t-30, t-60 min
- Rolling: mean over 1hr, 6hr, 24hr, same time last week
- Spatial: lat/lng, distance to Midtown, borough label, station cluster, N nearest station status
- Weather: current + 1hr forecast (temp, precipitation, wind, cloud cover)

**Models:**
- LightGBM (baseline) — global model with station as feature
- Temporal Fusion Transformer (planned extension)
- Load from MLflow registry by stage: `models:/bikeshare-lgbm/Production`

---

## Conventions
- Config via environment variables only — never hardcode values (`python-dotenv`)
- Raw SQL for TimescaleDB — no ORM
- MLflow run naming: `{model}_{feature_set}_{YYYYMMDD}`
- Logs via `loguru` module — no `print()` in package code
- Dead letter queue topics: `station_status_dlq`, `weather_dlq`
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
Project setup and infrastructure
