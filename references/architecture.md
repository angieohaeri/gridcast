# Electricity Demand Forecasting — Architecture Reference

## Overview

End-to-end ML portfolio project. Predicts short-term zonal electricity load on the
**PJM Interconnection** using EIA-930 hourly demand data, PJM real-time LMP data, and
weather data, on a streaming pipeline built around Kafka and TimescaleDB.

**Goal:** practice end-to-end ML engineering (ingestion → storage → feature engineering →
modeling → serving → deployment); public-facing portfolio project.

**Hardware:** Intel Mac Mini running Ubuntu 24.04 Server, self-hosted.

---

## Data Sources

### gridstatus (primary wrapper)
- Library: `https://github.com/gridstatus/gridstatus` — `pip install gridstatus`
- Docs: `https://opensource.gridstatus.io/en/latest/`
- Use the `PJM()` class for load, load forecasts, LMPs (day-ahead and real-time), and fuel mix
- Returns pandas DataFrames with timezone-aware columns; date args accept "today",
  "latest", an ISO-8601 string, or a `(start, end)` range in PJM's local timezone
- The open-source library hits PJM endpoints directly — no paid `gridstatusio` client needed

### EIA Open Data API v2
- `https://www.eia.gov/opendata/`, docs at `https://www.eia.gov/opendata/documentation.php`
- Free API key required
- `/v2/electricity/rto/` routes carry EIA-930 Hourly Electric Grid Monitor data: hourly
  demand, forecast demand, net generation, and interchange, with balancing authority and
  subregion facets
- Best source for the load and generation-mix features

### PJM Data Miner 2
- `https://dataminer2.pjm.com/`
- Real-time hourly LMP feed: `https://dataminer2.pjm.com/feed/rt_hrl_lmps`
- Browsable manually; programmatic access needs a free pjm.com account

### Open-Meteo API
- URL: https://open-meteo.com/ — free, no API key required
- Current conditions + hourly forecast (temp, precipitation, wind, cloud cover); historical data available for training
- Covers all 20 in-scope PJM zones via 30 representative stations; seven multi-climate zones average 2-3 stations into one reading. Started at 3-4 zones, scaled up August 12th — see `decisions.md` for how stations were chosen
- Poll every 5–10 minutes

### On frequency mismatch
EIA-930 is hourly; PJM LMPs are 5- or 15-minute. Never resampled silently — each lands in its own hypertable at native resolution, joined in a named, documented dbt model.

---

## Stack

![system_architecture](images/gridcast_system_architecture.svg)

### Ingestion
- **Apache Kafka** (KRaft mode — no ZooKeeper, stable since Kafka 3.3+)
- Three Kafka topics: `load`, `lmp`, `weather`
- Python producers using `confluent-kafka` poll EIA-930, PJM Data Miner, and Open-Meteo
  REST APIs (via `gridstatus` where possible) and publish
- Python consumers read topics and write to TimescaleDB

### Storage
- **TimescaleDB** (PostgreSQL extension — familiar tooling, time-series optimized)
- Hypertables for automatic time-based partitioning
- Continuous aggregates: materialized views that stay fresh as data arrives
- One hypertable each for `load`, `lmp`, and `weather` — kept at native resolution, joined explicitly downstream

### Feature Engineering / Orchestration
- **Prefect** for scheduling and orchestration
- **dbt** for feature transformations running against TimescaleDB
- Produces: lag features, rolling averages, joined weather+load feature tables, and the
  explicit hourly LMP alignment model

### Modeling
- **MLflow** for experiment tracking and model registry
- **LightGBM** as primary model (tabular spatiotemporal data, fast iteration)
- Optional extension: Temporal Fusion Transformer (multivariate time series with known future inputs — weather forecast)
- FastAPI serving layer loads production model directly from MLflow registry

### Serving
- **FastAPI** — `/predict` endpoint, refreshes predictions on a schedule
- Loads model from MLflow model registry (no manual file management)

### Dashboard
- **Shiny for Python** with **Pydeck** for geographic map (deck.gl, better than Folium for this)
- Live zone load on map, predicted load N hours ahead, weather overlay
- Calls FastAPI for predictions

### Deployment
- **Docker Compose** — all services in a single `docker-compose.yml` on the Mac Mini
- **Cloudflare Tunnel** — free public HTTPS URL, no port forwarding, runs as a container
- Ubuntu 24.04 LTS (supported through 2029)

---

## ML Details

### Prediction Target
Zonal `load_mw`, N hours ahead (regression). Hourly grain matches EIA-930, avoiding the frequency-mismatch problem for v1. LMP forecasting is a stretch extension once the load model and LMP-alignment pattern are proven.

### Features

**Temporal:** hour of day, day of week, is_weekend, is_holiday; cyclical (sin/cos) time-of-day encoding; lags at t-1h/t-3h/t-24h/t-168h; rolling means at 6hr/24hr/7d

**Zonal:** zone/BA as a categorical feature (global model); zone-level historical load profile stats (typical peak hour, weekday/weekend spread)

**Weather:** current + forecast temp/precipitation/wind/cloud cover per zone (composite zones averaged across stations before publishing). Including forecast, not just current conditions, is a meaningful differentiator

### Model Notes
- LightGBM is the starting point — strong baseline, handles missing values, fast
- TFT is worth adding later — designed for multivariate series with known future covariates (weather forecast)
- Global model with zone as a feature to start, given the small initial zone count; per-zone models worth trying later

---

## Deployment Notes

### Mac Mini — Ubuntu setup
```bash
# Disable sleep (required for always-on server)
sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target

# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

### Cloudflare Tunnel
- Create a free Cloudflare account at cloudflare.com
- Add `cloudflared` as a service in `docker-compose.yml`
- No port forwarding required on home router
- Automatic HTTPS with valid cert
- Optional: register a domain (~$10/yr) for a clean public URL

---

## Build Sequence (completed)

Producer → Kafka → consumer → TimescaleDB → backfill → LMP feed → dbt features → LightGBM/MLflow → FastAPI → Shiny → Cloudflare Tunnel, in that order. Current repo layout is CLAUDE.md's "Repo Layout"; CCDS directory conventions are in `ccds-practices.md`.
