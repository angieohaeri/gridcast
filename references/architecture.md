# Bike Share Demand Forecasting — Architecture Reference

## Overview

End-to-end ML portfolio project. Predicts short-term bike availability at Divvy (Chicago)
stations using live GBFS feeds, weather data, and a streaming infrastructure built around
Apache Kafka and TimescaleDB.

**Goal:** practice end-to-end ML engineering (ingestion → storage → feature engineering →
modeling → serving → deployment), build a public-facing portfolio project.

**Hardware:** Intel Mac Mini running Ubuntu 24.04 Server, self-hosted.

---

## Data Sources

### Divvy GBFS Feed
- Provider: Divvy (Chicago), operated by Lyft — GBFS 2.3 spec
- Station information (static): `https://gbfs.divvybikes.com/gbfs/2/en/station_information.json`
- Station status (live): `https://gbfs.divvybikes.com/gbfs/2/en/station_status.json`
- Update frequency: every 10–15 seconds; poll every 30–60 seconds
- Auth: none required; ~700 stations with lat/lng, dock counts, bike availability

### Open-Meteo API
- URL: https://open-meteo.com/ — free, no API key required
- Provides: current conditions + hourly forecast (temp, precipitation, wind, cloud cover)
- Historical data available (useful for training set)
- Poll every 5–10 minutes

---

## Stack

### Ingestion
- **Apache Kafka** (KRaft mode — no ZooKeeper, stable since Kafka 3.3+)
- Two Kafka topics: `station_status`, `weather`
- Python producers using `confluent-kafka` poll GBFS and Open-Meteo REST APIs and publish
- Python consumers read topics and write to TimescaleDB

### Storage
- **TimescaleDB** (PostgreSQL extension — familiar tooling, time-series optimized)
- Hypertables for automatic time-based partitioning
- Continuous aggregates: materialized views that stay fresh as data arrives
- One hypertable for station snapshots, one for weather observations

### Feature Engineering / Orchestration
- **Prefect** for scheduling and orchestration
- **dbt** for feature transformations running against TimescaleDB
- Produces: lag features, rolling averages, joined weather+station feature tables

### Modeling
- **MLflow** for experiment tracking and model registry
- **LightGBM** as primary model (tabular spatiotemporal data, fast iteration)
- Optional extension: Temporal Fusion Transformer (multivariate time series with known
  future inputs — weather forecast — good for interview discussion)
- FastAPI serving layer loads production model directly from MLflow registry

### Serving
- **FastAPI** — `/predict` endpoint, refreshes predictions on a schedule
- Loads model from MLflow model registry (no manual file management)

### Dashboard
- **Streamlit** with **Pydeck** for geographic map (deck.gl, better than Folium for this)
- Live station status on map, predicted availability in 30 min, weather overlay
- Calls FastAPI for predictions

### Deployment
- **Docker Compose** — all services in a single `docker-compose.yml` on the Mac Mini
- **Cloudflare Tunnel** — free public HTTPS URL, no port forwarding, runs as a container
- Ubuntu 24.04 LTS (supported through 2029)

---

## ML Details

### Prediction Target
`bikes_available` at each station in 30 minutes (regression)

### Features

**Temporal:**
- Hour of day, day of week, is_weekend, is_holiday
- Time since midnight (use cyclical encoding — sin/cos transforms)
- Lag features: `bikes_available` at t-15, t-30, t-60 minutes
- Rolling means: 1hr, 6hr, 24hr, same time last week

**Spatial:**
- Station lat/lng (raw coordinates or learned embeddings)
- Distance to city center
- Station cluster label (compute from historical demand patterns via k-means or DBSCAN)
- Current status of N nearest stations, weighted by distance — captures demand spillover

**Weather:**
- Current: temperature, precipitation rate, wind speed, cloud cover
- Forecast: same fields 1 hour ahead
- Including forecast (not just current conditions) is a meaningful differentiator

### Model Notes
- LightGBM is the right starting point — strong baseline, handles missing values, fast
- Temporal Fusion Transformer is worth adding later: designed exactly for multivariate
  time series with known future inputs (i.e., weather forecast is a "known future covariate")
- Train per-station models vs. a single global model with station as a feature — try both,
  the global model typically wins if you have enough data and good spatial features

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

## Suggested Project Structure

```
bikeshare/
├── docker-compose.yml
├── .env                    # secrets, not committed
├── producers/              # Kafka producer scripts (GBFS, weather)
├── consumers/              # Kafka consumer scripts (→ TimescaleDB)
├── dbt/                    # Feature engineering models
├── prefect/                # Orchestration flows
├── training/               # MLflow training scripts
├── api/                    # FastAPI prediction service
├── dashboard/              # Streamlit app
├── notebooks/              # EDA, model exploration
└── infra/                  # Cloudflare config, nginx, etc.
```

---

## Starting Point (in order)

1. Install Ubuntu 24.04 Server on Mac Mini; install Docker + Compose; disable sleep
2. Write a single Python producer that polls Divvy GBFS station status and prints to stdout
3. Wire producer to Kafka (single topic, no consumers yet) — verify messages flowing
4. Add consumer that writes raw snapshots to TimescaleDB
5. Build dbt models for lag + rolling features
6. Train LightGBM baseline with MLflow tracking
7. Wrap model in FastAPI; build Streamlit map dashboard
8. Wire up Cloudflare Tunnel for public access
