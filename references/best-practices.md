# Best Practices — Electricity Demand Forecasting Stack

---

## General / Repo

- [x] `.env` in `.gitignore`; commit `.env.example` with keys but no values
- [x] `pre-commit` hooks: ruff (lint), nbstripout (strip notebook outputs before commit)
- [x] DVC for data and model versioning — `.dvc` files committed, data in remote storage
- [x] Conventional commits: `feat:`, `fix:`, `data:`, `exp:`, `infra:`
- [ ] GitHub Actions CI: ruff + pytest on every push, fail fast
  - Cache pip deps with `actions/cache`
  - Separate lint and test into different jobs
  - Add `dbt compile` as a third job to catch broken SQL

---

## Kafka

- [x] Validate message schema in the consumer before writing to DB — never silently accept malformed data
- [x] Dead letter queue topic (`load_dlq`, `lmp_dlq`, `weather_dlq`) for messages that fail parsing — inspect and replay rather than drop
- [x] Enable idempotent producers to prevent duplicates on retry: `enable.idempotence=True`
- [x] Set `acks=all` on producers for durability
- [x] Run Kafka UI (Kafdrop or Redpanda Console) in Docker Compose — monitor consumer lag from a browser
- [x] Define explicit retention on topics (e.g. 7 days for raw status) — don't rely on defaults

---

## TimescaleDB

- [ ] Override default chunk interval from 7 days to 1 day: `chunk_time_interval => INTERVAL '1 day'`
- [ ] Add compression policy on chunks older than 7 days — columnar compression cuts storage 90%+ on time-series
- [x] Set a retention policy on raw snapshots (30–90 days of 30s resolution is enough; keep aggregates longer)
- [x] Add composite index on `(zone, time DESC)` — TimescaleDB creates the time index automatically, zone won't be there by default
- [x] Never query the raw hypertable for training — always go through a continuous aggregate or materialized dbt model
- [ ] Use `EXPLAIN ANALYZE` on slow queries before optimizing indexes

---

## dbt

- [x] Test every model: at minimum `not_null` and `unique` on primary keys, `accepted_values` on categoricals (zone, etc.)
- [x] Run `dbt build` (not `dbt run`) — runs models and tests together in dependency order
- [x] Write a one-line `description:` for every model in `schema.yml`
- [x] Use incremental models for anything touching raw snapshot tables — full refreshes over months of hourly zone history will be slow
- [x] Separate source definitions from models: define `sources:` in `schema.yml`, reference with `{{ source() }}` not raw table names
- [ ] Run `dbt test` in GitHub Actions CI

---

## Prefect

- [x] Add retry logic with exponential backoff on every task that calls an external system (EIA-930, PJM Data Miner, Open-Meteo, DB writes)
- [x] Configure flow failure alerts early — a silently dead pipeline is worse than a noisy one
- [ ] Cache weather fetches with task result caching — avoid unnecessary re-polls on downstream retries
- [x] Use `@flow` and `@task` decorators consistently; keep flows thin (orchestration only), business logic in tasks
- [x] Log task inputs and outputs at INFO level — makes debugging flow failures much faster

---

## MLflow

- [ ] Log the DVC data hash or training date range as a run tag on every experiment — reproducibility requires knowing exactly what data was used
- [x] Log model signatures on every logged model: `mlflow.models.infer_signature(X_train, y_pred)` — serving layer needs the expected input schema
- [x] Use the model registry with explicit stage transitions: Staging → Production
- [ ] Load models in FastAPI by stage (`models:/gridcast-lgbm/Production`), not by run ID — promotes without code changes
- [ ] Log per-zone error metrics (MAE per zone), not just aggregate RMSE — a poor-performing zone model is not production-ready regardless of overall score
- [ ] Tag runs with feature set version so you can compare feature experiments cleanly

---

## FastAPI

- [ ] Pydantic models for every request and response — FastAPI validates automatically
- [ ] `/health` endpoint returning model version, stage, and last prediction timestamp — used for Docker health checks and dashboard status display
- [ ] Structured JSON logging (`structlog` or `python-json-logger`) — makes logs grep-able in production
- [ ] Test with `httpx.AsyncClient` + pytest for async routes — not FastAPI's sync TestClient
- [ ] Handle model loading errors explicitly at startup — fail loudly rather than serve wrong predictions
- [ ] Rate limit the `/predict` endpoint if exposed publicly (`slowapi` is straightforward with FastAPI)

---

## Shiny for Python

- [ ] `@reactive.calc` on every function that calls FastAPI or queries the DB — memoizes until an input it depends on invalidates, instead of refetching on every reactive tick
- [ ] Handle FastAPI unavailability explicitly — show a clear error state, not a Python traceback
- [ ] Keep data fetching in a separate `data.py` module; server functions import from it — don't mix API calls with rendering logic
- [ ] Use `reactive.value` for anything that should persist across reactive updates (selected zone, time horizon)
- [ ] Set a reasonable auto-refresh interval via `reactive.invalidate_later()` (60–120 seconds is enough) — avoid hammering the API

---

## Docker Compose

- [ ] Health checks on every service; use `condition: service_healthy` in `depends_on` — prevents race conditions on startup
- [x] `restart: unless-stopped` on all services — everything comes back after a Mac Mini reboot
- [x] Named volumes for TimescaleDB and MLflow artifact storage — anonymous volumes are deleted on `docker compose down`
- [ ] Set memory limits on Kafka and TimescaleDB — without them one service can starve the others
- [x] Use a `.env` file for all config (ports, passwords, topic names) and reference with `${VAR}` in compose — no hardcoded values
- [ ] Separate `docker-compose.override.yml` for local dev settings (e.g. exposed ports, debug flags) that you don't want in the production compose file
