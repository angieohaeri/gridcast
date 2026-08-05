-- EIA-930 load, PJM real-time LMP, and Open-Meteo weather hypertables.
-- Kept at native resolution per source (load: hourly, lmp: hourly from rt_hrl_lmps,
-- weather: poll interval) — never resampled at ingestion; alignment happens in dbt.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- EIA-930 hourly demand (Kafka topic: load)
CREATE TABLE IF NOT EXISTS load (
    time                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    ba_code                 TEXT NOT NULL,
    subregion               TEXT,
    demand_mw               DOUBLE PRECISION NOT NULL,
    demand_forecast_mw      DOUBLE PRECISION,
    net_generation_mw       DOUBLE PRECISION,
    total_interchange_mw    DOUBLE PRECISION
);

SELECT create_hypertable('load', by_range('time'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS load_ba_code_time_idx
    ON load (ba_code, time DESC);

-- PJM Data Miner 2 rt_hrl_lmps real-time hourly LMP (Kafka topic: lmp)
CREATE TABLE IF NOT EXISTS lmp (
    time                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    pnode_id                TEXT NOT NULL,
    pnode_name              TEXT,
    zone                    TEXT NOT NULL,
    lmp                     DOUBLE PRECISION NOT NULL,
    congestion_price        DOUBLE PRECISION,
    marginal_loss_price     DOUBLE PRECISION
);

SELECT create_hypertable('lmp', by_range('time'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS lmp_zone_time_idx
    ON lmp (zone, time DESC);

-- Open-Meteo observations for representative zone cities (Kafka topic: weather)
CREATE TABLE IF NOT EXISTS weather (
    time                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    zone                    TEXT NOT NULL,
    temperature             DOUBLE PRECISION NOT NULL,
    precipitation           DOUBLE PRECISION NOT NULL,
    wind_speed              DOUBLE PRECISION NOT NULL,
    cloud_cover             DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable('weather', by_range('time'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS weather_zone_time_idx
    ON weather (zone, time DESC);
