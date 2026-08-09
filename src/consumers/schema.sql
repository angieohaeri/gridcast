-- EIA-930 load, PJM real-time LMP, and Open-Meteo weather hypertables.
-- Kept at native resolution per source (load: hourly, lmp: hourly from rt_hrl_lmps,
-- weather: poll interval) — never resampled at ingestion; alignment happens in dbt.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- EIA-930 hourly demand and PJM zonal load (Kafka topic: load).
-- Two independent sources land here: PJM's own metered zonal feed (source='pjm',
-- includes its own zone='RTO' system total) and EIA's grid monitor (source='eia',
-- RTO-level only, also zone='RTO' - the two are deliberately never merged into one
-- "RTO" row since they're different measurements of the same quantity; source is
-- what keeps them apart). is_verified only applies to source='pjm' rows - PJM
-- revises a given (time, zone) from unverified to verified over ~3 days after
-- publish; EIA does not expose an equivalent flag.
CREATE TABLE IF NOT EXISTS load (
    time                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    zone                    TEXT NOT NULL,
    source                  TEXT NOT NULL,
    demand_mw               DOUBLE PRECISION NOT NULL,
    demand_forecast_mw      DOUBLE PRECISION,
    net_generation_mw       DOUBLE PRECISION,
    total_interchange_mw    DOUBLE PRECISION,
    is_verified             BOOLEAN
);

SELECT create_hypertable('load', by_range('time'), if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS load_time_zone_source_uidx
    ON load (time, zone, source);

CREATE INDEX IF NOT EXISTS load_zone_time_idx
    ON load (zone, time DESC);

-- PJM Data Miner 2 rt_hrl_lmps real-time hourly LMP (Kafka topic: lmp). zone uses
-- this project's zone_id codes (data/processed/pjm_weather_zones.csv), not PJM's
-- raw Location Short Name (e.g. 'CE' not 'COMED', 'BC' not 'BGE') - the producer
-- maps explicitly, see src/producers/lmp_producer.py.
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

CREATE UNIQUE INDEX IF NOT EXISTS lmp_time_pnode_uidx
    ON lmp (time, pnode_id);

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
