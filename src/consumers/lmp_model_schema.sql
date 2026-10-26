-- Isolated raw landing tables for the LMP pricing model (see references/lmp-pricing-model/).
-- Lives in its own schema (raw_lmp), separate from public.load/lmp/weather, so this can be
-- dropped/rebuilt independently. Applied manually, not on container init (see schema.sql).

create table if not exists raw_lmp.marginal_value_rt (
    datetime_beginning_utc timestamptz not null,
    datetime_ending_utc timestamptz not null,
    monitored_facility text not null,
    contingency_facility text,
    transmission_constraint_penalty_factor numeric,
    limit_control_percentage numeric,
    shadow_price numeric not null,
    unique (datetime_beginning_utc, monitored_facility, contingency_facility)
);

select create_hypertable('raw_lmp.marginal_value_rt', 'datetime_beginning_utc', if_not_exists => true);

create index if not exists marginal_value_rt_facility_time_idx
    on raw_lmp.marginal_value_rt (monitored_facility, datetime_beginning_utc desc);

-- Day-ahead has no penalty factor / limit control percentage - those are RT-only fields
-- (see the feed's own field definitions on Data Miner).
create table if not exists raw_lmp.marginal_value_da (
    datetime_beginning_utc timestamptz not null,
    datetime_ending_utc timestamptz not null,
    monitored_facility text not null,
    contingency_facility text,
    shadow_price numeric not null,
    unique (datetime_beginning_utc, monitored_facility, contingency_facility)
);

select create_hypertable('raw_lmp.marginal_value_da', 'datetime_beginning_utc', if_not_exists => true);

create index if not exists marginal_value_da_facility_time_idx
    on raw_lmp.marginal_value_da (monitored_facility, datetime_beginning_utc desc);

-- Not a hypertable: ~90 rows per daily forecast execution (one per forecast_date in the
-- 90-day horizon), not a high-volume time series. RTO/West/Other only - not zonal.
create table if not exists raw_lmp.forecasted_generation_outages (
    forecast_execution_date timestamptz not null,
    forecast_date date not null,
    outage_mw_rto numeric,
    outage_mw_west numeric,
    outage_mw_other numeric,
    unique (forecast_execution_date, forecast_date)
);

-- gen_by_fuel (get_fuel_mix): hourly actual generation by fuel type, RTO-wide - informs
-- lambda (system-wide energy price), not the congestion term. Long format (one row per
-- fuel per hour), not the wide shape gridstatus returns, to match this project's other
-- categorical time series (load, instantaneous_load).
create table if not exists raw_lmp.generation_by_fuel (
    time timestamptz not null,
    fuel_type text not null,
    generation_mw numeric not null,
    unique (time, fuel_type)
);

select create_hypertable('raw_lmp.generation_by_fuel', 'time', if_not_exists => true);

create index if not exists generation_by_fuel_type_time_idx
    on raw_lmp.generation_by_fuel (fuel_type, time desc);

-- da_transconstraints (get_transmission_constraints_day_ahead_hourly): which facility/
-- contingency pairs bound in the day-ahead market and for how long - a duration signal,
-- no price magnitude (pairs with marginal_value_da's shadow price for the same facility).
-- "Day Ahead Congestion Event" from the raw feed is dropped - confirmed always identical
-- to Monitored Facility, redundant.
create table if not exists raw_lmp.transmission_constraints_da (
    datetime_beginning_utc timestamptz not null,
    datetime_ending_utc timestamptz not null,
    duration_hours integer not null,
    monitored_facility text not null,
    contingency_facility text,
    unique (datetime_beginning_utc, monitored_facility, contingency_facility)
);

select create_hypertable('raw_lmp.transmission_constraints_da', 'datetime_beginning_utc', if_not_exists => true);

create index if not exists transmission_constraints_da_facility_time_idx
    on raw_lmp.transmission_constraints_da (monitored_facility, datetime_beginning_utc desc);

-- da_hrl_lmps, location_type=ZONE: day-ahead zone-level hourly LMP, same grain as public.lmp
-- (zone x hour) but the DA market instead of RT - enables a DA-RT basis feature. zone uses
-- this project's zone_id codes via the same mapping as public.lmp's producer; MID-ATL/APS
-- (aggregate), OVEC (out of scope), and PJM-RTO (hub) are dropped at ingestion, same as
-- public.lmp. time = Interval End (Hour Ending), matching public.lmp's convention.
create table if not exists raw_lmp.lmp_da_hourly (
    time timestamptz not null,
    zone text not null,
    lmp numeric not null,
    congestion_price numeric,
    marginal_loss_price numeric,
    unique (time, zone)
);

select create_hypertable('raw_lmp.lmp_da_hourly', 'time', if_not_exists => true);

create index if not exists lmp_da_hourly_zone_time_idx
    on raw_lmp.lmp_da_hourly (zone, time desc);

-- The following 3 tables have no gridstatus wrapper method - pulled by calling
-- gridstatus's PJM._get_pjm_json() directly against the raw Data Miner 2 feed name
-- (reuses its auth/retry/pagination handling rather than writing a new HTTP client).
-- Feed names found via PJM's Data Miner 2 feed-definition pages, not gridstatus.

-- ops_init_commit: zonal(!) out-of-merit unit commitments, irregular event-level
-- timestamps (not hourly) - a "Constraint Management" reason ties directly to
-- congestion. No stable per-row id in the feed, so duplicates on the full natural key
-- collapse on upsert - confirmed empirically that rows sharing (time, zone, reason,
-- economic_max_mw) are re-posts of the same event, not distinct simultaneous
-- commitments (those differ in economic_max_mw). zone uses this project's zone_id
-- codes via the same mapping as public.lmp/lmp_da_hourly; OVEC (out of scope) dropped.
create table if not exists raw_lmp.operator_initiated_commitments (
    datetime_beginning_utc timestamptz not null,
    zone text not null,
    economic_max_mw numeric,
    reason text,
    unique (datetime_beginning_utc, zone, reason, economic_max_mw)
);

select create_hypertable('raw_lmp.operator_initiated_commitments', 'datetime_beginning_utc', if_not_exists => true);

create index if not exists operator_initiated_commitments_zone_time_idx
    on raw_lmp.operator_initiated_commitments (zone, datetime_beginning_utc desc);

-- rt_and_self_ecomax: hourly, RTO-wide self-scheduled generation (self_ecomax) - runs
-- regardless of price, distorts normal dispatch, informs lambda only (no zone field).
-- rt_ecomax is null whenever conf_disclaimer is set ("Confidentiality Rules Prohibit
-- Display", ~55% of rows) - a real suppression flag from PJM, not missing data; kept
-- as-is rather than imputed. conf_disclaimer itself dropped - it's just the static
-- explanatory text for why rt_ecomax is null, not a data column.
create table if not exists raw_lmp.scheduled_generation (
    time timestamptz not null,
    rt_ecomax numeric,
    self_ecomax numeric,
    unique (time)
);

select create_hypertable('raw_lmp.scheduled_generation', 'time', if_not_exists => true);

-- gen_ehv_losses: hourly, RTO-wide - the losses term of pi_i = lambda + sum(A_ik * mu_k),
-- smallest of the three LMP components and, until now, unsourced.
create table if not exists raw_lmp.generation_ehv_losses (
    time timestamptz not null,
    total_gen numeric,
    total_losses numeric,
    unique (time)
);

select create_hypertable('raw_lmp.generation_ehv_losses', 'time', if_not_exists => true);

-- EIA (not PJM) - electricity/electric-power-operational-data, cost-per-btu metric,
-- fueltypeid=NG, sectorid=98 (Electric Power). No gridstatus wrapper - gridstatus's
-- get_dataset() only supports 5 hardcoded EIA routes, none of which is this one - pulled
-- by calling gridstatus's EIA._fetch_page() directly (reuses its auth/pagination) with a
-- manually-built request instead of a new HTTP client. Monthly by state - pairs with
-- generation_by_fuel to approximate lambda (the "spark spread" signal). Null cost means
-- no NG-fired generation reported for that state/month, not missing/suppressed data.
-- Not a hypertable - ~2,500 rows total (49 states x ~44 months), same low-volume
-- reasoning as forecasted_generation_outages.
create table if not exists raw_lmp.natural_gas_fuel_cost (
    period date not null,
    location text not null,
    cost_per_mmbtu numeric,
    unique (period, location)
);

-- rt_hrl_lmps (verified real-time hourly LMP) at pnode level, for the GNN node set in
-- pnode_topology_nodes - not zonal like every other LMP table here. time = Interval End
-- (Hour Ending, UTC), matching public.lmp / lmp_da_hourly. Lean on purpose: ~25.7M rows,
-- so no per-row text (names/zone/voltage live in pnode_topology_nodes) and real (float4)
-- prices - the feed's values fit float4 to the cent. lmp = system_energy_price +
-- congestion_price + marginal_loss_price (to $0.01). Backfill:
-- _archive/scripts/backfill_rt_hrl_lmps_nodal.py -> load_rt_hrl_lmps_nodal.py.
create table if not exists raw_lmp.rt_hrl_lmps_nodal (
    time timestamptz not null,
    pnode_id bigint not null,
    system_energy_price real not null,
    lmp real not null,
    congestion_price real not null,
    marginal_loss_price real not null,
    -- false = from rt_unverified_hrl_lmps (hourly, live); true = verified rt_hrl_lmps, which
    -- always overwrites. Unverified never overwrites verified (raw_lmp_sync.py upserts).
    is_verified boolean not null default true,
    unique (time, pnode_id)
);

select create_hypertable('raw_lmp.rt_hrl_lmps_nodal', 'time', chunk_time_interval => interval '30 days', if_not_exists => true);

create index if not exists rt_hrl_lmps_nodal_pnode_time_idx
    on raw_lmp.rt_hrl_lmps_nodal (pnode_id, time desc);

-- The one compressed raw_lmp hypertable - exception to the revert noted below, accepted
-- 2026-10-22: at ~26M rows the size difference is worth losing Postico's row browser for
-- this table (query it with SQL instead). Chunks older than 30 days compress via policy.
alter table raw_lmp.rt_hrl_lmps_nodal set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pnode_id',
    timescaledb.compress_orderby = 'time desc'
);

select add_compression_policy('raw_lmp.rt_hrl_lmps_nodal', interval '30 days', if_not_exists => true);

-- da_hrl_lmps (day-ahead hourly LMP) for the same GNN node set - the DA counterpart of
-- rt_hrl_lmps_nodal, same lean shape, hour-ending time and compression. No is_verified:
-- DA prices are final when the market clears the day before, so they're known ahead of the
-- RT hour they price - a leakage-safe input, and the DA-RT spread is a congestion signal.
-- Backfill: _archive/scripts/backfill_da_hrl_lmps_nodal.py -> load_da_hrl_lmps_nodal.py;
-- kept current by sync_da_hrl_lmps_nodal in raw_lmp_sync.py.
create table if not exists raw_lmp.da_hrl_lmps_nodal (
    time timestamptz not null,
    pnode_id bigint not null,
    system_energy_price real not null,
    lmp real not null,
    congestion_price real not null,
    marginal_loss_price real not null,
    unique (time, pnode_id)
);

select create_hypertable('raw_lmp.da_hrl_lmps_nodal', 'time', chunk_time_interval => interval '30 days', if_not_exists => true);

create index if not exists da_hrl_lmps_nodal_pnode_time_idx
    on raw_lmp.da_hrl_lmps_nodal (pnode_id, time desc);

-- Compressed for the same reason as rt_hrl_lmps_nodal (size; query with SQL, not Postico's
-- row browser).
alter table raw_lmp.da_hrl_lmps_nodal set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pnode_id',
    timescaledb.compress_orderby = 'time desc'
);

select add_compression_policy('raw_lmp.da_hrl_lmps_nodal', interval '30 days', if_not_exists => true);

-- PJM load forecast (load_frcstd_hist), point in time: every forecast PJM issued, keyed by
-- when it was issued (evaluated_at), so training can pick the forecast that was actually
-- available at the 14:30 ET cutoff instead of a later revision. 5 evaluations a day
-- (~05:45, 09:45, 11:45, 17:45, 23:45 ET), each covering now through the end of tomorrow.
-- 11 coarse areas: 9 single zones, MIDATL (all other zones combined) and RTO. `time` is
-- Hour Ending UTC (feed's forecast_hour_beginning_utc + 1h) like the LMP tables.
-- Kept current by sync_load_forecast in raw_lmp_sync.py (backfill_load_forecast for history).
create table if not exists raw_lmp.load_forecast (
    evaluated_at timestamptz not null,
    time timestamptz not null,
    forecast_area text not null,
    forecast_load_mw real not null,
    unique (evaluated_at, time, forecast_area)
);

select create_hypertable('raw_lmp.load_forecast', 'time', chunk_time_interval => interval '30 days', if_not_exists => true);

-- Open-Meteo weather forecast per zone (the 30 stations of pjm_weather_zones averaged to their
-- zone, like the weather producer does). Two sources, so training and serving see the same
-- kind of forecast: `previous_day1` is the forecast Open-Meteo made a day before each target
-- hour (history back to 2024; issued_at is that nominal time, target - 24h), and `live` is a
-- daily snapshot of the hourly forecast taken before the 14:30 ET cutoff. `time` is Hour
-- Ending UTC, shifted +1h the same way weather_features aligns observations.
-- Kept current by sync_weather_forecast in raw_lmp_sync.py (backfill_weather_forecast for history).
create table if not exists raw_lmp.weather_forecast (
    time timestamptz not null,
    zone text not null,
    source text not null,
    issued_at timestamptz not null,
    temperature real,
    precipitation real,
    wind_speed real,
    cloud_cover real,
    unique (time, zone, source, issued_at)
);

select create_hypertable('raw_lmp.weather_forecast', 'time', chunk_time_interval => interval '30 days', if_not_exists => true);

-- Compression was tried and reverted (2026-08-23) on all 8 raw_lmp hypertables - see
-- decisions.md ("Compression tried and reverted"). Reason: Postgres validates system
-- columns across every chunk when planning a query against a hypertable, so a client
-- requesting ctid (Postico's row-browser grid does this) fails against the WHOLE table
-- the moment even one chunk anywhere in it is compressed - there's no partial-compression
-- state that keeps a GUI row browser working. Not reflected in this file: compression
-- was applied and fully undone directly against the live DB. Exceptions: rt_hrl_lmps_nodal
-- and da_hrl_lmps_nodal (added later, above) are compressed on purpose.
