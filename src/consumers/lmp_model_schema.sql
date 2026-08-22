-- Isolated raw landing tables for the LMP pricing model (see references/lmp-pricing-model.md).
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
