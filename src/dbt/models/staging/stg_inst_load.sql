{{ config(
    materialized='incremental',
    unique_key=['time', 'zone'],
    incremental_strategy='delete+insert'
) }}

-- unfiltered like stg_load (RTO kept, mirrors load's ingested-but-unmodelled RTO row) -
-- zone filtering happens in inst_load_features.sql, not here. Zone codes are already
-- normalized to project zone_ids at ingestion (inst_load_producer.py), unlike lmp's raw
-- table which needed staging-time cleanup - nothing to remap here.
--
-- unlike load/lmp, inst_load carries no settlement revision - the lookback below only
-- needs to cover the gap since dbt_build's last daily run, same as weather's.

select
    time,
    zone,
    instantaneous_load_mw
from {{ source('raw', 'instantaneous_load') }}

{% if is_incremental() %}
where time >= (select max(time) - interval '1 day' from {{ this }})
{% endif %}
