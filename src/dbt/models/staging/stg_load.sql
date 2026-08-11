{{ config(
	materialized='incremental',
	unique_key=['time', 'zone', 'source']
	incremental_strategy='delete+insert'
) }}

select time, zone, source, demand_mw, demand_forecast_mw, net_generation_mw, total_interchange_mw, is_verified from {{ 
source('raw', 'load') }} {% if is_incremental() %} where time >= (select max(time) - interval '4 days' from {{ this }})
{% endif %}
