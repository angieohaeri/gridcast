{#
  the forecast_cutoff_et deadline for a target hour: that time of day on the ET day before the
  target hour's day (hour-ending 00:00 belongs to the previous day). A forecast or snapshot
  issued after it is something the model could not have seen.
#}
{% macro forecast_cutoff(time_col='time') %}
(date_trunc('day', ({{ time_col }} - interval '1 hour') at time zone 'America/New_York')
    - interval '1 day' + interval '{{ var("forecast_cutoff_et") }}')
    at time zone 'America/New_York'
{%- endmacro %}
