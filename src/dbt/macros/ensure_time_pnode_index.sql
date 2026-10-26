{#
  index on (time, pnode_id) that survives --full-refresh. dbt swaps the new table in by
  renaming the old one to a backup, and the backup keeps the index name, so a plain
  `create index if not exists` is skipped and the index goes when the backup is dropped.
  Checking the table itself for the index, and clearing the name first, avoids that.
#}
{% macro ensure_time_pnode_index() %}
do $$
begin
    if not exists (
        select 1 from pg_indexes
        where schemaname = '{{ this.schema }}' and tablename = '{{ this.name }}'
          and indexdef like '%(time, pnode_id)%'
    ) then
        drop index if exists {{ this.schema }}.{{ this.name }}_time_pnode_idx;
        create index {{ this.name }}_time_pnode_idx on {{ this }} (time, pnode_id);
    end if;
end $$
{% endmacro %}
