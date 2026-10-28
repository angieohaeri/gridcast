drop view if exists public.facility_ends;

create view public.facility_ends as
with ends as (
    select facility_id, name, category, voltage_kv, rt_weight_365d, rt_weight_rank, 1 as end_no,
           end1_code as token, end1_substation_id as substation_id, end1_source as match_type, end2_substation_id as other_substation_id
    from public.facilities
    where category in ('line', 'substation_equipment', 'unclassified')
    union all
    select facility_id, name, category, voltage_kv, rt_weight_365d, rt_weight_rank, 2,
           end2_code, end2_substation_id, end2_source, end1_substation_id
    from public.facilities
    where category = 'line'
),
matched as (
    select e.*, s.substation_id as match_id, true as is_match
    from ends e
    join public.substations s on s.substation_id = e.substation_id
),
unmatched as (
    select e.*, c.substation_id as match_id, false as is_match
    from ends e
    left join public.substations c
        on length(regexp_replace(upper(e.token), '[^A-Z0-9]', '', 'g')) >= 4
       and (starts_with(regexp_replace(c.code, '[^A-Z0-9]', '', 'g'), regexp_replace(upper(e.token), '[^A-Z0-9]', '', 'g'))
         or starts_with(regexp_replace(upper(e.token), '[^A-Z0-9]', '', 'g'), regexp_replace(c.code, '[^A-Z0-9]', '', 'g')))
    where e.substation_id is null
)
select r.facility_id, r.name as facility, r.category, r.end_no, r.token as name_token, r.voltage_kv as facility_kv,
       r.rt_weight_365d, r.rt_weight_rank, r.is_match, r.match_type,
       s.substation_id, s.station_id, s.code as substation_code, s.tx_zone, s.source as substation_source, s.voltages_kv,
       case when not r.is_match and s.substation_id is not null then r.voltage_kv = any(s.voltages_kv) end as kv_fits,
       case when not r.is_match and s.substation_id is not null then s.tx_zone = o.tx_zone end as zone_matches_other_end
from (select * from matched union all select * from unmatched) r
left join public.substations s on s.substation_id = r.match_id
left join public.substations o on o.substation_id = r.other_substation_id;
