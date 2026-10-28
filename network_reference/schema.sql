create table if not exists public.substations (
    substation_id integer primary key,
    code text not null,
    tx_zone text,
    n_pnodes integer not null,
    n_load integer not null,
    n_gen integer not null,
    n_ehv integer not null,
    max_voltage_kv numeric,
    voltages_kv numeric[] not null,
    first_seen_date date,
    last_change_date date,
    in_gnn boolean not null,
    lat double precision,
    lon double precision,
    location_source text,
    location_conflict boolean,
    status text not null default 'active',
    source text not null,
    station_id integer not null,
    station_rule text not null,
    unique (tx_zone, code)
);

create index if not exists substations_station_idx on public.substations (station_id);

create table if not exists public.pnodes (
    pnode_id bigint primary key,
    pnode_type text not null,
    name text,
    substation_id integer references public.substations (substation_id),
    tx_zone text,
    voltage_text text,
    voltage_kv numeric,
    equipment text,
    bus_type text,
    effective_date date,
    first_seen_date date,
    last_change_date date,
    in_gnn boolean not null,
    gnn_role text,
    status text not null default 'active',
    source text not null
);

create index if not exists pnodes_substation_id_idx on public.pnodes (substation_id);
create index if not exists pnodes_type_idx on public.pnodes (pnode_type);

create table if not exists public.facilities (
    facility_id integer primary key,
    name text not null unique,
    is_monitored boolean not null,
    is_contingency boolean not null,
    in_contingency_list boolean,
    category text not null,
    equipment_type text,
    voltage_kv numeric,
    end1_code text,
    end1_substation_id integer references public.substations (substation_id),
    end1_n_matches integer,
    end1_source text,
    end2_code text,
    end2_substation_id integer references public.substations (substation_id),
    end2_n_matches integer,
    end2_source text,
    middle_codes text[],
    parse_status text not null,
    first_seen_date date not null,
    last_seen_date date not null,
    rt_weight_365d numeric,
    rt_weight_rank integer,
    da_weight_365d numeric,
    m2m_flowgate_id integer,
    m2m_match text,
    m2m_monitoring_rto text,
    status text not null default 'active'
);

create index if not exists facilities_end1_idx on public.facilities (end1_substation_id);
create index if not exists facilities_end2_idx on public.facilities (end2_substation_id);

create table if not exists public.contingency_equipment (
    contingency_name text not null,
    element_seq integer not null,
    is_active boolean not null,
    last_edit timestamp,
    action text not null,
    equipment_type text not null,
    substation_code text not null,
    substation_id integer references public.substations (substation_id),
    voltage_text text,
    voltage_kv numeric,
    equipment_id text,
    primary key (contingency_name, element_seq)
);

create index if not exists contingency_equipment_substation_idx on public.contingency_equipment (substation_id);

create table if not exists public.plants (
    plant_code integer primary key,
    plant_name text,
    state text,
    county text,
    lat double precision,
    lon double precision,
    nerc_region text,
    balancing_authority_code text,
    utility_id integer,
    utility_name text,
    sector_name text,
    transmission_owner text,
    grid_voltage_kv numeric,
    grid_voltage_2_kv numeric,
    grid_voltage_3_kv numeric
);

create table if not exists public.generators (
    plant_code integer not null references public.plants (plant_code),
    generator_id text not null,
    status_group text not null,
    technology text,
    prime_mover text,
    energy_source_1 text,
    nameplate_mw numeric,
    summer_mw numeric,
    winter_mw numeric,
    status text,
    operating_year integer,
    planned_retirement_year integer,
    retirement_year integer,
    lmp_node_designation text,
    pnode_id bigint references public.pnodes (pnode_id),
    pnode_link text,
    primary key (plant_code, generator_id, status_group)
);

create index if not exists generators_pnode_idx on public.generators (pnode_id);
