import re
import xml.etree.ElementTree as ET

from loguru import logger
from network_reference.eia860 import read_eia860
import pandas as pd
import psycopg2.extras

from gridcast.config import EXTERNAL_DATA_DIR, PROJ_ROOT, get_connection

MODEL_DIR = EXTERNAL_DATA_DIR / "lmp-bus-model-data"
BASE_MODEL = EXTERNAL_DATA_DIR / "lmp-bus-model.xlsx"
CURRENT_UPDATE = MODEL_DIR / "20260916-network-model-update.xlsx"
CONTINGENCY_LIST = MODEL_DIR / "contingencies" / "pjm-contingency-list.xml"
FACILITY_LABELS = PROJ_ROOT / "network_reference" / "facility_labels.csv"
FLOWGATES = EXTERNAL_DATA_DIR / "m2m-flowgates" / "miso" / "pjm_m2m_rt_ffe_flowgates_2026.csv"
UPDATE_DIRS = [MODEL_DIR / "2008-2019", MODEL_DIR / "2020-2026"]
BUS_COLUMNS = ["pnode_id", "tx_zone", "substation", "voltage_text", "equipment"]
SECTION = re.compile(r"^\s*(Bus+es|Aggregates)\s+(Added|Deleted|Changed)\s*$", re.IGNORECASE)
ACTIVE_DAYS = 365
NAME_SPLIT = re.compile(r"[-.+ /()\[\]]+")
MIN_TOKEN_CONTINGENCIES = 3
MIN_TOKEN_SHARE = 0.8
OUTAGE_PREFIX = re.compile(r"^(?:(?:TEMP|DBL)\s*[:. ]\s*)+", re.IGNORECASE)
VOLTAGE_PREFIX = re.compile(r"^(?:L(\d+)(?:\.|(?=[A-Za-z ]))|(\d{2,3})\.(?=[A-Za-z]))", re.IGNORECASE)


def kv(text: str | None) -> float | None:
    if not isinstance(text, str):
        return None
    match = re.match(r"\s*(\d+(?:\.\d+)?)\s*KV", text, re.IGNORECASE)
    return float(match.group(1)) if match else None


def read_base_model() -> pd.DataFrame:
    base = pd.read_excel(BASE_MODEL, header=2, dtype=str)
    base.columns = BUS_COLUMNS + ["bus_type"]
    base["pnode_id"] = base["pnode_id"].astype("int64")
    for column in ["tx_zone", "substation", "voltage_text", "equipment"]:
        base[column] = base[column].str.strip()
    return base


def read_update(path) -> pd.DataFrame:
    if path.suffix == ".csv":
        raw = pd.read_csv(path, header=None, names=range(8), dtype=str, on_bad_lines="skip", encoding_errors="replace")
    else:
        raw = pd.read_excel(path, header=None, dtype=str)
    rows = []
    section = None
    for cells in raw.itertuples(index=False):
        first = cells[0].strip() if isinstance(cells[0], str) else ""
        match = SECTION.match(first)
        if match:
            section = (match.group(1).lower()[:3], match.group(2).lower())
        elif section and first.replace(".0", "").isdigit():
            rows.append((pd.Timestamp(path.name[:8]).date(), section[0], section[1], int(float(first)), *[c.strip() if isinstance(c, str) else None for c in cells[1:5]]))
    return pd.DataFrame(rows, columns=["date", "kind", "action"] + BUS_COLUMNS)


def read_updates() -> pd.DataFrame:
    paths = [p for d in UPDATE_DIRS for p in sorted(d.iterdir()) if p.suffix.lower() in (".xls", ".xlsx", ".csv")]
    updates = pd.concat([read_update(p) for p in paths + [CURRENT_UPDATE]], ignore_index=True)
    logger.info(f"{len(paths) + 1} update files, {len(updates)} rows")
    return updates


def active_buses(base: pd.DataFrame, updates: pd.DataFrame) -> pd.DataFrame:
    sept = updates[(updates["date"] == pd.Timestamp("2026-09-16").date()) & (updates["kind"] == "bus")]
    added, deleted = sept[sept["action"] == "added"], sept[sept["action"] == "deleted"]
    logger.info(
        f"Sept 16 update: {len(added)} added ({added['pnode_id'].isin(base['pnode_id']).sum()} already in base), "
        f"{len(deleted)} deleted ({deleted['pnode_id'].isin(base['pnode_id']).sum()} found in base)"
    )
    new = added[~added["pnode_id"].isin(base["pnode_id"])][BUS_COLUMNS].assign(bus_type=None)
    buses = pd.concat([base[~base["pnode_id"].isin(deleted["pnode_id"])], new], ignore_index=True)
    buses["voltage_kv"] = buses["voltage_text"].map(kv)
    buses["source"] = "bus_model_2026-06 + update_2026-09-16"
    return buses


def add_history(buses: pd.DataFrame, updates: pd.DataFrame) -> pd.DataFrame:
    bus_updates = updates[(updates["kind"] == "bus") & (updates["action"].isin(["added", "changed"]))]
    first = bus_updates[bus_updates["action"] == "added"].groupby("pnode_id")["date"].min().rename("first_seen_date")
    last = bus_updates.groupby("pnode_id")["date"].max().rename("last_change_date")
    buses = buses.join(first, on="pnode_id").join(last, on="pnode_id")
    buses[["first_seen_date", "last_change_date"]] = buses[["first_seen_date", "last_change_date"]].apply(pd.to_datetime)
    return buses


def read_topology(conn) -> pd.DataFrame:
    return pd.read_sql(
        "select pnode_id, role as gnn_role, lat, lon, topo_source, conflict from raw_lmp.pnode_topology_nodes", conn
    )


def read_aggregates() -> pd.DataFrame:
    def sheet(path, id_column):
        raw = pd.read_excel(path, header=None, dtype=str)
        header = raw.index[raw[0].astype(str).str.replace("\n", " ").str.contains(id_column, na=False)][0]
        table = raw.iloc[header + 1 :].copy()
        table.columns = [str(c).replace("\n", " ") for c in raw.iloc[header]]
        return table

    aggregate = sheet(MODEL_DIR / "definitions" / "lmp-aggregate-definitions.xlsx", "Aggregate Pnode ID")
    hub = sheet(MODEL_DIR / "lmp-hub-definitions.xlsx", "Hub Pnode ID")
    interface = sheet(MODEL_DIR / "definitions" / "lmp-interface-definitions.xlsx", "Aggregate Pnode ID")
    parts = [
        aggregate.rename(columns={"Aggregate Pnode ID": "pnode_id", "Aggregate": "name", "Aggregate Type": "pnode_type", "Zone": "tx_zone"})[["pnode_id", "name", "pnode_type", "tx_zone"]].assign(source="aggregate_definitions_2026-09-16"),
        hub.rename(columns={"Hub Pnode ID": "pnode_id", "Hub": "name", "Hub Type": "pnode_type"})[["pnode_id", "name", "pnode_type"]].assign(tx_zone=None, source="hub_definitions_2026-09-16"),
        interface.rename(columns={"Aggregate Pnode ID": "pnode_id", "Aggregate": "name", "Aggregate Type": "pnode_type"})[["pnode_id", "name", "pnode_type"]].assign(tx_zone=None, source="interface_definitions_2023-08-23"),
    ]
    out = pd.concat(parts, ignore_index=True).dropna(subset=["pnode_id"]).drop_duplicates("pnode_id")
    out["pnode_id"] = out["pnode_id"].astype("int64")
    return out


def read_pnode_feed() -> pd.DataFrame:
    feed = pd.read_parquet(EXTERNAL_DATA_DIR / "pjm-pnodes" / "pjm_pnodes.parquet")
    feed = feed[feed["termination_date"].astype(str).str.startswith("9999")].copy()
    feed["pnode_id"] = feed["pnode_id"].astype("int64")
    feed["effective_date"] = pd.to_datetime(feed["effective_date"])
    return feed


def read_contingencies() -> pd.DataFrame:
    rows = []
    for contingency in ET.parse(CONTINGENCY_LIST).getroot().findall("Contingency"):
        name = contingency.findtext("Name").strip()
        edit = contingency.find("LastEdit")
        for seq, element in enumerate(contingency.findall("Equipment/ContingencyEquipment"), 1):
            rows.append((
                name, seq, contingency.findtext("ActiveFlag") == "Y", edit.get("Timestamp") if edit is not None else None,
                element.get("Action"), element.get("Type"), element.get("b1").strip(), element.get("b2"), element.get("b3"),
            ))
    equipment = pd.DataFrame(rows, columns=["contingency_name", "element_seq", "is_active", "last_edit", "action", "equipment_type", "substation_code", "voltage_text", "equipment_id"])
    equipment["last_edit"] = pd.to_datetime(equipment["last_edit"])
    equipment["voltage_kv"] = equipment["voltage_text"].map(kv)
    return equipment


def add_contingency_substations(subs: pd.DataFrame, equipment: pd.DataFrame) -> pd.DataFrame:
    new = equipment[~equipment["substation_code"].isin(subs["code"])]
    grouped = new.groupby("substation_code")["voltage_kv"]
    extra = pd.DataFrame({
        "code": grouped.size().index,
        "n_pnodes": 0, "n_load": 0, "n_gen": 0, "n_ehv": 0,
        "max_voltage_kv": grouped.max().values,
        "voltages_kv": [sorted(v.dropna().unique().tolist()) for _, v in grouped],
        "in_gnn": False,
        "source": "contingency_list",
    })
    extra.insert(0, "substation_id", range(subs["substation_id"].max() + 1, subs["substation_id"].max() + 1 + len(extra)))
    logger.info(f"{len(extra)} substations from the contingency list only")
    return pd.concat([subs.assign(source="bus_model"), extra], ignore_index=True)


def link_contingency_equipment(equipment: pd.DataFrame, subs: pd.DataFrame) -> pd.DataFrame:
    by_code = subs.groupby("code")["substation_id"].agg(list)
    ids = equipment["substation_code"].map(by_code)
    equipment["substation_id"] = ids.map(lambda m: m[0] if len(m) == 1 else None)
    return equipment


def name_tokens(name: str) -> list[str]:
    return [w.lower() for w in NAME_SPLIT.split(name) if re.fullmatch(r"[A-Za-z]{2,}", w)]


def breaker_stations(equipment: pd.DataFrame) -> pd.Series:
    breakers = equipment[equipment["equipment_type"] == "CB"]
    return breakers.groupby("contingency_name", sort=False)["substation_code"].agg(lambda s: list(dict.fromkeys(s)))


def token_stations(stations: pd.Series, subs: pd.DataFrame) -> dict:
    """Name token (and zone, or None for any zone) -> the one breaker station in at least 80% of the contingencies naming it."""
    zones = subs[subs["source"] == "bus_model"].groupby("code")["tx_zone"].agg(set)
    rows = []
    for name, codes in stations.items():
        contingency_zones = set().union(*[zones.get(c, set()) for c in codes]) | {None}
        rows += [(token, zone, codes) for token in set(name_tokens(name)) for zone in contingency_zones]
    out = {}
    for key, group in pd.DataFrame(rows, columns=["token", "zone", "codes"]).groupby(["token", "zone"], dropna=False):
        share = group["codes"].explode().value_counts() / len(group)
        top = share[share >= MIN_TOKEN_SHARE]
        if len(group) >= MIN_TOKEN_CONTINGENCIES and len(top) == 1:
            out[(key[0], None if pd.isna(key[1]) else key[1])] = top.index[0]
    return out


def assign_stations(subs: pd.DataFrame, equipment: pd.DataFrame, by_token: dict) -> pd.DataFrame:
    """Group bus-model codes into the contingency list's station codes: CARSON (500 kV bus) belongs to station CARSON4."""
    station_codes = set(equipment.loc[equipment["equipment_type"] == "CB", "substation_code"])
    ids = subs.groupby("code")["substation_id"].agg(list)
    unique_id = ids[ids.map(len) == 1].map(lambda m: m[0])
    station_kv = equipment.groupby("substation_code")["voltage_kv"].agg(lambda s: set(s.dropna()))
    subs["station_id"] = subs["substation_id"]
    subs["station_rule"] = "self"
    for i, row in subs[(subs["source"] == "bus_model") & ~subs["code"].isin(station_codes)].iterrows():
        if len(row["code"]) > 8 and row["code"][:8] in station_codes:
            target, rule = row["code"][:8], "truncated_code"
        elif row["n_ehv"] > 0:
            target, rule = by_token.get((row["code"].lower().replace(" ", ""), row["tx_zone"])), "name_token"
        else:
            continue
        if target in unique_id.index and row["max_voltage_kv"] in station_kv.get(target, set()):
            subs.at[i, "station_id"] = unique_id[target]
            subs.at[i, "station_rule"] = rule
    logger.info(f"station rules: {subs['station_rule'].value_counts().to_dict()}")
    return subs


def contingency_ends(name: str, codes: list[str], by_token: dict) -> tuple[str | None, str | None, list[str]] | None:
    """Endpoints of a contingency-named facility from its breaker stations, in name order where the name says it."""
    name = OUTAGE_PREFIX.sub("", name)
    tokens = name_tokens(name)
    position = {code: min((i for i, t in enumerate(tokens) if by_token.get((t, None)) == code), default=None) for code in codes}
    if len(codes) <= 2:
        ordered = sorted(codes, key=lambda c: (position[c] is None, position[c] or 0))
        return ordered[0], ordered[1] if len(ordered) == 2 else None, []
    first_element = name_tokens(re.sub(r"[(\[].*", "", name.split("+")[0]))
    ends = [by_token.get((first_element[0], None)), by_token.get((first_element[-1], None))] if first_element else [None, None]
    if None in ends or ends[0] == ends[1] or not set(ends) <= set(codes):
        return None
    return ends[0], ends[1], [c for c in codes if c not in ends]


def build_substations(buses: pd.DataFrame, topology: pd.DataFrame) -> pd.DataFrame:
    buses = buses.merge(topology, on="pnode_id", how="left")
    grouped = buses.groupby(["tx_zone", "substation"], sort=True)
    subs = grouped.agg(
        n_pnodes=("pnode_id", "size"),
        n_load=("bus_type", lambda s: (s == "LOAD").sum()),
        n_gen=("bus_type", lambda s: (s == "GEN").sum()),
        n_ehv=("bus_type", lambda s: (s == "EHV").sum()),
        max_voltage_kv=("voltage_kv", "max"),
        voltages_kv=("voltage_kv", lambda s: sorted(s.dropna().unique().tolist())),
        first_seen_date=("first_seen_date", "min"),
        last_change_date=("last_change_date", "max"),
        in_gnn=("gnn_role", lambda s: s.notna().any()),
    ).reset_index().rename(columns={"substation": "code"})
    located = buses[buses["lat"].notna()].sort_values("pnode_id").drop_duplicates(["tx_zone", "substation"])
    located = located.rename(columns={"substation": "code"})[["tx_zone", "code", "lat", "lon", "topo_source", "conflict"]]
    located["location_source"] = "gnn_topology:" + located["topo_source"]
    subs = subs.merge(located.drop(columns="topo_source").rename(columns={"conflict": "location_conflict"}), on=["tx_zone", "code"], how="left")
    subs.insert(0, "substation_id", range(1, len(subs) + 1))
    return subs


EQUIPMENT_KINDS = [
    ("transformer", r"XFORMER|\bTX\d*\b|\bTRAN\b|\bXF\b"),
    ("breaker", r"\bCB\b"),
    ("series_device", r"SER DEV"),
    ("disconnect", r"\bDIS\b"),
    ("phase_shifter", r"\bPS\d*\b"),
    ("ct_equipment", r"\bCT\b"),
]


def line_stations(body: str) -> list[str]:
    """Station names along the first element of a facility name; later elements joined by + or & are other facilities."""
    first = re.sub(r"[.(].*", "", re.split(r"\s*[+&]\s*|\s+and\s+", body)[0])
    first = re.sub(r"\s+\d+\s*$", "", first)
    return [s for s in (re.sub(r"\d{3,}[A-Za-z]?$", "", part.strip()) for part in first.split("-")) if s]


def parse_facility(name: str, codes: set[str]) -> dict:
    stripped = OUTAGE_PREFIX.sub("", name)
    has_prefix = stripped != name
    name = stripped
    if "INTERFACE" in name.upper():
        return {"category": "interface", "kind": "interface", "parse_status": "interface"}
    if re.search(r"\bl/o\b|_LN\b|^\d{4}_\d|\d+kV-\d", name) or re.match(r"^[A-Za-z][A-Za-z .]*\s-\s", name):
        return {"category": "external", "kind": "external_element", "parse_status": "not_pjm_style"}
    tail = re.search(r"(\d+(?:\.\d+)?\s*KV)\s*$", name)
    contingency_line = VOLTAGE_PREFIX.match(name)
    voltage = kv(tail.group(1)) if tail else float(contingency_line.group(1) or contingency_line.group(2)) if contingency_line else None
    if contingency_line or (has_prefix and "-" in name):
        stations = line_stations(name[contingency_line.end():] if contingency_line else name)
        ends = {"end1_code": stations[0], "end2_code": stations[-1]} if len(stations) > 1 else {}
        return {"category": "line", "kind": "line", "voltage_kv": voltage, **ends}
    equipment =next((kind for kind, pattern in EQUIPMENT_KINDS if re.search(pattern, name)), None)
    if equipment:
        first = [name[:8].strip(), name.split()[0]]
        return {"category": "substation_equipment", "kind": equipment, "voltage_kv": voltage, "end1_code": pick_code(first, codes)}
    if "-" in name[:12]:
        before, _, after = name.partition("-")
        end2 = [after[:8].strip(), after.split()[0] if after.split() else ""]
        return {"category": "line", "kind": "line", "voltage_kv": voltage, "end1_code": pick_code([before.strip(), name[:8].strip()], codes), "end2_code": pick_code(end2, codes)}
    return {"category": "unclassified", "kind": "unclassified", "voltage_kv": voltage, "end1_code": pick_code([name[:8].strip(), name.split()[0]], codes)}


def pick_code(options: list[str], codes: set[str]) -> str | None:
    known = [o for o in options if o in codes]
    return known[0] if known else options[0]


def normalize_name(name: str) -> str:
    text = name.lower().replace("_", " ")
    text = re.sub(r"\bflo\b", "l/o", text)
    text = re.sub(r"\s*-\s*", "-", text)
    text = re.sub(r"\s*kv\b", "kv", text)
    return re.sub(r"\s+", " ", text).strip()


def add_flowgates(facilities: pd.DataFrame) -> pd.DataFrame:
    flowgates = pd.read_csv(FLOWGATES).drop_duplicates("flowgate_id")
    flowgates["full"] = flowgates["flowgate_name"].map(normalize_name)
    flowgates["monitored"] = flowgates["full"].str.split(" l/o ").str[0]
    by_full = flowgates.drop_duplicates("full").set_index("full")
    monitored_counts = flowgates["monitored"].value_counts()
    by_monitored = flowgates.drop_duplicates("monitored", keep=False).set_index("monitored")
    full = facilities["name"].map(normalize_name)
    monitored = full.str.split(" l/o ").str[0]
    full_id = full.map(by_full["flowgate_id"])
    monitored_id = monitored.map(by_monitored["flowgate_id"])
    facilities["m2m_flowgate_id"] = full_id.fillna(monitored_id).astype("Int64")
    facilities["m2m_match"] = pd.Series("full_name", index=facilities.index).where(full_id.notna(), pd.Series("monitored_element", index=facilities.index).where(monitored.isin(monitored_counts.index), None))
    facilities.loc[facilities["m2m_flowgate_id"].isna() & facilities["m2m_match"].notna(), "m2m_match"] = "monitored_element_ambiguous"
    facilities["m2m_monitoring_rto"] = facilities["m2m_flowgate_id"].map(flowgates.set_index("flowgate_id")["mon_rto"])
    return facilities


def read_facility_names(conn) -> pd.DataFrame:
    feeds = [("marginal_value_rt", "datetime_beginning_utc"), ("marginal_value_da", "datetime_beginning_utc"), ("transmission_constraints_da", "datetime_beginning_utc")]
    frames = []
    for table, time_column in feeds:
        for column, role in [("monitored_facility", "is_monitored"), ("contingency_facility", "is_contingency")]:
            frame = pd.read_sql(
                f"select {column} as name, min({time_column})::date as first_seen_date, max({time_column})::date as last_seen_date "
                f"from raw_lmp.{table} where {column} is not null group by 1",
                conn,
            )
            frames.append(frame.assign(is_monitored=role == "is_monitored", is_contingency=role == "is_contingency"))
    names = pd.concat(frames, ignore_index=True).groupby("name", as_index=False).agg(
        first_seen_date=("first_seen_date", "min"),
        last_seen_date=("last_seen_date", "max"),
        is_monitored=("is_monitored", "max"),
        is_contingency=("is_contingency", "max"),
    )
    return names


def read_weights(conn, table: str, column: str) -> pd.Series:
    weights = pd.read_sql(
        f"select monitored_facility as name, sum(-shadow_price) as w from raw_lmp.{table} "
        f"where datetime_beginning_utc > (select max(datetime_beginning_utc) from raw_lmp.{table}) - interval '{ACTIVE_DAYS} days' group by 1",
        conn,
    )
    return weights.set_index("name")["w"].rename(column)


def station_voltages(subs: pd.DataFrame, equipment: pd.DataFrame) -> pd.Series:
    from_buses = subs.groupby("station_id")["voltages_kv"].agg(lambda s: {float(v) for vs in s for v in vs})
    contingency_kv = equipment.groupby("substation_code")["voltage_kv"].agg(lambda s: set(s.dropna()))
    from_contingencies = subs.set_index("station_id")["code"].map(contingency_kv).dropna().groupby(level=0).agg(lambda s: set().union(*s))
    return from_buses.combine(from_contingencies, lambda a, b: a | b if isinstance(b, set) else a, fill_value=set())


def apply_contingency_ends(facilities: pd.DataFrame, stations: pd.Series, by_token: dict) -> pd.DataFrame:
    for i, name in facilities.loc[facilities["name"].isin(stations.index), "name"].items():
        ends = contingency_ends(name, stations[name], by_token)
        if ends is None:
            facilities.at[i, "end1_code"], facilities.at[i, "end2_code"] = None, None
            facilities.at[i, "end1_source"] = facilities.at[i, "end2_source"] = "contingency_list_unresolved"
            continue
        facilities.at[i, "end1_code"], facilities.at[i, "end2_code"], facilities.at[i, "middle_codes"] = ends
        facilities.at[i, "end1_source"] = "contingency_list"
        facilities.at[i, "end2_source"] = "contingency_list" if ends[1] else None
    return facilities


def apply_labels(facilities: pd.DataFrame, subs: pd.DataFrame, voltages: pd.Series) -> pd.DataFrame:
    """Angie's hand labels, for facilities the contingency list does not resolve, when the station carries the facility's voltage."""
    labels = pd.read_csv(FACILITY_LABELS, dtype=str).set_index("name")
    ids = subs.groupby("code")["station_id"].agg(list)
    rejected = []
    unresolved = ~facilities["end1_source"].str.startswith("contingency_list", na=False)
    for i, row in facilities[facilities["name"].isin(labels.index) & unresolved].iterrows():
        for end in ("end1", "end2"):
            code = labels.at[row["name"], f"{end}_code"]
            if pd.isna(code):
                continue
            station = ids[code]
            if len(station) == 1 and (pd.isna(row["voltage_kv"]) or row["voltage_kv"] in voltages[station[0]]):
                facilities.at[i, f"{end}_code"], facilities.at[i, f"{end}_source"] = code, "label"
            else:
                rejected.append((row["name"], end, code))
    logger.info(f"{len(rejected)} labels rejected (code not unique or station lacks the facility voltage): {rejected}")
    return facilities


def build_facilities(conn, subs: pd.DataFrame, equipment: pd.DataFrame, by_token: dict) -> pd.DataFrame:
    names = read_facility_names(conn)
    cutoff = pd.Timestamp(names["last_seen_date"].max()) - pd.Timedelta(days=ACTIVE_DAYS)
    names = names[pd.to_datetime(names["last_seen_date"]) >= cutoff].reset_index(drop=True)
    codes = set(subs["code"])
    parsed = pd.DataFrame([parse_facility(n, codes) for n in names["name"]])
    facilities = pd.concat([names, parsed], axis=1)
    facilities["in_contingency_list"] = facilities["name"].isin(set(equipment["contingency_name"])).where(facilities["is_contingency"])
    facilities["equipment_type"] = facilities["kind"].where(facilities["category"] == "substation_equipment")
    facilities = facilities.drop(columns="kind")
    facilities[["end1_source", "end2_source", "middle_codes"]] = None
    facilities["end1_source"] = facilities["end1_source"].where(facilities["end1_code"].isna(), "parsed")
    facilities["end2_source"] = facilities["end2_source"].where(facilities["end2_code"].isna(), "parsed")
    facilities = apply_contingency_ends(facilities, breaker_stations(equipment), by_token)
    facilities = apply_labels(facilities, subs, station_voltages(subs, equipment))
    by_code = subs.groupby("code")["substation_id"].agg(list)
    for end in ("end1", "end2"):
        matches = facilities[f"{end}_code"].map(by_code)
        count = matches.map(lambda m: len(m) if isinstance(m, list) else 0)
        facilities[f"{end}_n_matches"] = count.where(facilities[f"{end}_code"].notna())
        facilities[f"{end}_substation_id"] = matches.map(lambda m: m[0] if isinstance(m, list) and len(m) == 1 else None)
    matched = facilities[["end1_substation_id", "end2_substation_id"]].notna().sum(axis=1)
    parsed = facilities["parse_status"].isna()
    lines = parsed & (facilities["category"] == "line")
    facilities.loc[lines, "parse_status"] = matched[lines].map({2: "both_ends", 1: "one_end", 0: "no_match"})
    single = parsed & ~lines
    facilities.loc[single, "parse_status"] = matched[single].gt(0).map({True: "end_matched", False: "no_match"})
    facilities = facilities.join(read_weights(conn, "marginal_value_rt", "rt_weight_365d"), on="name")
    facilities = facilities.join(read_weights(conn, "marginal_value_da", "da_weight_365d"), on="name")
    facilities["rt_weight_rank"] = facilities["rt_weight_365d"].rank(ascending=False, method="first")
    facilities = add_flowgates(facilities)
    facilities = facilities.sort_values("name").reset_index(drop=True)
    facilities.insert(0, "facility_id", range(1, len(facilities) + 1))
    return facilities


def clean(value):
    if value is None or value is pd.NA or value is pd.NaT or (isinstance(value, float) and pd.isna(value)):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def load(conn, table: str, frame: pd.DataFrame, columns: list[str]) -> None:
    rows = [tuple(clean(v) for v in row) for row in frame[columns].itertuples(index=False, name=None)]
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, f"insert into public.{table} ({', '.join(columns)}) values %s", rows, page_size=5000)
    logger.success(f"public.{table}: {len(rows)} rows")


def main() -> None:
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute((PROJ_ROOT / "network_reference" / "schema.sql").read_text())
    for table in ("generators", "plants", "contingency_equipment", "facilities", "pnodes", "substations"):
        cur.execute(f"delete from public.{table}")

    base = read_base_model()
    updates = read_updates()
    buses = add_history(active_buses(base, updates), updates)
    topology = read_topology(conn)
    subs = build_substations(buses, topology)

    buses = buses.merge(topology[["pnode_id", "gnn_role"]], on="pnode_id", how="left")
    buses = buses.merge(subs[["tx_zone", "code", "substation_id"]].rename(columns={"code": "substation"}), on=["tx_zone", "substation"], how="left")
    buses["pnode_type"] = "BUS"
    buses["in_gnn"] = buses["gnn_role"].notna()
    buses["name"] = None
    aggregates = read_aggregates()
    aggregates = aggregates[~aggregates["pnode_id"].isin(buses["pnode_id"])]
    aggregates["in_gnn"] = aggregates["pnode_id"].isin(topology["pnode_id"])
    aggregates = aggregates.merge(topology[["pnode_id", "gnn_role"]], on="pnode_id", how="left")
    pnodes = pd.concat([buses, aggregates], ignore_index=True)

    feed = read_pnode_feed()
    pnodes = pnodes.merge(feed[["pnode_id", "pnode_name", "effective_date"]], on="pnode_id", how="left")
    pnodes["name"] = pnodes["pnode_name"].fillna(pnodes["name"])
    missing = feed[~feed["pnode_id"].isin(pnodes["pnode_id"])]
    is_bus = missing["pnode_type"] == "BUS"
    new = pd.DataFrame({
        "pnode_id": missing["pnode_id"],
        "pnode_type": missing["pnode_type"].where(~is_bus, "BUS").where(is_bus, missing["pnode_subtype"]),
        "name": missing["pnode_name"],
        "tx_zone": missing["zone"],
        "voltage_text": missing["voltage_level"],
        "voltage_kv": missing["voltage_level"].map(kv),
        "bus_type": missing["pnode_subtype"].where(is_bus),
        "effective_date": missing["effective_date"],
        "source": "pjm_pnode_feed",
    })
    new["substation"] = new["name"].str[:8].str.strip().where(new["pnode_type"] == "BUS")
    new = new.merge(subs[["tx_zone", "code", "substation_id"]].rename(columns={"code": "substation"}), on=["tx_zone", "substation"], how="left")
    new = new.merge(topology[["pnode_id", "gnn_role"]], on="pnode_id", how="left")
    new["in_gnn"] = new["gnn_role"].notna()
    pnodes = pd.concat([pnodes, new], ignore_index=True)
    logger.info(f"pnode feed: {len(feed)} active, {len(new)} not in the bus model files")

    equipment = read_contingencies()
    subs = add_contingency_substations(subs, equipment)
    by_token = token_stations(breaker_stations(equipment), subs)
    subs = assign_stations(subs, equipment, by_token)
    load(conn, "substations", subs, ["substation_id", "code", "tx_zone", "n_pnodes", "n_load", "n_gen", "n_ehv", "max_voltage_kv", "voltages_kv", "first_seen_date", "last_change_date", "in_gnn", "lat", "lon", "location_source", "location_conflict", "source", "station_id", "station_rule"])
    load(conn, "pnodes", pnodes, ["pnode_id", "pnode_type", "name", "substation_id", "tx_zone", "voltage_text", "voltage_kv", "equipment", "bus_type", "effective_date", "first_seen_date", "last_change_date", "in_gnn", "gnn_role", "source"])
    facilities = build_facilities(conn, subs, equipment, by_token)
    load(conn, "facilities", facilities, ["facility_id", "name", "is_monitored", "is_contingency", "in_contingency_list", "category", "equipment_type", "voltage_kv", "end1_code", "end1_substation_id", "end1_n_matches", "end1_source", "end2_code", "end2_substation_id", "end2_n_matches", "end2_source", "middle_codes", "parse_status", "first_seen_date", "last_seen_date", "rt_weight_365d", "rt_weight_rank", "da_weight_365d", "m2m_flowgate_id", "m2m_match", "m2m_monitoring_rto"])
    load(conn, "contingency_equipment", link_contingency_equipment(equipment, subs), ["contingency_name", "element_seq", "is_active", "last_edit", "action", "equipment_type", "substation_code", "substation_id", "voltage_text", "voltage_kv", "equipment_id"])
    plants, generators = read_eia860(pnodes)
    load(conn, "plants", plants, list(plants.columns))
    load(conn, "generators", generators, list(generators.columns))
    cur.execute((PROJ_ROOT / "network_reference" / "facility_ends.sql").read_text())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
