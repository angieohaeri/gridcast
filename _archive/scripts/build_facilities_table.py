"""Build a facility-name-to-location lookup for raw_lmp.marginal_value_da/rt's
monitored_facility and contingency_facility columns.

Neither PJM's pnode data nor the LMP feeds carry coordinates for these facility
strings directly - the only source of real lat/lon is the HIFLD substations file.
Matching works because PJM truncates/pads substation names to a short code (e.g.
NOTTINGH, 02LYONS) shared across the pnode list, the HIFLD substations file, and
PJM's own lmp-aggregate-definitions.xlsx - so a facility string's substation code,
found anywhere in it (not just as a prefix - contingency_facility strings embed it
mid-string), resolves to a match across all three name sources.

Output: raw_lmp.facilities (reference table, not a hypertable - rebuilt wholesale
on each run, not incremental). See references/lmp-pricing-model/schema.md.
"""

import re

import geopandas as gpd
import gridstatus
import pandas as pd
import psycopg2.extras
from dotenv import load_dotenv
from loguru import logger

from gridcast.config import EXTERNAL_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

MIN_NAME_LEN = 4  # candidate names shorter than this match too much junk by substring
VOLTAGE_TOLERANCE_KV = 1.0

conn = get_connection()
cur = conn.cursor()
cur.execute(
    """
    select distinct monitored_facility from raw_lmp.marginal_value_da
    union select distinct monitored_facility from raw_lmp.marginal_value_rt
    union select distinct contingency_facility from raw_lmp.marginal_value_da
    union select distinct contingency_facility from raw_lmp.marginal_value_rt
    """
)
facilities = sorted(r[0] for r in cur.fetchall() if r[0])
logger.info(f"{len(facilities)} distinct facility names (monitored + contingency, union)")

pjm = gridstatus.PJM()
pnodes = pjm.get_pnode_ids()
zone_codes = set(pnodes["zone"].dropna().str.strip().str.upper()) | set(
    pnodes.loc[pnodes["pnode_subtype"] == "ZONE", "pnode_short_name"].dropna().str.strip().str.upper()
)

subs = gpd.read_file(EXTERNAL_DATA_DIR / "electric_substation_hifld_v4.gpkg")
zones = gpd.read_file(EXTERNAL_DATA_DIR / "pjm_zones.geojson").to_crs(subs.crs)
zones["geometry"] = zones.geometry.buffer(0)
subs = subs[subs.geometry.intersects(zones.geometry.union_all())].reset_index(drop=True)
logger.info(f"{len(subs)} HIFLD substations intersect the PJM footprint")
subs["name_u"] = subs["name"].str.strip().str.upper()

agg = pd.read_excel(EXTERNAL_DATA_DIR / "lmp-aggregate-definitions.xlsx", header=3)
agg["station_u"] = agg["Station"].str.strip().str.upper()


def parse_voltage_kv(text):
    m = re.search(r"(\d+(?:\.\d+)?)\s*[Kk][Vv]", text)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+)\s*/\s*\d+", text)
    if m:
        return float(m.group(1))
    return None


pnode_lookup = {}
for name, grp in pnodes.dropna(subset=["pnode_short_name"]).groupby(pnodes["pnode_short_name"].str.strip().str.upper()):
    if name in zone_codes:
        continue
    pnode_lookup[name] = [
        {"pnode_id": int(row.pnode_id), "voltage_kv": parse_voltage_kv(row.voltage_level) if pd.notna(row.voltage_level) else None}
        for row in grp.itertuples()
    ]

hifld_lookup = {}
for name, grp in subs.dropna(subset=["name_u"]).groupby("name_u"):
    hifld_lookup[name] = [
        {"lat": row.latitude, "lon": row.longitude, "voltage_kv": row.max_volt if row.max_volt != -999999 else None}
        for row in grp.itertuples()
    ]

agg_lookup = {}
for name, grp in agg.dropna(subset=["station_u"]).groupby("station_u"):
    agg_lookup[name] = [
        {
            "pnode_id": int(row._5) if pd.notna(row._5) else None,
            "voltage_kv": parse_voltage_kv(row.Voltage) if pd.notna(row.Voltage) else None,
        }
        for row in grp.itertuples()
    ]

known_names = (set(pnode_lookup) | set(hifld_lookup) | set(agg_lookup)) - zone_codes
long_known = sorted((n for n in known_names if len(n) >= MIN_NAME_LEN), key=len, reverse=True)


def candidate_prefixes(name):
    name = name.strip()
    first_token = name.split(" ")[0]
    cands = []
    if "_" in first_token:
        cands.append(name.split("_")[0].strip().upper())
    elif "-" in first_token and not first_token[0].isdigit():
        cands.extend(p.strip().upper() for p in first_token.split("-") if p.strip())
    cands.append(name[:8].strip().upper())
    return [c for c in dict.fromkeys(cands) if c not in zone_codes]


def match_facility(name):
    for cand in candidate_prefixes(name):
        if cand in known_names:
            return cand
    name_u = name.upper()
    for known in long_known:
        if known in name_u:
            return known
    return None


rows = []
for facility in facilities:
    matched_name = match_facility(facility)
    if matched_name is None:
        rows.append((facility, None, None, None, None, None, None))
        continue

    hifld_entries = hifld_lookup.get(matched_name, [])
    pnode_entries = pnode_lookup.get(matched_name, [])
    agg_entries = agg_lookup.get(matched_name, [])
    facility_voltage = parse_voltage_kv(facility)

    def best_entry(entries):
        if not entries:
            return None
        if facility_voltage is None or len(entries) == 1:
            return entries[0]
        with_voltage = [e for e in entries if e.get("voltage_kv") is not None]
        on_voltage = [e for e in with_voltage if abs(facility_voltage - e["voltage_kv"]) < VOLTAGE_TOLERANCE_KV]
        return on_voltage[0] if on_voltage else entries[0]

    source = "hifld_substation" if hifld_entries else ("pnode" if pnode_entries else "aggregate_def")
    hifld_best = best_entry(hifld_entries)
    pnode_best = best_entry(pnode_entries)
    agg_best = best_entry(agg_entries)

    lat = hifld_best["lat"] if hifld_best else None
    lon = hifld_best["lon"] if hifld_best else None
    pnode_id = None
    if pnode_best:
        pnode_id = pnode_best["pnode_id"]
    elif agg_best and agg_best["pnode_id"] is not None:
        pnode_id = agg_best["pnode_id"]

    candidate_voltages = [
        e["voltage_kv"] for e in hifld_entries + pnode_entries + agg_entries if e.get("voltage_kv") is not None
    ]
    voltage_match = None
    if facility_voltage is not None and candidate_voltages:
        voltage_match = any(abs(facility_voltage - v) < VOLTAGE_TOLERANCE_KV for v in candidate_voltages)

    rows.append((facility, matched_name, pnode_id, lat, lon, voltage_match, source))

matched_count = sum(1 for r in rows if r[1] is not None)
logger.info(f"{matched_count} / {len(rows)} facilities matched ({matched_count / len(rows):.1%})")

cur.execute("drop table if exists raw_lmp.facilities;")
cur.execute(
    """
    create table raw_lmp.facilities (
        facility text primary key,
        matched_name text,
        pnode_id bigint,
        lat double precision,
        lon double precision,
        voltage_match boolean,
        source text
    );
    """
)
psycopg2.extras.execute_values(
    cur,
    "insert into raw_lmp.facilities (facility, matched_name, pnode_id, lat, lon, voltage_match, source) values %s",
    rows,
)
logger.success(f"loaded {len(rows)} rows into raw_lmp.facilities")
conn.close()
