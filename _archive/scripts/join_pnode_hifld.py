"""Attach PJM BUS pnodes to the HIFLD transmission graph (raw_lmp.transmission_nodes/edges)
for GNN edge construction.

pnode -> substation: first 8 chars of pnode_name (fixed-width; agrees 100% with the
2008-2019 LMP Bus Model Update files' Substation column). Substation -> HIFLD substation:
name match within zone (exact, then 8-char truncation), voltage tie-break for ambiguous
names. HIFLD substation -> transmission node: nearest line endpoint within SNAP_KM.

Edges are built downstream in build_pnode_topology.py, together with GridSFM.

Inputs: data/external/pjm_pnodes.parquet (pull_pnode_metadata.py),
data/external/electric_substation_hifld_v4.gpkg (DVC), raw_lmp.transmission_nodes.
Output: data/interim/pnode_hifld_attach.parquet
"""

import re

from dotenv import load_dotenv
import geopandas as gpd
from loguru import logger
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from gridcast.config import EXTERNAL_DATA_DIR, INTERIM_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

SNAP_KM = 1.0

LOCATION_TO_ZONE = {
    "AECO": "AE", "AEP": "AEP", "APS": "AP", "ATSI": "ATSI", "BGE": "BC", "COMED": "CE", "DAY": "DAY",
    "DEOK": "DEOK", "DOM": "DOM", "DPL": "DPL", "DUQ": "DUQ", "EKPC": "EKPC", "JCPL": "JC", "METED": "ME",
    "PECO": "PE", "PEPCO": "PEP", "PPL": "PL", "PENELEC": "PN", "PSEG": "PS", "RECO": "RECO",
}


def kv(text: str) -> float:
    return float(re.match(r"\s*([\d.]+)", text).group(1))


def nospace(s: pd.Series) -> pd.Series:
    return s.str.replace(r"[^A-Z0-9]", "", regex=True)


pn = pd.read_parquet(EXTERNAL_DATA_DIR / "pjm_pnodes.parquet")
bus = pn[pn["pnode_type"] == "BUS"].copy()
bus["zone_id"] = bus["zone"].map(LOCATION_TO_ZONE)
bus = bus[bus["zone_id"].notna()]  # CPL/DUKE/OVEC are outside the project's 20 zones
bus["substation"] = bus["pnode_name"].str[:8].str.strip().str.upper()
bus["kv"] = bus["voltage_level"].map(kv)
psub = bus.groupby(["zone_id", "substation"]).agg(n_bus=("pnode_id", "size"), max_kv=("kv", "max")).reset_index()
psub["key"] = nospace(psub["substation"])
logger.info(f"{len(bus)} in-scope BUS pnodes, {len(psub)} substations")

subs = gpd.read_file(EXTERNAL_DATA_DIR / "electric_substation_hifld_v4.gpkg")
zones = gpd.read_file(EXTERNAL_DATA_DIR / "pjm_zones.geojson").to_crs(subs.crs)
zones["geometry"] = zones.geometry.buffer(0)
subs = subs[subs.geometry.intersects(zones.geometry.union_all().buffer(5000))].reset_index(drop=True)
j = gpd.sjoin_nearest(subs, zones[["zone_id", "geometry"]], how="left", distance_col="_d")
subs["zone_id"] = j[~j.index.duplicated()]["zone_id"].values
subs = subs[~subs["name"].str.upper().str.startswith(("UNKNOWN", "TAP"))].copy()
subs["name_u"] = subs["name"].str.upper().str.strip()
subs["key_full"] = nospace(subs["name_u"])
subs["key8"] = nospace(subs["name_u"].str[:8])
subs["max_volt"] = subs["max_volt"].where(subs["max_volt"] > 0)
subs["min_volt"] = subs["min_volt"].where(subs["min_volt"] > 0)
logger.info(f"{len(subs)} named HIFLD substations in/near PJM")

cands = pd.concat([
    psub.merge(subs, left_on=["zone_id", "key"], right_on=["zone_id", "key_full"]).assign(how="exact"),
    psub.merge(subs, left_on=["zone_id", "key"], right_on=["zone_id", "key8"]).assign(how="trunc8"),
]).drop_duplicates(["zone_id", "substation", "id"])
cands["volt_ok"] = (cands["max_kv"] <= cands["max_volt"] + 1) & (cands["max_kv"] >= cands["min_volt"] - 1)
cands["n_cand"] = cands.groupby(["zone_id", "substation"])["id"].transform("nunique")
amb_ok = cands[(cands["n_cand"] > 1) & cands["volt_ok"]]
amb_ok = amb_ok[amb_ok.groupby(["zone_id", "substation"])["id"].transform("nunique") == 1]
matched = pd.concat([cands[cands["n_cand"] == 1], amb_ok]).drop_duplicates(["zone_id", "substation"])
logger.info(f"matched {len(matched)} / {len(psub)} substations ({matched['how'].value_counts().to_dict()})")

conn = get_connection()
tn = pd.read_sql("select node_id, lon, lat from raw_lmp.transmission_nodes", conn)
conn.close()

tree = BallTree(np.radians(tn[["lat", "lon"]].values), metric="haversine")
dist, idx = tree.query(np.radians(matched[["latitude", "longitude"]].values), k=1)
matched["snap_km"] = dist[:, 0] * 6371.0
matched["node_id"] = tn["node_id"].values[idx[:, 0]]
att = matched[matched["snap_km"] <= SNAP_KM]
bus_att = bus.merge(att[["zone_id", "substation", "node_id"]], on=["zone_id", "substation"])
logger.info(f"attached {len(att)} substations / {len(bus_att)} BUS pnodes ({len(bus_att) / len(bus):.1%}) within {SNAP_KM} km")

bus_att[["pnode_id", "pnode_name", "zone_id", "substation", "kv", "node_id"]].to_parquet(INTERIM_DATA_DIR / "pnode_hifld_attach.parquet")
logger.success(f"saved {INTERIM_DATA_DIR / 'pnode_hifld_attach.parquet'}")
