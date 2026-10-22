"""Attach PJM BUS pnodes to a transmission graph and build substation-to-substation edges for
the GNN, merging GridSFM (OSM-derived network) with the HIFLD attach from join_pnode_hifld.py.

GridSFM (huggingface.co/datasets/microsoft/GridSFM_US_power_grid, 16h/pjm_model.json, MIT) has
one bus per voltage yard; same-name yards within SITE_KM are one physical substation. pnode
substation codes (first 8 chars of pnode_name) match GridSFM names within zone (exact, then
8-char truncation). Ambiguous names: candidates all within SITE_KM merge into one site;
otherwise the HIFLD location picks the nearest candidate; otherwise dropped. Where both sources
match, locations agree to ~30 m median - pairs over CONFLICT_KM apart are flagged and kept out of
the edges.

Edges: attached substations reachable within MAX_HOPS through unattached graph nodes, from each
graph separately, unioned on pnode substation (zone_id|substation) with a source tag.

Inputs: data/external/{pjm_pnodes.parquet, gridsfm_pjm_network.json, pjm_zones.geojson},
data/interim/pnode_hifld_attach.parquet, raw_lmp.transmission_nodes/edges.
Outputs: data/interim/pnode_topology_attach.parquet, data/interim/pnode_topology_edges.parquet
"""

import json

from dotenv import load_dotenv
import geopandas as gpd
from loguru import logger
import networkx as nx
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from gridcast.config import EXTERNAL_DATA_DIR, INTERIM_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

SITE_KM = 2.0
CONFLICT_KM = 5.0
MAX_HOPS = 4
LOCATION_TO_ZONE = {
    "AECO": "AE", "AEP": "AEP", "APS": "AP", "ATSI": "ATSI", "BGE": "BC", "COMED": "CE", "DAY": "DAY",
    "DEOK": "DEOK", "DOM": "DOM", "DPL": "DPL", "DUQ": "DUQ", "EKPC": "EKPC", "JCPL": "JC", "METED": "ME",
    "PECO": "PE", "PEPCO": "PEP", "PPL": "PL", "PENELEC": "PN", "PSEG": "PS", "RECO": "RECO",
}
SUFFIX = r"\b(SUBSTATION|SWITCHING STATION|SWITCHYARD|SWITCH YARD|SWITCHING|SUBSTA|SUBS|SUB|STATION|SS)\b"
KEY = ["zone_id", "substation"]


def nospace(s: pd.Series) -> pd.Series:
    return s.str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)


def hav_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371 * np.arcsin(np.sqrt(a))


def contract(G: nx.Graph, attached: set) -> nx.Graph:
    H = nx.Graph()
    H.add_nodes_from(attached)
    for s in attached:
        frontier, seen = [s], {s}
        for _ in range(MAX_HOPS):
            nxt = []
            for u in frontier:
                for v in G[u]:
                    if v in seen:
                        continue
                    seen.add(v)
                    if v in attached:
                        H.add_edge(s, v)
                    else:
                        nxt.append(v)
            frontier = nxt
    return H


# --- pnode substations ---------------------------------------------------------------
pn = pd.read_parquet(EXTERNAL_DATA_DIR / "pjm_pnodes.parquet")
bus = pn[pn["pnode_type"] == "BUS"].copy()
bus["zone_id"] = bus["zone"].map(LOCATION_TO_ZONE)
bus = bus[bus["zone_id"].notna()]  # CPL/DUKE/OVEC are outside the project's 20 zones
bus["substation"] = bus["pnode_name"].str[:8].str.strip().str.upper()
bus["kv"] = bus["voltage_level"].str.extract(r"([\d.]+)")[0].astype(float)
psub = bus.groupby(KEY).agg(max_kv=("kv", "max")).reset_index()
psub["key"] = nospace(psub["substation"])

# --- GridSFM buses -> physical substations ------------------------------------------
net = json.loads((EXTERNAL_DATA_DIR / "gridsfm_pjm_network.json").read_text())
gb = pd.DataFrame(net["bus"].values())[["bus_i", "name", "lat", "lon", "base_kv"]]
gb["name"] = gb["name"].fillna("")
gb["site"] = ""
for name, grp in gb.groupby("name"):
    tree = BallTree(np.radians(grp[["lat", "lon"]].values), metric="haversine")
    nbrs = tree.query_radius(np.radians(grp[["lat", "lon"]].values), r=SITE_KM / 6371.0)
    yg = nx.Graph()
    yg.add_nodes_from(range(len(grp)))
    yg.add_edges_from((i, j) for i, js in enumerate(nbrs) for j in js if i != j)
    for k, comp in enumerate(nx.connected_components(yg)):
        gb.loc[grp.index[list(comp)], "site"] = f"{name}|{k}"
sites = gb.groupby("site").agg(name=("name", "first"), lat=("lat", "mean"), lon=("lon", "mean"), src_kv=("base_kv", "max")).reset_index()

zones = gpd.read_file(EXTERNAL_DATA_DIR / "pjm_zones.geojson").to_crs("EPSG:3857")
zones["geometry"] = zones.geometry.buffer(0)
g = gpd.GeoDataFrame(sites, geometry=gpd.points_from_xy(sites["lon"], sites["lat"]), crs="EPSG:4326").to_crs("EPSG:3857")
g = g[g.geometry.intersects(zones.geometry.union_all().buffer(5000))]
j = gpd.sjoin_nearest(g, zones[["zone_id", "geometry"]], how="left")
g["zone_id"] = j[~j.index.duplicated()]["zone_id"].values
sites_pjm = pd.DataFrame(g.drop(columns="geometry"))
logger.info(f"GridSFM: {len(gb)} buses -> {len(sites)} sites, {len(sites_pjm)} in PJM footprint")

br = pd.DataFrame(net["branch"].values())
br = br[br["br_status"] == 1]
bus_site = gb.set_index("bus_i")["site"]
SG = nx.Graph()
SG.add_edges_from(
    (a, b) for a, b in zip(br["f_bus"].map(bus_site), br["t_bus"].map(bus_site)) if a != b
)

# --- name match within zone ----------------------------------------------------------
sites_pjm["key_full"] = nospace(sites_pjm["name"].str.upper().str.replace(SUFFIX, "", regex=True))
sites_pjm["key8"] = sites_pjm["key_full"].str[:8]
cands = pd.concat([
    psub.merge(sites_pjm, left_on=["zone_id", "key"], right_on=["zone_id", "key_full"]).assign(how="exact"),
    psub.merge(sites_pjm, left_on=["zone_id", "key"], right_on=["zone_id", "key8"]).assign(how="trunc8"),
]).drop_duplicates(KEY + ["site"])
cands["n_cand"] = cands.groupby(KEY)["site"].transform("nunique")
amb_volt = cands[(cands["n_cand"] > 1) & (cands["max_kv"] <= cands["src_kv"] + 1)]
amb_volt = amb_volt[amb_volt.groupby(KEY)["site"].transform("nunique") == 1]
gm = pd.concat([cands[cands["n_cand"] == 1], amb_volt]).drop_duplicates(KEY)
amb = cands[cands["n_cand"] > 1].merge(gm[KEY], how="left", indicator=True)
amb = amb[amb["_merge"] == "left_only"]
logger.info(f"GridSFM name match: {len(gm)} / {len(psub)}, ambiguous {amb.groupby(KEY).ngroups}")

# --- resolve ambiguous: same-site merge, then HIFLD location -------------------------
hif = pd.read_parquet(INTERIM_DATA_DIR / "pnode_hifld_attach.parquet")[KEY + ["node_id"]].drop_duplicates()
conn = get_connection()
tn = pd.read_sql("select node_id, lon, lat from raw_lmp.transmission_nodes", conn)
te = pd.read_sql("select node1_id, node2_id from raw_lmp.transmission_edges", conn)
conn.close()
hif = hif.merge(tn, on="node_id")
hif_xy = hif.set_index(KEY)[["lat", "lon"]]

site_merges, recovered = {}, []
for (z, s), c in amb.groupby(KEY):
    c = c.reset_index(drop=True)
    d = hav_km(c["lat"].values[:, None], c["lon"].values[:, None], c["lat"].values[None, :], c["lon"].values[None, :])
    if d.max() <= SITE_KM:
        keep = c.loc[c["src_kv"].idxmax()].to_dict()
        site_merges.update({o: keep["site"] for o in c["site"]})
        recovered.append(keep | {"how": "site_merge"})
    elif (z, s) in hif_xy.index:
        dist = hav_km(*hif_xy.loc[(z, s)].values, c["lat"].values, c["lon"].values)
        srt = np.sort(dist)
        if srt[0] <= SITE_KM and srt[1] > 2 * max(srt[0], 0.5):
            recovered.append(c.loc[dist.argmin()].to_dict() | {"how": "hifld_anchor"})
rec = pd.DataFrame(recovered)
logger.info(f"recovered {len(rec)} ambiguous: {rec['how'].value_counts().to_dict()}")
SG = nx.relabel_nodes(SG, site_merges)
SG.remove_edges_from(nx.selfloop_edges(SG))
gm = pd.concat([gm, rec])[KEY + ["site", "lat", "lon", "how"]]

# --- merge with HIFLD, flag location conflicts ---------------------------------------
att = psub[KEY].merge(gm.rename(columns={"lat": "g_lat", "lon": "g_lon", "how": "gridsfm_how"}), how="left", on=KEY)
att = att.merge(hif.rename(columns={"node_id": "hifld_node", "lat": "h_lat", "lon": "h_lon"}), how="left", on=KEY)
att = att[att["site"].notna() | att["hifld_node"].notna()].copy()
att["source"] = np.select([att["site"].notna() & att["hifld_node"].notna(), att["site"].notna()], ["both", "gridsfm"], "hifld")
att["gap_km"] = hav_km(att["g_lat"], att["g_lon"], att["h_lat"], att["h_lon"])
att["conflict"] = att["gap_km"] > CONFLICT_KM
att["lat"] = att["g_lat"].fillna(att["h_lat"])
att["lon"] = att["g_lon"].fillna(att["h_lon"])
att["sub_key"] = att["zone_id"] + "|" + att["substation"]
both = att[att["source"] == "both"]
logger.info(
    f"attached {len(att)} / {len(psub)} pnode substations {att['source'].value_counts().to_dict()}; "
    f"both-source gap median {both['gap_km'].median() * 1000:.0f} m, conflicts {att['conflict'].sum()}"
)

# --- edges ---------------------------------------------------------------------------
ok = att[~att["conflict"]]
g_key = dict(zip(ok.loc[ok["site"].notna(), "site"], ok.loc[ok["site"].notna(), "sub_key"]))
h_key = dict(zip(ok.loc[ok["hifld_node"].notna(), "hifld_node"].astype(int), ok.loc[ok["hifld_node"].notna(), "sub_key"]))
HG = nx.Graph()
HG.add_nodes_from(tn["node_id"])
HG.add_edges_from(te[["node1_id", "node2_id"]].itertuples(index=False, name=None))

src = {}
for G, km, label in ((SG, g_key, "gridsfm"), (HG, h_key, "hifld")):
    for u, v in contract(G, set(km) & set(G.nodes)).edges:
        src.setdefault(tuple(sorted((km[u], km[v]))), set()).add(label)
edges = pd.DataFrame([(a, b, "+".join(sorted(s))) for (a, b), s in src.items()], columns=["sub_a", "sub_b", "source"])

M = nx.Graph()
M.add_nodes_from(att["sub_key"])
M.add_edges_from(edges[["sub_a", "sub_b"]].itertuples(index=False, name=None))
logger.info(
    f"<= {MAX_HOPS} hops: {len(edges)} edges {edges['source'].value_counts().to_dict()}, "
    f"largest component {len(max(nx.connected_components(M), key=len))} / {len(att)}"
)

pn_att = bus[["pnode_id", "pnode_name", "zone_id", "substation", "kv", "pnode_subtype"]].merge(
    att[KEY + ["sub_key", "site", "hifld_node", "lat", "lon", "source", "conflict"]], on=KEY
)
logger.info(f"BUS pnodes attached: {len(pn_att)} / {len(bus)} ({len(pn_att) / len(bus):.1%})")
pn_att.to_parquet(INTERIM_DATA_DIR / "pnode_topology_attach.parquet")
edges.to_parquet(INTERIM_DATA_DIR / "pnode_topology_edges.parquet")
logger.success(f"saved pnode_topology_attach.parquet, pnode_topology_edges.parquet -> {INTERIM_DATA_DIR}")
