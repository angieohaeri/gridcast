"""Build a substation-graph nodes/edges table from the HIFLD transmission lines dataset.

Uses each line's own endpoint geometry as the node identity - not SUB_1/SUB_2 name-joins
against the separate substations file, which validated at only ~25% name-match even at
~0m distance (see references/lmp-pricing-model/lmp-pricing-model.md, "Validated 2026-08-22"). Endpoints
within COORD_SNAP_M of each other are treated as the same node (shared substation).

Filtered to lines intersecting the PJM footprint (12,434 of 94,619 nationally, ~13%) -
the rest isn't relevant to this project. Zone attribution is a point-in-polygon join
against pjm_zones.geojson, not name-matching, for the same reason.

Output: raw_lmp.transmission_nodes / raw_lmp.transmission_edges (reference tables, not
hypertables - static-ish data, rebuilt wholesale on each run, not incremental).
"""

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from gridcast.config import EXTERNAL_DATA_DIR, get_connection, setup_logging
from loguru import logger

setup_logging()

COORD_SNAP_M = 5  # endpoints within this distance share a node (EPSG:3857, meters)

lines = gpd.read_file(EXTERNAL_DATA_DIR / "US_Electric_Power_Transmission_Lines.gpkg")
zones = gpd.read_file(EXTERNAL_DATA_DIR / "pjm_zones.geojson")

# reproject zones into lines' CRS (EPSG:3857, meters), not the other way around - node
# snapping below needs meters, and lines.crs is already meters
zones = zones.to_crs(lines.crs)
zones["geometry"] = zones.geometry.buffer(0)  # fixes self-intersection topology errors

pjm_union = zones.geometry.union_all()
lines = lines[lines.geometry.intersects(pjm_union)].reset_index(drop=True)
logger.info(f"{len(lines)} lines intersect the PJM footprint")


def endpoints(geom):
    if geom.geom_type == "MultiLineString":
        geom = max(geom.geoms, key=lambda g: g.length)
    coords = list(geom.coords)
    return Point(coords[0]), Point(coords[-1])


pts = lines.geometry.apply(endpoints)
lines["pt1"] = pts.apply(lambda t: t[0])
lines["pt2"] = pts.apply(lambda t: t[1])

# snap endpoints to a coordinate grid so near-identical points (same physical substation,
# per the ~0m-distance validation) collapse to one node
def snap(pt):
    return (round(pt.x / COORD_SNAP_M) * COORD_SNAP_M, round(pt.y / COORD_SNAP_M) * COORD_SNAP_M)


lines["node1_key"] = lines["pt1"].apply(snap)
lines["node2_key"] = lines["pt2"].apply(snap)

node_keys = pd.unique(pd.concat([lines["node1_key"], lines["node2_key"]]))
nodes = pd.DataFrame(node_keys.tolist(), columns=["x", "y"])
nodes["node_id"] = nodes.index
node_lookup = {(row.x, row.y): row.node_id for row in nodes.itertuples()}

lines["node1_id"] = lines["node1_key"].map(node_lookup)
lines["node2_id"] = lines["node2_key"].map(node_lookup)

nodes_gdf = gpd.GeoDataFrame(
    nodes, geometry=gpd.points_from_xy(nodes["x"], nodes["y"]), crs=zones.crs
)
nodes_zoned = gpd.sjoin(nodes_gdf, zones[["zone_id", "geometry"]], how="left", predicate="within")
nodes_zoned = nodes_zoned[~nodes_zoned.index.duplicated()]  # a node exactly on a border joins once
nodes["zone_id"] = nodes_zoned["zone_id"].where(nodes_zoned["zone_id"].notna(), None).values

# pjm_zones.geojson was built for dashboard visuals, not authoritative boundaries - it has
# real coverage gaps (not just border imprecision; one zone, EKPC, was missing ~250 nodes'
# worth of territory in a 2026-08-22 check). Fall back to nearest-zone for anything the
# strict polygon join misses - median gap distance was ~1km, so "nearest zone" is a much
# better answer than leaving it unattributed.
missing = nodes["zone_id"].isna()
if missing.any():
    nearest = gpd.sjoin_nearest(nodes_gdf[missing], zones[["zone_id", "geometry"]], distance_col="_dist")
    nearest = nearest[~nearest.index.duplicated()]
    nodes.loc[missing, "zone_id"] = nearest["zone_id"].values
    logger.info(f"nearest-zone fallback recovered {missing.sum()} nodes (median gap {nearest['_dist'].median():.0f}m)")

nodes_wgs = nodes_gdf.to_crs("EPSG:4326")
nodes["lon"] = nodes_wgs.geometry.x
nodes["lat"] = nodes_wgs.geometry.y

logger.info(f"{len(nodes)} distinct nodes, {nodes['zone_id'].notna().sum()} zone-attributed")

edges = lines[["ID", "node1_id", "node2_id", "VOLTAGE", "VOLT_CLASS", "INFERRED", "Shape__Len"]].rename(
    columns={
        "ID": "line_id",
        "VOLTAGE": "voltage_kv",
        "VOLT_CLASS": "volt_class",
        "INFERRED": "inferred",
        "Shape__Len": "length_m",
    }
)
edges["voltage_kv"] = edges["voltage_kv"].where(edges["voltage_kv"] != -999999, None)

nodes = nodes[["node_id", "lon", "lat", "zone_id"]]

conn = get_connection()
cur = conn.cursor()
cur.execute("drop table if exists raw_lmp.transmission_edges;")
cur.execute("drop table if exists raw_lmp.transmission_nodes;")
cur.execute("""
    create table raw_lmp.transmission_nodes (
        node_id integer primary key,
        lon double precision not null,
        lat double precision not null,
        zone_id text
    );
""")
cur.execute("""
    create table raw_lmp.transmission_edges (
        line_id text,
        node1_id integer references raw_lmp.transmission_nodes(node_id),
        node2_id integer references raw_lmp.transmission_nodes(node_id),
        voltage_kv numeric,
        volt_class text,
        inferred text,
        length_m numeric
    );
""")

import psycopg2.extras

psycopg2.extras.execute_values(
    cur,
    "insert into raw_lmp.transmission_nodes (node_id, lon, lat, zone_id) values %s",
    list(nodes.itertuples(index=False, name=None)),
)
psycopg2.extras.execute_values(
    cur,
    "insert into raw_lmp.transmission_edges (line_id, node1_id, node2_id, voltage_kv, volt_class, inferred, length_m) values %s",
    list(edges.itertuples(index=False, name=None)),
)
logger.success(f"loaded {len(nodes)} nodes, {len(edges)} edges into raw_lmp")
conn.close()
