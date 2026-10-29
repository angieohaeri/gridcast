"""OpenStreetMap locations for GNN nodes the original topology could not place, and map edges for them.

Coordinates come from network_reference/osm_station_locations.csv (matched to OSM and reviewed by hand,
see references/lmp-pricing-model/*-osm-locations-review.csv). Nodes still without a location get them,
with topo_source 'osm'. Every located station is attached to the nearest HIFLD transmission-line
endpoint (raw_lmp.transmission_nodes) within SNAP_KM, the rule the original map edges used
(_archive/scripts/join_pnode_hifld.py), and attached stations within MAX_HOPS on the HIFLD line graph
are joined. Pairs that include a newly located station and are not already edges are written with
source 'hifld_osm'. Several codes at one OSM site share its edges but get none between themselves.
Reruns are safe: only 'osm' locations and 'hifld_osm' edges are replaced. Run after
network_reference.build and constraint_nodes, then rerun network_reference.line_edges.
"""

from collections import defaultdict

from loguru import logger
from network_reference.build import OSM_LOCATIONS
import numpy as np
import pandas as pd
import psycopg2.extras
from sklearn.neighbors import BallTree

from gridcast.config import get_connection

SNAP_KM = 1.0
MAX_HOPS = 4
SOURCE = "hifld_osm"

NODES = """
select n.pnode_id, n.sub_key, n.lat, n.lon, n.topo_source, s.code, coalesce(s.tx_zone, '') as tx_zone
from raw_lmp.pnode_topology_nodes n
join public.pnodes p using (pnode_id)
join public.substations s on s.substation_id = p.substation_id
where n.role <> 'aggregate'
"""


def query(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(sql)
    return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def located_nodes(conn) -> pd.DataFrame:
    """GNN nodes with their coordinates, OSM filling those the topology left empty (`is_osm`)."""
    nodes = query(conn, NODES)
    osm = pd.read_csv(OSM_LOCATIONS)
    osm["tx_zone"] = osm["tx_zone"].fillna("")
    nodes = nodes.merge(osm[["code", "tx_zone", "lat", "lon"]].rename(columns={"lat": "osm_lat", "lon": "osm_lon"}), on=["code", "tx_zone"], how="left")
    nodes["is_osm"] = (nodes["lat"].isna() | nodes["topo_source"].eq("osm")) & nodes["osm_lat"].notna()
    nodes.loc[nodes["is_osm"], "lat"] = nodes["osm_lat"]
    nodes.loc[nodes["is_osm"], "lon"] = nodes["osm_lon"]
    return nodes.drop(columns=["osm_lat", "osm_lon"])


def hifld_pairs(conn, nodes: pd.DataFrame) -> pd.DataFrame:
    """Node pairs (sub_a < sub_b) within MAX_HOPS on the HIFLD line graph that include a newly located station."""
    endpoints = query(conn, "select node_id, lat, lon from raw_lmp.transmission_nodes")
    lines = query(conn, "select node1_id, node2_id from raw_lmp.transmission_edges")
    subs = nodes.dropna(subset=["lat"]).drop_duplicates("sub_key")
    dist, idx = BallTree(np.radians(endpoints[["lat", "lon"]].to_numpy()), metric="haversine").query(np.radians(subs[["lat", "lon"]].to_numpy()), k=1)
    subs = subs.assign(endpoint=endpoints["node_id"].to_numpy()[idx[:, 0]])[dist[:, 0] * 6371 <= SNAP_KM]
    at_endpoint = subs.groupby("endpoint")["sub_key"].agg(list)
    neighbors = defaultdict(set)
    for a, b in lines.itertuples(index=False, name=None):
        neighbors[a].add(b)
        neighbors[b].add(a)
    new = set(subs.loc[subs["is_osm"], "sub_key"])
    pairs = set()
    for start in at_endpoint.index:
        seen, frontier = {start}, [start]
        for _ in range(MAX_HOPS):
            next_frontier = []
            for endpoint in frontier:
                for other in neighbors[endpoint] - seen:
                    seen.add(other)
                    if other not in at_endpoint.index:
                        next_frontier.append(other)
                        continue
                    for a in at_endpoint[start]:
                        for b in at_endpoint[other]:
                            if a != b and (a in new or b in new):
                                pairs.add(tuple(sorted((a, b))))
            frontier = next_frontier
    logger.info(f"{subs['is_osm'].sum()} OSM-located stations within {SNAP_KM} km of a HIFLD line endpoint")
    return pd.DataFrame(sorted(pairs), columns=["sub_a", "sub_b"])


def new_edges(conn, nodes: pd.DataFrame) -> pd.DataFrame:
    pairs = hifld_pairs(conn, nodes)
    existing = query(conn, f"select sub_a, sub_b from raw_lmp.pnode_topology_edges where source <> '{SOURCE}'")
    existing = set(map(tuple, np.sort(existing.to_numpy(), axis=1)))
    return pairs[[pair not in existing for pair in pairs.itertuples(index=False, name=None)]]


def main() -> None:
    conn = get_connection()
    conn.autocommit = False
    nodes = located_nodes(conn)
    edges = new_edges(conn, nodes)
    cur = conn.cursor()
    psycopg2.extras.execute_values(
        cur,
        "update raw_lmp.pnode_topology_nodes n set lat = v.lat, lon = v.lon, topo_source = 'osm' from (values %s) as v (pnode_id, lat, lon) where n.pnode_id = v.pnode_id",
        list(nodes.loc[nodes["is_osm"], ["pnode_id", "lat", "lon"]].itertuples(index=False, name=None)),
    )
    cur.execute("delete from raw_lmp.pnode_topology_edges where source = %s", (SOURCE,))
    psycopg2.extras.execute_values(
        cur,
        "insert into raw_lmp.pnode_topology_edges (sub_a, sub_b, source, built_at) values %s",
        [(a, b, SOURCE) for a, b in edges.itertuples(index=False, name=None)],
        template="(%s, %s, %s, now())",
    )
    conn.commit()
    logger.success(f"{nodes['is_osm'].sum()} nodes located from OSM, {len(edges)} '{SOURCE}' edges")
    conn.close()


if __name__ == "__main__":
    main()
