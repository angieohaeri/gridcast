"""Physical GNN edges from transmission lines whose two ends are linked to stations.

Lines come from the contingency list (data/interim/contingency_lines.csv, written by
network_reference.build) and from facility names (public.facilities). Sources, in the order a
station pair keeps them:
- 'line_' + method: a contingency-list line with a resolved far end (see build.read_lines)
- 'line_same_circuit': stations on the same numbered tapped line (ComEd, see same_circuit_pairs)
- 'facility_line': a facility name with both ends linked
- 'line_via_1', 'line_via_2': GNN stations joined by contingency-list lines through 1 or 2 stations
  that have no GNN node
Each station with a GNN node is represented by its highest-voltage node (then lowest pnode_id). A
pair is skipped when the map-based edges already have it. Replaces earlier rows from these sources,
so reruns are safe. Run after network_reference.build and constraint_nodes.
"""

from collections import defaultdict
import itertools
import re

from loguru import logger
from network_reference.build import LINES
import numpy as np
import pandas as pd
import psycopg2.extras

from gridcast.config import get_connection

MAX_VIA = 2
SOURCES = ["line_single_outage", "line_only_candidate", "line_id_letters", "line_same_circuit", "facility_line"] + [f"line_via_{n}" for n in range(1, MAX_VIA + 1)]
NUMBERED_LINE = re.compile(r"^(\d{4,6})[ _]\d+$")

STATION_NODES = """
select distinct on (s.station_id) s.station_id, n.sub_key
from raw_lmp.pnode_topology_nodes n
join public.pnodes p using (pnode_id)
join public.substations s on s.substation_id = p.substation_id
where n.role <> 'aggregate'
order by s.station_id, n.voltage_kv desc nulls last, n.pnode_id
"""

STATIONS = "select substation_id, station_id from public.substations"

FACILITY_LINES = """
select a.station_id as station_a, b.station_id as station_b, 'facility_line' as source
from public.facilities f
join public.substations a on a.substation_id = f.end1_substation_id
join public.substations b on b.substation_id = f.end2_substation_id
where f.category = 'line' and a.station_id <> b.station_id
"""


def query(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(sql)
    return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def same_circuit_pairs(lines: pd.DataFrame) -> pd.DataFrame:
    """ComEd numbers a tapped line and its segments ('13806 1', '13806 2'), so a segment's ID does not name its far end.
    Every station with a segment of the same numbered line in one outage is on that circuit."""
    numbered = lines.assign(number=lines["equipment_id"].str.extract(NUMBERED_LINE)[0]).dropna(subset=["number", "station_a"])
    rows = [
        pair
        for stations in numbered.groupby(["contingency_name", "number"])["station_a"].agg(lambda s: sorted(set(s)))
        for pair in itertools.combinations(stations, 2)
    ]
    return pd.DataFrame(rows, columns=["station_a", "station_b"]).assign(source="line_same_circuit")


def via_pairs(direct: pd.DataFrame, gnn: set) -> pd.DataFrame:
    """GNN station pairs joined by lines through 1 to MAX_VIA stations that have no GNN node (shortest path only)."""
    neighbors = defaultdict(set)
    for a, b in zip(direct["station_a"], direct["station_b"]):
        neighbors[a].add(b)
        neighbors[b].add(a)
    rows = []
    for start in gnn & neighbors.keys():
        seen, frontier = {start}, [start]
        for via in range(MAX_VIA + 1):
            next_frontier = []
            for station in frontier:
                for other in neighbors[station] - seen:
                    seen.add(other)
                    if other not in gnn:
                        next_frontier.append(other)
                    elif via > 0:
                        rows.append((start, other, f"line_via_{via}"))
            frontier = next_frontier
    return pd.DataFrame(rows, columns=["station_a", "station_b", "source"])


def line_pairs(conn) -> pd.DataFrame:
    """GNN node pairs (sub_a < sub_b) from every source, each pair once with its first source in SOURCES."""
    sub_key = query(conn, STATION_NODES).set_index("station_id")["sub_key"]
    station = query(conn, STATIONS).set_index("substation_id")["station_id"]
    contingency_lines = pd.read_csv(LINES)
    contingency_lines["station_a"] = contingency_lines["near_substation_id"].map(station)
    contingency_lines["station_b"] = contingency_lines["far_substation_id"].map(station)
    resolved = contingency_lines[contingency_lines["method"] != "unresolved"]
    direct = pd.concat([
        resolved[["station_a", "station_b"]].assign(source="line_" + resolved["method"]),
        same_circuit_pairs(contingency_lines),
    ]).dropna()
    direct = direct[direct["station_a"] != direct["station_b"]]
    lines = pd.concat([direct, query(conn, FACILITY_LINES), via_pairs(direct, set(sub_key.index))])
    pairs = pd.DataFrame({"sub_a": lines["station_a"].map(sub_key), "sub_b": lines["station_b"].map(sub_key), "source": lines["source"]}).dropna()
    pairs[["sub_a", "sub_b"]] = np.sort(pairs[["sub_a", "sub_b"]].to_numpy(), axis=1)
    return pairs.sort_values("source", key=lambda s: s.map(SOURCES.index)).drop_duplicates(["sub_a", "sub_b"])


def main() -> None:
    conn = get_connection()
    pairs = line_pairs(conn)
    existing = query(conn, f"select sub_a, sub_b from raw_lmp.pnode_topology_edges where source not in ({', '.join(repr(s) for s in SOURCES)})")
    existing = set(map(tuple, np.sort(existing.to_numpy(), axis=1)))
    new = pairs[[pair not in existing for pair in pairs[["sub_a", "sub_b"]].itertuples(index=False, name=None)]]
    cur = conn.cursor()
    cur.execute("delete from raw_lmp.pnode_topology_edges where source = any(%s)", (SOURCES,))
    psycopg2.extras.execute_values(
        cur,
        "insert into raw_lmp.pnode_topology_edges (sub_a, sub_b, source, built_at) values %s",
        list(new.itertuples(index=False, name=None)),
        template="(%s, %s, %s, now())",
    )
    conn.commit()
    logger.success(f"{len(pairs)} node pairs, {len(new)} not already map edges: {new['source'].value_counts().to_dict()}")
    conn.close()


if __name__ == "__main__":
    main()
