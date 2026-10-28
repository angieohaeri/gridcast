"""Physical GNN edges from transmission lines whose two ends are linked to stations (public.facilities).

Each station with a GNN node is represented by its highest-voltage node (then lowest pnode_id).
A line between two such stations becomes a raw_lmp.pnode_topology_edges row with source
'facility_line', unless the map-based edges already have that pair. Replaces earlier
'facility_line' rows, so reruns are safe. Run after network_reference.build and constraint_nodes.
"""

from loguru import logger
import numpy as np
import pandas as pd
import psycopg2.extras

from gridcast.config import get_connection

SOURCE = "facility_line"

STATION_NODES = """
select distinct on (s.station_id) s.station_id, n.sub_key
from raw_lmp.pnode_topology_nodes n
join public.pnodes p using (pnode_id)
join public.substations s on s.substation_id = p.substation_id
where n.role <> 'aggregate'
order by s.station_id, n.voltage_kv desc nulls last, n.pnode_id
"""

LINES = """
select a.station_id as station_a, b.station_id as station_b
from public.facilities f
join public.substations a on a.substation_id = f.end1_substation_id
join public.substations b on b.substation_id = f.end2_substation_id
where f.category = 'line' and a.station_id <> b.station_id
"""


def query(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(sql)
    return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def main() -> None:
    conn = get_connection()
    sub_key = query(conn, STATION_NODES).set_index("station_id")["sub_key"]
    lines = query(conn, LINES)
    pairs = pd.DataFrame({"sub_a": lines["station_a"].map(sub_key), "sub_b": lines["station_b"].map(sub_key)}).dropna()
    pairs[["sub_a", "sub_b"]] = np.sort(pairs[["sub_a", "sub_b"]].to_numpy(), axis=1)
    pairs = pairs.drop_duplicates()
    existing = query(conn, f"select sub_a, sub_b from raw_lmp.pnode_topology_edges where source <> '{SOURCE}'")
    existing = set(map(tuple, np.sort(existing.to_numpy(), axis=1)))
    new = pairs[[pair not in existing for pair in pairs.itertuples(index=False, name=None)]]
    cur = conn.cursor()
    cur.execute(f"delete from raw_lmp.pnode_topology_edges where source = '{SOURCE}'")
    psycopg2.extras.execute_values(
        cur,
        "insert into raw_lmp.pnode_topology_edges (sub_a, sub_b, source, built_at) values %s",
        [(a, b, SOURCE) for a, b in new.itertuples(index=False, name=None)],
        template="(%s, %s, %s, now())",
    )
    conn.commit()
    logger.success(f"{len(lines)} linked lines -> {len(pairs)} node pairs, {len(new)} not already map edges, written as '{SOURCE}'")
    conn.close()


if __name__ == "__main__":
    main()
