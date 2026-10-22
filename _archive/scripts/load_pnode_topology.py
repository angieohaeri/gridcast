"""Load the GNN node set and substation edges into raw_lmp as plain reference tables.

Not hypertables - topology has no time axis. Rebuilt wholesale (DROP/recreate) like
transmission_nodes/edges and facilities; built_at records which build is loaded, since PJM's
pnode list changes quarterly.

pnode_topology_nodes: every pnode in lmp_backfill_nodes (aggregate / attached / fill_in), with
zone_id, sub_key (zone_id|substation, the edge key) and, for attached pnodes, coordinates and
which source (gridsfm / hifld / both) attached them. Fill-ins have a sub_key but no edges yet.
pnode_topology_edges: substation pairs within 4 hops in GridSFM and/or HIFLD
(build_pnode_topology.py), keyed by sub_key.

Inputs: data/interim/{lmp_backfill_nodes, pnode_topology_attach, pnode_topology_edges}.parquet
"""

from dotenv import load_dotenv
from loguru import logger
import pandas as pd
import psycopg2.extras

from gridcast.config import INTERIM_DATA_DIR, get_connection, setup_logging

load_dotenv()
setup_logging()

LOCATION_TO_ZONE = {
    "AECO": "AE", "AEP": "AEP", "APS": "AP", "ATSI": "ATSI", "BGE": "BC", "COMED": "CE", "DAY": "DAY",
    "DEOK": "DEOK", "DOM": "DOM", "DPL": "DPL", "DUQ": "DUQ", "EKPC": "EKPC", "JCPL": "JC", "METED": "ME",
    "PECO": "PE", "PEPCO": "PEP", "PPL": "PL", "PENELEC": "PN", "PSEG": "PS", "RECO": "RECO",
}

nodes = pd.read_parquet(INTERIM_DATA_DIR / "lmp_backfill_nodes.parquet")
att = pd.read_parquet(INTERIM_DATA_DIR / "pnode_topology_attach.parquet")
edges = pd.read_parquet(INTERIM_DATA_DIR / "pnode_topology_edges.parquet")

is_zone_agg = nodes["pnode_subtype"] == "ZONE"
nodes["zone"] = nodes["zone"].where(~is_zone_agg, nodes["pnode_name"])
nodes["zone_id"] = nodes["zone"].map(LOCATION_TO_ZONE)
nodes["sub_key"] = (nodes["zone_id"] + "|" + nodes["substation"]).where(nodes["role"] != "aggregate")
nodes = nodes.merge(
    att[["pnode_id", "lat", "lon", "source", "conflict"]].rename(columns={"source": "topo_source"}), how="left", on="pnode_id"
)
nodes = nodes[
    ["pnode_id", "pnode_name", "pnode_subtype", "role", "zone", "zone_id", "substation", "sub_key", "kv", "lat", "lon", "topo_source", "conflict"]
]

missing = set(edges["sub_a"]) | set(edges["sub_b"])
missing -= set(nodes["sub_key"].dropna())
assert not missing, f"edge endpoints without a node: {sorted(missing)[:5]}"

conn = get_connection()
cur = conn.cursor()
cur.execute("drop table if exists raw_lmp.pnode_topology_edges;")
cur.execute("drop table if exists raw_lmp.pnode_topology_nodes;")
cur.execute("""
    create table raw_lmp.pnode_topology_nodes (
        pnode_id bigint primary key,
        pnode_name text not null,
        pnode_subtype text not null,
        role text not null,
        zone text,
        zone_id text,
        substation text,
        sub_key text,
        voltage_kv numeric,
        lat double precision,
        lon double precision,
        topo_source text,
        conflict boolean,
        built_at timestamptz not null default now()
    );
""")
cur.execute("create index on raw_lmp.pnode_topology_nodes (sub_key);")
cur.execute("""
    create table raw_lmp.pnode_topology_edges (
        sub_a text not null,
        sub_b text not null,
        source text not null,
        built_at timestamptz not null default now(),
        primary key (sub_a, sub_b)
    );
""")
cur.execute("create index on raw_lmp.pnode_topology_edges (sub_b);")

psycopg2.extras.execute_values(
    cur,
    """insert into raw_lmp.pnode_topology_nodes (pnode_id, pnode_name, pnode_subtype, role, zone, zone_id, substation,
       sub_key, voltage_kv, lat, lon, topo_source, conflict) values %s""",
    [tuple(None if pd.isna(v) else v for v in row) for row in nodes.itertuples(index=False, name=None)],
)
psycopg2.extras.execute_values(
    cur,
    "insert into raw_lmp.pnode_topology_edges (sub_a, sub_b, source) values %s",
    list(edges[["sub_a", "sub_b", "source"]].itertuples(index=False, name=None)),
)
logger.success(f"loaded {len(nodes)} nodes ({nodes['role'].value_counts().to_dict()}), {len(edges)} edges into raw_lmp")
conn.close()
