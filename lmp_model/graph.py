from loguru import logger
import numpy as np
import pandas as pd

from lmp_model.config import PROCESSED_DATA_DIR
from lmp_model.dataset import CHUNK_DAYS, load_nodes, query

NEIGHBORS = 3
MIN_OVERLAP_HOURS = 500
SPREAD_CLIP = 300


def geographic_edges(nodes: pd.DataFrame) -> pd.DataFrame:
    """Substation edges from pnode_topology_edges (map-based and facility lines), mapped to each substation's pnode."""
    edges = query("SELECT sub_a, sub_b FROM raw_lmp.pnode_topology_edges;")
    members = nodes[nodes["role"] != "aggregate"]
    pnode_by_sub = pd.Series(members.index, index=members["sub_key"])
    return pd.DataFrame(
        {
            "src": edges["sub_a"].map(pnode_by_sub),
            "dst": edges["sub_b"].map(pnode_by_sub),
            "kind": "geographic",
        }
    )


def _spread_chunk(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    spread = query(
        "SELECT time, pnode_id, spread FROM analytics_lmp.spread WHERE time >= %s AND time < %s;",
        (start, end),
    )
    return spread.pivot(index="time", columns="pnode_id", values="spread").astype("float32")


def load_spread_wide(train_start: pd.Timestamp, train_end: pd.Timestamp) -> pd.DataFrame:
    """Hours x pnodes spread with the hourly cross-node median removed, so what is left is
    node-specific (congestion) rather than the system-wide component every node shares."""
    starts = pd.date_range(train_start, train_end, freq=f"{CHUNK_DAYS}D", inclusive="left")
    wide = pd.concat(
        [_spread_chunk(s, min(s + pd.Timedelta(days=CHUNK_DAYS), train_end)) for s in starts]
    )
    return wide.sub(wide.median(axis=1), axis=0).clip(-SPREAD_CLIP, SPREAD_CLIP)


def correlation_edges(nodes: pd.DataFrame, corr: pd.DataFrame, linked: pd.Index) -> pd.DataFrame:
    """Links every node without a geographic edge to its most correlated real nodes in its
    zone (any zone when its zone has no real nodes, as for hubs and interfaces)."""
    corr = corr.reindex(index=nodes.index, columns=nodes.index)
    members = nodes[nodes["role"] != "aggregate"]
    rows = []
    for pnode_id in nodes.index.difference(linked):
        in_zone = members[members["zone"] == nodes.at[pnode_id, "zone"]]
        candidates = (in_zone if len(in_zone) else members).index.drop(pnode_id, errors="ignore")
        for neighbor in corr.loc[pnode_id, candidates].nlargest(NEIGHBORS).index:
            rows.append((pnode_id, neighbor, "correlation"))
    return pd.DataFrame(rows, columns=["src", "dst", "kind"])


def build_edges(train_start: pd.Timestamp, train_end: pd.Timestamp) -> pd.DataFrame:
    """Undirected pnode edge list, with correlation edges learned only from [train_start,
    train_end) so the graph structure never sees the evaluation period."""
    nodes = load_nodes()
    corr = load_spread_wide(train_start, train_end).corr(min_periods=MIN_OVERLAP_HOURS)
    geographic = geographic_edges(nodes)
    linked = pd.Index(pd.concat([geographic["src"], geographic["dst"]]).unique())
    edges = pd.concat([geographic, correlation_edges(nodes, corr, linked)], ignore_index=True)
    edges[["src", "dst"]] = np.sort(edges[["src", "dst"]].to_numpy(), axis=1)
    edges = edges.drop_duplicates(["src", "dst"], ignore_index=True)

    isolated = nodes.index.difference(pd.concat([edges["src"], edges["dst"]]))
    logger.info(f"{len(edges)} edges, {len(isolated)} of {len(nodes)} nodes isolated")
    path = PROCESSED_DATA_DIR / f"pnode_edges_{train_end:%Y%m%d}.parquet"
    edges.to_parquet(path, index=False)
    return edges


def edge_index(edges: pd.DataFrame, pnode_ids: pd.Index) -> np.ndarray:
    """[2, 2E] node-position pairs with both directions, for message passing."""
    src = pnode_ids.get_indexer(edges["src"])
    dst = pnode_ids.get_indexer(edges["dst"])
    return np.stack([np.concatenate([src, dst]), np.concatenate([dst, src])])
