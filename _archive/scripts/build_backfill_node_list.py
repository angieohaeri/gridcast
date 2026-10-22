"""Select the pnodes to backfill hourly LMPs for (GNN node set).

- aggregate: every active ZONE / HUB / INTERFACE pnode (economic reference prices).
- attached: one representative BUS per substation in pnode_topology_attach (has graph edges).
- fill_in: in zones the topology barely covers (ComEd 0%, EKPC/Dayton 1%, DEOK 4%), one
  representative BUS per substation with max voltage >= FILL_MIN_KV - price history now,
  edges later (learned adjacency or a ComEd name fix) without re-pulling.

Representative bus: highest voltage, LOAD before GEN (gen-bus prices can carry unit-local
constraints), then lowest pnode_id.

Pull estimate: Data Miner 2 only accepts pnode_id filters on non-archived data (last 731 days
for rt_hrl_lmps / da_hrl_lmps); ~150 ids fit in one request URL; 50,000 rows per page; 6
requests/min on the free tier.

Inputs: data/external/pjm_pnodes.parquet, data/interim/pnode_topology_attach.parquet
Output: data/interim/lmp_backfill_nodes.parquet
"""

import math

from loguru import logger
import pandas as pd

from gridcast.config import EXTERNAL_DATA_DIR, INTERIM_DATA_DIR, setup_logging

setup_logging()

FILL_ZONES = ["COMED", "EKPC", "DAY", "DEOK"]
FILL_MIN_KV = 138
IDS_PER_REQUEST = 150
ROWS_PER_PAGE = 50_000
REQUESTS_PER_MIN = 6
WINDOW_DAYS = 731

pn = pd.read_parquet(EXTERNAL_DATA_DIR / "pjm_pnodes.parquet")
att = pd.read_parquet(INTERIM_DATA_DIR / "pnode_topology_attach.parquet")

agg = pn[pn["pnode_subtype"].isin(["ZONE", "HUB", "INTERFACE"])].assign(role="aggregate")

bus = pn[pn["pnode_type"] == "BUS"].copy()
bus["substation"] = bus["pnode_name"].str[:8].str.strip().str.upper()
bus["kv"] = bus["voltage_level"].str.extract(r"([\d.]+)")[0].astype(float)
bus["is_gen"] = bus["pnode_subtype"] == "GEN"


def representative(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["kv", "is_gen", "pnode_id"], ascending=[False, True, True]).drop_duplicates(["zone", "substation"])


attached = representative(bus[bus["pnode_id"].isin(att["pnode_id"])]).assign(role="attached")

weak = bus[bus["zone"].isin(FILL_ZONES)]
weak_max = weak.groupby(["zone", "substation"])["kv"].transform("max")
taken = set(zip(attached["zone"], attached["substation"]))
fill = weak[(weak_max >= FILL_MIN_KV) & [k not in taken for k in zip(weak["zone"], weak["substation"])]]
fill = representative(fill).assign(role="fill_in")

nodes = pd.concat([agg, attached, fill])[
    ["pnode_id", "pnode_name", "pnode_type", "pnode_subtype", "zone", "voltage_level", "substation", "kv", "role"]
]
assert nodes["pnode_id"].is_unique
logger.info(f"{len(nodes)} pnodes: {nodes['role'].value_counts().to_dict()}")
logger.info(f"fill-ins by zone: {fill['zone'].value_counts().to_dict()}")
logger.info(f"BUS nodes by zone: {nodes[nodes['role'] != 'aggregate']['zone'].value_counts().to_dict()}")

hours = WINDOW_DAYS * 24
chunks = [min(IDS_PER_REQUEST, len(nodes) - i) for i in range(0, len(nodes), IDS_PER_REQUEST)]
requests = sum(math.ceil(c * hours / ROWS_PER_PAGE) for c in chunks)
logger.info(
    f"pull estimate per market: {len(nodes) * hours / 1e6:.1f}M rows, {len(chunks)} id-chunks, "
    f"{requests} requests, ~{requests / REQUESTS_PER_MIN / 60:.1f} h at {REQUESTS_PER_MIN}/min"
)

nodes.to_parquet(INTERIM_DATA_DIR / "lmp_backfill_nodes.parquet")
logger.success(f"saved {INTERIM_DATA_DIR / 'lmp_backfill_nodes.parquet'}")
