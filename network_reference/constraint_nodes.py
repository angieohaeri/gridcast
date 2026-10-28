"""Add GNN nodes (role 'constraint') for heavily constrained facilities that touch no GNN node.

For the TOP_FACILITIES facilities by RT shadow-price weight whose stations have pnodes but none in
the GNN, pick one station per facility (the end with the higher-voltage bus) and one pnode per
station: highest voltage, LOAD before GEN, then lowest pnode_id (the fill-in rule in
_archive/scripts/build_backfill_node_list.py). Replaces earlier 'constraint' rows, so reruns are
safe. Run after network_reference.build; prices come from network_reference.backfill_node_prices.
"""

from loguru import logger
from network_reference.build import clean, read_pnode_feed
import pandas as pd
import psycopg2.extras

from gridcast.config import PROJ_ROOT, get_connection

TOP_FACILITIES = 100
OUTPUT = PROJ_ROOT / "network_reference" / "constraint_nodes.csv"

UNCOVERED_FACILITIES = """
with ends as (
    select f.facility_id, f.rt_weight_365d as w, s.station_id
    from public.facilities f
    join public.substations s on s.substation_id in (f.end1_substation_id, f.end2_substation_id)
    where f.category not in ('external', 'interface')
),
gnn_stations as (
    select distinct m.station_id from public.substations m join public.pnodes p using (substation_id)
    where p.pnode_id in (select pnode_id from raw_lmp.pnode_topology_nodes where role <> 'constraint')
),
priced_stations as (
    select distinct m.station_id from public.substations m join public.pnodes p using (substation_id)
)
select facility_id, max(w) as w,
       array_agg(distinct station_id) filter (where station_id in (select station_id from priced_stations)) as stations
from ends
group by facility_id
having not bool_or(station_id in (select station_id from gnn_stations))
   and bool_or(station_id in (select station_id from priced_stations))
order by w desc nulls last
limit %s
"""

STATION_BUSES = """
select m.station_id, p.pnode_id, p.tx_zone as zone, s.code as substation, p.voltage_kv
from public.substations m
join public.pnodes p using (substation_id)
join public.substations s on s.substation_id = p.substation_id
where m.station_id = any(%s) and p.pnode_type = 'BUS'
"""


def query(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(sql, params)
    return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])


def pick_nodes(conn) -> pd.DataFrame:
    facilities = query(conn, UNCOVERED_FACILITIES, (TOP_FACILITIES,))
    station_ids = sorted({s for ss in facilities["stations"] for s in ss})
    feed = read_pnode_feed()[["pnode_id", "pnode_name", "pnode_subtype"]]
    buses = query(conn, STATION_BUSES, (station_ids,)).merge(feed, on="pnode_id")
    buses["voltage_kv"] = buses["voltage_kv"].astype(float)
    buses["type_rank"] = buses["pnode_subtype"].map({"EHV": 0, "LOAD": 1, "GEN": 2}).fillna(3)
    best = buses.sort_values(["station_id", "voltage_kv", "type_rank", "pnode_id"], ascending=[True, False, True, True])
    best = best.drop_duplicates("station_id").set_index("station_id")
    chosen = []
    for stations in facilities["stations"]:
        if not set(stations) & set(chosen):
            chosen.append(max((s for s in stations if s in best.index), key=lambda s: best.at[s, "voltage_kv"]))
    logger.info(f"top {len(facilities)} uncovered facilities -> {len(chosen)} stations")
    return best.loc[chosen].reset_index()


def main() -> None:
    conn = get_connection()
    nodes = pick_nodes(conn)
    zone_ids = query(conn, "select distinct zone, zone_id from raw_lmp.pnode_topology_nodes where zone_id is not null").set_index("zone")["zone_id"]
    nodes["zone_id"] = nodes["zone"].map(zone_ids)
    nodes["sub_key"] = nodes["zone_id"].fillna(nodes["zone"]) + "|" + nodes["substation"]
    nodes["role"] = "constraint"
    nodes.to_csv(OUTPUT, index=False)
    columns = ["pnode_id", "pnode_name", "pnode_subtype", "role", "zone", "zone_id", "substation", "sub_key", "voltage_kv"]
    cur = conn.cursor()
    cur.execute("delete from raw_lmp.pnode_topology_nodes where role = 'constraint'")
    rows = [tuple(clean(v) for v in row) for row in nodes[columns].itertuples(index=False, name=None)]
    psycopg2.extras.execute_values(cur, f"insert into raw_lmp.pnode_topology_nodes ({', '.join(columns)}, built_at) values %s", rows, template=f"({', '.join(['%s'] * len(columns))}, now())")
    conn.commit()
    logger.success(f"raw_lmp.pnode_topology_nodes: {len(rows)} constraint nodes ({nodes['zone'].value_counts().to_dict()})")
    conn.close()


if __name__ == "__main__":
    main()
