from dataclasses import dataclass
from loguru import logger
import numpy as np
import pandas as pd
from gridcast.config import get_connection, setup_logging
from gridcast.features import cyclical_features, holiday_flag, weekend_flag

setup_logging()

FEATURES = [
    "da_lmp",
    "da_congestion_price",
    "da_marginal_loss_price",
    "spread_last_verified_day",
    "congestion_spread_last_verified_day",
    "spread_mean_7d",
    "spread_std_7d",
    "spike_share_7d",
    "congestion_spread_mean_7d",
]
SYSTEM_FEATURES = ["hour_sin", "hour_cos", "month_sin", "month_cos", "is_weekend", "is_holiday"]
TARGET = "spread"
CHUNK_DAYS = 30


@dataclass(frozen=True)
class Window:
    """Dense hours x nodes arrays. x and y are NaN where a value is missing: y is NaN for
    hours not yet verified, and everything is NaN for a node before it starts trading
    (exists is False there)."""

    times: pd.DatetimeIndex
    pnode_ids: pd.Index
    x: np.ndarray  # [time, node, feature]
    system: np.ndarray  # [time, system feature], identical for every node
    areas: pd.Index  # PJM load forecast areas
    area_forecast: np.ndarray  # [time, area] forecast MW issued by the cutoff
    node_area: np.ndarray  # [node] position in areas of each node's forecast area
    y: np.ndarray  # [time, node]
    exists: np.ndarray  # [time, node] bool


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    cur = get_connection().cursor()
    cur.execute(sql, params)
    return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])


def load_nodes() -> pd.DataFrame:
    nodes = query("SELECT pnode_id, role, zone, sub_key FROM raw_lmp.pnode_topology_nodes;")
    return nodes.set_index("pnode_id").sort_index()


def load_system(times: pd.DatetimeIndex) -> np.ndarray:
    """Calendar features (ET) for each hour."""
    system = holiday_flag(weekend_flag(cyclical_features(pd.DataFrame({"time": times}))))
    return system[SYSTEM_FEATURES].astype("float32").to_numpy()


def load_area_forecast(times: pd.DatetimeIndex) -> tuple[pd.Index, np.ndarray]:
    forecast = query("SELECT time, forecast_area, forecast_load_mw FROM analytics_lmp.lmp_load_forecast_features;")
    forecast["time"] = pd.to_datetime(forecast["time"], utc=True)
    wide = forecast.pivot(index="time", columns="forecast_area", values="forecast_load_mw")
    wide = wide.reindex(times).astype("float32")
    return wide.columns, wide.to_numpy()


def load_node_area(areas: pd.Index, pnode_ids: pd.Index) -> np.ndarray:
    node_areas = query("SELECT pnode_id, forecast_area FROM analytics_lmp.lmp_node_areas;")
    by_node = node_areas.set_index("pnode_id")["forecast_area"].reindex(pnode_ids)
    return areas.get_indexer(by_node)


def load_window(start: pd.Timestamp, end: pd.Timestamp, pnode_ids: pd.Index) -> Window:
    """lmp_baseline_features for [start, end), scattered onto a fixed hours x nodes grid so
    node i here is node i in the edge index."""
    times = pd.date_range(start, end, freq="h", inclusive="left")
    x = np.full((len(times), len(pnode_ids), len(FEATURES)), np.nan, dtype="float32")
    y = np.full((len(times), len(pnode_ids)), np.nan, dtype="float32")
    exists = np.zeros((len(times), len(pnode_ids)), dtype=bool)

    columns = ", ".join(FEATURES + [TARGET])
    for chunk_start in pd.date_range(start, end, freq=f"{CHUNK_DAYS}D", inclusive="left"):
        chunk_end = min(chunk_start + pd.Timedelta(days=CHUNK_DAYS), end)
        chunk = query(
            f"SELECT time, pnode_id, {columns} FROM analytics_lmp.lmp_baseline_features "
            "WHERE time >= %s AND time < %s;",
            (chunk_start, chunk_end),
        )
        t = times.get_indexer(pd.to_datetime(chunk["time"], utc=True))
        n = pnode_ids.get_indexer(chunk["pnode_id"])
        x[t, n] = chunk[FEATURES].astype("float32").to_numpy()
        y[t, n] = chunk[TARGET].astype("float32").to_numpy()
        exists[t, n] = True

    logger.info(f"{len(times)} hours x {len(pnode_ids)} nodes, {exists.mean():.1%} populated")
    areas, area_forecast = load_area_forecast(times)
    node_area = load_node_area(areas, pnode_ids)
    return Window(times, pnode_ids, x, load_system(times), areas, area_forecast, node_area, y, exists)
