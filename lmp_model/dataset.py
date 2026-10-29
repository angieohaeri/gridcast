from dataclasses import dataclass

from loguru import logger
import numpy as np
import pandas as pd

from gridcast.config import get_connection, setup_logging
from gridcast.features import cyclical_features, holiday_flag, weekend_flag
from lmp_model.config import PROCESSED_DATA_DIR

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
ZONE_FEATURES = [
    "temperature", "precipitation", "wind_speed", "cloud_cover", "hdd", "cdd",
    "commitment_count", "commitment_mw", "constraint_commitment_count", "constraint_commitment_mw",
    "border_interchange_prev_day",
]
CALENDAR_FEATURES = ["hour_sin", "hour_cos", "month_sin", "month_cos", "is_weekend", "is_holiday"]
SYSTEM_TABLE_FEATURES = [
    "da_constraint_count", "da_shadow_total", "da_shadow_min",
    "rt_constraint_count_prev_day", "rt_shadow_total_prev_day", "rt_shadow_min_prev_day",
    "rt_constraint_count_mean_7d", "rt_shadow_total_mean_7d",
    "da_constraint_count_prev_day", "da_constraint_hours_prev_day",
    "generation_mw_prev_day", "gas_share_prev_day", "coal_share_prev_day", "nuclear_share_prev_day",
    "wind_share_prev_day", "solar_share_prev_day", "self_scheduled_mw_prev_day", "loss_ratio_prev_day",
    "outage_mw_rto", "outage_mw_west", "outage_mw_other", "gas_cost_per_mmbtu",
    "apsouth_util", "bcpep_util", "bed_bla_util", "central_util", "east_util", "west_util",
    "net_interchange_prev_day",
]
SYSTEM_FEATURES = CALENDAR_FEATURES + SYSTEM_TABLE_FEATURES
TARGET = "spread"
CHUNK_DAYS = 30
CACHE_PATH = PROCESSED_DATA_DIR / "train_npz" / "window.npz"


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
    zones: pd.Index  # PJM zones in zone_features
    zone_features: np.ndarray  # [time, zone, zone feature] as of the cutoff
    node_zone: np.ndarray  # [node] position in zones, -1 for nodes without a zone
    y: np.ndarray  # [time, node]
    exists: np.ndarray  # [time, node] bool


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    cur = get_connection().cursor()
    cur.execute(sql, params)
    return pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])


def load_nodes() -> pd.DataFrame:
    nodes = query("SELECT pnode_id, role, zone, zone_id, sub_key, forecast_area FROM analytics_lmp.nodes;")
    return nodes.set_index("pnode_id").sort_index()


def load_system(times: pd.DatetimeIndex) -> np.ndarray:
    """Calendar features (ET) and system_features for each hour."""
    calendar = holiday_flag(weekend_flag(cyclical_features(pd.DataFrame({"time": times}))))
    table = query(f"SELECT time, {', '.join(SYSTEM_TABLE_FEATURES)} FROM analytics_lmp.system_features;")
    table = table.set_index(pd.to_datetime(table["time"], utc=True))[SYSTEM_TABLE_FEATURES].reindex(times)
    return np.hstack([calendar[CALENDAR_FEATURES].to_numpy(), table.to_numpy()]).astype("float32")


def load_area_forecast(times: pd.DatetimeIndex) -> tuple[pd.Index, np.ndarray]:
    forecast = query("SELECT time, forecast_area, forecast_load_mw FROM analytics_lmp.load_forecast_features;")
    forecast["time"] = pd.to_datetime(forecast["time"], utc=True)
    wide = forecast.pivot(index="time", columns="forecast_area", values="forecast_load_mw")
    wide = wide.reindex(times).astype("float32")
    return wide.columns, wide.to_numpy()


def load_zone_features(times: pd.DatetimeIndex) -> tuple[pd.Index, np.ndarray]:
    rows = query(f"SELECT time, zone, {', '.join(ZONE_FEATURES)} FROM analytics_lmp.zone_features;")
    rows["time"] = pd.to_datetime(rows["time"], utc=True)
    zones = pd.Index(sorted(rows["zone"].unique()))
    grid = rows.set_index(["time", "zone"])[ZONE_FEATURES].astype("float32")
    index = pd.MultiIndex.from_product([times, zones], names=["time", "zone"])
    values = grid.reindex(index).to_numpy().reshape(len(times), len(zones), len(ZONE_FEATURES))
    return zones, values


def load_window(start: pd.Timestamp, end: pd.Timestamp, pnode_ids: pd.Index) -> Window:
    """baseline_features for [start, end), scattered onto a fixed hours x nodes grid so
    node i here is node i in the edge index."""
    times = pd.date_range(start, end, freq="h", inclusive="left")
    x = np.full((len(times), len(pnode_ids), len(FEATURES)), np.nan, dtype="float32")
    y = np.full((len(times), len(pnode_ids)), np.nan, dtype="float32")
    exists = np.zeros((len(times), len(pnode_ids)), dtype=bool)

    columns = ", ".join(FEATURES + [TARGET])
    for chunk_start in pd.date_range(start, end, freq=f"{CHUNK_DAYS}D", inclusive="left"):
        chunk_end = min(chunk_start + pd.Timedelta(days=CHUNK_DAYS), end)
        chunk = query(
            f"SELECT time, pnode_id, {columns} FROM analytics_lmp.baseline_features "
            "WHERE time >= %s AND time < %s;",
            (chunk_start, chunk_end),
        )
        t = times.get_indexer(pd.to_datetime(chunk["time"], utc=True))
        n = pnode_ids.get_indexer(chunk["pnode_id"])
        x[t, n] = chunk[FEATURES].astype("float32").to_numpy()
        y[t, n] = chunk[TARGET].astype("float32").to_numpy()
        exists[t, n] = True

    logger.info(f"{len(times)} hours x {len(pnode_ids)} nodes, {exists.mean():.1%} populated")
    nodes = load_nodes().reindex(pnode_ids)
    areas, area_forecast = load_area_forecast(times)
    zones, zone_features = load_zone_features(times)
    return Window(
        times, pnode_ids, x, load_system(times), areas, area_forecast, areas.get_indexer(nodes["forecast_area"]),
        zones, zone_features, zones.get_indexer(nodes["zone_id"]), y, exists,
    )


def build_cache(start: pd.Timestamp, end: pd.Timestamp, pnode_ids: pd.Index) -> None:
    """Raw, unscaled window for the whole period, written once and sliced per fold."""
    window = load_window(start, end, pnode_ids)
    np.savez_compressed(
        CACHE_PATH,
        times=window.times.asi8, x=window.x, y=window.y, exists=window.exists, system=window.system,
        area_forecast=window.area_forecast, node_area=window.node_area, zone_features=window.zone_features,
        node_zone=window.node_zone, areas=window.areas.to_numpy(dtype=str),
        zones=window.zones.to_numpy(dtype=str),
    )


def load_cache() -> dict[str, np.ndarray]:
    return dict(np.load(CACHE_PATH))
