"""Per-node exposure to transmission constraints. A node's congestion price is a weighted sum
of the binding constraints' shadow prices; the weights (shift factors) are not published, so
they are learned per fold by ridge regression of each node's verified RT congestion price on the
hourly RT shadow prices of the top monitored facilities, using that fold's training hours only.
The weights are then applied to the target hour's DA shadow prices and to the RT shadow prices
at the same hour on the last day available at the 14:30 ET cutoff."""

import numpy as np
import pandas as pd
import yaml

from lmp_model.config import PROCESSED_DATA_DIR, PROJ_ROOT
from lmp_model.dataset import CHUNK_DAYS, query

EXPOSURE_FEATURES = ["da_exposure", "rt_exposure_prev_day"]
TOP_FACILITIES = 100
RIDGE_ALPHA = 0.01
CACHE_PATH = PROCESSED_DATA_DIR / "train_npz" / "exposure.npz"

_availability = yaml.safe_load((PROJ_ROOT / "src" / "dbt" / "dbt_project.yml").read_text())["vars"]["feature_availability_days"]
RT_LAG_HOURS = (1 - _availability["marginal_value_rt"]) * 24


def load_shadow(view: str, value: str, times: pd.DatetimeIndex, facilities: pd.Index | None = None) -> pd.DataFrame:
    """Hourly shadow price per monitored facility (summed over contingencies), hours x facilities.
    0 where a facility did not bind; NaN after the last loaded ET day, like system_features."""
    rows = query(
        f"SELECT time, monitored_facility, {value} AS shadow_price FROM analytics_lmp.{view} "
        "WHERE time >= %s AND time < %s GROUP BY 1, 2;",
        (times[0], times[-1] + pd.Timedelta(hours=1)),
    )
    rows["time"] = pd.to_datetime(rows["time"], utc=True)
    wide = rows.pivot(index="time", columns="monitored_facility", values="shadow_price").astype("float32")
    wide = wide.reindex(index=times, columns=facilities if facilities is not None else sorted(wide.columns)).fillna(0)
    last = query(f"SELECT max(time) AS time FROM analytics_lmp.{view};")["time"].iloc[0]
    through = (pd.Timestamp(last) - pd.Timedelta(hours=1)).tz_convert("America/New_York").normalize() + pd.Timedelta(days=1)
    wide[times > through] = np.nan
    return wide


def load_rt_congestion(times: pd.DatetimeIndex, pnode_ids: pd.Index) -> np.ndarray:
    """Verified RT congestion price, [time, node], NaN where not verified."""
    out = np.full((len(times), len(pnode_ids)), np.nan, dtype="float32")
    end = times[-1] + pd.Timedelta(hours=1)
    for chunk_start in pd.date_range(times[0], end, freq=f"{CHUNK_DAYS}D", inclusive="left"):
        chunk = query(
            "SELECT time, pnode_id, congestion_price FROM raw_lmp.lmp_rt "
            "WHERE is_verified AND time >= %s AND time < %s;",
            (chunk_start, min(chunk_start + pd.Timedelta(days=CHUNK_DAYS), end)),
        )
        t = times.get_indexer(pd.to_datetime(chunk["time"], utc=True))
        n = pnode_ids.get_indexer(chunk["pnode_id"])
        keep = (t >= 0) & (n >= 0)
        out[t[keep], n[keep]] = chunk["congestion_price"].astype("float32").to_numpy()[keep]
    return out


def build_cache(times: pd.DatetimeIndex, pnode_ids: pd.Index) -> None:
    """Fold-independent inputs on the same hours and nodes as the window cache. Only facilities
    that ever bound in RT can get weights, so DA is kept to those."""
    rt = load_shadow("stg_marginal_value_rt", "sum(shadow_price_sum) / 12.0", times)
    da = load_shadow("stg_marginal_value_da", "sum(shadow_price)", times, rt.columns)
    np.savez_compressed(
        CACHE_PATH,
        times=times.asi8, facilities=rt.columns.to_numpy(dtype=str), rt_shadow=rt.to_numpy(),
        da_shadow=da.to_numpy(), rt_congestion=load_rt_congestion(times, pnode_ids),
    )


def load_cache() -> dict[str, np.ndarray]:
    return dict(np.load(CACHE_PATH))


def fit_weights(cache: dict[str, np.ndarray], train: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Top facilities by RT shadow-price weight over the training hours, and ridge weights
    [facility, node] from their RT shadow prices to each node's RT congestion price. Nodes with
    the same missing hours are solved together, on their own verified hours only."""
    shadow = cache["rt_shadow"][train]
    target = cache["rt_congestion"][train]
    rows = ~np.isnan(shadow).any(axis=1)
    shadow, target = shadow[rows], target[rows]
    top = np.argsort(np.abs(shadow).sum(axis=0))[::-1][:TOP_FACILITIES]
    x = shadow[:, top].astype("float64")

    weights = np.zeros((len(top), target.shape[1]))
    observed = ~np.isnan(target)
    patterns, node_pattern = np.unique(np.packbits(observed, axis=0), axis=1, return_inverse=True)
    for p in range(patterns.shape[1]):
        nodes = np.flatnonzero(node_pattern.ravel() == p)
        hours = observed[:, nodes[0]]
        if hours.sum() < TOP_FACILITIES:
            continue
        xc = x[hours] - x[hours].mean(axis=0)
        yc = target[hours][:, nodes] - target[hours][:, nodes].mean(axis=0)
        gram = xc.T @ xc
        alpha = RIDGE_ALPHA * np.trace(gram) / len(top)
        weights[:, nodes] = np.linalg.solve(gram + alpha * np.eye(len(top)), xc.T @ yc)
    return top, weights.astype("float32")


def exposure_features(cache: dict[str, np.ndarray], top: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """[time, node, EXPOSURE_FEATURES]: the weights times the target hour's DA shadow prices, and
    times the RT shadow prices RT_LAG_HOURS earlier (NaN for the first RT_LAG_HOURS hours)."""
    da = cache["da_shadow"][:, top] @ weights
    rt = np.full_like(da, np.nan)
    rt[RT_LAG_HOURS:] = cache["rt_shadow"][:-RT_LAG_HOURS, top] @ weights
    return np.stack([da, rt], axis=-1)
