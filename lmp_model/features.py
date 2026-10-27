import numpy as np

CLIP_PERCENTILES = [0.1, 99.9]


def fit_scaler(x: np.ndarray) -> tuple[np.ndarray, ...]:
    """Clip bounds and the mean and std of the clipped values, per last-axis column."""
    flat = x.reshape(-1, x.shape[-1])
    lo, hi = np.nanpercentile(flat, CLIP_PERCENTILES, axis=0)
    clipped = np.clip(flat, lo, hi)
    return lo, hi, np.nanmean(clipped, axis=0), np.nanstd(clipped, axis=0)


def scale(x: np.ndarray, scaler: tuple[np.ndarray, ...]) -> np.ndarray:
    lo, hi, mean, std = scaler
    return (np.clip(x, lo, hi) - mean) / std


def fit_scalers(data: dict[str, np.ndarray], mask: np.ndarray) -> dict[str, tuple[np.ndarray, ...]]:
    """Scalers fit on the masked (train) hours only."""
    return {
        "x": fit_scaler(data["x"][mask]),
        "area": fit_scaler(data["area_forecast"][mask]),
        "weather": fit_scaler(data["weather"][mask]),
    }


def prepare(data: dict[str, np.ndarray], mask: np.ndarray, scalers: dict) -> dict[str, np.ndarray]:
    """Scaled inputs (NaN filled with 0) and the loss mask for the masked hours. A cell is in
    the loss when the node exists, y is verified, and all raw input features are present."""

    def scaled(key: str, scaler: tuple[np.ndarray, ...]) -> np.ndarray:
        return np.nan_to_num(scale(data[key][mask], scaler)).astype("float32")

    y = data["y"][mask]
    has_features = ~np.isnan(data["x"][mask]).any(axis=-1)
    return {
        "x": scaled("x", scalers["x"]),
        "area": scaled("area_forecast", scalers["area"]),
        "weather": scaled("weather", scalers["weather"]),
        "system": data["system"][mask],
        "y": y,
        "loss_mask": data["exists"][mask] & ~np.isnan(y) & has_features,
    }
