from pathlib import Path

from loguru import logger
from tqdm import tqdm
import typer

from gridcast.config import MODELS_DIR, PROCESSED_DATA_DIR

app = typer.Typer()

# Multi-horizon models (1h/24h/72h ahead), decided 2026-08-12: feature columns
# (lags/rolling from load_features.sql) are shared across all three - staleness
# only depends on "now" vs. settlement lag, not on the forecast horizon. Only
# the label differs: for each horizon N, shift demand_mw forward N hours per
# zone (sorted by time) to build y, keep X as-is. Train one model per horizon.

# def train():

#   
#     trainrange = pd.date_range(start='2023-01-01', end='2024-12-31', freq='D').tz_localize(None)
#     valrange = pd.date_range(start='2025-01-01', end='2025-12-31', freq='D').tz_localize(None)
#     testrange = pd.date_range(start='2026-01-01', end='2026-08-10', freq='D').tz_localize(None)

#     df = features['time'].dt.tz_localize(None).dt.normalize()

#     train = features[df.isin(trainrange)]
#     validation = features[df.isin(valrange)]
#     test = features[df.isin(testrange)]

    # train = train[~train.demand_lag_168h.isna()]
#     return (train, validation, test)

@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    features_path: Path = PROCESSED_DATA_DIR / "features.csv",
    labels_path: Path = PROCESSED_DATA_DIR / "labels.csv",
    model_path: Path = MODELS_DIR / "model.pkl",
    # -----------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Training some model...")
    for i in tqdm(range(10), total=10):
        if i == 5:
            logger.info("Something happened for iteration 5.")
    logger.success("Modeling training complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
