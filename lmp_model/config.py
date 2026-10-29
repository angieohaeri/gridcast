from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
import mlflow
import numpy as np
import pandas as pd

# Load environment variables from .env file if it exists
load_dotenv()

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
logger.info(f"PROJ_ROOT path is: {PROJ_ROOT}")

DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

MODELS_DIR = PROJ_ROOT / "models"

REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"


SAMPLE_ROWS = 5000


def log_datasets(splits: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray, pd.DatetimeIndex]], 
                 feature_names: list[str], pnode_ids: pd.Index, cache_path: Path, seed: int = 0) -> None:
    """Log each split's inputs to the active MLflow run as a dataset: a random sample of the
    (hour, node) cells in the loss, with the scaled model inputs and the target. `splits` maps
    split name to (x [hour, node, feature], y [hour, node], loss mask, hour timestamps)."""
    rng = np.random.default_rng(seed)
    stat = cache_path.stat()
    mlflow.set_tags({"cache_path": str(cache_path), 
                     "cache_modified": pd.Timestamp(stat.st_mtime, unit="s", tz="UTC").isoformat(), 
                     "cache_mb": round(stat.st_size / 1e6)})
    for name, (x, y, mask, times) in splits.items():
        hours, nodes = np.nonzero(mask)
        pick = rng.choice(len(hours), size=min(SAMPLE_ROWS, len(hours)), replace=False)
        hours, nodes = hours[pick], nodes[pick]
        sample = pd.DataFrame(x[hours, nodes], columns=feature_names)
        sample.insert(0, "pnode_id", pnode_ids[nodes])
        sample.insert(0, "time", times[hours])
        sample["spread"] = y[hours, nodes]
        dataset = mlflow.data.from_pandas(sample, source=str(cache_path), name=f"window_{name}", targets="spread")
        mlflow.log_input(dataset, context=name, 
                         tags={"hours": str(x.shape[0]), "nodes": str(x.shape[1]), "cells_in_loss": str(int(mask.sum()))})
