# CCDS Best Practices

---

## Data Directory Contract

Treat each subdirectory as a strict pipeline stage — data flows forward, never backward.

| Directory | Purpose | Rule |
|---|---|---|
| `data/raw/` | Original source data (Citi Bike CSVs, station metadata) | Never write here programmatically. Read-only. |
| `data/external/` | Third-party data (Open-Meteo history, borough GeoJSON) | Write once, treat as stable |
| `data/interim/` | Partially cleaned/transformed data | Output of cleaning steps |
| `data/processed/` | Final model-ready feature tables | What training scripts actually read |
| `models/` | Serialized model artifacts | MLflow handles this; directory exists as fallback |

None of these are committed to git — all in `.gitignore`. DVC handles version tracking.

---

## `config.py` — Never Hardcode Paths

CCDS generates `bikeshare/config.py` with project-root-relative paths. Import from it
everywhere instead of constructing paths manually.

```python
# bikeshare/config.py
from pathlib import Path

PROJ_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR       = PROJ_ROOT / "data"
RAW_DATA_DIR   = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"
MODELS_DIR     = PROJ_ROOT / "models"
REPORTS_DIR    = PROJ_ROOT / "reports"
```

Usage:

```python
from bikeshare.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

trips = pd.read_csv(RAW_DATA_DIR / "citibike_2024_01.csv")
features.to_parquet(PROCESSED_DATA_DIR / "features_2024_01.parquet")
```

- Never use `os.getcwd()` or paths relative to `__file__` in scripts — both break when
  run from a different working directory
- Never hardcode `/Users/angieohaeri/...` anywhere in the codebase

---

## Notebook Naming and Discipline

Follow CCDS naming convention: `<step>-<description>.ipynb`

```
notebooks/
├── 01-eda-station-availability-patterns.ipynb
├── 02-eda-weather-demand-correlation.ipynb
├── 03-feature-lag-window-exploration.ipynb
├── 04-model-lgbm-baseline.ipynb
```

**The workflow:**

1. Explore and prototype in a notebook
2. Once something works, refactor it into `dataset.py`, `features.py`, or `modeling/train.py`
3. Import it back into the notebook to verify
4. If a function appears in more than one notebook → it belongs in the package

Notebooks are for exploration, not production. Never import from a notebook.

---

## Logging, Not Printing

Set up logging once in `bikeshare/__init__.py`:

```python
# bikeshare/__init__.py
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
```

Use a module-scoped logger in every package file:

```python
# bikeshare/dataset.py
import logging

logger = logging.getLogger(__name__)

def load_station_snapshots(...):
    logger.info("Loading snapshots for %d stations", len(station_ids))
    ...
    logger.warning("No data found for station %s", station_id)
```

- No `print()` in package code — it can't be silenced, filtered by level, or redirected
- `print()` is fine in notebooks and throwaway scripts
- Use log levels correctly: `DEBUG` for verbose internals, `INFO` for progress,
  `WARNING` for recoverable issues, `ERROR` for failures

---

## Makefile as the Project Interface

Extend the CCDS-generated Makefile so any common workflow is one command. The goal:
someone clones the repo and understands the full pipeline just by reading the targets.

```makefile
## Install package in editable mode
install:
	pip install -e .

## Download and validate raw Citi Bike trip CSVs
data:
	python -m bikeshare.dataset download_trips

## Build processed feature tables from raw data
features:
	dbt build --select features

## Train LightGBM baseline and log to MLflow
train:
	python -m bikeshare.modeling.train

## Run all tests
test:
	pytest tests/

## Lint and format
lint:
	ruff check . --fix

## Start the full Docker stack
up:
	docker compose up -d

## Tear down Docker stack (preserves named volumes)
down:
	docker compose down
```

---

## Package Structure and Responsibilities

Each module has a single responsibility. Don't let logic bleed between them.

```
bikeshare/
├── __init__.py        # logging setup only
├── config.py          # path constants
├── dataset.py         # data loading and access (reads from data/ or TimescaleDB)
├── features.py        # feature engineering functions (Python-side transforms)
└── modeling/
    ├── train.py       # reads processed features → trains → logs to MLflow
    └── predict.py     # loads from MLflow registry → exposes predict()
```

**`dataset.py`** — anything that reads from a source and returns a dataframe:
```python
def load_station_snapshots(start: str, end: str) -> pd.DataFrame: ...
def load_historical_trips(path: str) -> pd.DataFrame: ...
```

**`features.py`** — Python-side transforms that complement dbt SQL models:
```python
def encode_cyclical(df: pd.DataFrame, col: str, max_val: int) -> pd.DataFrame: ...
def add_neighbor_features(df: pd.DataFrame, n_neighbors: int = 5) -> pd.DataFrame: ...
```

**`modeling/train.py`** — runnable as a script, does nothing except:
1. Read from `PROCESSED_DATA_DIR`
2. Train model
3. Log to MLflow
4. Register model

**`modeling/predict.py`** — loaded by FastAPI:
```python
def predict(features: pd.DataFrame) -> np.ndarray: ...
```

Neither `train.py` nor `predict.py` should contain data loading or feature engineering
logic — those belong in `dataset.py` and `features.py`.

---

## Tests Mirroring the Package

```
tests/
├── test_dataset.py
├── test_features.py
└── modeling/
    ├── test_train.py
    └── test_predict.py
```

- Test package functions, not notebooks or pipeline scripts
- Focus on functions that are easy to get subtly wrong:
  - `encode_cyclical` → output bounded between -1 and 1, original column dropped
  - neighbor features → correct distance weighting, handles edge stations
  - data loaders → correct schema, handles missing stations
- Run with `make test` or `pytest tests/` — should also run in GitHub Actions CI

---

## Editable Install

Run once after cloning or adding new modules:

```bash
pip install -e .
```

Verify it works:

```bash
python -c "import bikeshare; print('ok')"
```

After this, all package imports work from anywhere in the project — notebooks, scripts,
tests, producers, consumers — without path manipulation.
