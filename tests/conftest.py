from pathlib import Path
import sys

# src/producers and src/consumers scripts do bare sibling imports (e.g. `from
# kafka_client import ...`) resolved via their own directory on sys.path - same
# pattern as src/prefect/deployments.py.
SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC / "producers"))
sys.path.insert(0, str(SRC / "consumers"))
