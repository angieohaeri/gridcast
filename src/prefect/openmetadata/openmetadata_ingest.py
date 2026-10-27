import os
from pathlib import Path
import shutil
import subprocess
import sys

from dotenv import load_dotenv
from loguru import logger
from openmetadata_lineage import main as write_lineage

from gridcast.config import PROJ_ROOT, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

# `metadata` lives in its own uv tool env (`uv tool install "openmetadata-ingestion[postgres]==2.0.6"`,
# baked into the Dockerfile) - its pins downgrade dbt's metricflow/sqlglot, so it can't share the
# project venv.
METADATA = shutil.which("metadata")
DBT = Path(sys.executable).parent / "dbt"
DBT_PROJECT_DIR = PROJ_ROOT / "src" / "dbt"
CONFIG_DIR = Path(__file__).parent

# the dbt ingestion config reads artifact paths from this - its relative paths resolve against
# the ingestion package, not the cwd
os.environ["DBT_TARGET_DIR"] = str(DBT_PROJECT_DIR / "target")


def run(command: list) -> None:
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"{command[0]} {command[1]} exited with code {result.returncode}")


@flow(name="openmetadata_ingest", description="Ingests TimescaleDB table metadata, dbt lineage, MLflow models, Prefect flows, Kafka topics, profiles and cross-system lineage into OpenMetadata.", log_prints=True)
def main():
    run([METADATA, "ingest", "-c", CONFIG_DIR / "openmetadata_postgres.yaml"])
    # the committed catalog.json is stale unless regenerated - needs a db connection, same env as dbt_build
    run([DBT, "docs", "generate", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR])
    run([METADATA, "ingest", "-c", CONFIG_DIR / "openmetadata_dbt.yaml"])
    run([METADATA, "ingest", "-c", CONFIG_DIR / "openmetadata_mlflow.yaml"])
    run([METADATA, "ingest", "-c", CONFIG_DIR / "openmetadata_prefect.yaml"])
    run([METADATA, "ingest", "-c", CONFIG_DIR / "openmetadata_kafka.yaml"])
    run([METADATA, "profile", "-c", CONFIG_DIR / "openmetadata_profiler.yaml"])
    write_lineage()

    logger.success("OpenMetadata ingestion finished")


if __name__ == "__main__":
    main()
