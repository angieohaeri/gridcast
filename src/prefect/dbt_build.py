from pathlib import Path
import subprocess
import sys

from dotenv import load_dotenv
from loguru import logger

from gridcast.config import PROJ_ROOT, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

# dbt's profiles.yml reads TIMESCALEDB_* via env_var(), so the subprocess needs the
# .env values load_dotenv() just put on os.environ - it inherits them by default.
DBT = Path(sys.executable).parent / "dbt"
DBT_PROJECT_DIR = PROJ_ROOT / "src" / "dbt"


@flow(name="dbt_build", description="Builds dbt seeds, staging views, and feature tables.", log_prints=True)
def main():
    result = subprocess.run(
        [DBT, "build", "--project-dir", DBT_PROJECT_DIR],
        capture_output=True,
        text=True,
        check=False,
    )
    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt build exited with code {result.returncode}")

    logger.success("dbt build finished")


if __name__ == "__main__":
    main()
