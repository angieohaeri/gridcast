from dotenv import load_dotenv
from loguru import logger
from prefect.concurrency.sync import concurrency
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from gridcast.config import PROJ_ROOT, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

# dbt's profiles.yml reads TIMESCALEDB_* via env_var(); dbt runs in this process, so it
# sees them from load_dotenv() here, or straight from compose when this runs in the
# prefect-deployments container.
DBT_PROJECT_DIR = PROJ_ROOT / "src" / "dbt"


@flow(name="dbt_build", description="Builds one dbt selector from selectors.yml: load, lmp_history or lmp_serving.", log_prints=True)
def main(selector: str = "load"):
    runner = PrefectDbtRunner(settings=PrefectDbtSettings(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR))
    # one dbt build at a time: overlapping builds lock each other's tables (baseline_features
    # reads what spread rewrites) and share the project's target/ dir
    with concurrency("dbt-build", occupy=1, strict=True):
        runner.invoke(["build", "--selector", selector])

    logger.success(f"dbt build --selector {selector} finished")


if __name__ == "__main__":
    main()
