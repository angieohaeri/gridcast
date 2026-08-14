from datetime import UTC, datetime
import os
import subprocess

from dotenv import load_dotenv
from loguru import logger

from gridcast.config import PROJ_ROOT, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

BACKUP_DIR = PROJ_ROOT / "backups"
RETENTION_COUNT = 14

# both live on the same timescaledb instance/credentials - `mlflow` isn't its own
# TIMESCALEDB_DB-style env var since docker-compose already hardcodes it as the
# backend-store db name for the mlflow service
DATABASES = [os.environ["TIMESCALEDB_DB"], "mlflow"]


def dump_database(db_name: str) -> None:
    dump_path = BACKUP_DIR / f"{db_name}_{datetime.now(UTC):%Y%m%dT%H%M%SZ}.dump"

    result = subprocess.run(
        [
            "pg_dump",
            "-h", os.environ["TIMESCALEDB_HOST"],
            "-p", os.environ["TIMESCALEDB_PORT"],
            "-U", os.environ["TIMESCALEDB_USER"],
            "-d", db_name,
            "-Fc",
            "-f", str(dump_path),
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "PGPASSWORD": os.environ["TIMESCALEDB_PASSWORD"]},
        check=False,
    )

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"pg_dump exited with code {result.returncode}")

    logger.success(f"wrote {dump_path}")

    # cron runs this once/day, so "last RETENTION_COUNT dumps" == "last RETENTION_COUNT days" -
    # pruned per-database so one db's dump cadence never eats into another's retention
    dumps = sorted(BACKUP_DIR.glob(f"{db_name}_*.dump"))
    for stale in dumps[:-RETENTION_COUNT]:
        stale.unlink()
        logger.info(f"pruned {stale.name}")


@flow(name="db_backup", description="Nightly pg_dump of the gridcast and mlflow databases, pruned to the last 14 dumps each.", log_prints=True)
def main():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    for db_name in DATABASES:
        dump_database(db_name)

    # `sync` (not `copy`) so the remote mirrors BACKUP_DIR exactly - the prune above
    # already reduced it to the last RETENTION_COUNT dumps per database, so this
    # carries that same retention to the remote instead of tracking it twice
    remote = os.environ["RCLONE_REMOTE"]
    sync_result = subprocess.run(
        ["rclone", "sync", str(BACKUP_DIR), remote],
        capture_output=True,
        text=True,
        check=False,
    )
    if sync_result.returncode != 0:
        logger.error(sync_result.stderr)
        raise RuntimeError(f"rclone sync exited with code {sync_result.returncode}")

    logger.success(f"synced {BACKUP_DIR} to {remote}")


if __name__ == "__main__":
    main()
