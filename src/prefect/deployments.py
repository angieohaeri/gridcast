from datetime import timedelta
from pathlib import Path
import sys

from gridcast.modeling.train import main as train_flow
from prefect import serve

# Producer/consumer scripts do a bare sibling import (e.g. `from kafka_client import
# ...`) resolved via their own directory on sys.path - add both here so that works too.
SRC = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SRC / "producers"))
sys.path.insert(0, str(SRC / "consumers"))

from api_ping import main as api_ping_flow

# from db_backup import main as db_backup_flow  # disabled until RCLONE_REMOTE/backup setup is done
from dashboard_ping import main as dashboard_ping_flow
from data_center_sync import main as data_center_sync_flow
from dbt_build import main as dbt_build_flow
from lmp_consumer import main as lmp_consumer_flow
from lmp_producer import main as lmp_producer_flow
from load_consumer import main as load_consumer_flow
from load_producer import main as load_producer_flow
from weather_consumer import main as weather_consumer_flow
from weather_producer import main as weather_producer_flow

if __name__ == "__main__":
    serve(
        load_producer_flow.to_deployment(name="load_producer", cron="10 * * * *"),
        lmp_producer_flow.to_deployment(name="lmp_producer", cron="10 * * * *"),
        weather_producer_flow.to_deployment(name="weather_producer", interval=timedelta(minutes=20)),
        load_consumer_flow.to_deployment(name="load_consumer", cron="15 * * * *"),
        lmp_consumer_flow.to_deployment(name="lmp_consumer", cron="15 * * * *"),
        weather_consumer_flow.to_deployment(name="weather_consumer", interval=timedelta(minutes=20)),
        dbt_build_flow.to_deployment(name="dbt_build", cron="25 * * * *"),
        data_center_sync_flow.to_deployment(name="data_center_sync", cron="0 5 * * *"),
        # db_backup_flow.to_deployment(name="db_backup", cron="0 3 * * *"),  # disabled until RCLONE_REMOTE/backup setup is done
        # weekly retrain - adjust cadence once there's a sense of actual weekly drift
        train_flow.to_deployment(name="train", cron="0 4 * * 0"),
        # keeps the api's DB/model working set resident between dashboard visits on
        # the memory-constrained host, instead of it getting swapped out and paged
        # back in slowly on the next real request - see references/decisions.md.
        # Paused 2026-08-21: significant memory use observed, root cause not yet
        # identified - re-enable once that's understood.
        api_ping_flow.to_deployment(name="api_ping", interval=timedelta(minutes=4), paused=True),
        # same idea, for the dashboard's Shiny process - a plain GET only fetches the
        # static shell (sessions start over websocket on connect), so this is cheap
        # and doesn't run the reactive.calc chain in src/dashboard/app.py. Paused to
        # start, alongside api_ping above, pending that memory investigation.
        dashboard_ping_flow.to_deployment(name="dashboard_ping", interval=timedelta(minutes=4), paused=True),
    )
