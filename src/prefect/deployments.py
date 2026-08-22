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

        load_producer_flow.to_deployment(name="load_producer", cron="0 9 * * *"),
        lmp_producer_flow.to_deployment(name="lmp_producer", cron="0 9 * * *"),
        weather_producer_flow.to_deployment(name="weather_producer", interval=timedelta(minutes=20)),
        load_consumer_flow.to_deployment(name="load_consumer", cron="10 9 * * *"),
        lmp_consumer_flow.to_deployment(name="lmp_consumer", cron="10 9 * * *"),
        weather_consumer_flow.to_deployment(name="weather_consumer", interval=timedelta(minutes=20)),
        dbt_build_flow.to_deployment(name="dbt_build", cron="25 9 * * *"),
        data_center_sync_flow.to_deployment(name="data_center_sync", cron="0 5 * * *"),
        # db_backup_flow.to_deployment(name="db_backup", cron="0 3 * * *"),  # disabled until RCLONE_REMOTE/backup setup is done
        # weekly retrain - adjust cadence once there's a sense of actual weekly drift
        train_flow.to_deployment(name="train", cron="0 4 * * 0"),
        api_ping_flow.to_deployment(name="api_ping", interval=timedelta(minutes=4), paused=True),
        dashboard_ping_flow.to_deployment(name="dashboard_ping", interval=timedelta(minutes=4), paused=True),
    )
