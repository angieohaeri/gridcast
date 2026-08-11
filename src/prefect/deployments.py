from datetime import timedelta
from pathlib import Path
import sys

from prefect import serve

# Each producer/consumer script does a bare `from kafka_client import ...` /
# `from kafka_consumer_client import ...` sibling import, resolved by having its
# own directory on sys.path - add both directories before importing so those
# sibling imports succeed here too.
SRC = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SRC / "producers"))
sys.path.insert(0, str(SRC / "consumers"))

from dbt_build import main as dbt_build_flow  # noqa: E402
from lmp_consumer import main as lmp_consumer_flow  # noqa: E402
from lmp_producer import main as lmp_producer_flow  # noqa: E402
from load_consumer import main as load_consumer_flow  # noqa: E402
from load_producer import main as load_producer_flow  # noqa: E402
from weather_consumer import main as weather_consumer_flow  # noqa: E402
from weather_producer import main as weather_producer_flow  # noqa: E402

if __name__ == "__main__":
    serve(
        load_producer_flow.to_deployment(name="load_producer", cron="10 * * * *"),
        lmp_producer_flow.to_deployment(name="lmp_producer", cron="10 * * * *"),
        weather_producer_flow.to_deployment(name="weather_producer", interval=timedelta(minutes=20)),
        load_consumer_flow.to_deployment(name="load_consumer", cron="15 * * * *"),
        lmp_consumer_flow.to_deployment(name="lmp_consumer", cron="15 * * * *"),
        weather_consumer_flow.to_deployment(name="weather_consumer", interval=timedelta(minutes=20)),
        dbt_build_flow.to_deployment(name="dbt_build", cron="25 * * * *"),
    )
