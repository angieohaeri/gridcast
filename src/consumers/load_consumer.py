from dotenv import load_dotenv
from kafka_consumer_client import build_consumer, build_dlq_producer, consume_and_load
from loguru import logger

from gridcast.config import get_connection, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

UPSERT = """
INSERT INTO load (time, zone, source, demand_mw, demand_forecast_mw,
                   net_generation_mw, total_interchange_mw, is_verified)
VALUES (%(time)s, %(zone)s, %(source)s, %(demand_mw)s, %(demand_forecast_mw)s,
        %(net_generation_mw)s, %(total_interchange_mw)s, %(is_verified)s)
ON CONFLICT (time, zone, source) DO UPDATE SET
    demand_mw = EXCLUDED.demand_mw,
    demand_forecast_mw = EXCLUDED.demand_forecast_mw,
    net_generation_mw = EXCLUDED.net_generation_mw,
    total_interchange_mw = EXCLUDED.total_interchange_mw,
    is_verified = EXCLUDED.is_verified;
"""

@flow(name="load_consumer", description="Consumes load messages from Kafka and upserts to Postgres.", log_prints=True)
def main():
    conn = get_connection()
    cur = conn.cursor()

    consumer = build_consumer(group_id="load-consumer", topic="load")
    dlq_producer = build_dlq_producer()

    written, dead_lettered = consume_and_load(
        consumer,
        dlq_producer,
        dlq_topic="load_dlq",
        upsert=lambda record: cur.execute(UPSERT, record),
    )

    consumer.close()
    conn.close()
    logger.success(f"Wrote {written} load rows, dead-lettered {dead_lettered}")


if __name__ == "__main__":
    main()
