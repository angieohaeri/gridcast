from dotenv import load_dotenv
from kafka_consumer_client import build_consumer, build_dlq_producer, consume_and_load
from loguru import logger

from gridcast.config import get_connection, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

UPSERT = """
INSERT INTO instantaneous_load (time, zone, instantaneous_load_mw)
VALUES (%(time)s, %(zone)s, %(instantaneous_load_mw)s)
ON CONFLICT (time, zone) DO UPDATE SET
    instantaneous_load_mw = EXCLUDED.instantaneous_load_mw;
"""

@flow(name="inst_load_consumer", description="Consumes inst_load messages from Kafka and upserts to Postgres.", log_prints=True)
def main():
    conn = get_connection()
    cur = conn.cursor()

    consumer = build_consumer(group_id="inst-load-consumer", topic="inst_load")
    dlq_producer = build_dlq_producer()

    written, dead_lettered = consume_and_load(
        consumer,
        dlq_producer,
        dlq_topic="inst_load_dlq",
        upsert=lambda record: cur.execute(UPSERT, record),
    )

    consumer.close()
    conn.close()
    logger.success(f"Wrote {written} inst_load rows, dead-lettered {dead_lettered}")


if __name__ == "__main__":
    main()
