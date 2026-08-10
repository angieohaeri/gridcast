from dotenv import load_dotenv
from kafka_consumer_client import build_consumer, build_dlq_producer, consume_and_load
from loguru import logger

from gridcast.config import get_connection, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

# No unique constraint on weather - it's an append-only log of poll snapshots,
# not a revised/upserted feed like load or lmp (see references/schema.md).
INSERT = """
INSERT INTO weather (time, zone, temperature, precipitation, wind_speed, cloud_cover)
VALUES (%(time)s, %(zone)s, %(temperature)s, %(precipitation)s, %(wind_speed)s, %(cloud_cover)s);
"""

@flow(name="weather_consumer", description="Consumes weather messages from Kafka and inserts to Postgres.", log_prints=True)
def main():
    conn = get_connection()
    cur = conn.cursor()

    consumer = build_consumer(group_id="weather-consumer", topic="weather")
    dlq_producer = build_dlq_producer()

    written, dead_lettered = consume_and_load(
        consumer,
        dlq_producer,
        dlq_topic="weather_dlq",
        upsert=lambda record: cur.execute(INSERT, record),
    )

    consumer.close()
    conn.close()
    logger.success(f"Wrote {written} weather rows, dead-lettered {dead_lettered}")


if __name__ == "__main__":
    main()
