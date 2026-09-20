from dotenv import load_dotenv
from kafka_consumer_client import build_consumer, build_dlq_producer, consume_and_load
from loguru import logger

from gridcast.config import get_connection, setup_logging
from prefect import flow

load_dotenv()
setup_logging()

UPSERT = """
INSERT INTO raw_lmp.lmp_rt_unverified_fivemin (time, zone, lmp, congestion_price, marginal_loss_price)
VALUES (%(time)s, %(zone)s, %(lmp)s, %(congestion_price)s, %(marginal_loss_price)s)
ON CONFLICT (time, zone) DO UPDATE SET
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price;
"""

@flow(
    name="raw_lmp_lmp_rt_unverified_fivemin_consumer",
    description="Consumes real-time unverified 5-min LMP messages from Kafka and upserts to Postgres.",
    log_prints=True,
)
def main():
    conn = get_connection()
    cur = conn.cursor()

    consumer = build_consumer(group_id="raw-lmp-lmp-rt-unverified-fivemin-consumer", topic="raw_lmp_lmp_rt_unverified_fivemin")
    dlq_producer = build_dlq_producer()

    written, dead_lettered = consume_and_load(
        consumer,
        dlq_producer,
        dlq_topic="raw_lmp_lmp_rt_unverified_fivemin_dlq",
        upsert=lambda record: cur.execute(UPSERT, record),
    )

    consumer.close()
    conn.close()
    logger.success(f"Wrote {written} raw_lmp.lmp_rt_unverified_fivemin rows, dead-lettered {dead_lettered}")


if __name__ == "__main__":
    main()
