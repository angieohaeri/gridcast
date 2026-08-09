from db_client import get_connection
from dotenv import load_dotenv
from kafka_client import build_consumer, build_dlq_producer, consume_and_load
from loguru import logger

from gridcast.config import setup_logging

load_dotenv()
setup_logging()

UPSERT = """
INSERT INTO lmp (time, pnode_id, pnode_name, zone, lmp, congestion_price, marginal_loss_price)
VALUES (%(time)s, %(pnode_id)s, %(pnode_name)s, %(zone)s, %(lmp)s,
        %(congestion_price)s, %(marginal_loss_price)s)
ON CONFLICT (time, pnode_id) DO UPDATE SET
    pnode_name = EXCLUDED.pnode_name,
    zone = EXCLUDED.zone,
    lmp = EXCLUDED.lmp,
    congestion_price = EXCLUDED.congestion_price,
    marginal_loss_price = EXCLUDED.marginal_loss_price;
"""


def main():
    conn = get_connection()
    cur = conn.cursor()

    consumer = build_consumer(group_id="lmp-consumer", topic="lmp")
    dlq_producer = build_dlq_producer()

    written, dead_lettered = consume_and_load(
        consumer,
        dlq_producer,
        dlq_topic="lmp_dlq",
        upsert=lambda record: cur.execute(UPSERT, record),
    )

    consumer.close()
    conn.close()
    logger.success(f"Wrote {written} lmp rows, dead-lettered {dead_lettered}")


if __name__ == "__main__":
    main()
