from collections.abc import Callable
import json
import os

from confluent_kafka import Consumer, Producer
from loguru import logger
import psycopg2


def build_consumer(group_id: str, topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def build_dlq_producer() -> Producer:
    return Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})


def consume_and_load(
    consumer: Consumer,
    dlq_producer: Producer,
    dlq_topic: str,
    upsert: Callable[[dict], None],
    poll_timeout: float = 5.0,
) -> tuple[int, int]:
    """Drain currently-available messages, upsert each via `upsert`, and commit
    offsets one at a time. Malformed messages or rows the DB rejects (bad/missing
    fields) go to the DLQ topic and are committed past - reprocessing them would
    fail the same way every time. A database connectivity error stops the run
    without committing, so the next run resumes from the same message.

    poll() commonly returns None on early calls while the group rebalance is
    still in progress, not just when the topic is genuinely drained - stop only
    after several consecutive empty polls, not the first one.
    """
    written = 0
    dead_lettered = 0
    empty_polls = 0
    while empty_polls < 3:
        msg = consumer.poll(timeout=poll_timeout)
        if msg is None:
            empty_polls += 1
            continue
        empty_polls = 0
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        try:
            record = json.loads(msg.value())
            upsert(record)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.error("Database connection failed - stopping without committing")
            raise
        except Exception as e:  # noqa: BLE001 - anything here means "not a valid row", route to DLQ
            logger.error(f"Bad message on {msg.topic()}, routing to {dlq_topic}: {e}")
            dlq_producer.produce(dlq_topic, key=msg.key(), value=msg.value())
            dlq_producer.flush()
            dead_lettered += 1
            consumer.commit(msg)
            continue

        consumer.commit(msg)
        written += 1

    return written, dead_lettered
