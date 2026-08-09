import json
import os

from confluent_kafka import Producer
from loguru import logger
import numpy as np
import pandas as pd


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def _delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for {msg.topic()} key={msg.key()}: {err}")


def _json_safe(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def produce_json(producer: Producer, topic: str, key: str, record: dict) -> None:
    """Serialize record to JSON and produce it, keyed by `key` (zone, per convention)."""
    clean_record = {k: _json_safe(v) for k, v in record.items()}
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(clean_record).encode("utf-8"),
        callback=_delivery_report,
    )
    producer.poll(0)
