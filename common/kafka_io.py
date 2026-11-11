import json
import os

from confluent_kafka import Consumer, Producer


def consumer(group, topic):
    client = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "broker.address.family": "v4",
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    client.subscribe([topic])
    return client


def producer():
    return Producer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "broker.address.family": "v4",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 30000,
        }
    )


def publish(client, topic, message, key=None):
    errors = []
    client.produce(
        topic,
        key=key,
        value=json.dumps(message, allow_nan=False).encode(),
        on_delivery=lambda error, msg: errors.append(error) if error else None,
    )
    if client.flush(35) or errors:
        raise RuntimeError(f"Kafka delivery failed: {errors}")
