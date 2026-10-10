import json
import logging

from confluent_kafka import KafkaException, Producer
from django.conf import settings

logger = logging.getLogger(__name__)

_producer: Producer | None = None


class EventPublishError(Exception):
    """Событие не удалось доставить в Kafka."""

def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "order-service",
                "enable.idempotence": True,
            }
        )
    return _producer


def publish_event(topic: str, key: str, payload: dict, timeout: float = 5.0) -> None:
    producer = get_producer()
    delivery_errors: list = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            delivery_errors.append(err)

    try:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=on_delivery,
        )
    except (BufferError, KafkaException) as exc:
        raise EventPublishError(str(exc)) from exc

    not_delivered = producer.flush(timeout)
    if not_delivered or delivery_errors:
        raise EventPublishError(
            f"Delivery failed: not_delivered={not_delivered}, errors={delivery_errors}"
        )


