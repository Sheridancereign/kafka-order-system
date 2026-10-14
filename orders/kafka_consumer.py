import logging
import time

from confluent_kafka import Consumer, KafkaError
from django.conf import settings
from django.core.management.base import BaseCommand

from orders.producer import publish_event

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3


class PoisonMessage(Exception):
    """Сообщение, которое невозможно обработать ни при каких повторах."""


class KafkaConsumerCommand(BaseCommand):
    group_id: str
    topics: list[str]
    dlq_topic: str

    def process(self, msg) -> None:
        raise NotImplementedError

    def handle(self, *args, **options):
        consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe(self.topics)
        self.stdout.write(f"Consumer started (group={self.group_id}, topics={self.topics})")

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error: %s", msg.error())
                    continue

                self._handle_with_retries(msg)
                consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            self.stdout.write("Stopping...")
        finally:
            consumer.close()

    def _handle_with_retries(self, msg) -> None:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                self.process(msg)
                return
            except PoisonMessage as exc:
                self._send_to_dlq(msg, str(exc))
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Attempt %s/%s failed (topic=%s partition=%s offset=%s): %s",
                    attempt, MAX_ATTEMPTS, msg.topic(), msg.partition(), msg.offset(), exc,
                )
                time.sleep(2**attempt)
        self._send_to_dlq(msg, f"Failed after {MAX_ATTEMPTS} attempts")

    def _send_to_dlq(self, msg, reason: str) -> None:
        publish_event(
            topic=self.dlq_topic,
            key=msg.key().decode("utf-8", errors="replace") if msg.key() else "unknown",
            payload={
                "original_value": (msg.value() or b"").decode("utf-8", errors="replace"),
                "error": reason,
                "source_topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            },
        )
        self.stdout.write(self.style.ERROR(f"DLQ: {msg.topic()} offset {msg.offset()} ({reason})"))