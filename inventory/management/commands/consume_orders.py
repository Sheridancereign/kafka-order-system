import json
import logging
import time

from confluent_kafka import Consumer, KafkaError
from django.conf import settings
from django.core.management.base import BaseCommand

from config import kafka_topics
from inventory.services import PoisonMessage, reserve_stock
from orders.models import Order
from orders.producer import publish_event

logger = logging.getLogger(__name__)

GROUP_ID = "inventory-service"
MAX_ATTEMPTS = 3


class Command(BaseCommand):
    help = "Inventory consumer: reserves stock for order.created events"

    def _on_assign(self, consumer, partitions) -> None:
        ids = sorted(p.partition for p in partitions)
        self.stdout.write(self.style.WARNING(f"Assigned partitions: {ids}"))

    def _on_revoke(self, consumer, partitions) -> None:
        ids = sorted(p.partition for p in partitions)
        self.stdout.write(self.style.WARNING(f"Revoked partitions: {ids}"))

    def handle(self, *args, **options):
        consumer = Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": GROUP_ID,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        consumer.subscribe(
            [kafka_topics.ORDER_CREATED],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        self.stdout.write(f"Inventory consumer started (group={GROUP_ID})")

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
                self._process(msg)
                return
            except PoisonMessage as exc:
                self._send_to_dlq(msg, str(exc))
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Attempt %s/%s failed (partition=%s offset=%s): %s",
                    attempt, MAX_ATTEMPTS, msg.partition(), msg.offset(), exc,
                )
                time.sleep(2 ** attempt)
        self._send_to_dlq(msg, f"Failed after {MAX_ATTEMPTS} attempts")

    def _process(self, msg) -> None:
        try:
            order_id = int(json.loads(msg.value())["order_id"])
        except (ValueError, KeyError, TypeError) as exc:
            raise PoisonMessage(f"Malformed payload: {exc}") from exc

        outcome = reserve_stock(order_id)

        topic = (
            kafka_topics.ORDER_CONFIRMED
            if outcome.status == Order.Status.CONFIRMED
            else kafka_topics.ORDER_REJECTED
        )
        publish_event(
            topic=topic,
            key=str(outcome.product_id),
            payload={
                "order_id": outcome.order_id,
                "product_id": outcome.product_id,
                "quantity": outcome.quantity,
                "status": outcome.status,
                "reason": outcome.reason,
            },
        )
        self.stdout.write(
            f"[p{msg.partition()}] order {outcome.order_id} -> {outcome.status} {outcome.reason}"
        )

    def _send_to_dlq(self, msg, reason: str) -> None:
        publish_event(
            topic=kafka_topics.ORDER_CREATED_DLQ,
            key=msg.key().decode("utf-8", errors="replace") if msg.key() else "unknown",
            payload={
                "original_value": (msg.value() or b"").decode("utf-8", errors="replace"),
                "error": reason,
                "source_topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            },
        )
        self.stdout.write(self.style.ERROR(f"DLQ: offset {msg.offset()} ({reason})"))
