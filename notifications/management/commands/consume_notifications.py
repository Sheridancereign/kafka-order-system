import json

from config import kafka_topics
from notifications.services import notify_order_event
from orders.kafka_consumer import KafkaConsumerCommand, PoisonMessage


class Command(KafkaConsumerCommand):
    help = "Notification consumer: Telegram messages for order.confirmed / order.rejected"

    group_id = "notification-service"
    topics = [kafka_topics.ORDER_CONFIRMED, kafka_topics.ORDER_REJECTED]
    dlq_topic = kafka_topics.ORDER_EVENTS_DLQ

    def process(self, msg) -> None:
        try:
            payload = json.loads(msg.value())
            payload["order_id"]
        except (ValueError, KeyError, TypeError) as exc:
            raise PoisonMessage(f"Malformed payload: {exc}") from exc

        sent = notify_order_event(msg.topic(), payload)
        outcome = "sent" if sent else "duplicate, skipped"
        self.stdout.write(f"order {payload['order_id']} [{msg.topic()}]: {outcome}")