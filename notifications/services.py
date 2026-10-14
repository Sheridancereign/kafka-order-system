from django.utils import timezone

from config import kafka_topics
from notifications.models import Notification
from notifications.telegram import send_message


def build_message(event_type: str, payload: dict) -> str:
    order_id = payload["order_id"]
    if event_type == kafka_topics.ORDER_CONFIRMED:
        return (
            f"Order #{order_id} confirmed: "
            f"Item {payload['product_id']}, {payload['quantity']} шт."
        )
    reason = payload.get("reason") or "причина не указана"
    return f"Order #{order_id} declined: {reason}"


def notify_order_event(event_type: str, payload: dict) -> bool:

    notification, _ = Notification.objects.get_or_create(
        order_id=payload["order_id"],
        event_type=event_type,
    )
    if notification.status == Notification.Status.SENT:
        return False

    send_message(build_message(event_type, payload))

    notification.status = Notification.Status.SENT
    notification.sent_at = timezone.now()
    notification.save(update_fields=["status", "sent_at"])
    return True