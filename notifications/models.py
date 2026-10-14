from django.db import models


class Notification(models.Model):
    class Status(models.TextChoices):
        PENDING = "PENDING", "pending"
        SENT = "SENT", "sent"

    order_id = models.PositiveIntegerField()
    event_type = models.CharField(max_length=64)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.PENDING)
    created_at = models.DateTimeField(auto_now_add=True)
    sent_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["order_id", "event_type"],
                name="uniq_notification_per_order_event",
            )
        ]

    def __str__(self) -> str:
        return f"{self.event_type} for order {self.order_id} [{self.status}]"