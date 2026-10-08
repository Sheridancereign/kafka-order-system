from django.db import models


class Order(models.Model):
    class Status(models.TextChoices):
        PENDING = "PENDING", "Pending"
        CONFIRMED = "CONFIRMED", "Confirmed"
        REJECTED = "REJECTED", "Rejected"

    product = models.ForeignKey(
        "inventory.Product",
            on_delete=models.CASCADE,
            related_name="orders",
        )
    quantity = models.PositiveIntegerField()
    status = models.CharField(
        max_length=11,
        choices=Status.choices,
        default=Status.PENDING,
    )
    rejected_reason = models.TextField(
        max_length=255,
        null=True,
        blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self) -> str:
        return f"Order #{self.pk} [{self.status}]"

