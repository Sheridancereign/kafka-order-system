from dataclasses import dataclass
from django.db import transaction

from inventory.models import Product
from orders.models import Order

class PoisonMessage(Exception):
    """The message cannot be handled by any repository."""

@dataclass(frozen=True)
class OrderOutcome:
    order_id: int
    product_id: int
    quantity: int
    status: str
    reason: str = ""

def reserve_stock(order_id: int) -> OrderOutcome:
    with transaction.atomic():
        try:
            order = Order.objects.select_for_update().get(pk=order_id)
        except Order.DoesNotExist as exc:
            raise PoisonMessage(f"Order {order_id} does not exist") from exc

        if order.status == Order.Status.PENDING:
            product = Product.objects.select_for_update().get(pk=order.product_id)
            if product.stock >= order.quantity:
                product.stock -= order.quantity
                product.save(update_fields=["stock"])
                order.status = Order.Status.CONFIRMED
            else:
                order.status = Order.Status.REJECTED
                order.rejected_reason = "Insufficient stock"
            order.save(update_fields=["status", "rejected_reason", "updated_at"])

        return OrderOutcome(
            order_id=order_id,
            product_id=order.product_id,
            quantity=order.quantity,
            status=order.status,
            reason=order.rejected_reason or "",
        )




