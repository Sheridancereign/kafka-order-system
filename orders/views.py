import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from config import kafka_topics
from orders.models import Order
from orders.producer import EventPublishError, publish_event
from orders.serializers import OrderCreateSerializer

logger = logging.getLogger(__name__)


class OrderCreateView(APIView):
    def post(self, request):
        serializer = OrderCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        order = serializer.save()

        try:
            publish_event(
                topic=kafka_topics.ORDER_CREATED,
                key=str(order.product_id),
                payload={
                    "order_id": order.pk,
                    "product_id": order.product_id,
                    "quantity": order.quantity,
                    "created_at": order.created_at.isoformat(),
                },
            )
        except EventPublishError:
            logger.exception("Failed to publish order.created for order %s", order.pk)
            order.status = Order.Status.REJECTED
            order.reject_reason = "Event bus unavailable"
            order.save(update_fields=["status", "reject_reason", "updated_at"])
            return Response(
                {"detail": "Service temporarily unavailable, order was not accepted."},
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        return Response(OrderCreateSerializer(order).data, status=status.HTTP_202_ACCEPTED)