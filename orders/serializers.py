from rest_framework import serializers

from orders.models import Order


class OrderCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = Order
        fields = ("id", "product", "quantity", "status", "created_at")
        read_only_fields = ("id", "status", "created_at")
        extra_kwargs = {"quantity": {"min_value": 1}}