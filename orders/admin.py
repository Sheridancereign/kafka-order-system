from django.contrib import admin

from orders.models import Order


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ("id", "product", "quantity", "status", "created_at", "updated_at")
    list_filter = ("status",)