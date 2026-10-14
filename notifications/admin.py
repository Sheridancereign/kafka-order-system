from django.contrib import admin

from notifications.models import Notification


@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):
    list_display = ("id", "order_id", "event_type", "status", "sent_at")
    list_filter = ("status", "event_type")