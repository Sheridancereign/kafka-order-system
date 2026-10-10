from django.urls import path

from orders.views import OrderCreateView

urlpatterns = [
    path("orders/", OrderCreateView.as_view(), name="order-create"),
]