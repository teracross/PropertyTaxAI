from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('export/<str:query_id>/', views.export_results, name='export_results'),
]
