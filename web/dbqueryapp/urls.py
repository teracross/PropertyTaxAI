from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('export/<str:query_key>/', views.export_results, name='export_results'),
]
