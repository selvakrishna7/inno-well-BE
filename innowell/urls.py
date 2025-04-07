"""
URL configuration for innowell project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from innoApp.views import get_unique_owners,get_energy_stats,get_voltage_stats,get_ghg_emission,get_energy_consumption,get_power_factor,get_current_stats,get_frequency_stats

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/owners/', get_unique_owners, name='get_unique_owners'),
    path('api/get_energy_stats/', get_energy_stats, name='get_energy_stats'),
    path('api/get_ghg_emission/', get_ghg_emission, name='get_ghg_emission'),
    path('api/get_energy_consumption/', get_energy_consumption, name='get_energy_consumption'),
    path('api/get_power_factor/', get_power_factor, name='get_power_factor'),
    path('api/get_voltage_stats/', get_voltage_stats, name='get_voltage_stats'),
    path('api/get_current_stats/', get_current_stats, name='get_current_stats'),
    path('api/get_frequency_stats/', get_frequency_stats, name='get_frequency_stats'),
    
]
