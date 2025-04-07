# innoApp/apps.py

from django.apps import AppConfig
import threading
from . import mqtt_client

class InnoappConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'innoApp'

    def ready(self):
        threading.Thread(target=mqtt_client.start_forwarding, daemon=True).start()

