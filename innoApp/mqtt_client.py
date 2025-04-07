import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import threading
import time

# === Configuration ===
MQTT_BROKER = "clouddata.jupiterbrothers.com"
MQTT_PORT = 1883
MQTT_TOPIC = "sample_data"

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "iot_raw_data"

# === Kafka Producer Setup ===
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# === Shared Message Buffer ===
latest_message = {"data": None}
lock = threading.Lock()

# === MQTT Callback Functions ===
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"❌ Failed to connect to MQTT Broker, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print(f"📥 MQTT -> Topic: {msg.topic} | Message: {payload}")

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {"raw": payload}

        ts = data.get("ts", "")
        if ts.endswith(":00"):
            with lock:
                latest_message["data"] = data
                print("✅ Message accepted (ts ends in :00)")
        else:
            print("⏩ Skipped (not :00 seconds)")

    except Exception as e:
        print(f"❌ Error processing message: {e}")

# === Kafka Sender Thread ===
def kafka_sender():
    while True:
        time.sleep(60)  # Send once per minute
        with lock:
            if latest_message["data"]:
                try:
                    producer.send(KAFKA_TOPIC, value=latest_message["data"])
                    print(f"📤 Sent to Kafka -> Topic: {KAFKA_TOPIC}")
                    latest_message["data"] = None  # Clear after sending
                except Exception as e:
                    print(f"❌ Kafka Send Error: {e}")
            else:
                print("⚠️ No message to send in this interval")

# === MQTT to Kafka Forwarding ===
def start_forwarding():
    threading.Thread(target=kafka_sender, daemon=True).start()

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    print("🚀 Starting MQTT to Kafka forwarding (only :00 messages every 60s)...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except Exception as e:
        print(f"❌ MQTT Connection Error: {e}")

# === Entry Point ===
if __name__ == "__main__":
    start_forwarding()
