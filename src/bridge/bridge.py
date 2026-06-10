import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import time
import os
from datetime import datetime, timezone

# MQTT settings
BROKER = os.getenv("MQTT_BROKER", "mqtt")
PORT = int(os.getenv("MQTT_PORT", 1883))
TOPIC = os.getenv("MQTT_TOPIC", "air-quality/sensor1")

# Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air-quality")

# Kafka Producer setup
producer = None
message_count = 0
last_log_time = time.time()


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def connect_kafka():
    global producer
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x, separators=(",", ":")).encode("utf-8"),
                linger_ms=20,
                batch_size=65536,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=5,
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return True
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with result code " + str(rc))
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    global message_count
    global last_log_time

    try:
        data = json.loads(msg.payload.decode())
        data["bridge_received_at"] = utc_now_iso()
        
        if producer:
            data["kafka_sent_at"] = utc_now_iso()
            producer.send(KAFKA_TOPIC, value=data)
            message_count += 1

            now = time.time()
            if now - last_log_time >= 5:
                producer.flush(timeout=2)
                rate = message_count / max(now - last_log_time, 1)
                print(
                    f"Bridge forwarding rate: {rate:.1f} msg/sec "
                    f"to Kafka topic={KAFKA_TOPIC}"
                )
                message_count = 0
                last_log_time = now
    except Exception as e:
        print(f"Error bridging message: {e}")

# Initialize Kafka connection
connect_kafka()

# MQTT Client setup
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT with retry
while True:
    try:
        mqtt_client.connect(BROKER, PORT, 60)
        print("Connected to MQTT")
        break
    except Exception as e:
        print(f"Failed to connect to MQTT: {e}. Retrying in 5 seconds...")
        time.sleep(5)

mqtt_client.loop_forever()
