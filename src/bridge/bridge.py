import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import time
import os

# MQTT settings
BROKER = os.getenv("MQTT_BROKER", "mqtt")
PORT = int(os.getenv("MQTT_PORT", 1883))
TOPIC = os.getenv("MQTT_TOPIC", "air-quality/sensor1")

# Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air-quality")

# Kafka Producer setup
producer = None

def connect_kafka():
    global producer
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
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
    try:
        data = json.loads(msg.payload.decode())
        print(f"Received from MQTT: {data}")
        
        if producer:
            producer.send(KAFKA_TOPIC, value=data)
            producer.flush()
            print(f"Sent to Kafka topic '{KAFKA_TOPIC}'")
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
