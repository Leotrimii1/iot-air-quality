import time
import random
import json
import paho.mqtt.client as mqtt

# MQTT settings
BROKER = "mqtt"
PORT = 1883
TOPIC = "air_quality/sensor1"

def generate_air_quality_data():
    return {
        "timestamp": time.time(),
        "pm25": random.uniform(0, 50),  # PM2.5 in µg/m³
        "pm10": random.uniform(0, 100),  # PM10 in µg/m³
        "co2": random.uniform(400, 2000),  # CO2 in ppm
        "temperature": random.uniform(15, 35),  # Temperature in °C
        "humidity": random.uniform(30, 80),  # Humidity in %
        "voc": random.uniform(0, 10)  # Volatile Organic Compounds index
    }

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with result code " + str(rc))

client = mqtt.Client()
client.on_connect = on_connect

# Connect to MQTT with retry
while True:
    try:
        client.connect(BROKER, PORT, 60)
        print("Connected to MQTT")
        break
    except Exception as e:
        print(f"Failed to connect to MQTT: {e}. Retrying in 5 seconds...")
        time.sleep(5)

client.loop_start()

try:
    while True:
        data = generate_air_quality_data()
        payload = json.dumps(data)
        client.publish(TOPIC, payload)
        print(f"Published: {payload}")
        time.sleep(5)  # Send data every 5 seconds
except KeyboardInterrupt:
    print("Stopping simulator...")
    client.loop_stop()
    client.disconnect()