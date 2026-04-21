import json
import paho.mqtt.client as mqtt
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time
from datetime import datetime

# MQTT settings
BROKER = "mqtt"
PORT = 1883
TOPIC = "air_quality/sensor1"

# Cassandra settings
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE = "air_quality"

# Global session reference
session = None
cluster = None

def create_keyspace_and_table(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS air_quality
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS air_quality.measurements (
            sensor_id text,
            timestamp timestamp,
            pm25 float,
            pm10 float,
            co2 float,
            temperature float,
            humidity float,
            voc float,
            PRIMARY KEY (sensor_id, timestamp)
        )
    """)

def connect_cassandra():
    """Establish Cassandra connection with retry logic"""
    global session, cluster
    
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            create_keyspace_and_table(session)
            session.set_keyspace(KEYSPACE)
            print("Connected to Cassandra")
            return True
        except Exception as e:
            print(f"Failed to connect to Cassandra: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def insert_data(data):
    """Insert data with automatic reconnection if session is lost"""
    global session
    
    # Reconnect if session is None
    if session is None:
        print("Session is None, attempting to reconnect to Cassandra...")
        connect_cassandra()
    
    try:
        session.execute("""
            INSERT INTO measurements (sensor_id, timestamp, pm25, pm10, co2, temperature, humidity, voc)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "sensor1",
            datetime.fromtimestamp(data["timestamp"]),
            data["pm25"],
            data["pm10"],
            data["co2"],
            data["temperature"],
            data["humidity"],
            data["voc"]
        ))
        print("Data inserted into Cassandra")
        return True
    except Exception as e:
        print(f"Failed to insert data: {e}")
        # Reset session so it reconnects on next attempt
        session = None
        return False

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with result code " + str(rc))
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        print(f"Received: {data}")
        insert_data(data)
    except json.JSONDecodeError as e:
        print(f"Error decoding message: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")
        import traceback
        traceback.print_exc()

# Initial Cassandra connection
connect_cassandra()

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