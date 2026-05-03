import csv
import json
import os
import time
from datetime import datetime

import paho.mqtt.client as mqtt


BROKER = os.getenv("MQTT_BROKER", "mqtt")
PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "air-quality/sensor1")
DATASET_PATH = os.getenv("DATASET_PATH", "/app/prishtina_pm1_pm2.5.csv")
SLEEP_SECONDS = float(os.getenv("SIMULATOR_SLEEP_SECONDS", "1"))


def parse_float(value):
    if value is None or value.strip() == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_timestamp(value):
    if value is None or value.strip() == "":
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def load_dataset(path):
    readings = []
    with open(path, newline="", encoding="utf-8-sig") as dataset:
        reader = csv.DictReader(dataset)
        columns = set(reader.fieldnames or [])

        if {"timestamp", "pm1", "pm25"}.issubset(columns):
            for row in reader:
                timestamp = parse_timestamp(row.get("timestamp"))
                pm1 = parse_float(row.get("pm1"))
                pm25 = parse_float(row.get("pm25"))
                if timestamp is None or pm1 is None or pm25 is None:
                    continue
                readings.append(
                    {
                        "timestamp": timestamp.isoformat(),
                        "pm1": pm1,
                        "pm25": pm25,
                        "sensor_id": row.get("sensor_id") or "airgradient_prishtina_001",
                        "location": row.get("location") or "Prishtina, Kosovo",
                        "latitude": parse_float(row.get("latitude")),
                        "longitude": parse_float(row.get("longitude")),
                    }
                )
        elif {"timestamp", "parameter", "value"}.issubset(columns):
            wide_rows = {}
            for row in reader:
                timestamp = parse_timestamp(row.get("timestamp"))
                parameter = (row.get("parameter") or "").lower().replace(".", "")
                value = parse_float(row.get("value"))
                if timestamp is None or value is None or parameter not in {"pm1", "pm25"}:
                    continue

                key = timestamp.isoformat()
                reading = wide_rows.setdefault(
                    key,
                    {
                        "timestamp": key,
                        "sensor_id": row.get("sensor_id") or "airgradient_prishtina_001",
                        "location": row.get("location") or "Prishtina, Kosovo",
                        "latitude": parse_float(row.get("latitude")),
                        "longitude": parse_float(row.get("longitude")),
                    },
                )
                reading[parameter] = value

            readings = [
                reading
                for reading in wide_rows.values()
                if reading.get("pm1") is not None and reading.get("pm25") is not None
            ]
        else:
            raise ValueError(
                "Dataset must contain either timestamp,pm1,pm25 columns or "
                "timestamp,parameter,value columns."
            )

    readings.sort(key=lambda item: item["timestamp"])
    if not readings:
        raise ValueError("Dataset has no valid PM1/PM2.5 readings after cleaning.")
    return readings


def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")


def connect_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect

    while True:
        try:
            client.connect(BROKER, PORT, 60)
            print(f"Connected to MQTT at {BROKER}:{PORT}")
            return client
        except Exception as exc:
            print(f"Failed to connect to MQTT: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


def main():
    readings = load_dataset(DATASET_PATH)
    print(f"Loaded {len(readings)} cleaned real readings from {DATASET_PATH}")

    client = connect_mqtt()
    client.loop_start()

    try:
        while True:
            for reading in readings:
                payload = json.dumps(reading)
                client.publish(TOPIC, payload)
                print(f"Published to MQTT topic '{TOPIC}': {payload}")
                time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt:
        print("Stopping simulator...")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
