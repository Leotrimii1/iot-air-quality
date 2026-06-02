#!/bin/sh
set -eu

echo "Waiting for Kafka to become available..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  sleep 2
done

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic air-quality \
  --partitions 1 \
  --replication-factor 1

echo "Kafka topic air-quality is ready."
