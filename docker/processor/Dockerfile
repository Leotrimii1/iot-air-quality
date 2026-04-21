FROM apache/spark:3.5.1

USER root
RUN pip install --no-cache-dir cassandra-driver

WORKDIR /app
COPY src/processor/processor.py .

ENV PYTHONUNBUFFERED=1

# Spark packages for Kafka and Cassandra
# Note: Using version compatible with Spark 3.5.0
CMD ["spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1", \
     "processor.py"]
