from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, StringType
import os
import time
from cassandra.cluster import Cluster

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air_quality")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = "air_quality"
CASSANDRA_TABLE = "measurements"

def init_cassandra():
    """Ensure keyspace and table exist before Spark starts"""
    print("Waiting for Cassandra and initializing schema...")
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
                WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
            """)
            
            session.execute(f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (
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
            print("Cassandra schema initialized.")
            cluster.shutdown()
            break
        except Exception as e:
            print(f"Failed to connect to Cassandra: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Initialize schema
init_cassandra()

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AirQualityProcessor") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .getOrCreate()

# Define schema for Kafka messages
schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("co2", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("voc", DoubleType(), True)
])

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and transform
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp")))) \
    .withColumn("sensor_id", lit("sensor1")) # For now, hardcode sensor_id

# Write to Cassandra
query = parsed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", CASSANDRA_KEYSPACE) \
    .option("table", CASSANDRA_TABLE) \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .outputMode("append") \
    .start()

print(f"Streaming from Kafka ({KAFKA_TOPIC}) to Cassandra ({CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE})...")
query.awaitTermination()
