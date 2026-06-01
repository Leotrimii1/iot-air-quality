import os
import time

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air-quality")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "air_quality")


def init_cassandra():
    print("Waiting for Cassandra and initializing schema...")
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()

            session.execute(
                f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
                WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
                """
            )

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (
                    sensor_id text,
                    timestamp timestamp,
                    pm1 float,
                    pm2_5 float,
                    status text,
                    location text,
                    PRIMARY KEY (sensor_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
                """
            )

            try:
                session.execute(
                    f"""
                    ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}
                    ADD pm2_5 float
                    """
                )
            except Exception:
                pass

            print(
                f"Cassandra schema initialized: "
                f"{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}"
            )
            cluster.shutdown()
            break
        except Exception as exc:
            print(f"Failed to connect to Cassandra: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


init_cassandra()

spark = (
    SparkSession.builder.appName("AirQualityProcessor")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("timestamp", StringType(), False),
        StructField("pm1", DoubleType(), False),
        StructField("pm2.5", DoubleType(), False),
        StructField("sensor_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .filter(
        col("timestamp").isNotNull()
        & col("pm1").isNotNull()
        & col("`pm2.5`").isNotNull()
    )
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("pm2_5", col("`pm2.5`"))
    .withColumn(
        "sensor_id",
        when(col("sensor_id").isNull(), lit("airgradient_prishtina_001")).otherwise(
            col("sensor_id")
        ),
    )
    .withColumn(
        "location",
        when(col("location").isNull(), lit("Prishtina, Kosovo")).otherwise(
            col("location")
        ),
    )
    .withColumn(
        "status",
        when(col("pm2_5") <= 15, lit("Good"))
        .when(col("pm2_5") <= 35, lit("Moderate"))
        .when(col("pm2_5") <= 55, lit("Unhealthy"))
        .otherwise(lit("Very Unhealthy")),
    )
    .select("sensor_id", "timestamp", "pm1", "pm2_5", "status", "location")
)

query = (
    parsed_df.writeStream.format("org.apache.spark.sql.cassandra")
    .option("keyspace", CASSANDRA_KEYSPACE)
    .option("table", CASSANDRA_TABLE)
    .option("checkpointLocation", "/tmp/spark_checkpoint/air-quality")
    .outputMode("append")
    .start()
)

print(
    f"Streaming from Kafka topic '{KAFKA_TOPIC}' to "
    f"Cassandra table '{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}'..."
)
query.awaitTermination()
