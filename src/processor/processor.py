import os
import time

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air-quality")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "air_quality")
SENSOR_METADATA_TABLE = os.getenv("SENSOR_METADATA_TABLE", "sensor_metadata")
QUALITY_RANKS_TABLE = os.getenv("QUALITY_RANKS_TABLE", "quality_ranks")

DEFAULT_PM2_5_RANKS = {
    "Good": 15.0,
    "Moderate": 35.0,
    "Unhealthy": 55.0,
}


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
                    pm1 double,
                    pm2_5 double,
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
                    ADD location text
                    """
                )
            except Exception:
                pass

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{SENSOR_METADATA_TABLE} (
                    sensor_id text PRIMARY KEY,
                    sensor_type text,
                    firmware text,
                    location text,
                    latitude double,
                    longitude double,
                    unit text,
                    updated_at timestamp
                )
                """
            )

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{QUALITY_RANKS_TABLE} (
                    pollutant text,
                    rank_order int,
                    rank_name text,
                    max_value double,
                    PRIMARY KEY (pollutant, rank_order)
                ) WITH CLUSTERING ORDER BY (rank_order ASC)
                """
            )

            for rank_order, (rank_name, max_value) in enumerate(
                DEFAULT_PM2_5_RANKS.items(),
                start=1,
            ):
                session.execute(
                    f"""
                    INSERT INTO {CASSANDRA_KEYSPACE}.{QUALITY_RANKS_TABLE}
                    (pollutant, rank_order, rank_name, max_value)
                    VALUES ('PM2.5', {rank_order}, '{rank_name}', {max_value})
                    """
                )

            rows = session.execute(
                f"""
                SELECT rank_name, max_value
                FROM {CASSANDRA_KEYSPACE}.{QUALITY_RANKS_TABLE}
                WHERE pollutant = 'PM2.5'
                """
            )
            quality_ranks = {row.rank_name: row.max_value for row in rows}
            for rank_name, max_value in DEFAULT_PM2_5_RANKS.items():
                quality_ranks.setdefault(rank_name, max_value)

            print(
                f"Cassandra schema initialized. Tables: "
                f"{CASSANDRA_TABLE}, {SENSOR_METADATA_TABLE}, {QUALITY_RANKS_TABLE}"
            )
            print(f"Loaded PM2.5 quality ranks: {quality_ranks}")
            cluster.shutdown()
            return quality_ranks
        except Exception as exc:
            print(f"Failed to connect to Cassandra: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


quality_ranks = init_cassandra()
GOOD_MAX = float(quality_ranks["Good"])
MODERATE_MAX = float(quality_ranks["Moderate"])
UNHEALTHY_MAX = float(quality_ranks["Unhealthy"])

spark = (
    SparkSession.builder.appName("AirQualityProcessor")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sensor_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("firmware", StringType(), True),
        StructField("location", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("unit", StringType(), True),
    ]
)

measurements_schema = StructType(
    [
        StructField("pm1", DoubleType(), True),
        StructField("pm2.5", DoubleType(), True),
        StructField("relative_humidity", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("um003", DoubleType(), True),
    ]
)

health_schema = StructType(
    [
        StructField("battery", DoubleType(), True),
        StructField("signal", IntegerType(), True),
        StructField("status", StringType(), True),
    ]
)

schema = StructType(
    [
        StructField("message_id", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("sensor", sensor_schema, True),
        StructField("measurements", measurements_schema, True),
        StructField("health", health_schema, True),
    ]
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

structured_df = (
    raw_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("sensor_id", col("sensor").getField("id"))
    .withColumn("location", col("sensor").getField("location"))
    .withColumn("pm1", col("measurements").getField("pm1"))
    .withColumn("pm2_5", col("measurements").getField("pm2.5"))
    .filter(
        col("timestamp").isNotNull()
        & col("sensor_id").isNotNull()
        & col("pm1").isNotNull()
        & col("pm2_5").isNotNull()
    )
    .withColumn(
        "location",
        when(col("location").isNull(), lit("Unknown")).otherwise(col("location")),
    )
)

measurements_df = (
    structured_df.withColumn(
        "status",
        when(col("pm2_5") <= lit(GOOD_MAX), lit("Good"))
        .when(col("pm2_5") <= lit(MODERATE_MAX), lit("Moderate"))
        .when(col("pm2_5") <= lit(UNHEALTHY_MAX), lit("Unhealthy"))
        .otherwise(lit("Very Unhealthy")),
    )
    .select("sensor_id", "timestamp", "pm1", "pm2_5", "status", "location")
)

sensor_metadata_df = structured_df.select(
    "sensor_id",
    col("sensor").getField("type").alias("sensor_type"),
    col("sensor").getField("firmware").alias("firmware"),
    col("sensor").getField("location").alias("location"),
    col("sensor").getField("latitude").alias("latitude"),
    col("sensor").getField("longitude").alias("longitude"),
    col("sensor").getField("unit").alias("unit"),
    col("timestamp").alias("updated_at"),
)

measurements_query = (
    measurements_df.writeStream.format("org.apache.spark.sql.cassandra")
    .option("keyspace", CASSANDRA_KEYSPACE)
    .option("table", CASSANDRA_TABLE)
    .option("checkpointLocation", "/tmp/spark_checkpoint/air-quality-measurements")
    .outputMode("append")
    .start()
)

metadata_query = (
    sensor_metadata_df.writeStream.format("org.apache.spark.sql.cassandra")
    .option("keyspace", CASSANDRA_KEYSPACE)
    .option("table", SENSOR_METADATA_TABLE)
    .option("checkpointLocation", "/tmp/spark_checkpoint/sensor-metadata")
    .outputMode("append")
    .start()
)

print(
    f"Streaming from Kafka topic '{KAFKA_TOPIC}' to Cassandra tables "
    f"'{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}' and "
    f"'{CASSANDRA_KEYSPACE}.{SENSOR_METADATA_TABLE}'..."
)
spark.streams.awaitAnyTermination()
