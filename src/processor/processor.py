import atexit
import logging
import os
import time
from typing import Dict, Optional, Tuple

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from alerts import AlarmNotifier
from anomaly_model import AnomalyModelManager


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("air-quality.processor")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air-quality")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "air_quality")
SENSOR_METADATA_TABLE = os.getenv("SENSOR_METADATA_TABLE", "sensor_metadata")
QUALITY_RANKS_TABLE = os.getenv("QUALITY_RANKS_TABLE", "quality_ranks")
ALARM_STATE_TABLE = os.getenv("ALARM_STATE_TABLE", "alarm_state")
ALARM_EVENTS_TABLE = os.getenv("ALARM_EVENTS_TABLE", "alarm_events")
ANOMALY_PROFILE_TABLE = os.getenv("ANOMALY_PROFILE_TABLE", "sensor_ai_profiles")
ANOMALY_EVENTS_TABLE = os.getenv("ANOMALY_EVENTS_TABLE", "anomaly_events")
TRAINING_SAMPLE_TABLE = os.getenv("TRAINING_SAMPLE_TABLE", "sensor_ai_samples")
MODEL_WINDOW_SIZE = int(os.getenv("AI_MODEL_WINDOW_SIZE", "200"))
MIN_TRAINING_SAMPLES = int(os.getenv("AI_MODEL_MIN_TRAINING_SAMPLES", "30"))
MODEL_CONTAMINATION = float(os.getenv("AI_MODEL_CONTAMINATION", "0.08"))
MODEL_RETRAIN_AFTER = int(os.getenv("AI_MODEL_RETRAIN_AFTER", "10"))

DEFAULT_PM2_5_RANKS = {
    "Good": 15.0,
    "Moderate": 35.0,
    "Unhealthy": 55.0,
}


class CassandraContext:
    def __init__(self, cluster: Cluster, session, quality_ranks: Dict[str, float]):
        self.cluster = cluster
        self.session = session
        self.quality_ranks = quality_ranks


SensorMetadataKey = Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[float],
    Optional[float],
    Optional[str],
]


cassandra_context: Optional[CassandraContext] = None


def shutdown_cassandra():
    global cassandra_context
    if cassandra_context is not None:
        try:
            cassandra_context.cluster.shutdown()
        except Exception as exc:
            LOGGER.warning("Failed to shut down Cassandra cleanly: %s", exc)
        cassandra_context = None


atexit.register(shutdown_cassandra)


def init_cassandra() -> CassandraContext:
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
                    anomaly_score double,
                    is_anomaly boolean,
                    anomaly_reason text,
                    PRIMARY KEY (sensor_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
                """
            )

            for alter_statement in [
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD location text",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD anomaly_score double",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD is_anomaly boolean",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD anomaly_reason text",
            ]:
                try:
                    session.execute(alter_statement)
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

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{ALARM_STATE_TABLE} (
                    sensor_id text PRIMARY KEY,
                    last_status text,
                    last_email_sent_at timestamp,
                    last_sms_sent_at timestamp,
                    updated_at timestamp
                )
                """
            )

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{ALARM_EVENTS_TABLE} (
                    sensor_id text,
                    event_time timestamp,
                    notification_channel text,
                    event_type text,
                    status text,
                    pm2_5 double,
                    location text,
                    message text,
                    PRIMARY KEY ((sensor_id), event_time, notification_channel)
                ) WITH CLUSTERING ORDER BY (event_time DESC, notification_channel ASC)
                """
            )

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{ANOMALY_PROFILE_TABLE} (
                    sensor_id text PRIMARY KEY,
                    sample_count int,
                    trained_on_samples int,
                    last_trained_at timestamp,
                    last_scored_at timestamp,
                    updated_at timestamp
                )
                """
            )

            for alter_statement in [
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{ANOMALY_PROFILE_TABLE} ADD trained_on_samples int",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{ANOMALY_PROFILE_TABLE} ADD last_trained_at timestamp",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{ANOMALY_PROFILE_TABLE} ADD last_scored_at timestamp",
            ]:
                try:
                    session.execute(alter_statement)
                except Exception:
                    pass

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{TRAINING_SAMPLE_TABLE} (
                    sensor_id text,
                    timestamp timestamp,
                    pm1 double,
                    pm2_5 double,
                    relative_humidity double,
                    temperature double,
                    PRIMARY KEY (sensor_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
                """
            )

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{ANOMALY_EVENTS_TABLE} (
                    sensor_id text,
                    event_time timestamp,
                    anomaly_score double,
                    is_anomaly boolean,
                    reason text,
                    pm1 double,
                    pm2_5 double,
                    relative_humidity double,
                    temperature double,
                    status text,
                    location text,
                    PRIMARY KEY ((sensor_id), event_time)
                ) WITH CLUSTERING ORDER BY (event_time DESC)
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
                f"{CASSANDRA_TABLE}, {SENSOR_METADATA_TABLE}, {QUALITY_RANKS_TABLE}, "
                f"{ALARM_STATE_TABLE}, {ALARM_EVENTS_TABLE}, "
                f"{ANOMALY_PROFILE_TABLE}, {TRAINING_SAMPLE_TABLE}, {ANOMALY_EVENTS_TABLE}"
            )
            print(f"Loaded PM2.5 quality ranks: {quality_ranks}")
            return CassandraContext(cluster, session, quality_ranks)
        except Exception as exc:
            print(f"Failed to connect to Cassandra: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


cassandra_context = init_cassandra()
session = cassandra_context.session
quality_ranks = cassandra_context.quality_ranks

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
    .withColumn("relative_humidity", col("measurements").getField("relative_humidity"))
    .withColumn("temperature", col("measurements").getField("temperature"))
    .filter(
        col("timestamp").isNotNull()
        & col("sensor_id").isNotNull()
        & col("pm1").isNotNull()
        & col("pm2_5").isNotNull()
        & col("relative_humidity").isNotNull()
        & col("temperature").isNotNull()
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
    .select(
        "sensor_id",
        "timestamp",
        "pm1",
        "pm2_5",
        "relative_humidity",
        "temperature",
        "status",
        "location",
        col("sensor").getField("type").alias("sensor_type"),
        col("sensor").getField("firmware").alias("firmware"),
        col("sensor").getField("latitude").alias("latitude"),
        col("sensor").getField("longitude").alias("longitude"),
        col("sensor").getField("unit").alias("unit"),
    )
)

notifier = AlarmNotifier(session=session, keyspace=CASSANDRA_KEYSPACE, logger=LOGGER)
_sensor_metadata_cache: Dict[str, SensorMetadataKey] = {}
_upsert_sensor_metadata = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_KEYSPACE}.{SENSOR_METADATA_TABLE}
    (sensor_id, sensor_type, firmware, location, latitude, longitude, unit, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
)


def sensor_metadata_key(row) -> SensorMetadataKey:
    return (
        row["sensor_type"],
        row["firmware"],
        row["location"],
        row["latitude"],
        row["longitude"],
        row["unit"],
    )


def upsert_sensor_metadata_if_changed(row) -> None:
    sensor_id = row["sensor_id"]
    metadata_key = sensor_metadata_key(row)
    if _sensor_metadata_cache.get(sensor_id) == metadata_key:
        return

    session.execute(
        _upsert_sensor_metadata,
        [
            sensor_id,
            row["sensor_type"],
            row["firmware"],
            row["location"],
            row["latitude"],
            row["longitude"],
            row["unit"],
            row["timestamp"],
        ],
    )
    _sensor_metadata_cache[sensor_id] = metadata_key


def load_sensor_metadata_cache() -> None:
    rows = session.execute(
        f"""
        SELECT sensor_id, sensor_type, firmware, location, latitude, longitude, unit
        FROM {CASSANDRA_KEYSPACE}.{SENSOR_METADATA_TABLE}
        LIMIT 10000
        """
    )
    for row in rows:
        _sensor_metadata_cache[row.sensor_id] = (
            row.sensor_type,
            row.firmware,
            row.location,
            row.latitude,
            row.longitude,
            row.unit,
        )
    LOGGER.info("Loaded %s sensor metadata records into memory cache", len(_sensor_metadata_cache))


anomaly_model = AnomalyModelManager(
    session=session,
    keyspace=CASSANDRA_KEYSPACE,
    profile_table=ANOMALY_PROFILE_TABLE,
    training_sample_table=TRAINING_SAMPLE_TABLE,
    anomaly_events_table=ANOMALY_EVENTS_TABLE,
    measurement_table=CASSANDRA_TABLE,
    model_window_size=MODEL_WINDOW_SIZE,
    min_training_samples=MIN_TRAINING_SAMPLES,
    model_contamination=MODEL_CONTAMINATION,
    model_retrain_after=MODEL_RETRAIN_AFTER,
    logger=LOGGER,
)


def process_measurement_batch(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    LOGGER.info("Processing %s sensor readings in batch %s", len(rows), batch_id)
    for row in rows:
        try:
            sensor_id = row["sensor_id"]
            timestamp = row["timestamp"]
            upsert_sensor_metadata_if_changed(row)
            profile = anomaly_model.load_profile(sensor_id)
            anomaly = anomaly_model.score_with_model(sensor_id, row, profile)

            if not anomaly["is_anomaly"]:
                anomaly_model.insert_training_sample(row)
                profile.sample_count += 1

            profile.last_scored_at = timestamp

            anomaly_model.persist_profile(sensor_id, profile, timestamp)

            anomaly_model.insert_measurement(row, anomaly)

            if anomaly["is_anomaly"]:
                anomaly_model.insert_anomaly_event(row, anomaly)

                LOGGER.info(
                    "Anomaly detected for %s at %s score=%s reason=%s",
                    sensor_id,
                    timestamp,
                    anomaly["score"],
                    anomaly["reason"],
                )

            notifier.handle_measurement(
                sensor_id=sensor_id,
                timestamp=timestamp,
                pm2_5=row["pm2_5"],
                status=row["status"],
                location=row["location"],
            )
        except Exception as exc:
            LOGGER.warning(
                "Failed to process sensor reading in batch %s: %s",
                batch_id,
                exc,
            )


load_sensor_metadata_cache()

measurements_query = (
    measurements_df.writeStream.foreachBatch(process_measurement_batch)
    .option("checkpointLocation", "/tmp/spark_checkpoint/air-quality-measurements")
    .outputMode("append")
    .start()
)

print(
    f"Streaming from Kafka topic '{KAFKA_TOPIC}' to Cassandra tables "
    f"'{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}' and "
    f"'{CASSANDRA_KEYSPACE}.{SENSOR_METADATA_TABLE}'..."
)
print(
    f"Alarm evaluation enabled with email threshold '{notifier.config.email_min_status}' "
    f"and SMS threshold '{notifier.config.sms_min_status}'"
)
print(
    f"Real-time anomaly detection enabled with profile table '{ANOMALY_PROFILE_TABLE}' "
    f"and anomaly event table '{ANOMALY_EVENTS_TABLE}'"
)

spark.streams.awaitAnyTermination()
