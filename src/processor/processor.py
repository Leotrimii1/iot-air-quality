import atexit
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from alerts import AlarmNotifier


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


@dataclass
class SensorAnomalyProfile:
    sample_count: int = 0
    trained_on_samples: int = 0
    last_trained_at: Optional[datetime] = None
    last_scored_at: Optional[datetime] = None


@dataclass
class TrainedAnomalyModel:
    pipeline: Pipeline
    trained_on_samples: int
    trained_at: datetime


def feature_vector(row) -> List[float]:
    return [
        float(row["pm1"] or 0.0),
        float(row["pm2_5"] or 0.0),
        float(row["relative_humidity"] or 0.0),
        float(row["temperature"] or 0.0),
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
    )
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

notifier = AlarmNotifier(session=session, keyspace=CASSANDRA_KEYSPACE, logger=LOGGER)

_model_cache: Dict[str, TrainedAnomalyModel] = {}

_select_anomaly_profile = session.prepare(
    f"""
    SELECT sensor_id, sample_count, trained_on_samples, last_trained_at, last_scored_at, updated_at
    FROM {CASSANDRA_KEYSPACE}.{ANOMALY_PROFILE_TABLE}
    WHERE sensor_id = ?
    """
)
_upsert_anomaly_profile = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_KEYSPACE}.{ANOMALY_PROFILE_TABLE}
    (sensor_id, sample_count, trained_on_samples, last_trained_at, last_scored_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
    """
)
_select_training_samples = session.prepare(
    f"""
    SELECT pm1, pm2_5, relative_humidity, temperature
    FROM {CASSANDRA_KEYSPACE}.{TRAINING_SAMPLE_TABLE}
    WHERE sensor_id = ?
    LIMIT {MODEL_WINDOW_SIZE}
    """
)
_insert_training_sample = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_KEYSPACE}.{TRAINING_SAMPLE_TABLE}
    (sensor_id, timestamp, pm1, pm2_5, relative_humidity, temperature)
    VALUES (?, ?, ?, ?, ?, ?)
    """
)
_insert_anomaly_event = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_KEYSPACE}.{ANOMALY_EVENTS_TABLE}
    (sensor_id, event_time, anomaly_score, is_anomaly, reason, pm1, pm2_5, relative_humidity, temperature, status, location)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
)
_insert_measurement = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}
    (sensor_id, timestamp, pm1, pm2_5, status, location, anomaly_score, is_anomaly, anomaly_reason)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
)


def build_profile(row) -> SensorAnomalyProfile:
    profile = SensorAnomalyProfile(
        sample_count=int(row.sample_count or 0),
        trained_on_samples=int(row.trained_on_samples or 0),
        last_trained_at=row.last_trained_at,
        last_scored_at=row.last_scored_at,
    )
    return profile


def load_profile(sensor_id: str) -> SensorAnomalyProfile:
    row = session.execute(_select_anomaly_profile, [sensor_id]).one()
    if row is None:
        return SensorAnomalyProfile()
    return build_profile(row)


def persist_profile(sensor_id: str, profile: SensorAnomalyProfile, updated_at: datetime) -> None:
    session.execute(
        _upsert_anomaly_profile,
        [
            sensor_id,
            profile.sample_count,
            profile.trained_on_samples,
            profile.last_trained_at,
            profile.last_scored_at,
            updated_at,
        ],
    )


def load_training_samples(sensor_id: str) -> List[List[float]]:
    rows = session.execute(_select_training_samples, [sensor_id])
    return [
        [
            float(row.pm1 or 0.0),
            float(row.pm2_5 or 0.0),
            float(row.relative_humidity or 0.0),
            float(row.temperature or 0.0),
        ]
        for row in rows
    ]


def train_model(sensor_id: str, clean_sample_count: int) -> Optional[TrainedAnomalyModel]:
    samples = load_training_samples(sensor_id)
    if len(samples) < MIN_TRAINING_SAMPLES:
        return None

    pipeline = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "isolation_forest",
                IsolationForest(
                    n_estimators=100,
                    contamination=MODEL_CONTAMINATION,
                    random_state=42,
                    n_jobs=1,
                ),
            ),
        ]
    )
    pipeline.fit(samples)

    trained = TrainedAnomalyModel(
        pipeline=pipeline,
        trained_on_samples=clean_sample_count,
        trained_at=datetime.utcnow(),
    )
    _model_cache[sensor_id] = trained
    LOGGER.info(
        "Trained IsolationForest for %s on %s samples",
        sensor_id,
        trained.trained_on_samples,
    )
    return trained


def get_trained_model(sensor_id: str, profile: SensorAnomalyProfile) -> Optional[TrainedAnomalyModel]:
    cached = _model_cache.get(sensor_id)
    if cached is not None:
        if (
            profile.trained_on_samples == cached.trained_on_samples
            and profile.last_trained_at == cached.trained_at
        ):
            return cached
        if profile.sample_count - cached.trained_on_samples < MODEL_RETRAIN_AFTER:
            return cached

    trained = train_model(sensor_id, profile.sample_count)
    if trained is not None:
        profile.trained_on_samples = trained.trained_on_samples
        profile.last_trained_at = trained.trained_at
    return trained


def score_with_model(sensor_id: str, row, profile: SensorAnomalyProfile) -> Dict[str, object]:
    model = get_trained_model(sensor_id, profile)
    if model is None:
        status = row["status"] or ""
        pm2_5 = float(row["pm2_5"] or 0.0)
        if status in {"Unhealthy", "Very Unhealthy"} or pm2_5 >= 70.0:
            return {
                "is_anomaly": True,
                "score": 0.9,
                "reason": "Warm-up guard: insufficient clean history for Isolation Forest",
            }
        return {
            "is_anomaly": False,
            "score": 0.0,
            "reason": "Isolation Forest warming up",
        }

    features = [feature_vector(row)]
    decision = float(model.pipeline.decision_function(features)[0])
    prediction = int(model.pipeline.predict(features)[0])
    anomaly_score = round(max(0.0, -decision), 4)
    is_anomaly = prediction == -1
    reason = (
        f"Isolation Forest flagged anomaly (decision={decision:.4f}, "
        f"trained_on={model.trained_on_samples})"
        if is_anomaly
        else f"Isolation Forest normal (decision={decision:.4f}, trained_on={model.trained_on_samples})"
    )
    return {
        "is_anomaly": is_anomaly,
        "score": anomaly_score,
        "reason": reason,
    }


def process_measurement_batch(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    LOGGER.info("Processing %s sensor readings in batch %s", len(rows), batch_id)
    for row in rows:
        try:
            sensor_id = row["sensor_id"]
            timestamp = row["timestamp"]
            profile = load_profile(sensor_id)
            anomaly = score_with_model(sensor_id, row, profile)

            if not anomaly["is_anomaly"]:
                session.execute(
                    _insert_training_sample,
                    [
                        sensor_id,
                        timestamp,
                        row["pm1"],
                        row["pm2_5"],
                        row["relative_humidity"],
                        row["temperature"],
                    ],
                )
                profile.sample_count += 1

            profile.last_scored_at = timestamp

            persist_profile(sensor_id, profile, timestamp)

            session.execute(
                _insert_measurement,
                [
                    sensor_id,
                    timestamp,
                    row["pm1"],
                    row["pm2_5"],
                    row["status"],
                    row["location"],
                    anomaly["score"],
                    anomaly["is_anomaly"],
                    anomaly["reason"],
                ],
            )

            if anomaly["is_anomaly"]:
                session.execute(
                    _insert_anomaly_event,
                    [
                        sensor_id,
                        timestamp,
                        anomaly["score"],
                        anomaly["is_anomaly"],
                        anomaly["reason"],
                        row["pm1"],
                        row["pm2_5"],
                        row["relative_humidity"],
                        row["temperature"],
                        row["status"],
                        row["location"],
                    ],
                )

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


metadata_query = (
    sensor_metadata_df.writeStream.format("org.apache.spark.sql.cassandra")
    .option("keyspace", CASSANDRA_KEYSPACE)
    .option("table", SENSOR_METADATA_TABLE)
    .option("checkpointLocation", "/tmp/spark_checkpoint/sensor-metadata")
    .outputMode("append")
    .start()
)

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
