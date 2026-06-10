import atexit
import logging
import os
import math
import statistics
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, to_timestamp, when
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
FORECAST_TABLE = os.getenv("FORECAST_TABLE", "pm25_forecasts")
PERFORMANCE_METRICS_TABLE = os.getenv("PERFORMANCE_METRICS_TABLE", "performance_metrics")
MODEL_WINDOW_SIZE = int(os.getenv("AI_MODEL_WINDOW_SIZE", "200"))
MIN_TRAINING_SAMPLES = int(os.getenv("AI_MODEL_MIN_TRAINING_SAMPLES", "30"))
MODEL_CONTAMINATION = float(os.getenv("AI_MODEL_CONTAMINATION", "0.08"))
MODEL_RETRAIN_AFTER = int(os.getenv("AI_MODEL_RETRAIN_AFTER", "10"))
FORECAST_WINDOW_SIZE = int(os.getenv("FORECAST_WINDOW_SIZE", "60"))
FORECAST_HORIZON_MINUTES = int(os.getenv("FORECAST_HORIZON_MINUTES", "10"))
FORECAST_HORIZONS_MINUTES = [
    int(item.strip())
    for item in os.getenv("FORECAST_HORIZONS_MINUTES", "10,30,60").split(",")
    if item.strip()
]
FORECAST_MIN_SAMPLES = int(os.getenv("FORECAST_MIN_SAMPLES", "8"))
FORECAST_MAX_DELTA = float(os.getenv("FORECAST_MAX_DELTA", "12.0"))

DEFAULT_PM2_5_RANKS = {
    "Good": 15.0,
    "Moderate": 35.0,
    "Unhealthy": 55.0,
}
DEFAULT_PM1_RANKS = {
    "Good": 10.0,
    "Moderate": 20.0,
    "Unhealthy": 35.0,
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


SensorMetadataKey = Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[float],
    Optional[float],
    Optional[str],
]


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
                    message_id text,
                    pm1 double,
                    pm2_5 double,
                    pm1_status text,
                    pm2_5_status text,
                    status text,
                    location text,
                    published_at timestamp,
                    bridge_received_at timestamp,
                    kafka_sent_at timestamp,
                    processed_at timestamp,
                    stored_at timestamp,
                    latency_ms double,
                    forecast_pm2_5_10m double,
                    forecast_pm2_5_30m double,
                    forecast_pm2_5_60m double,
                    anomaly_score double,
                    is_anomaly boolean,
                    anomaly_reason text,
                    PRIMARY KEY (sensor_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
                """
            )

            for alter_statement in [
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD location text",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD message_id text",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD pm1_status text",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD pm2_5_status text",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD published_at timestamp",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD bridge_received_at timestamp",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD kafka_sent_at timestamp",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD processed_at timestamp",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD stored_at timestamp",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD latency_ms double",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD forecast_pm2_5_10m double",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD forecast_pm2_5_30m double",
                f"ALTER TABLE {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} ADD forecast_pm2_5_60m double",
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

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{FORECAST_TABLE} (
                    sensor_id text,
                    forecast_time timestamp,
                    created_at timestamp,
                    horizon_minutes int,
                    forecast_pm2_5 double,
                    last_pm2_5 double,
                    method text,
                    location text,
                    PRIMARY KEY ((sensor_id), forecast_time)
                ) WITH CLUSTERING ORDER BY (forecast_time DESC)
                """
            )

            session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{PERFORMANCE_METRICS_TABLE} (
                    metric_scope text,
                    recorded_at timestamp,
                    batch_id int,
                    input_rows int,
                    batch_duration_ms double,
                    avg_latency_ms double,
                    p95_latency_ms double,
                    p99_latency_ms double,
                    throughput_rows_per_sec double,
                    PRIMARY KEY ((metric_scope), recorded_at)
                ) WITH CLUSTERING ORDER BY (recorded_at DESC)
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

            for rank_order, (rank_name, max_value) in enumerate(
                DEFAULT_PM1_RANKS.items(),
                start=1,
            ):
                session.execute(
                    f"""
                    INSERT INTO {CASSANDRA_KEYSPACE}.{QUALITY_RANKS_TABLE}
                    (pollutant, rank_order, rank_name, max_value)
                    VALUES ('PM1', {rank_order}, '{rank_name}', {max_value})
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
                f"{ANOMALY_PROFILE_TABLE}, {TRAINING_SAMPLE_TABLE}, {ANOMALY_EVENTS_TABLE}, "
                f"{FORECAST_TABLE}, {PERFORMANCE_METRICS_TABLE}"
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
PM1_GOOD_MAX = float(DEFAULT_PM1_RANKS["Good"])
PM1_MODERATE_MAX = float(DEFAULT_PM1_RANKS["Moderate"])
PM1_UNHEALTHY_MAX = float(DEFAULT_PM1_RANKS["Unhealthy"])

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
        StructField("published_at", StringType(), True),
        StructField("bridge_received_at", StringType(), True),
        StructField("kafka_sent_at", StringType(), True),
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
    .withColumn("published_at", to_timestamp(col("published_at")))
    .withColumn("bridge_received_at", to_timestamp(col("bridge_received_at")))
    .withColumn("kafka_sent_at", to_timestamp(col("kafka_sent_at")))
    .withColumn("processed_at", current_timestamp())
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
        "pm2_5_status",
        when(col("pm2_5") <= lit(GOOD_MAX), lit("Good"))
        .when(col("pm2_5") <= lit(MODERATE_MAX), lit("Moderate"))
        .when(col("pm2_5") <= lit(UNHEALTHY_MAX), lit("Unhealthy"))
        .otherwise(lit("Very Unhealthy")),
    )
    .withColumn(
        "pm1_status",
        when(col("pm1") <= lit(PM1_GOOD_MAX), lit("Good"))
        .when(col("pm1") <= lit(PM1_MODERATE_MAX), lit("Moderate"))
        .when(col("pm1") <= lit(PM1_UNHEALTHY_MAX), lit("Unhealthy"))
        .otherwise(lit("Very Unhealthy")),
    )
    .withColumn("status", col("pm2_5_status"))
    .select(
        "message_id",
        "sensor_id",
        "timestamp",
        "published_at",
        "bridge_received_at",
        "kafka_sent_at",
        "processed_at",
        "pm1",
        "pm2_5",
        "relative_humidity",
        "temperature",
        "pm1_status",
        "pm2_5_status",
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

_model_cache: Dict[str, TrainedAnomalyModel] = {}
_sensor_metadata_cache: Dict[str, SensorMetadataKey] = {}

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
    (sensor_id, timestamp, message_id, pm1, pm2_5, pm1_status, pm2_5_status, status, location,
     published_at, bridge_received_at, kafka_sent_at, processed_at, stored_at, latency_ms,
     forecast_pm2_5_10m, forecast_pm2_5_30m, forecast_pm2_5_60m,
     anomaly_score, is_anomaly, anomaly_reason)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
)
_upsert_sensor_metadata = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_KEYSPACE}.{SENSOR_METADATA_TABLE}
    (sensor_id, sensor_type, firmware, location, latitude, longitude, unit, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
)
_insert_forecast = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_KEYSPACE}.{FORECAST_TABLE}
    (sensor_id, forecast_time, created_at, horizon_minutes, forecast_pm2_5, last_pm2_5, method, location)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
)
_select_recent_measurements_for_forecast = session.prepare(
    f"""
    SELECT timestamp, pm2_5
    FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}
    WHERE sensor_id = ?
    LIMIT {FORECAST_WINDOW_SIZE}
    """
)
_insert_performance_metric = session.prepare(
    f"""
    INSERT INTO {CASSANDRA_KEYSPACE}.{PERFORMANCE_METRICS_TABLE}
    (metric_scope, recorded_at, batch_id, input_rows, batch_duration_ms, avg_latency_ms, p95_latency_ms, p99_latency_ms, throughput_rows_per_sec)
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


def percentile(values: List[float], percentile_value: float) -> Optional[float]:
    if not values:
        return None
    sorted_values = sorted(values)
    index = min(
        len(sorted_values) - 1,
        max(0, math.ceil((percentile_value / 100) * len(sorted_values)) - 1),
    )
    return round(sorted_values[index], 2)


def milliseconds_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    if start is None or end is None:
        return None
    if start.tzinfo is not None:
        start = start.replace(tzinfo=None)
    if end.tzinfo is not None:
        end = end.replace(tzinfo=None)
    return round((end - start).total_seconds() * 1000, 2)


def forecast_pm25(
    sensor_id: str,
    current_timestamp: datetime,
    current_pm2_5: float,
    horizon_minutes: int,
) -> Optional[float]:
    rows = list(session.execute(_select_recent_measurements_for_forecast, [sensor_id]))
    points = [
        (row.timestamp, float(row.pm2_5))
        for row in rows
        if row.timestamp is not None and row.pm2_5 is not None
    ]
    points.append((current_timestamp, float(current_pm2_5)))
    points = sorted(points, key=lambda item: item[0])[-FORECAST_WINDOW_SIZE:]

    if len(points) < FORECAST_MIN_SAMPLES:
        return None

    y_values = [value for _, value in points]
    midpoint = max(1, len(y_values) // 2)
    earlier_median = statistics.median(y_values[:midpoint])
    recent_median = statistics.median(y_values[midpoint:])
    current_value = float(current_pm2_5)

    smoothed_now = (0.55 * current_value) + (0.45 * recent_median)
    trend_delta = recent_median - earlier_median
    horizon_factor = max(1.0, horizon_minutes / 10)
    max_delta = FORECAST_MAX_DELTA * math.sqrt(horizon_factor)
    bounded_delta = max(-max_delta, min(max_delta, trend_delta * horizon_factor))
    forecast_value = smoothed_now + bounded_delta

    lower_bound = max(0.0, recent_median - max_delta)
    upper_bound = min(250.0, recent_median + max_delta)
    forecast_value = max(lower_bound, min(upper_bound, forecast_value))
    return round(forecast_value, 2)


def process_measurement_batch(batch_df, batch_id: int) -> None:
    batch_started = time.perf_counter()
    rows = batch_df.toLocalIterator()
    latencies: List[float] = []
    processed_count = 0
    for row in rows:
        try:
            processed_count += 1
            sensor_id = row["sensor_id"]
            timestamp = row["timestamp"]
            published_at = row["published_at"] or timestamp
            processed_at = row["processed_at"] or datetime.utcnow()
            stored_at = datetime.utcnow()
            latency_ms = milliseconds_between(published_at, stored_at)
            if latency_ms is not None:
                latencies.append(latency_ms)
            upsert_sensor_metadata_if_changed(row)
            profile = load_profile(sensor_id)
            anomaly = score_with_model(sensor_id, row, profile)
            pm25_forecasts = {
                horizon: forecast_pm25(sensor_id, timestamp, row["pm2_5"], horizon)
                for horizon in FORECAST_HORIZONS_MINUTES
            }
            pm25_forecast_10m = pm25_forecasts.get(10)

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
                    row["message_id"],
                    row["pm1"],
                    row["pm2_5"],
                    row["pm1_status"],
                    row["pm2_5_status"],
                    row["status"],
                    row["location"],
                    published_at,
                    row["bridge_received_at"],
                    row["kafka_sent_at"],
                    processed_at,
                    stored_at,
                    latency_ms,
                    pm25_forecasts.get(10),
                    pm25_forecasts.get(30),
                    pm25_forecasts.get(60),
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

            for horizon_minutes, forecast_value in pm25_forecasts.items():
                if forecast_value is None:
                    continue
                forecast_time = datetime.utcfromtimestamp(
                    timestamp.timestamp() + (horizon_minutes * 60)
                )
                session.execute(
                    _insert_forecast,
                    [
                        sensor_id,
                        forecast_time,
                        stored_at,
                        horizon_minutes,
                        forecast_value,
                        row["pm2_5"],
                        "robust_recent_median_trend",
                        row["location"],
                    ],
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

    if processed_count == 0:
        return

    batch_duration_ms = round((time.perf_counter() - batch_started) * 1000, 2)
    throughput = round(processed_count / max(batch_duration_ms / 1000, 0.001), 2)
    avg_latency = round(sum(latencies) / len(latencies), 2) if latencies else None
    session.execute(
        _insert_performance_metric,
        [
            "spark_to_cassandra",
            datetime.utcnow(),
            int(batch_id),
            processed_count,
            batch_duration_ms,
            avg_latency,
            percentile(latencies, 95),
            percentile(latencies, 99),
            throughput,
        ],
    )
    LOGGER.info(
        "Batch %s metrics rows=%s duration_ms=%s throughput=%s avg_latency_ms=%s p95=%s p99=%s",
        batch_id,
        processed_count,
        batch_duration_ms,
        throughput,
        avg_latency,
        percentile(latencies, 95),
        percentile(latencies, 99),
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
