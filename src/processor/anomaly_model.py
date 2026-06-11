import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


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


class AnomalyModelManager:
    def __init__(
        self,
        session,
        keyspace: str,
        profile_table: str,
        training_sample_table: str,
        anomaly_events_table: str,
        measurement_table: str,
        model_window_size: int,
        min_training_samples: int,
        model_contamination: float,
        model_retrain_after: int,
        logger: Optional[logging.Logger] = None,
    ):
        self.session = session
        self.keyspace = keyspace
        self.profile_table = profile_table
        self.training_sample_table = training_sample_table
        self.anomaly_events_table = anomaly_events_table
        self.measurement_table = measurement_table
        self.model_window_size = model_window_size
        self.min_training_samples = min_training_samples
        self.model_contamination = model_contamination
        self.model_retrain_after = model_retrain_after
        self.logger = logger or logging.getLogger(__name__)
        self._model_cache: Dict[str, TrainedAnomalyModel] = {}

        self._select_anomaly_profile = self.session.prepare(
            f"""
            SELECT sensor_id, sample_count, trained_on_samples, last_trained_at, last_scored_at, updated_at
            FROM {self.keyspace}.{self.profile_table}
            WHERE sensor_id = ?
            """
        )
        self._upsert_anomaly_profile = self.session.prepare(
            f"""
            INSERT INTO {self.keyspace}.{self.profile_table}
            (sensor_id, sample_count, trained_on_samples, last_trained_at, last_scored_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        self._select_training_samples = self.session.prepare(
            f"""
            SELECT pm1, pm2_5, relative_humidity, temperature
            FROM {self.keyspace}.{self.training_sample_table}
            WHERE sensor_id = ?
            LIMIT {self.model_window_size}
            """
        )
        self._insert_training_sample = self.session.prepare(
            f"""
            INSERT INTO {self.keyspace}.{self.training_sample_table}
            (sensor_id, timestamp, pm1, pm2_5, relative_humidity, temperature)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        )
        self._insert_anomaly_event = self.session.prepare(
            f"""
            INSERT INTO {self.keyspace}.{self.anomaly_events_table}
            (sensor_id, event_time, anomaly_score, is_anomaly, reason, pm1, pm2_5, relative_humidity, temperature, status, location)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self._insert_measurement = self.session.prepare(
            f"""
            INSERT INTO {self.keyspace}.{self.measurement_table}
            (sensor_id, timestamp, pm1, pm2_5, status, location, anomaly_score, is_anomaly, anomaly_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    def build_profile(self, row) -> SensorAnomalyProfile:
        return SensorAnomalyProfile(
            sample_count=int(row.sample_count or 0),
            trained_on_samples=int(row.trained_on_samples or 0),
            last_trained_at=row.last_trained_at,
            last_scored_at=row.last_scored_at,
        )

    def load_profile(self, sensor_id: str) -> SensorAnomalyProfile:
        row = self.session.execute(self._select_anomaly_profile, [sensor_id]).one()
        if row is None:
            return SensorAnomalyProfile()
        return self.build_profile(row)

    def persist_profile(
        self, sensor_id: str, profile: SensorAnomalyProfile, updated_at: datetime
    ) -> None:
        self.session.execute(
            self._upsert_anomaly_profile,
            [
                sensor_id,
                profile.sample_count,
                profile.trained_on_samples,
                profile.last_trained_at,
                profile.last_scored_at,
                updated_at,
            ],
        )

    def load_training_samples(self, sensor_id: str) -> List[List[float]]:
        rows = self.session.execute(self._select_training_samples, [sensor_id])
        return [
            [
                float(row.pm1 or 0.0),
                float(row.pm2_5 or 0.0),
                float(row.relative_humidity or 0.0),
                float(row.temperature or 0.0),
            ]
            for row in rows
        ]

    def train_model(
        self, sensor_id: str, clean_sample_count: int
    ) -> Optional[TrainedAnomalyModel]:
        samples = self.load_training_samples(sensor_id)
        if len(samples) < self.min_training_samples:
            return None

        pipeline = Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                (
                    "isolation_forest",
                    IsolationForest(
                        n_estimators=100,
                        contamination=self.model_contamination,
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
        self._model_cache[sensor_id] = trained
        self.logger.info(
            "Trained IsolationForest for %s on %s samples",
            sensor_id,
            trained.trained_on_samples,
        )
        return trained

    def get_trained_model(
        self, sensor_id: str, profile: SensorAnomalyProfile
    ) -> Optional[TrainedAnomalyModel]:
        cached = self._model_cache.get(sensor_id)
        if cached is not None:
            if (
                profile.trained_on_samples == cached.trained_on_samples
                and profile.last_trained_at == cached.trained_at
            ):
                return cached
            if profile.sample_count - cached.trained_on_samples < self.model_retrain_after:
                return cached

        trained = self.train_model(sensor_id, profile.sample_count)
        if trained is not None:
            profile.trained_on_samples = trained.trained_on_samples
            profile.last_trained_at = trained.trained_at
        return trained

    def score_with_model(
        self, sensor_id: str, row, profile: SensorAnomalyProfile
    ) -> Dict[str, object]:
        model = self.get_trained_model(sensor_id, profile)
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

    def insert_training_sample(self, row) -> None:
        self.session.execute(
            self._insert_training_sample,
            [
                row["sensor_id"],
                row["timestamp"],
                row["pm1"],
                row["pm2_5"],
                row["relative_humidity"],
                row["temperature"],
            ],
        )

    def insert_measurement(self, row, anomaly: Dict[str, object]) -> None:
        self.session.execute(
            self._insert_measurement,
            [
                row["sensor_id"],
                row["timestamp"],
                row["pm1"],
                row["pm2_5"],
                row["status"],
                row["location"],
                anomaly["score"],
                anomaly["is_anomaly"],
                anomaly["reason"],
            ],
        )

    def insert_anomaly_event(self, row, anomaly: Dict[str, object]) -> None:
        self.session.execute(
            self._insert_anomaly_event,
            [
                row["sensor_id"],
                row["timestamp"],
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
