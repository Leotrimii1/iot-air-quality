from __future__ import annotations

import base64
import logging
import os
import smtplib
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Optional


STATUS_PRIORITY = {
    "Good": 0,
    "Moderate": 1,
    "Unhealthy": 2,
    "Very Unhealthy": 3,
}


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(name: str, default: str = "") -> list[str]:
    value = os.getenv(name, default)
    return [item.strip() for item in value.split(",") if item.strip()]


def _coerce_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def status_rank(status: Optional[str]) -> int:
    return STATUS_PRIORITY.get(status or "", -1)


@dataclass(frozen=True)
class AlertConfig:
    smtp_host: str = os.getenv("ALERT_SMTP_HOST", "mailpit")
    smtp_port: int = int(os.getenv("ALERT_SMTP_PORT", "1025"))
    smtp_username: str = os.getenv("ALERT_SMTP_USERNAME", "")
    smtp_password: str = os.getenv("ALERT_SMTP_PASSWORD", "")
    smtp_use_tls: bool = _env_bool("ALERT_SMTP_USE_TLS", False)
    email_from: str = os.getenv("ALERT_EMAIL_FROM", "alerts@airwatch-prishtina.com")
    email_to: tuple[str, ...] = tuple(_env_list("ALERT_EMAIL_TO", "operations@airwatch-prishtina.com"))
    email_min_status: str = os.getenv("ALERT_EMAIL_MIN_STATUS", "Unhealthy")
    sms_provider: str = os.getenv("ALERT_SMS_PROVIDER", "").strip().lower()
    sms_from: str = os.getenv("ALERT_SMS_FROM", "")
    sms_to: tuple[str, ...] = tuple(_env_list("ALERT_SMS_TO"))
    sms_min_status: str = os.getenv("ALERT_SMS_MIN_STATUS", "Unhealthy")
    email_cooldown_seconds: int = int(os.getenv("ALERT_EMAIL_COOLDOWN_SECONDS", "1800"))
    sms_cooldown_seconds: int = int(os.getenv("ALERT_SMS_COOLDOWN_SECONDS", "3600"))
    send_recovery_email: bool = _env_bool("ALERT_SEND_RECOVERY_EMAIL", True)
    twilio_account_sid: str = os.getenv("TWILIO_ACCOUNT_SID", "")
    twilio_auth_token: str = os.getenv("TWILIO_AUTH_TOKEN", "")

    @classmethod
    def from_env(cls) -> "AlertConfig":
        return cls()


@dataclass
class SensorAlertState:
    last_status: Optional[str]
    last_email_sent_at: Optional[datetime]
    last_sms_sent_at: Optional[datetime]
    updated_at: Optional[datetime]


class AlarmNotifier:
    def __init__(
        self,
        session,
        keyspace: str,
        logger: Optional[logging.Logger] = None,
        config: Optional[AlertConfig] = None,
    ) -> None:
        self.session = session
        self.keyspace = keyspace
        self.logger = logger or logging.getLogger(__name__)
        self.config = config or AlertConfig.from_env()

        self.email_threshold = status_rank(self.config.email_min_status)
        self.sms_threshold = status_rank(self.config.sms_min_status)

        self._select_state = self.session.prepare(
            f"""
            SELECT last_status, last_email_sent_at, last_sms_sent_at, updated_at
            FROM {self.keyspace}.alarm_state
            WHERE sensor_id = ?
            """
        )
        self._upsert_state = self.session.prepare(
            f"""
            INSERT INTO {self.keyspace}.alarm_state
            (sensor_id, last_status, last_email_sent_at, last_sms_sent_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """
        )
        for alter_statement in [
            f"ALTER TABLE {self.keyspace}.alarm_events ADD pm1 double",
            f"ALTER TABLE {self.keyspace}.alarm_events ADD pollutant text",
        ]:
            try:
                self.session.execute(alter_statement)
            except Exception:
                pass
        self._insert_event = self.session.prepare(
            f"""
            INSERT INTO {self.keyspace}.alarm_events
            (sensor_id, event_time, notification_channel, event_type, status, pm2_5, location, message, pm1, pollutant)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

    def handle_measurement(
        self,
        sensor_id: str,
        timestamp: Optional[datetime],
        pm2_5: Optional[float],
        status: Optional[str],
        location: Optional[str],
        pm1: Optional[float] = None,
        pm1_status: Optional[str] = None,
        pm2_5_status: Optional[str] = None,
    ) -> None:
        if not sensor_id:
            return

        self._handle_pollutant(
            sensor_id=sensor_id,
            state_id=f"{sensor_id}:PM2.5",
            timestamp=timestamp,
            pollutant="PM2.5",
            value=pm2_5,
            status=pm2_5_status or status,
            location=location,
            pm1=pm1,
            pm2_5=pm2_5,
        )
        self._handle_pollutant(
            sensor_id=sensor_id,
            state_id=f"{sensor_id}:PM1",
            timestamp=timestamp,
            pollutant="PM1",
            value=pm1,
            status=pm1_status,
            location=location,
            pm1=pm1,
            pm2_5=pm2_5,
        )

    def _handle_pollutant(
        self,
        sensor_id: str,
        state_id: str,
        timestamp: Optional[datetime],
        pollutant: str,
        value: Optional[float],
        status: Optional[str],
        location: Optional[str],
        pm1: Optional[float],
        pm2_5: Optional[float],
    ) -> None:
        if status is None or value is None:
            return

        now = _coerce_utc(timestamp) or datetime.now(timezone.utc)
        current_rank = status_rank(status)
        current_location = location or "Unknown"
        previous = self._load_state(state_id)

        if current_rank < self.email_threshold:
            if (
                self.config.send_recovery_email
                and previous is not None
                and previous.last_status is not None
                and status_rank(previous.last_status) >= self.email_threshold
            ):
                subject = f"AirWatch Prishtina recovery: {pollutant} at {current_location}"
                message = self._format_message(
                    sensor_id=sensor_id,
                    location=current_location,
                    pollutant=pollutant,
                    value=value,
                    status=status,
                    pm2_5=pm2_5,
                    pm1=pm1,
                    timestamp=now,
                    event_type="recovery",
                )
                if self._send_email(subject, message):
                    self._record_event(
                        sensor_id=sensor_id,
                        event_time=now,
                        channel=f"email_{pollutant.lower().replace('.', '_')}",
                        event_type="recovery",
                        status=status,
                        pm2_5=pm2_5,
                        pm1=pm1,
                        pollutant=pollutant,
                        location=current_location,
                        message=message,
                    )

            self._save_state(
                sensor_id=state_id,
                last_status=status,
                last_email_sent_at=previous.last_email_sent_at if previous else None,
                last_sms_sent_at=previous.last_sms_sent_at if previous else None,
                updated_at=now,
            )
            return

        should_send_email = self._should_send(
            current_rank=current_rank,
            current_status=status,
            threshold=self.email_threshold,
            previous=previous,
            last_sent_at=previous.last_email_sent_at if previous else None,
            cooldown_seconds=self.config.email_cooldown_seconds,
        )
        should_send_sms = self._should_send(
            current_rank=current_rank,
            current_status=status,
            threshold=self.sms_threshold,
            previous=previous,
            last_sent_at=previous.last_sms_sent_at if previous else None,
            cooldown_seconds=self.config.sms_cooldown_seconds,
        )

        if should_send_email:
            subject = f"AirWatch Prishtina alert: {pollutant} {status} at {current_location}"
            message = self._format_message(
                sensor_id=sensor_id,
                location=current_location,
                pollutant=pollutant,
                value=value,
                status=status,
                pm2_5=pm2_5,
                pm1=pm1,
                timestamp=now,
                event_type="alert",
            )
            if self._send_email(subject, message):
                self._record_event(
                    sensor_id=sensor_id,
                    event_time=now,
                    channel=f"email_{pollutant.lower().replace('.', '_')}",
                    event_type="alert",
                    status=status,
                    pm2_5=pm2_5,
                    pm1=pm1,
                    pollutant=pollutant,
                    location=current_location,
                    message=message,
                )

        if should_send_sms:
            sms_message = self._format_sms_message(
                sensor_id=sensor_id,
                location=current_location,
                pollutant=pollutant,
                value=value,
                status=status,
                pm2_5=pm2_5,
            )
            if self._send_sms(sms_message):
                self._record_event(
                    sensor_id=sensor_id,
                    event_time=now,
                    channel=f"sms_{pollutant.lower().replace('.', '_')}",
                    event_type="alert",
                    status=status,
                    pm2_5=pm2_5,
                    pm1=pm1,
                    pollutant=pollutant,
                    location=current_location,
                    message=sms_message,
                )

        self._save_state(
            sensor_id=state_id,
            last_status=status,
            last_email_sent_at=now if should_send_email else (previous.last_email_sent_at if previous else None),
            last_sms_sent_at=now if should_send_sms else (previous.last_sms_sent_at if previous else None),
            updated_at=now,
        )

    def _load_state(self, sensor_id: str) -> Optional[SensorAlertState]:
        row = self.session.execute(self._select_state, [sensor_id]).one()
        if row is None:
            return None
        return SensorAlertState(
            last_status=row.last_status,
            last_email_sent_at=_coerce_utc(row.last_email_sent_at),
            last_sms_sent_at=_coerce_utc(row.last_sms_sent_at),
            updated_at=_coerce_utc(row.updated_at),
        )

    def _save_state(
        self,
        sensor_id: str,
        last_status: Optional[str],
        last_email_sent_at: Optional[datetime],
        last_sms_sent_at: Optional[datetime],
        updated_at: datetime,
    ) -> None:
        self.session.execute(
            self._upsert_state,
            [
                sensor_id,
                last_status,
                _coerce_utc(last_email_sent_at),
                _coerce_utc(last_sms_sent_at),
                _coerce_utc(updated_at),
            ],
        )

    def _record_event(
        self,
        sensor_id: str,
        event_time: datetime,
        channel: str,
        event_type: str,
        status: Optional[str],
        pm2_5: Optional[float],
        pm1: Optional[float],
        pollutant: Optional[str],
        location: Optional[str],
        message: str,
    ) -> None:
        self.session.execute(
            self._insert_event,
            [
                sensor_id,
                _coerce_utc(event_time),
                channel,
                event_type,
                status,
                pm2_5,
                location or "Unknown",
                message,
                pm1,
                pollutant,
            ],
        )

    def _should_send(
        self,
        current_rank: int,
        current_status: str,
        threshold: int,
        previous: Optional[SensorAlertState],
        last_sent_at: Optional[datetime],
        cooldown_seconds: int,
    ) -> bool:
        if current_rank < threshold:
            return False
        if previous is None:
            return True
        if status_rank(previous.last_status) < threshold:
            return True
        if previous.last_status != current_status:
            return True

        last_sent_at = _coerce_utc(last_sent_at)
        if last_sent_at is None:
            return True
        return datetime.now(timezone.utc) - last_sent_at >= timedelta(seconds=cooldown_seconds)

    def _send_email(self, subject: str, body: str) -> bool:
        if not self.config.email_to:
            self.logger.info("Skipping email alert because ALERT_EMAIL_TO is empty.")
            return False

        message = EmailMessage()
        message["From"] = self.config.email_from
        message["To"] = ", ".join(self.config.email_to)
        message["Subject"] = subject
        message.set_content(body)

        try:
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port, timeout=10) as smtp:
                if self.config.smtp_use_tls:
                    context = ssl.create_default_context()
                    smtp.starttls(context=context)
                if self.config.smtp_username:
                    smtp.login(self.config.smtp_username, self.config.smtp_password)
                smtp.send_message(message)
            self.logger.info("Sent email alert to %s", ", ".join(self.config.email_to))
            return True
        except Exception as exc:
            self.logger.warning("Failed to send email alert: %s", exc)
            return False

    def _send_sms(self, body: str) -> bool:
        if not self.config.sms_to:
            self.logger.info("Skipping SMS alert because ALERT_SMS_TO is empty.")
            return False

        if self.config.sms_provider != "twilio":
            self.logger.info(
                "Skipping SMS alert because ALERT_SMS_PROVIDER is not set to twilio."
            )
            return False

        if not all(
            [
                self.config.twilio_account_sid,
                self.config.twilio_auth_token,
                self.config.sms_from,
            ]
        ):
            self.logger.warning(
                "Skipping SMS alert because Twilio credentials or ALERT_SMS_FROM are missing."
            )
            return False

        auth = base64.b64encode(
            f"{self.config.twilio_account_sid}:{self.config.twilio_auth_token}".encode("utf-8")
        ).decode("ascii")
        url = (
            f"https://api.twilio.com/2010-04-01/Accounts/"
            f"{self.config.twilio_account_sid}/Messages.json"
        )

        try:
            for recipient in self.config.sms_to:
                payload = urllib.parse.urlencode(
                    {
                        "From": self.config.sms_from,
                        "To": recipient,
                        "Body": body,
                    }
                ).encode("utf-8")
                request = urllib.request.Request(url, data=payload, method="POST")
                request.add_header("Authorization", f"Basic {auth}")
                request.add_header("Content-Type", "application/x-www-form-urlencoded")
                with urllib.request.urlopen(request, timeout=10):
                    pass
            self.logger.info("Sent SMS alert to %s", ", ".join(self.config.sms_to))
            return True
        except urllib.error.URLError as exc:
            self.logger.warning("Failed to send SMS alert: %s", exc)
            return False
        except Exception as exc:
            self.logger.warning("Unexpected SMS alert failure: %s", exc)
            return False

    def _format_message(
        self,
        sensor_id: str,
        location: str,
        pollutant: str,
        value: float,
        status: str,
        pm2_5: Optional[float],
        pm1: Optional[float],
        timestamp: datetime,
        event_type: str,
    ) -> str:
        lines = [
            f"AirWatch Prishtina {event_type} notice",
            f"Monitoring area: {location}",
            f"Sensor ID: {sensor_id}",
            f"Pollutant: {pollutant}",
            f"Air quality status: {status}",
            f"{pollutant} reading: {value:.2f} ug/m3",
        ]
        if pm1 is not None:
            lines.append(f"PM1 latest: {pm1:.2f} ug/m3")
        if pm2_5 is not None:
            lines.append(f"PM2.5 latest: {pm2_5:.2f} ug/m3")
        lines.extend(
            [
                f"Checked at: {timestamp.isoformat()}",
                "Recommended action: review the dashboard and confirm whether the reading is temporary or persistent.",
            ]
        )
        return "\n".join(lines) + "\n"

    def _format_sms_message(
        self,
        sensor_id: str,
        location: str,
        pollutant: str,
        value: float,
        status: str,
        pm2_5: Optional[float],
    ) -> str:
        return (
            f"AirWatch Prishtina alert: {pollutant} {status} at {location} "
            f"(sensor {sensor_id}, {pollutant} {value:.2f})."
        )
