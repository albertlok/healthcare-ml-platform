"""
Synthetic appointment event producer.

Emits realistic (but fully synthetic) healthcare appointment lifecycle events
to the Kafka topic `dev.healthcare.appointment.scheduled` using Avro serialization
and Confluent Schema Registry.

Usage:
    # Continuous stream for 5 minutes
    python appointment_producer.py --duration 300

    # Single batch of 1000 events
    python appointment_producer.py --count 1000 --once
"""

from __future__ import annotations

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

log = structlog.get_logger()
fake = Faker()

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "appointment_event.avsc"
TOPIC = os.getenv("KAFKA_TOPIC_APPOINTMENTS", "dev.healthcare.appointment.scheduled")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

APPOINTMENT_TYPES = [
    "NEW_PATIENT",
    "FOLLOW_UP",
    "ANNUAL_WELLNESS",
    "URGENT_CARE",
    "TELEHEALTH",
    "SPECIALIST_REFERRAL",
    "PROCEDURE",
    "LAB_REVIEW",
]
EVENT_TYPES = ["SCHEDULED", "RESCHEDULED", "CANCELLED", "COMPLETED", "NO_SHOW"]
EVENT_TYPE_WEIGHTS = [0.55, 0.10, 0.12, 0.18, 0.05]

INSURANCE_TYPES = ["COMMERCIAL", "MEDICARE", "MEDICAID", "SELF_PAY", "TRICARE"]
REMINDER_CHANNELS = ["SMS", "EMAIL", "PHONE"]
CANCELLATION_REASONS = [
    "PATIENT_REQUEST",
    "PROVIDER_UNAVAILABLE",
    "EMERGENCY",
    "TRANSPORTATION",
    "WORK_CONFLICT",
    "FORGOT",
    "WEATHER",
]

# Stable pool of 500 patients and 50 providers for referential consistency
_PATIENT_IDS = [str(uuid.uuid4()) for _ in range(500)]
_PROVIDER_IDS = [str(uuid.uuid4()) for _ in range(50)]
_CLINIC_IDS = [str(uuid.uuid4()) for _ in range(10)]


def _build_appointment_event(
    patient_id: str | None = None,
    provider_id: str | None = None,
) -> dict[str, Any]:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS)[0]
    appointment_type = random.choice(APPOINTMENT_TYPES)
    now = datetime.now(tz=timezone.utc)

    # Appointments scheduled 0–60 days from now or in the past
    scheduled_offset_days = random.randint(-30, 60)
    scheduled_start = now + timedelta(days=scheduled_offset_days)
    lead_time_hours = max(0, scheduled_offset_days * 24 + random.randint(-12, 12))

    is_reminder_sent = random.random() > 0.3
    channels = random.sample(REMINDER_CHANNELS, k=random.randint(1, 3)) if is_reminder_sent else []

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": int(now.timestamp() * 1000),
        "appointment_id": str(uuid.uuid4()),
        "patient_id": patient_id or random.choice(_PATIENT_IDS),
        "provider_id": provider_id or random.choice(_PROVIDER_IDS),
        "clinic_id": random.choice(_CLINIC_IDS),
        "appointment_type": appointment_type,
        "scheduled_start_ts": int(scheduled_start.timestamp() * 1000),
        "scheduled_duration_minutes": random.choice([15, 20, 30, 45, 60]),
        "is_reminder_sent": is_reminder_sent,
        "reminder_channels": channels,
        "insurance_type": random.choice(INSURANCE_TYPES) if random.random() > 0.05 else None,
        "copay_amount_usd": round(random.uniform(0, 75), 2) if random.random() > 0.2 else None,
        "cancellation_reason": (
            random.choice(CANCELLATION_REASONS) if event_type in ("CANCELLED", "NO_SHOW") else None
        ),
        "lead_time_hours": lead_time_hours if scheduled_offset_days > 0 else None,
        "metadata": {
            "source_system": random.choice(["ehr_v2", "patient_portal", "call_center"]),
            "ingestion_version": "1.0.0",
        },
    }


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        log.error("delivery_failed", topic=msg.topic(), error=str(err))
    else:
        log.debug(
            "delivery_success",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


def build_producer() -> tuple[Producer, AvroSerializer]:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_str = SCHEMA_PATH.read_text()
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            "linger.ms": 50,
            "batch.size": 65536,
        }
    )
    return producer, avro_serializer


def produce_events(
    producer: Producer,
    avro_serializer: AvroSerializer,
    count: int = 1,
) -> int:
    """Produce `count` appointment events. Returns number of events sent."""
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    sent = 0
    for _ in range(count):
        event = _build_appointment_event()
        producer.produce(
            topic=TOPIC,
            key=event["patient_id"],
            value=avro_serializer(event, ctx),
            on_delivery=_delivery_report,
        )
        sent += 1
        # Poll to trigger delivery callbacks and avoid buffer overflow
        if sent % 100 == 0:
            producer.poll(0)
    producer.flush()
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic appointment event producer")
    parser.add_argument(
        "--duration", type=int, default=0, help="Produce for N seconds (0 = use --count)"
    )
    parser.add_argument(
        "--count", type=int, default=100, help="Events per batch (or total if --once)"
    )
    parser.add_argument("--once", action="store_true", help="Produce one batch and exit")
    parser.add_argument("--rate", type=int, default=10, help="Events per second in streaming mode")
    args = parser.parse_args()

    producer, avro_serializer = build_producer()
    log.info("producer_started", topic=TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS)

    if args.once or args.duration == 0:
        sent = produce_events(producer, avro_serializer, count=args.count)
        log.info("batch_complete", events_sent=sent)
        return

    start = time.monotonic()
    total_sent = 0
    batch_size = max(1, args.rate)

    while time.monotonic() - start < args.duration:
        batch_start = time.monotonic()
        sent = produce_events(producer, avro_serializer, count=batch_size)
        total_sent += sent
        elapsed = time.monotonic() - batch_start
        log.info("batch_produced", total_sent=total_sent, batch_size=sent)

        # Throttle to maintain target rate
        sleep_time = max(0.0, 1.0 - elapsed)
        time.sleep(sleep_time)

    log.info("producer_finished", total_sent=total_sent, duration_s=args.duration)


if __name__ == "__main__":
    main()
