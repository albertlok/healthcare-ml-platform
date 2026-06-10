"""
Synthetic appointment event producer.

Emits realistic (but fully synthetic) healthcare appointment lifecycle events
to the Kafka topic `dev-healthcare-appointment-scheduled` using Avro serialization
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

# structlog gives us structured JSON logs (key=value pairs) instead of plain strings.
# This makes logs much easier to search and parse in tools like Datadog, Splunk, or CloudWatch.
log = structlog.get_logger()
fake = Faker()

# Schema path is resolved relative to this file so the script works from any working directory.
SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "appointment_event.avsc"
# All config comes from env vars so the same code runs in dev, staging, and prod
# without code changes — only the .env file changes.
TOPIC = os.getenv("KAFKA_TOPIC_APPOINTMENTS", "dev-healthcare-appointment-scheduled")
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
# Weights control the realistic distribution of event types in synthetic data.
# ~5% NO_SHOW matches real-world healthcare no-show rates (typically 5-30% depending on setting).
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

# Fixed pools of IDs are generated once at import time and reused across events.
# This creates realistic referential integrity — the same patient shows up in multiple
# appointment events, which is essential for computing per-patient features like
# no_show_rate_90d. Without this, every event would have a unique patient and you'd
# never accumulate history for any individual.
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
        # Timestamps are stored as epoch milliseconds (not seconds) — this is the Kafka/Avro
        # convention and matches what most downstream systems (Spark, DuckDB) expect.
        # To convert back: datetime.fromtimestamp(ts / 1000)
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
            # acks="all" means the broker waits for all in-sync replicas to acknowledge
            # the write before confirming success. Slower than acks=1 but guarantees
            # no data loss if a broker fails immediately after a write.
            "acks": "all",
            # Idempotence + acks=all = exactly-once delivery per partition.
            # Without this, a network timeout could cause the producer to retry and
            # create a duplicate message.
            "enable.idempotence": True,
            # Snappy is a good default: fast compression with ~2-3x size reduction.
            # Use lz4 if CPU is the bottleneck; gzip if storage is the bottleneck.
            "compression.type": "snappy",
            # linger.ms=50 waits up to 50ms to accumulate more messages into a single batch.
            # This trades a small amount of latency for better throughput and compression.
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
            # Keying by patient_id ensures all events for the same patient land on
            # the same Kafka partition. This guarantees ordering per patient and enables
            # efficient compaction if you ever use log compaction on this topic.
            key=event["patient_id"],
            value=avro_serializer(event, ctx),
            on_delivery=_delivery_report,
        )
        sent += 1
        # producer.poll(0) flushes the internal callback queue without blocking.
        # Calling it periodically prevents the internal buffer from growing unbounded
        # when producing thousands of messages without waiting for acknowledgements.
        if sent % 100 == 0:
            producer.poll(0)
    # flush() blocks until all buffered messages are delivered or an error occurs.
    # Always call this before the program exits to avoid silently dropping messages.
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
