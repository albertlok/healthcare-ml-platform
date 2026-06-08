"""
Synthetic patient event producer.

Emits patient registration and profile update events to the Kafka topic
`dev.healthcare.patient.registered` using Avro serialization.

Usage:
    python patient_producer.py --count 500 --once
    python patient_producer.py --duration 300
"""

from __future__ import annotations

import argparse
import os
import random
import time
import uuid
from datetime import date, datetime, timezone
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

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "patient_event.avsc"
TOPIC = os.getenv("KAFKA_TOPIC_PATIENTS", "dev.healthcare.patient.registered")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

GENDERS = ["MALE", "FEMALE", "NON_BINARY", "PREFER_NOT_TO_SAY", "UNKNOWN"]
GENDER_WEIGHTS = [0.47, 0.48, 0.02, 0.02, 0.01]

INSURANCE_TYPES = [
    "COMMERCIAL",
    "MEDICARE",
    "MEDICAID",
    "SELF_PAY",
    "WORKERS_COMP",
    "TRICARE",
    "UNKNOWN",
]
INSURANCE_WEIGHTS = [0.45, 0.25, 0.18, 0.07, 0.02, 0.02, 0.01]

COMM_CHANNELS = ["SMS", "EMAIL", "PHONE", "PORTAL"]
COMM_WEIGHTS = [0.35, 0.40, 0.10, 0.15]

EVENT_TYPES = ["REGISTERED", "PROFILE_UPDATED", "INSURANCE_UPDATED", "DEACTIVATED"]
EVENT_WEIGHTS = [0.65, 0.20, 0.13, 0.02]

REG_SOURCES = ["WEB_PORTAL", "MOBILE_APP", "FRONT_DESK", "REFERRAL", "IMPORTED"]
REG_SOURCE_WEIGHTS = [0.30, 0.25, 0.25, 0.15, 0.05]

_EPOCH = date(1970, 1, 1)

US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]


def _date_to_epoch_days(d: date) -> int:
    return (d - _EPOCH).days


def _random_dob() -> date:
    """Return a random DOB for an adult patient (18–95 years old)."""
    today = date.today()
    age_days = random.randint(18 * 365, 95 * 365)
    return today - __import__("datetime").timedelta(days=age_days)


def _build_patient_event(patient_id: str | None = None) -> dict[str, Any]:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    now = datetime.now(tz=timezone.utc)
    insurance = random.choices(INSURANCE_TYPES, weights=INSURANCE_WEIGHTS)[0]

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": int(now.timestamp() * 1000),
        "patient_id": patient_id or str(uuid.uuid4()),
        "date_of_birth": _date_to_epoch_days(_random_dob()),
        "gender": random.choices(GENDERS, weights=GENDER_WEIGHTS)[0],
        "zip_code": fake.zipcode(),
        "state": random.choice(US_STATES),
        "insurance_type": insurance,
        "insurance_plan_name": (
            fake.company() + " " + random.choice(["Health", "Select", "Plus", "Basic"])
            if insurance != "SELF_PAY"
            else None
        ),
        "preferred_language": random.choices(
            ["en", "es", "zh", "fr", "vi", "ko", "ar"],
            weights=[0.75, 0.12, 0.04, 0.03, 0.02, 0.02, 0.02],
        )[0],
        "preferred_communication_channel": random.choices(COMM_CHANNELS, weights=COMM_WEIGHTS)[0],
        "distance_to_clinic_miles": (
            round(random.uniform(0.5, 50.0), 1) if random.random() > 0.05 else None
        ),
        "has_chronic_condition": random.random() > 0.65,
        "registration_source": random.choices(REG_SOURCES, weights=REG_SOURCE_WEIGHTS)[0],
    }


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        log.error("delivery_failed", topic=msg.topic(), error=str(err))
    else:
        log.debug("delivery_success", topic=msg.topic(), offset=msg.offset())


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
        }
    )
    return producer, avro_serializer


def produce_events(
    producer: Producer,
    avro_serializer: AvroSerializer,
    count: int = 1,
) -> int:
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    sent = 0
    for _ in range(count):
        event = _build_patient_event()
        producer.produce(
            topic=TOPIC,
            key=event["patient_id"],
            value=avro_serializer(event, ctx),
            on_delivery=_delivery_report,
        )
        sent += 1
        if sent % 100 == 0:
            producer.poll(0)
    producer.flush()
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic patient event producer")
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--rate", type=int, default=5)
    args = parser.parse_args()

    producer, avro_serializer = build_producer()
    log.info("producer_started", topic=TOPIC)

    if args.once or args.duration == 0:
        sent = produce_events(producer, avro_serializer, count=args.count)
        log.info("batch_complete", events_sent=sent)
        return

    start = time.monotonic()
    total_sent = 0
    while time.monotonic() - start < args.duration:
        sent = produce_events(producer, avro_serializer, count=max(1, args.rate))
        total_sent += sent
        log.info("batch_produced", total_sent=total_sent)
        time.sleep(1.0)

    log.info("producer_finished", total_sent=total_sent)


if __name__ == "__main__":
    main()
