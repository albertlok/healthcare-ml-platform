"""
Simplified Kafka → Delta Lake bronze sink (no PySpark required).

Uses confluent_kafka for Avro deserialization and the deltalake Python library
for writing to Delta tables on MinIO. Intended for local dev where Spark is not
available. For production, use delta_sink.py (PySpark) or Kafka Connect S3 Sink.

Usage:
    python delta_sink_simple.py --max-messages 1000
    python delta_sink_simple.py --run-forever
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import pyarrow as pa
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from deltalake import DeltaTable, write_deltalake

log = structlog.get_logger()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

STORAGE_OPTIONS = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "aws_allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
    "region": "us-east-1",
}

TOPICS = {
    "dev.healthcare.appointment.scheduled": "s3://healthcare/bronze/appointments_raw",
    "dev.healthcare.patient.registered": "s3://healthcare/bronze/patients_raw",
}

CONSUMER_GROUP = "delta-sink-simple-consumer"


def _flatten(record: dict[str, Any]) -> dict[str, Any]:
    """Serialize complex Avro types to strings for Delta Lake storage."""
    flat = {}
    for k, v in record.items():
        if isinstance(v, list):
            flat[k] = json.dumps(v)
        elif isinstance(v, dict):
            flat[k] = json.dumps(v)
        else:
            flat[k] = v
    now = datetime.now(tz=timezone.utc)
    flat["ingestion_date"] = now.date().isoformat()
    flat["_ingested_at"] = now.isoformat()
    return flat


def _write_batch(topic: str, records: list[dict[str, Any]]) -> None:
    path = TOPICS[topic]
    df = pd.DataFrame([_flatten(r) for r in records])
    # Cast all object columns that hold None to string so PyArrow doesn't infer Null type
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].where(df[col].notna(), other=None).astype(str).replace("None", pd.NA)
    table = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(
        path,
        table,
        storage_options=STORAGE_OPTIONS,
        mode="append",
        schema_mode="merge",
    )
    log.info("batch_written_to_delta", topic=topic, rows=len(records), path=path)


def run(max_messages: int = 0, batch_size: int = 100) -> None:
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    deserializers = {}
    for topic in TOPICS:
        deserializers[topic] = AvroDeserializer(registry)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(list(TOPICS.keys()))

    running = [True]

    def _stop(sig, frame):  # noqa: ANN001
        running[0] = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    buffers: dict[str, list] = {t: [] for t in TOPICS}
    total = 0

    log.info("delta_sink_started", topics=list(TOPICS.keys()), batch_size=batch_size)

    try:
        while running[0]:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                # Flush partial buffers on idle
                for topic, buf in buffers.items():
                    if buf:
                        _write_batch(topic, buf)
                        buffers[topic] = []
                        consumer.commit(asynchronous=False)
                if max_messages > 0 and total >= max_messages:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            topic = msg.topic()
            ctx = SerializationContext(topic, MessageField.VALUE)
            record = deserializers[topic](msg.value(), ctx)
            buffers[topic].append(record)
            total += 1

            if len(buffers[topic]) >= batch_size:
                _write_batch(topic, buffers[topic])
                buffers[topic] = []
                consumer.commit(asynchronous=False)

            if max_messages > 0 and total >= max_messages:
                break

    finally:
        for topic, buf in buffers.items():
            if buf:
                _write_batch(topic, buf)
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass  # _NO_OFFSET is expected when nothing left to commit
        consumer.close()
        log.info("delta_sink_stopped", total_consumed=total)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka → Delta Lake bronze sink (no Spark)")
    parser.add_argument("--max-messages", type=int, default=0, help="0 = run until empty then stop")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--run-forever", action="store_true")
    args = parser.parse_args()

    max_msg = 0 if args.run_forever else (args.max_messages or 10_000)
    run(max_messages=max_msg, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
