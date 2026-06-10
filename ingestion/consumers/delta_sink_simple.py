"""
Simplified Kafka → Delta Lake bronze sink (no PySpark required).

Uses confluent_kafka for Avro deserialization and the deltalake Python library
for writing to Delta tables on MinIO. Intended for local dev where Spark is not
available. For production, use delta_sink.py (PySpark) or Kafka Connect S3 Sink.

Dead-letter queue (DLQ):
  Messages that fail Avro deserialization are routed to a DLQ topic
  (dev-healthcare-dlq-<original-topic>) rather than silently dropped.
  The dlq_reprocessor DAG retries correctable messages after schema fixes and
  writes irrecoverable ones to a poison Delta table for human review.

Usage:
    python delta_sink_simple.py --max-messages 1000
    python delta_sink_simple.py --run-forever
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import pyarrow as pa
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from deltalake import write_deltalake

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
    "dev-healthcare-appointment-scheduled": "s3://healthcare/bronze/appointments_raw",
    "dev-healthcare-patient-registered": "s3://healthcare/bronze/patients_raw",
}

CONSUMER_GROUP = "delta-sink-simple-consumer"


def _dlq_topic(original_topic: str) -> str:
    """Map a source topic to its dead-letter queue topic.

    "dev-healthcare-appointment-scheduled"
    → "dev-healthcare-dlq-appointment-scheduled"

    The DLQ topic preserves the environment + domain prefix so Kafka ACLs and
    retention policies can be managed at the {env}-{domain}-dlq-* wildcard level.
    """
    parts = original_topic.split("-", 2)  # ["dev", "healthcare", "appointment-scheduled"]
    return f"{parts[0]}-{parts[1]}-dlq-{parts[2]}"


def _publish_to_dlq(
    producer: Producer,
    msg: Any,
    error_type: str,
    error_message: str,
    retry_count: int = 0,
) -> None:
    """Route a failed message to its DLQ topic with a structured JSON envelope.

    The envelope preserves the original raw bytes (base64-encoded) so the
    dlq_reprocessor DAG can retry deserialization after a schema fix without
    needing the original Kafka messages to still be within retention.

    Args:
        producer: Kafka Producer for publishing to the DLQ topic.
        msg: The original Kafka Message that failed deserialization.
        error_type: Class name of the exception (e.g. "SerializerException").
        error_message: Exception string, truncated to 2 KB to avoid huge payloads.
        retry_count: How many times this specific message has been retried.
    """
    ts_info = msg.timestamp()
    envelope = {
        "original_topic": msg.topic(),
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "kafka_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
        "kafka_timestamp_ms": ts_info[1] if ts_info[0] != 0 else None,
        "raw_value_b64": base64.b64encode(msg.value() or b"").decode("utf-8"),
        "error_type": error_type,
        "error_message": str(error_message)[:2000],
        "failed_at": datetime.now(tz=timezone.utc).isoformat(),
        "retry_count": retry_count,
    }
    dlq = _dlq_topic(msg.topic())
    try:
        producer.produce(dlq, value=json.dumps(envelope).encode("utf-8"))
        producer.flush()
        log.warning(
            "message_routed_to_dlq",
            dlq_topic=dlq,
            original_topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error_type=error_type,
        )
    except Exception as publish_exc:
        # DLQ publish failed — log both errors but don't crash the consumer.
        # The message offset will NOT be committed, so it's retried on restart.
        log.error(
            "dlq_publish_failed_message_will_retry",
            dlq_topic=dlq,
            original_offset=msg.offset(),
            original_error=error_type,
            publish_error=str(publish_exc),
        )


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
        engine="rust",
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

    # Separate producer for DLQ — reuses the same broker connection pool but
    # is kept distinct so DLQ publish failures don't affect the main consumer.
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    running = [True]

    def _stop(sig, frame):  # noqa: ANN001
        running[0] = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    buffers: dict[str, list] = {t: [] for t in TOPICS}
    total = 0
    dlq_counts: dict[str, int] = {t: 0 for t in TOPICS}

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

            try:
                record = deserializers[topic](msg.value(), ctx)
            except Exception as exc:
                # Deserialization failed — route to DLQ instead of dropping.
                # The offset is NOT committed, so the consumer re-sees this
                # message on restart. The DLQ envelope holds the raw bytes for
                # retry analysis by the dlq_reprocessor DAG.
                _publish_to_dlq(dlq_producer, msg, type(exc).__name__, str(exc))
                dlq_counts[topic] = dlq_counts.get(topic, 0) + 1
                continue

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
        dlq_producer.flush()
        log.info(
            "delta_sink_stopped",
            total_consumed=total,
            dlq_totals=dlq_counts,
        )


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
