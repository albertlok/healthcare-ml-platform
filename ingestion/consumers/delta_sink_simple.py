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

Resilience:
  DlqCircuitBreaker — three-state (CLOSED/OPEN/HALF_OPEN) circuit breaker on the
    DLQ producer.  When the DLQ broker is unhealthy, publishes are skipped and the
    run loop applies exponential backpressure (poll_delay_seconds) so the consumer
    slows ingestion instead of accumulating uncommitted offsets at full throughput.
    After cooldown_seconds the circuit enters HALF_OPEN and allows one probe publish;
    success closes the circuit, failure reopens it with a fresh timer.

  SchemaIncidentThrottle — per-topic consecutive-failure counter.  When a topic
    exceeds `threshold` consecutive deserialization failures the run loop sleeps an
    exponentially growing delay before each poll, preventing the consumer from racing
    through thousands of bad messages during a schema incident.

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
import time
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


class DlqCircuitBreaker:
    """Three-state circuit breaker for the DLQ Kafka producer.

    CLOSED   — normal; all publishes attempted.
    OPEN     — DLQ broker unhealthy; publishes skipped, consumer backpressure applied.
    HALF_OPEN — cooldown elapsed; one probe publish allowed to test recovery.

    The run loop reads poll_delay_seconds before each consumer.poll() so the consumer
    automatically slows ingestion during an outage instead of accumulating uncommitted
    offsets at full throughput.
    """

    _CLOSED = "closed"
    _OPEN = "open"
    _HALF_OPEN = "half_open"

    def __init__(
        self,
        failure_threshold: int = 5,
        cooldown_seconds: float = 60.0,
        max_backpressure_seconds: float = 30.0,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._max_backpressure_seconds = max_backpressure_seconds
        self._consecutive_failures = 0
        self._state = self._CLOSED
        self._opened_at: float | None = None

    @property
    def state(self) -> str:
        if self._state == self._OPEN and self._opened_at is not None:
            if time.monotonic() - self._opened_at >= self._cooldown_seconds:
                return self._HALF_OPEN
        return self._state

    def record_success(self) -> None:
        prev = self._state
        self._state = self._CLOSED
        self._consecutive_failures = 0
        self._opened_at = None
        if prev != self._CLOSED:
            log.info("dlq_circuit_closed", previous_state=prev)

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        current = self.state
        if current == self._HALF_OPEN:
            # Probe failed — reopen with a fresh cooldown timer.
            self._state = self._OPEN
            self._opened_at = time.monotonic()
            log.error("dlq_circuit_probe_failed_reopened", cooldown_seconds=self._cooldown_seconds)
        elif current == self._CLOSED and self._consecutive_failures >= self._failure_threshold:
            self._state = self._OPEN
            self._opened_at = time.monotonic()
            log.error(
                "dlq_circuit_opened",
                consecutive_failures=self._consecutive_failures,
                cooldown_seconds=self._cooldown_seconds,
            )

    @property
    def is_open(self) -> bool:
        """True when DLQ publishes should be skipped (fully OPEN, not probing)."""
        return self.state == self._OPEN

    @property
    def poll_delay_seconds(self) -> float:
        """Seconds the run loop should sleep before the next consumer.poll().

        Zero when the circuit is CLOSED or HALF_OPEN (probe is allowed).
        Grows exponentially with excess failures while the circuit is OPEN.
        """
        if self.state != self._OPEN:
            return 0.0
        excess = max(0, self._consecutive_failures - self._failure_threshold)
        return min(self._max_backpressure_seconds, 1.0 * (2 ** min(excess, 5)))


class SchemaIncidentThrottle:
    """Per-topic backpressure for sustained Avro deserialization failures.

    When a topic's consecutive failure count exceeds `threshold`, delay_for() returns
    an exponentially growing sleep time so the consumer slows down during a schema
    incident rather than racing through thousands of bad messages and overloading the
    DLQ producer.
    """

    def __init__(self, threshold: int = 10, max_delay_seconds: float = 5.0) -> None:
        self._threshold = threshold
        self._max_delay = max_delay_seconds
        self._consecutive: dict[str, int] = {}

    def record_failure(self, topic: str) -> None:
        self._consecutive[topic] = self._consecutive.get(topic, 0) + 1
        if self._consecutive[topic] == self._threshold:
            log.warning(
                "schema_incident_throttle_activated",
                topic=topic,
                consecutive_failures=self._consecutive[topic],
            )

    def record_success(self, topic: str) -> None:
        prev = self._consecutive.get(topic, 0)
        if prev > 0:
            log.info("schema_incident_throttle_cleared", topic=topic, was_consecutive=prev)
        self._consecutive[topic] = 0

    def delay_for(self, topic: str) -> float:
        n = self._consecutive.get(topic, 0)
        if n < self._threshold:
            return 0.0
        excess = n - self._threshold
        return min(self._max_delay, 0.1 * (2 ** min(excess, 6)))


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
    circuit_breaker: DlqCircuitBreaker,
    msg: Any,
    error_type: str,
    error_message: str,
    retry_count: int = 0,
) -> None:
    """Route a failed message to its DLQ topic with a structured JSON envelope.

    The envelope preserves the original raw bytes (base64-encoded) so the
    dlq_reprocessor DAG can retry deserialization after a schema fix without
    needing the original Kafka messages to still be within retention.

    When the DlqCircuitBreaker is OPEN the publish is skipped entirely; the offset
    stays uncommitted so the message replays on restart once the DLQ recovers.
    When the circuit is HALF_OPEN one probe publish is allowed; success closes the
    circuit, failure reopens it with a fresh cooldown timer.

    Args:
        producer: Kafka Producer for publishing to the DLQ topic.
        circuit_breaker: Shared circuit breaker tracking DLQ producer health.
        msg: The original Kafka Message that failed deserialization.
        error_type: Class name of the exception (e.g. "SerializerException").
        error_message: Exception string, truncated to 2 KB to avoid huge payloads.
        retry_count: How many times this specific message has been retried.
    """
    if circuit_breaker.is_open:
        log.warning(
            "dlq_publish_skipped_circuit_open",
            original_topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            backpressure_seconds=circuit_breaker.poll_delay_seconds,
        )
        return

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
        circuit_breaker.record_success()
        log.warning(
            "message_routed_to_dlq",
            dlq_topic=dlq,
            original_topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error_type=error_type,
        )
    except Exception as publish_exc:
        circuit_breaker.record_failure()
        # Offset NOT committed — message replays on restart.
        log.error(
            "dlq_publish_failed_message_will_retry",
            dlq_topic=dlq,
            original_offset=msg.offset(),
            original_error=error_type,
            publish_error=str(publish_exc),
            circuit_state=circuit_breaker.state,
            backpressure_seconds=circuit_breaker.poll_delay_seconds,
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

    # Separate producer for DLQ — kept distinct so DLQ publish failures don't
    # affect the main consumer connection pool.
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Resilience objects — see module docstring for behaviour.
    dlq_circuit = DlqCircuitBreaker()
    schema_throttle = SchemaIncidentThrottle()

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
            # --- Backpressure gate -------------------------------------------
            # Combines DLQ circuit breaker delay (broker outage) and per-topic
            # schema-incident throttle.  Slept in 2-second chunks so SIGINT/SIGTERM
            # is checked frequently; loop re-evaluates both on each wake-up so the
            # delay drains naturally once the underlying condition clears.
            backpressure = max(
                dlq_circuit.poll_delay_seconds,
                max((schema_throttle.delay_for(t) for t in TOPICS), default=0.0),
            )
            if backpressure > 0:
                log.debug(
                    "consumer_backpressure_applied",
                    delay_seconds=backpressure,
                    circuit_state=dlq_circuit.state,
                )
                time.sleep(min(backpressure, 2.0))
                continue
            # ----------------------------------------------------------------

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
                schema_throttle.record_success(topic)
            except Exception as exc:
                # Deserialization failed — route to DLQ instead of dropping.
                # The offset is NOT committed so the message replays on restart.
                schema_throttle.record_failure(topic)
                _publish_to_dlq(dlq_producer, dlq_circuit, msg, type(exc).__name__, str(exc))
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
            dlq_circuit_state=dlq_circuit.state,
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
