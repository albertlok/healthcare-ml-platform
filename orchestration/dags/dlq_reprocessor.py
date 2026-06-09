"""
dlq_reprocessor DAG

Daily sweep of the Kafka dead-letter queue (DLQ) topics populated by
delta_sink_simple.py when Avro deserialization fails. For each DLQ message:

  1. Decode the JSON envelope and extract the original raw bytes.
  2. Attempt re-deserialization using the *current* schema from Schema Registry.
     This succeeds when the failure was caused by a transient schema bug that
     has since been fixed (schema forward-compatibility, wrong magic-byte strip, etc.).
  3. Correctable: re-produce to the original topic so the next delta_sink run
     picks it up and writes it to the bronze Delta table normally.
  4. Poison: messages that still fail deserialization are written to a Delta
     table at s3://healthcare/dlq_poison/<topic>/ for human review.

Schedule: @daily — DLQ volume is normally low (schema bugs, one-off issues).
  Trigger manually for urgent replay: `airflow dags trigger dlq_reprocessor`.

Owner: data-engineering
Upstream: delta_sink_simple.py → dev.healthcare.dlq.* topics
Downstream: corrected messages → original Kafka topics → bronze Delta tables
"""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from airflow.decorators import dag, task
from airflow.models import Connection
from airflow.utils.dates import days_ago

log = structlog.get_logger()


def _maybe_slack_callback(context: Any) -> None:
    """Fire Slack alert on task failure if the connection is configured."""
    try:
        Connection.get_connection_from_secrets("slack_data_alerts")
    except Exception:
        return
    from airflow.providers.slack.notifications.slack_webhook import (
        send_slack_webhook_notification,
    )

    send_slack_webhook_notification(
        slack_webhook_conn_id="slack_data_alerts",
        text="DAG {{ dag.dag_id }} task {{ task.task_id }} failed at {{ ts }}.",
    )(context)


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": _maybe_slack_callback,
}

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

# DLQ topic → original topic it shadows
DLQ_TOPIC_MAP = {
    "dev.healthcare.dlq.appointment.scheduled": "dev.healthcare.appointment.scheduled",
    "dev.healthcare.dlq.patient.registered": "dev.healthcare.patient.registered",
}

POISON_BASE_PATH = "s3://healthcare/dlq_poison"
# Max messages to consume per DLQ topic per DAG run. Caps the task runtime so
# a burst of DLQ messages doesn't cause the daily run to time out. Increase if
# a large schema incident has backlogged many messages.
MAX_REPROCESS_PER_RUN = int(os.getenv("DLQ_MAX_REPROCESS_PER_RUN", "500"))
DLQ_CONSUMER_GROUP = "dlq-reprocessor-consumer"


@dag(
    dag_id="dlq_reprocessor",
    description="Retry DLQ messages; route correctable to source topic, poison to Delta table",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["healthcare", "kafka", "dlq", "self-healing"],
    doc_md=__doc__,
)
def dlq_reprocessor() -> None:

    @task(task_id="reprocess_appointments_dlq")
    def reprocess_appointments_dlq(**context: Any) -> dict:
        """Retry DLQ messages for the appointment topic."""
        return _reprocess_dlq_topic(
            dlq_topic="dev.healthcare.dlq.appointment.scheduled",
            original_topic="dev.healthcare.appointment.scheduled",
            partition_date=context["ds"],
        )

    @task(task_id="reprocess_patients_dlq")
    def reprocess_patients_dlq(**context: Any) -> dict:
        """Retry DLQ messages for the patient topic."""
        return _reprocess_dlq_topic(
            dlq_topic="dev.healthcare.dlq.patient.registered",
            original_topic="dev.healthcare.patient.registered",
            partition_date=context["ds"],
        )

    @task(task_id="dlq_summary")
    def dlq_summary(appt_result: dict, patient_result: dict, **context: Any) -> dict:
        """Log combined reprocessing stats and alert if poison messages accumulate."""
        total_reprocessed = appt_result.get("reprocessed", 0) + patient_result.get("reprocessed", 0)
        total_poison = appt_result.get("poison", 0) + patient_result.get("poison", 0)

        log.info(
            "dlq_reprocess_summary",
            partition_date=context["ds"],
            total_reprocessed=total_reprocessed,
            total_poison=total_poison,
            appointments=appt_result,
            patients=patient_result,
        )

        if total_poison > 0:
            log.warning(
                "poison_messages_require_review",
                count=total_poison,
                path=POISON_BASE_PATH,
            )
            _maybe_slack_callback(context)

        return {"total_reprocessed": total_reprocessed, "total_poison": total_poison}

    appt = reprocess_appointments_dlq()
    patients = reprocess_patients_dlq()
    dlq_summary(appt, patients)


def _reprocess_dlq_topic(
    dlq_topic: str,
    original_topic: str,
    partition_date: str,
) -> dict:
    """
    Consume up to MAX_REPROCESS_PER_RUN messages from one DLQ topic.

    Each message is a JSON envelope (written by _publish_to_dlq in delta_sink_simple.py)
    containing the original raw bytes and failure metadata. The reprocessor:
      - Decodes raw_value_b64 back to bytes
      - Attempts Avro deserialization with the current Schema Registry schema
      - On success: re-produces to the original topic (at-least-once; delta-sink
        deduplicates at the bronze→silver MERGE)
      - On failure: appends the envelope to the poison Delta table for human review

    Returns a summary dict with consumed / reprocessed / poison counts.
    """
    from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    import pandas as pd
    import pyarrow as pa
    from deltalake import write_deltalake

    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(registry)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": DLQ_CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([dlq_topic])

    # Re-produce corrected messages to the original topic
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    consumed = 0
    reprocessed = 0
    poison_records: list[dict] = []

    try:
        while consumed < MAX_REPROCESS_PER_RUN:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                break  # DLQ is empty for now

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())

            consumed += 1

            try:
                envelope = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                log.error("dlq_envelope_parse_failed", offset=msg.offset(), error=str(exc))
                continue

            raw_bytes = base64.b64decode(envelope.get("raw_value_b64", ""))
            retry_count = envelope.get("retry_count", 0) + 1
            ctx = SerializationContext(original_topic, MessageField.VALUE)

            try:
                # Re-attempt deserialization with the current schema.
                # This succeeds when the failure was caused by a schema that has
                # since been corrected in the Schema Registry.
                deserializer(raw_bytes, ctx)
                producer.produce(original_topic, value=raw_bytes)
                producer.flush()
                reprocessed += 1
                log.info(
                    "dlq_message_reprocessed",
                    original_topic=original_topic,
                    original_offset=envelope.get("kafka_offset"),
                    retry_count=retry_count,
                )
            except Exception as exc:
                # Still cannot deserialize — this is a poison message.
                poison_records.append(
                    {
                        **envelope,
                        "retry_count": retry_count,
                        "last_retry_error": str(exc)[:2000],
                        "last_retry_at": datetime.now(tz=timezone.utc).isoformat(),
                        "poison_partition_date": partition_date,
                    }
                )
                log.warning(
                    "dlq_message_is_poison",
                    original_topic=original_topic,
                    original_offset=envelope.get("kafka_offset"),
                    retry_count=retry_count,
                    error=str(exc)[:200],
                )

        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass  # _NO_OFFSET when queue was already empty

    finally:
        consumer.close()
        producer.flush()

    if poison_records:
        # Persist poison envelopes to Delta for human review.
        # Use the original topic name (dots → underscores) as the table sub-path
        # so different topics don't collide.
        safe_topic = original_topic.replace(".", "_")
        poison_path = f"{POISON_BASE_PATH}/{safe_topic}"
        poison_df = pd.DataFrame(poison_records)
        write_deltalake(
            poison_path,
            pa.Table.from_pandas(poison_df, preserve_index=False),
            storage_options=STORAGE_OPTIONS,
            mode="append",
            schema_mode="merge",
        )
        log.warning(
            "poison_messages_written_to_delta",
            count=len(poison_records),
            path=poison_path,
        )

    return {
        "dlq_topic": dlq_topic,
        "consumed": consumed,
        "reprocessed": reprocessed,
        "poison": len(poison_records),
    }


dlq_reprocessor()
