"""
Kafka → Delta Lake Bronze sink.

Consumes Avro-serialized events from appointment and patient topics and writes
them to the Delta Lake bronze layer on MinIO (S3-compatible). Each topic lands
in its own Delta table partitioned by ingestion date.

Run:
    python delta_sink.py --topics appointments patients --max-messages 10000

Production equivalent:
    - AWS: Kafka Connect S3 Sink Connector or MSK → Kinesis → Lambda → S3 → Delta
    - Azure: Event Hubs Capture → ADLS Gen2 → Delta
    - Databricks: Auto Loader (cloudFiles) reading from S3/ADLS
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

log = structlog.get_logger()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "healthcare-ml-consumer")
DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://healthcare")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

TOPIC_TABLE_MAP = {
    "dev.healthcare.appointment.scheduled": "appointments_raw",
    "dev.healthcare.patient.registered": "patients_raw",
}

SCHEMA_DIR = Path(__file__).parent.parent / "schemas"

# Avro → Spark schema mappings per topic
APPOINTMENT_SPARK_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", LongType(), False),
    StructField("appointment_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("provider_id", StringType(), False),
    StructField("clinic_id", StringType(), False),
    StructField("appointment_type", StringType(), False),
    StructField("scheduled_start_ts", LongType(), False),
    StructField("scheduled_duration_minutes", IntegerType(), False),
    StructField("is_reminder_sent", BooleanType(), False),
    StructField("reminder_channels", StringType(), True),   # serialized JSON array
    StructField("insurance_type", StringType(), True),
    StructField("copay_amount_usd", FloatType(), True),
    StructField("cancellation_reason", StringType(), True),
    StructField("lead_time_hours", IntegerType(), True),
    StructField("metadata_json", StringType(), True),       # serialized map
    StructField("_ingested_at", TimestampType(), False),
    StructField("_partition_date", StringType(), False),
])

PATIENT_SPARK_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", LongType(), False),
    StructField("patient_id", StringType(), False),
    StructField("date_of_birth", IntegerType(), False),     # epoch days
    StructField("gender", StringType(), False),
    StructField("zip_code", StringType(), False),
    StructField("state", StringType(), False),
    StructField("insurance_type", StringType(), False),
    StructField("insurance_plan_name", StringType(), True),
    StructField("preferred_language", StringType(), False),
    StructField("preferred_communication_channel", StringType(), False),
    StructField("distance_to_clinic_miles", FloatType(), True),
    StructField("has_chronic_condition", BooleanType(), False),
    StructField("registration_source", StringType(), False),
    StructField("_ingested_at", TimestampType(), False),
    StructField("_partition_date", StringType(), False),
])

TOPIC_SCHEMA_MAP = {
    "dev.healthcare.appointment.scheduled": APPOINTMENT_SPARK_SCHEMA,
    "dev.healthcare.patient.registered": PATIENT_SPARK_SCHEMA,
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("KafkaDeltaBronzeSink")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )


def _normalize_record(topic: str, record: dict[str, Any]) -> dict[str, Any]:
    """Flatten / serialize complex fields to make the record Spark-writable."""
    now = datetime.now(tz=timezone.utc)
    record["_ingested_at"] = now
    record["_partition_date"] = now.strftime("%Y-%m-%d")

    if "reminder_channels" in record:
        record["reminder_channels"] = json.dumps(record.get("reminder_channels", []))
    if "metadata" in record:
        record["metadata_json"] = json.dumps(record.pop("metadata", {}))

    return record


def write_batch_to_delta(
    spark: SparkSession,
    topic: str,
    records: list[dict[str, Any]],
) -> None:
    """Write a micro-batch of records to the Delta bronze table for this topic."""
    table_name = TOPIC_TABLE_MAP[topic]
    delta_path = f"{DELTA_LAKE_PATH}/bronze/{table_name}"
    schema = TOPIC_SCHEMA_MAP[topic]

    normalized = [_normalize_record(topic, r) for r in records]

    # Build DataFrame from the normalized records
    rows = [tuple(r.get(f.name) for f in schema.fields) for r in normalized]
    df = spark.createDataFrame(rows, schema=schema)

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("_partition_date")
        .option("mergeSchema", "true")
        .save(delta_path)
    )

    log.info(
        "delta_write_complete",
        topic=topic,
        table=table_name,
        records=len(records),
        path=delta_path,
    )


def build_consumer(topics: list[str]) -> tuple[Consumer, dict[str, AvroDeserializer]]:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    deserializers: dict[str, AvroDeserializer] = {}
    for topic in topics:
        schema_name = "appointment_event" if "appointment" in topic else "patient_event"
        schema_str = (SCHEMA_DIR / f"{schema_name}.avsc").read_text()
        deserializers[topic] = AvroDeserializer(schema_registry_client, schema_str)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,   # manual commit after Delta write
            "max.poll.interval.ms": 300000,
        }
    )
    consumer.subscribe(topics)
    return consumer, deserializers


def run(topics: list[str], batch_size: int = 500, max_messages: int = 0) -> None:
    spark = build_spark()
    consumer, deserializers = build_consumer(topics)

    # Graceful shutdown on SIGINT / SIGTERM
    running = [True]

    def _shutdown(signum: int, frame: Any) -> None:
        log.info("shutdown_signal_received")
        running[0] = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    buffers: dict[str, list[dict]] = {t: [] for t in topics}
    total_processed = 0

    log.info("consumer_started", topics=topics, batch_size=batch_size)

    try:
        while running[0]:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush any partial batches that have been sitting > 30s
                for topic, buf in buffers.items():
                    if buf:
                        write_batch_to_delta(spark, topic, buf)
                        buffers[topic] = []
                        consumer.commit(asynchronous=False)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("partition_eof", partition=msg.partition())
                else:
                    raise KafkaException(msg.error())
                continue

            topic = msg.topic()
            ctx = SerializationContext(topic, MessageField.VALUE)
            record = deserializers[topic](msg.value(), ctx)

            buffers[topic].append(record)
            total_processed += 1

            # Write micro-batch when buffer is full
            if len(buffers[topic]) >= batch_size:
                write_batch_to_delta(spark, topic, buffers[topic])
                buffers[topic] = []
                consumer.commit(asynchronous=False)
                log.info("batch_flushed", topic=topic, total_processed=total_processed)

            if max_messages > 0 and total_processed >= max_messages:
                log.info("max_messages_reached", total_processed=total_processed)
                break

    finally:
        # Flush remaining records
        for topic, buf in buffers.items():
            if buf:
                write_batch_to_delta(spark, topic, buf)
        consumer.commit(asynchronous=False)
        consumer.close()
        spark.stop()
        log.info("consumer_stopped", total_processed=total_processed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka → Delta Lake bronze sink")
    parser.add_argument(
        "--topics",
        nargs="+",
        choices=["appointments", "patients"],
        default=["appointments", "patients"],
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-messages", type=int, default=0, help="0 = run forever")
    args = parser.parse_args()

    topic_map = {
        "appointments": "dev.healthcare.appointment.scheduled",
        "patients": "dev.healthcare.patient.registered",
    }
    resolved_topics = [topic_map[t] for t in args.topics]

    run(resolved_topics, batch_size=args.batch_size, max_messages=args.max_messages)


if __name__ == "__main__":
    main()
