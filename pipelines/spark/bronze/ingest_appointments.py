"""
Bronze ingestion job: Kafka structured streaming → Delta Lake.

Reads appointment events directly from Kafka using Spark Structured Streaming
(alternative to the batch delta_sink consumer) and writes them as-is to the
bronze Delta table. This job is idempotent — re-running from the same Kafka
offsets produces the same result thanks to Delta's transaction log.

Spark submit:
    spark-submit \
      --packages io.delta:delta-spark_2.12:3.1.0,\
                 org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
                 org.apache.kafka:kafka-clients:3.7.0 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      pipelines/spark/bronze/ingest_appointments.py

Production equivalents:
    - Databricks: Databricks Auto Loader (cloudFiles) or Delta Live Tables
    - AWS Glue: Glue Streaming ETL job with Kafka source
    - GCP Dataproc: same script, change fs.gs.* hadoop configs
"""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StringType

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://healthcare")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

TOPIC = "dev.healthcare.appointment.scheduled"
BRONZE_PATH = f"{DELTA_LAKE_PATH}/bronze/appointments_raw"
CHECKPOINT_PATH = f"{DELTA_LAKE_PATH}/_checkpoints/bronze_appointments"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("BronzeAppointmentIngestion")
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
        # Delta optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def fetch_avro_schema(topic: str) -> str:
    """Fetch the latest Avro schema for a topic from Schema Registry."""
    import requests

    subject = f"{topic}-value"
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()["schema"]


def run() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    avro_schema = fetch_avro_schema(TOPIC)

    # Read from Kafka as a structured streaming source
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", "spark-bronze-appointments")
        # Skip the first 5 bytes (Confluent wire format magic byte + schema ID)
        .load()
        .select(
            F.col("key").cast(StringType()).alias("kafka_key"),
            F.col("value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
    )

    # Strip Confluent magic byte (1 byte) + schema ID (4 bytes) before Avro parsing
    stripped = raw_stream.withColumn("avro_value", F.expr("substring(value, 6, length(value) - 5)"))

    # Deserialize Avro payload
    parsed = stripped.select(
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        from_avro(F.col("avro_value"), avro_schema).alias("data"),
    ).select(
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "data.*",
    )

    # Add ingestion metadata columns
    enriched = (
        parsed.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_partition_date", F.to_date(F.current_timestamp()))
        .withColumn(
            "event_timestamp_ts",
            (F.col("event_timestamp") / 1000).cast("timestamp"),
        )
        .withColumn(
            "scheduled_start_ts_ts",
            (F.col("scheduled_start_ts") / 1000).cast("timestamp"),
        )
    )

    # Write to Delta bronze — append-only, partition by ingestion date
    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .partitionBy("_partition_date")
        .trigger(processingTime="60 seconds")  # micro-batch every 60s
        .start(BRONZE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
