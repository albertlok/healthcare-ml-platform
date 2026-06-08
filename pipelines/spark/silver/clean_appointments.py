"""
Silver transformation job: bronze appointments → cleaned silver Delta table.

Applies:
  - Deduplication on (appointment_id, event_type) using Delta MERGE
  - Null handling and type casting
  - Derived columns (age_at_appointment, lead_time_category, etc.)
  - Data quality tagging (_dq_passed flag)

This job is designed to be run by Airflow on a schedule and is idempotent —
re-running for the same partition_date produces the same output.

Spark submit:
    spark-submit \
      --packages io.delta:delta-spark_2.12:3.1.0 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      pipelines/spark/silver/clean_appointments.py --date 2024-01-15
"""

from __future__ import annotations

import argparse
import os
from datetime import date, timedelta

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, StringType, TimestampType
from pyspark.sql.window import Window

load_dotenv()

DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://healthcare")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_PATH = f"{DELTA_LAKE_PATH}/bronze/appointments_raw"
SILVER_PATH = f"{DELTA_LAKE_PATH}/silver/appointments"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("SilverCleanAppointments")
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
        # shuffle.partitions controls how many files Spark creates during joins/aggregations.
        # The default is 200, which creates 200 tiny files for small datasets — very inefficient.
        # Set it to 2-4x the number of CPU cores for local dev. For a 100GB+ dataset,
        # tune this to roughly dataset_size_MB / 128.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_bronze(spark: SparkSession, partition_date: str) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(BRONZE_PATH)
        .filter(F.col("_partition_date") == partition_date)
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the latest record per (appointment_id, event_type) by kafka_offset."""
    # A window function assigns a row number within each (appointment_id, event_type) group,
    # ordered so that the highest kafka_offset (most recently produced message) gets row 1.
    # We then filter to row 1 only, which is equivalent to "keep the latest duplicate."
    # kafka_offset is a better dedup key than event_timestamp because it's monotonically
    # increasing per partition and set by the broker — it can't be faked or clock-skewed.
    window = Window.partitionBy("appointment_id", "event_type").orderBy(
        F.col("kafka_offset").desc()
    )
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def cast_and_clean(df: DataFrame) -> DataFrame:
    return (
        df
        # Convert epoch milliseconds → proper Spark TimestampType.
        # Division by 1000 converts ms → seconds, then cast() handles the rest.
        # Storing timestamps as proper types (not strings/longs) lets Spark push date
        # predicates down to the file reader, which dramatically speeds up range queries.
        .withColumn(
            "event_timestamp_ts",
            (F.col("event_timestamp") / 1000).cast(TimestampType()),
        )
        .withColumn(
            "scheduled_start_ts",
            (F.col("scheduled_start_ts") / 1000).cast(TimestampType()),
        )
        # Normalize enums to uppercase so downstream joins don't break on case differences.
        # trim() strips accidental whitespace that sometimes leaks from source systems.
        .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
        .withColumn("appointment_type", F.upper(F.trim(F.col("appointment_type"))))
        # coalesce fills nulls with a sentinel value so downstream GROUP BYs don't silently
        # drop null rows. "UNKNOWN" is explicit and queryable; NULL would be invisible.
        .withColumn(
            "insurance_type",
            F.upper(F.trim(F.coalesce(F.col("insurance_type"), F.lit("UNKNOWN")))),
        )
        # Defensive casts: even though the bronze schema specifies types, in practice
        # schema evolution can leave columns as StringType after a merge. These casts
        # make silver the authoritative typed layer.
        .withColumn(
            "scheduled_duration_minutes", F.col("scheduled_duration_minutes").cast(IntegerType())
        )
        .withColumn("copay_amount_usd", F.col("copay_amount_usd").cast(FloatType()))
        .withColumn("lead_time_hours", F.col("lead_time_hours").cast(IntegerType()))
        # coalesce(bool_col, False) makes booleans null-safe: ML models can't handle NULL
        # in feature columns and will either crash or silently drop the row.
        .withColumn("is_reminder_sent", F.coalesce(F.col("is_reminder_sent"), F.lit(False)))
        # Kafka metadata (partition, offset) is only meaningful for the bronze layer.
        # Carrying it forward to silver increases storage cost with no analytical value.
        .drop("kafka_key", "kafka_partition", "kafka_offset", "kafka_timestamp")
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df
        # Is this a no-show or cancellation?
        .withColumn(
            "is_no_show",
            F.col("event_type") == "NO_SHOW",
        )
        .withColumn(
            "is_cancelled",
            F.col("event_type") == "CANCELLED",
        )
        .withColumn(
            "is_completed",
            F.col("event_type") == "COMPLETED",
        )
        # Lead time bucketing for ML features
        .withColumn(
            "lead_time_category",
            F.when(F.col("lead_time_hours") <= 24, "SAME_DAY")
            .when(F.col("lead_time_hours") <= 72, "SHORT")
            .when(F.col("lead_time_hours") <= 168, "MEDIUM")
            .when(F.col("lead_time_hours") > 168, "LONG")
            .otherwise("UNKNOWN"),
        )
        # Day of week and hour for temporal features
        .withColumn("scheduled_day_of_week", F.dayofweek(F.col("scheduled_start_ts")))
        .withColumn("scheduled_hour", F.hour(F.col("scheduled_start_ts")))
        .withColumn("scheduled_month", F.month(F.col("scheduled_start_ts")))
        # Is the appointment in the AM or PM?
        .withColumn(
            "is_morning_appointment",
            F.col("scheduled_hour").between(8, 12),
        )
        # Ingestion metadata
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn(
            "_partition_date",
            F.to_date(F.col("scheduled_start_ts")).cast(StringType()),
        )
    )


def tag_data_quality(df: DataFrame) -> DataFrame:
    """Tag rows with _dq_passed = False if they fail basic quality rules."""
    # We TAG failures rather than DROP them. This preserves the full audit trail in silver:
    # bad rows are still there for investigation, but the dbt staging model filters
    # `WHERE _dq_passed = true` so they never reach gold or the feature store.
    # This "soft fail" pattern lets you diagnose upstream data quality issues without
    # silently losing data.
    return df.withColumn(
        "_dq_passed",
        F.col("appointment_id").isNotNull()
        & F.col("patient_id").isNotNull()
        & F.col("provider_id").isNotNull()
        & F.col("event_type").isin("SCHEDULED", "RESCHEDULED", "CANCELLED", "COMPLETED", "NO_SHOW")
        # A duration of <5 min or >8 hours (480 min) is almost certainly a data error.
        & F.col("scheduled_duration_minutes").between(5, 480),
    )


def merge_to_silver(spark: SparkSession, silver_df: DataFrame, partition_date: str) -> None:
    """Upsert into the silver Delta table on (appointment_id, event_type)."""
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        # First run — bootstrap the table. On subsequent runs, we use MERGE instead of
        # overwrite so that partitions from other dates aren't erased.
        (
            silver_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("_partition_date")
            .save(SILVER_PATH)
        )
        return

    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    # Delta MERGE is the equivalent of SQL MERGE / UPSERT.
    # The match key is (appointment_id, event_type) — one row per appointment event type.
    # This makes the silver layer idempotent: re-running for the same date overwrites
    # existing rows rather than appending duplicates, so Airflow retries are safe.
    (
        silver_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.appointment_id = source.appointment_id "
            "AND target.event_type = source.event_type",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(partition_date: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Processing bronze → silver for partition_date={partition_date}")

    bronze_df = read_bronze(spark, partition_date)
    record_count = bronze_df.count()
    print(f"Bronze records read: {record_count}")

    if record_count == 0:
        print("No records found in bronze for this date. Exiting.")
        spark.stop()
        return

    silver_df = (
        bronze_df.transform(deduplicate)
        .transform(cast_and_clean)
        .transform(add_derived_columns)
        .transform(tag_data_quality)
    )

    dq_fail_count = silver_df.filter(~F.col("_dq_passed")).count()
    print(f"DQ failures: {dq_fail_count} / {record_count}")

    merge_to_silver(spark, silver_df, partition_date)
    print(f"Silver merge complete for {partition_date}")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze → Silver appointment cleaner")
    parser.add_argument(
        "--date",
        type=str,
        default=str(date.today() - timedelta(days=1)),
        help="Partition date to process (YYYY-MM-DD). Defaults to yesterday.",
    )
    args = parser.parse_args()
    run(partition_date=args.date)


if __name__ == "__main__":
    main()
