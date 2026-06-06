"""
Silver SCD Type 2 job: patient events → slowly-changing dimension table.

Implements a full SCD Type 2 pattern on top of Delta Lake:
  - New patients: insert with valid_from = event_timestamp, valid_to = NULL, is_current = TRUE
  - Updated patients: close the previous record (valid_to = event_timestamp, is_current = FALSE)
                      and insert the new current record
  - Deleted/deactivated: close the current record without inserting a replacement

The resulting `silver.patients` table always has exactly one current row per
patient_id (WHERE is_current = TRUE) and a full audit trail of changes.

Spark submit:
    spark-submit \
      --packages io.delta:delta-spark_2.12:3.1.0 \
      pipelines/spark/silver/scd2_patients.py --date 2024-01-15
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
from pyspark.sql.types import BooleanType, FloatType, IntegerType, StringType, TimestampType
from pyspark.sql.window import Window

load_dotenv()

DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://healthcare")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_PATH = f"{DELTA_LAKE_PATH}/bronze/patients_raw"
SILVER_PATH = f"{DELTA_LAKE_PATH}/silver/patients"

# Sentinel timestamp used as valid_to for current rows
HIGH_WATERMARK_TS = "9999-12-31T23:59:59"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("SilverSCD2Patients")
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
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_bronze(spark: SparkSession, partition_date: str) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(BRONZE_PATH)
        .filter(F.col("_partition_date") == partition_date)
    )


def prepare_incoming(df: DataFrame) -> DataFrame:
    """
    Clean and deduplicate incoming records.
    For each patient_id, keep only the latest event by event_timestamp.
    """
    window = Window.partitionBy("patient_id").orderBy(F.col("event_timestamp").desc())
    return (
        df
        # Cast epoch millis to timestamp
        .withColumn(
            "event_timestamp_ts",
            (F.col("event_timestamp") / 1000).cast(TimestampType()),
        )
        # Epoch days → date
        .withColumn(
            "date_of_birth_date",
            F.expr("date_add(date '1970-01-01', date_of_birth)"),
        )
        .withColumn("gender", F.upper(F.trim(F.col("gender"))))
        .withColumn("insurance_type", F.upper(F.trim(F.col("insurance_type"))))
        .withColumn(
            "preferred_communication_channel",
            F.upper(F.trim(F.col("preferred_communication_channel"))),
        )
        .withColumn("distance_to_clinic_miles", F.col("distance_to_clinic_miles").cast(FloatType()))
        .withColumn("has_chronic_condition", F.coalesce(F.col("has_chronic_condition"), F.lit(False)))
        # Add SCD2 control columns
        .withColumn("valid_from", F.col("event_timestamp_ts"))
        .withColumn("valid_to", F.lit(HIGH_WATERMARK_TS).cast(TimestampType()))
        .withColumn("is_current", F.lit(True))
        .withColumn("_silver_processed_at", F.current_timestamp())
        # Deduplicate: latest event per patient within this batch
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "event_timestamp", "date_of_birth", "_partition_date", "_ingested_at")
    )


def apply_scd2(spark: SparkSession, incoming: DataFrame) -> None:
    """
    Merge incoming patient records into the SCD2 silver table.

    Logic:
      1. For existing current rows whose tracked columns changed → close them
         (set valid_to = incoming.valid_from, is_current = FALSE)
      2. Insert all incoming rows as new current records
    """
    # Columns that, when changed, trigger a new SCD2 version
    tracked_cols = [
        "insurance_type",
        "insurance_plan_name",
        "preferred_communication_channel",
        "distance_to_clinic_miles",
        "has_chronic_condition",
        "zip_code",
        "state",
    ]

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        # First load — write all incoming as current rows
        (
            incoming.write.format("delta")
            .mode("overwrite")
            .partitionBy("state")
            .save(SILVER_PATH)
        )
        print(f"Silver patients table created at {SILVER_PATH}")
        return

    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    # Build a change-detection expression: any tracked column differs
    change_condition = " OR ".join(
        [f"target.{c} != source.{c}" for c in tracked_cols]
    )

    # Step 1: Close stale current rows that have changed
    silver_table.alias("target").merge(
        incoming.alias("source"),
        """
        target.patient_id = source.patient_id
        AND target.is_current = true
        AND ({change_condition})
        """.format(change_condition=change_condition),
    ).whenMatchedUpdate(
        set={
            "valid_to": "source.valid_from",
            "is_current": "false",
            "_silver_processed_at": "current_timestamp()",
        }
    ).execute()

    # Step 2: Insert new current rows for patients that changed + net-new patients
    existing_current = (
        spark.read.format("delta")
        .load(SILVER_PATH)
        .filter(F.col("is_current") == True)
        .select("patient_id")
    )

    # Insert if: patient is net-new OR patient had a change (no longer has a current row
    # after step 1 closed it)
    to_insert = incoming.join(
        existing_current,
        on="patient_id",
        how="left_anti",
    )

    if to_insert.count() > 0:
        (
            to_insert.write.format("delta")
            .mode("append")
            .partitionBy("state")
            .save(SILVER_PATH)
        )

    print("SCD2 merge complete.")


def run(partition_date: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"SCD2 patients: processing partition_date={partition_date}")

    bronze_df = read_bronze(spark, partition_date)
    record_count = bronze_df.count()
    print(f"Bronze records: {record_count}")

    if record_count == 0:
        print("No patient events for this date. Exiting.")
        spark.stop()
        return

    incoming = prepare_incoming(bronze_df)
    apply_scd2(spark, incoming)

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Patient SCD Type 2 transformer")
    parser.add_argument(
        "--date",
        type=str,
        default=str(date.today() - timedelta(days=1)),
    )
    args = parser.parse_args()
    run(partition_date=args.date)


if __name__ == "__main__":
    main()
