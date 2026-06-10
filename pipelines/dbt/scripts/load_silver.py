"""
Load silver Delta tables from MinIO into DuckDB so dbt can run locally.

This mirrors what the Airflow dbt_run_gold task does before invoking dbt.
Run from the repo root or via `make dbt-run`.

Usage:
    python pipelines/dbt/scripts/load_silver.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import structlog
from deltalake import DeltaTable

log = structlog.get_logger()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(Path(__file__).parent.parent / "data" / "dev.duckdb"))

STORAGE_OPTIONS = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "aws_allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
    "region": "us-east-1",
}

SILVER_TABLES = {
    "appointments": "s3://healthcare/silver/appointments",
    "patients": "s3://healthcare/silver/patients",
}


def main() -> None:
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

    for table_name, delta_path in SILVER_TABLES.items():
        try:
            df = DeltaTable(delta_path, storage_options=STORAGE_OPTIONS).to_pandas()
        except Exception as exc:
            log.error(
                "silver_table_load_failed",
                table=table_name,
                path=delta_path,
                error=str(exc),
            )
            conn.close()
            sys.exit(
                f"ERROR: Could not read {delta_path}\n"
                "Run `make kafka-sink` or `make seed-bronze` first to populate silver data.\n"
                f"Detail: {exc}"
            )

        conn.execute(f"CREATE OR REPLACE TABLE silver.{table_name} AS SELECT * FROM df")
        log.info("silver_loaded", table=f"silver.{table_name}", rows=len(df))

    conn.close()


if __name__ == "__main__":
    main()
