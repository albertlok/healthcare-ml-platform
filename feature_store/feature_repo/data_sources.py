"""
Feast data source definitions.

Each source points to the dbt-generated gold layer exported as Parquet.
In dev: parquet files under feature_store/feature_repo/data/.
In prod: swap FileSource for SnowflakeSource / BigQuerySource / RedshiftSource.

The DuckDB offline store (feature_store.yaml) reads these parquet files via
DuckDB's native parquet scan, so no DuckDBSource contrib module is required.
"""

from feast import FileSource

# Paths are relative to the feature repo root (feature_store/feature_repo/).
# The Airflow gold task exports DuckDB gold tables to these parquet files
# after dbt run completes.
_DATA_DIR = "data"

patient_stats_source = FileSource(
    name="patient_appointment_stats_source",
    path=f"{_DATA_DIR}/patient_appointment_stats.parquet",
    timestamp_field="feature_timestamp",
    description="Rolling appointment statistics per patient from dbt gold mart.",
)

patient_demographics_source = FileSource(
    name="patient_demographics_source",
    path=f"{_DATA_DIR}/patient_demographics.parquet",
    timestamp_field="feature_timestamp",
    description="Patient demographic features from SCD2 silver patients.",
)

appointment_context_source = FileSource(
    name="appointment_context_source",
    path=f"{_DATA_DIR}/appointment_context.parquet",
    timestamp_field="event_timestamp_ts",
    description="Appointment-level contextual features.",
)

provider_stats_source = FileSource(
    name="provider_stats_source",
    path=f"{_DATA_DIR}/provider_stats.parquet",
    timestamp_field="feature_timestamp",
    description="Provider-level no-show rate statistics.",
)
