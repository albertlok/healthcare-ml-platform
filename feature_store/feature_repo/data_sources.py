"""
Feast data source definitions.

Each source points to the dbt-generated gold layer.
In dev: DuckDB files.
In prod: swap DuckdbSource for SnowflakeSource / BigQuerySource / RedshiftSource.
"""

from feast.infra.offline_stores.contrib.duckdb_offline_store.duckdb_source import DuckDBSource

DUCKDB_PATH = "data/dev.duckdb"

patient_stats_source = DuckDBSource(
    name="patient_appointment_stats_source",
    path=DUCKDB_PATH,
    query="SELECT * FROM gold.ml_patient_appointment_stats",
    timestamp_field="feature_timestamp",
    description="Rolling appointment statistics per patient from dbt gold mart.",
)

patient_demographics_source = DuckDBSource(
    name="patient_demographics_source",
    path=DUCKDB_PATH,
    query="SELECT * FROM gold.ml_patient_demographics",
    timestamp_field="feature_timestamp",
    description="Patient demographic features from SCD2 silver patients.",
)

appointment_context_source = DuckDBSource(
    name="appointment_context_source",
    path=DUCKDB_PATH,
    query="SELECT * FROM gold.ml_appointment_context",
    timestamp_field="event_timestamp_ts",
    description="Appointment-level contextual features.",
)

provider_stats_source = DuckDBSource(
    name="provider_stats_source",
    path=DUCKDB_PATH,
    query="SELECT * FROM gold.ml_provider_stats",
    timestamp_field="feature_timestamp",
    description="Provider-level no-show rate statistics.",
)
