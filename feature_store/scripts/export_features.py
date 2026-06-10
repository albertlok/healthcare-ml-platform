"""
Export DuckDB gold/silver tables to the parquet files expected by Feast FileSource.

Run after `make dbt-run` (which populates DuckDB) and before `make feast-apply` /
`make feast-materialize`.

Usage:
    python feature_store/scripts/export_features.py
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import structlog

log = structlog.get_logger()

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    str(Path(__file__).parents[2] / "pipelines" / "dbt" / "data" / "dev.duckdb"),
)
DATA_DIR = Path(__file__).parent.parent / "feature_repo" / "data"

# ── Encoding maps ─────────────────────────────────────────────────────────────
GENDER_MAP = {"MALE": 0, "FEMALE": 1, "NON_BINARY": 2, "PREFER_NOT_TO_SAY": 2, "UNKNOWN": 2}
INSURANCE_MAP = {
    "COMMERCIAL": 0, "MEDICARE": 1, "MEDICAID": 2,
    "SELF_PAY": 3, "WORKERS_COMP": 4, "TRICARE": 5, "UNKNOWN": 6,
}
COMM_MAP = {"EMAIL": 0, "SMS": 1, "PHONE": 2, "PORTAL": 3, "UNKNOWN": 4}
APPT_TYPE_MAP = {
    "ROUTINE": 0, "FOLLOW_UP": 1, "URGENT": 2, "SPECIALIST": 3,
    "PREVENTIVE": 4, "TELEHEALTH": 5, "UNKNOWN": 6,
}


def _encode(series: pd.Series, mapping: dict[str, int]) -> pd.Series:
    return series.map(mapping).fillna(len(mapping) - 1).astype("int32")


def export_patient_appointment_stats(conn: duckdb.DuckDBPyConnection) -> None:
    df = conn.execute("SELECT * FROM main_ml.ml_patient_appointment_stats").df()
    # Feast requires timestamp columns to be tz-aware
    df["feature_timestamp"] = pd.to_datetime(df["feature_timestamp"], utc=True)
    out = DATA_DIR / "patient_appointment_stats.parquet"
    df.to_parquet(out, index=False)
    log.info("exported", file=str(out), rows=len(df))


def export_patient_demographics(conn: duckdb.DuckDBPyConnection) -> None:
    df = conn.execute("SELECT * FROM main_staging.stg_patients").df()
    df["feature_timestamp"] = pd.to_datetime(df["_silver_processed_at"], utc=True)
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
    df["age_at_appointment"] = (
        (df["feature_timestamp"].dt.tz_localize(None) - df["date_of_birth"])
        .dt.days // 365
    ).clip(0, 120).astype("int32")
    df["gender_encoded"] = _encode(df["gender"], GENDER_MAP)
    df["insurance_type_encoded"] = _encode(df["insurance_type"], INSURANCE_MAP)
    df["preferred_communication_encoded"] = _encode(
        df["preferred_communication_channel"], COMM_MAP
    )
    df["has_chronic_condition"] = df["has_chronic_condition"].astype(bool)
    df["distance_to_clinic_miles"] = df["distance_to_clinic_miles"].astype("float32")

    cols = [
        "patient_id", "feature_timestamp",
        "age_at_appointment", "gender_encoded", "insurance_type_encoded",
        "distance_to_clinic_miles", "preferred_communication_encoded",
        "has_chronic_condition",
    ]
    out = DATA_DIR / "patient_demographics.parquet"
    df[cols].to_parquet(out, index=False)
    log.info("exported", file=str(out), rows=len(df))


def export_appointment_context(conn: duckdb.DuckDBPyConnection) -> None:
    df = conn.execute(
        "SELECT * FROM main_staging.stg_appointments WHERE event_type IN ('SCHEDULED','RESCHEDULED')"
    ).df()
    # data_sources.py uses timestamp_field="event_timestamp_ts"
    df["event_timestamp_ts"] = pd.to_datetime(df["event_at"], utc=True)
    df["appointment_type_encoded"] = _encode(df["appointment_type"], APPT_TYPE_MAP)
    df["is_morning_appointment"] = df["is_morning_appointment"].astype(bool)
    df["is_reminder_sent"] = df["is_reminder_sent"].astype(bool)

    cols = [
        "patient_id", "event_timestamp_ts",
        "appointment_type_encoded", "day_of_week", "hour_of_day",
        "appointment_month", "is_morning_appointment", "is_reminder_sent",
        "lead_time_hours",
    ]
    out = DATA_DIR / "appointment_context.parquet"
    df[cols].rename(columns={"appointment_month": "scheduled_month"}).to_parquet(out, index=False)
    log.info("exported", file=str(out), rows=len(df))


def export_provider_stats(conn: duckdb.DuckDBPyConnection) -> None:
    df = conn.execute("""
        SELECT
            provider_id,
            now()::timestamp                                      AS feature_timestamp,
            avg(is_no_show::int)::float                           AS provider_no_show_rate_90d,
            count(*)::int                                         AS provider_total_appointments_90d,
            avg(duration_minutes::float)::float                   AS provider_avg_appointment_duration
        FROM main_staging.stg_appointments
        WHERE scheduled_at >= now() - INTERVAL '90 days'
          AND event_type IN ('COMPLETED', 'NO_SHOW', 'CANCELLED')
        GROUP BY provider_id
    """).df()
    df["feature_timestamp"] = pd.to_datetime(df["feature_timestamp"], utc=True)
    df["provider_no_show_rate_90d"] = df["provider_no_show_rate_90d"].astype("float32")
    df["provider_avg_appointment_duration"] = df["provider_avg_appointment_duration"].astype("float32")
    out = DATA_DIR / "provider_stats.parquet"
    df.to_parquet(out, index=False)
    log.info("exported", file=str(out), rows=len(df))


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        export_patient_appointment_stats(conn)
        export_patient_demographics(conn)
        export_appointment_context(conn)
        export_provider_stats(conn)
    finally:
        conn.close()
    log.info("feature_export_complete", output_dir=str(DATA_DIR))


if __name__ == "__main__":
    main()
