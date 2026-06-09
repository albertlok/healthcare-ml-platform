"""
quarantine_review DAG

Daily sweep of the bronze-layer quarantine tables populated by
patient_risk_pipeline's GE quality gate when it finds bad rows but the
failure rate is below the hard-fail threshold.

For each quarantine table (appointments, patients):
  1. Re-validate quarantined rows against current row-level quality rules.
     The upstream system may have sent corrected events in the meantime.
  2. Rows that now pass → append back to the original bronze Delta table so the
     next patient_risk_pipeline run picks them up and promotes them to silver.
  3. Rows that still fail AND were quarantined > STALE_THRESHOLD_HOURS ago →
     fire a Slack alert so an engineer can investigate the root cause.

Schedule: @daily
Owner: data-engineering
Dependencies:
  Upstream: patient_risk_pipeline bronze quality gate (writes quarantine tables)
  Downstream: bronze Delta tables (recovered rows appended for re-processing)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
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

# Quarantine → bronze table pairs. Recovered rows are written back to bronze
# so the normal patient_risk_pipeline promotes them to silver on the next run.
QUARANTINE_CONFIG = [
    {
        "name": "appointments",
        "quarantine_path": "s3://healthcare/quarantine/bronze_appointments",
        "bronze_path": "s3://healthcare/bronze/appointments_raw",
        "entity_key": "appointment_id",
        "valid_event_types": {"SCHEDULED", "RESCHEDULED", "CANCELLED", "COMPLETED", "NO_SHOW"},
    },
    {
        "name": "patients",
        "quarantine_path": "s3://healthcare/quarantine/bronze_patients",
        "bronze_path": "s3://healthcare/bronze/patients_raw",
        "entity_key": "patient_id",
        "valid_event_types": {"REGISTERED", "UPDATED", "DEACTIVATED"},
    },
]

# Rows still failing after this many hours trigger a Slack alert.
STALE_THRESHOLD_HOURS = int(os.getenv("QUARANTINE_STALE_THRESHOLD_HOURS", "24"))


@dag(
    dag_id="quarantine_review",
    description="Re-validate quarantined rows, recover good ones to bronze, alert on stale",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["healthcare", "data-quality", "self-healing"],
    doc_md=__doc__,
)
def quarantine_review() -> None:

    @task(task_id="review_appointments_quarantine")
    def review_appointments_quarantine(**context: Any) -> dict:
        """Re-validate and recover quarantined appointment rows."""
        cfg = next(c for c in QUARANTINE_CONFIG if c["name"] == "appointments")
        return _review_quarantine(cfg, context["ds"])

    @task(task_id="review_patients_quarantine")
    def review_patients_quarantine(**context: Any) -> dict:
        """Re-validate and recover quarantined patient rows."""
        cfg = next(c for c in QUARANTINE_CONFIG if c["name"] == "patients")
        return _review_quarantine(cfg, context["ds"])

    @task(task_id="quarantine_summary")
    def quarantine_summary(appt: dict, patients: dict, **context: Any) -> None:
        """Log combined summary and alert on stale unresolvable rows."""
        total_recovered = appt.get("recovered", 0) + patients.get("recovered", 0)
        total_stale = appt.get("stale", 0) + patients.get("stale", 0)

        log.info(
            "quarantine_review_summary",
            partition_date=context["ds"],
            total_recovered=total_recovered,
            total_stale=total_stale,
            appointments=appt,
            patients=patients,
        )

        if total_stale > 0:
            log.warning(
                "stale_quarantine_rows_require_attention",
                count=total_stale,
                threshold_hours=STALE_THRESHOLD_HOURS,
            )
            _maybe_slack_callback(context)

    appt_result = review_appointments_quarantine()
    patient_result = review_patients_quarantine()
    quarantine_summary(appt_result, patient_result)


def _passes_row_quality(row: Any, entity_key: str, valid_event_types: set) -> bool:
    """Return True if this quarantined row now meets the silver promotion bar.

    Mirrors the row-level mask used in check_bronze_appointments/patients.
    Called via DataFrame.apply() — row is a pandas Series.
    """
    import pandas as pd

    if pd.isna(row.get(entity_key)):
        return False

    event_type = str(row.get("event_type", "")).upper()
    if event_type not in valid_event_types:
        return False

    if entity_key == "appointment_id":
        return not pd.isna(row.get("patient_id")) and not pd.isna(row.get("provider_id"))

    return True


def _review_quarantine(cfg: dict, partition_date: str) -> dict:
    """
    Core review logic for one quarantine table.

    Steps:
    1. Read the quarantine Delta table (skip gracefully if it doesn't exist yet).
    2. Re-apply the row-level quality check to each row.
    3. Recovered rows → append to the original bronze Delta table.
       The next patient_risk_pipeline run will promote them to silver normally.
    4. Still-bad rows that are older than STALE_THRESHOLD_HOURS → counted for alert.
    """
    import pandas as pd
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    quarantine_path = cfg["quarantine_path"]
    bronze_path = cfg["bronze_path"]
    entity_key = cfg["entity_key"]
    valid_event_types = cfg["valid_event_types"]
    name = cfg["name"]

    empty_result = {
        "name": name,
        "total": 0,
        "recovered": 0,
        "still_bad": 0,
        "stale": 0,
    }

    try:
        quarantine_df = DeltaTable(quarantine_path, storage_options=STORAGE_OPTIONS).to_pandas()
    except Exception as exc:
        log.info(
            "quarantine_table_not_found_skipping",
            path=quarantine_path,
            reason=str(exc),
        )
        return empty_result

    if quarantine_df.empty:
        log.info("quarantine_table_empty", path=quarantine_path)
        return empty_result

    # Re-validate each row against the current quality rules
    quarantine_df["_revalidation_passed"] = quarantine_df.apply(
        lambda row: _passes_row_quality(row, entity_key, valid_event_types),
        axis=1,
    )

    recovered_df = quarantine_df[quarantine_df["_revalidation_passed"]].copy()
    still_bad_df = quarantine_df[~quarantine_df["_revalidation_passed"]].copy()

    recovered = 0
    if not recovered_df.empty:
        # Strip quarantine-only columns before writing back to bronze
        drop_cols = [
            c
            for c in recovered_df.columns
            if c.startswith("_quarantine") or c == "_revalidation_passed"
        ]
        bronze_ready = recovered_df.drop(columns=drop_cols, errors="ignore")

        try:
            write_deltalake(
                bronze_path,
                pa.Table.from_pandas(bronze_ready, preserve_index=False),
                storage_options=STORAGE_OPTIONS,
                mode="append",
                schema_mode="merge",
            )
            recovered = len(bronze_ready)
            log.info(
                "quarantine_rows_recovered_to_bronze",
                count=recovered,
                bronze_path=bronze_path,
                entity_key=entity_key,
            )
        except Exception as exc:
            log.error(
                "quarantine_bronze_write_failed",
                error=str(exc),
                bronze_path=bronze_path,
            )

    # Count stale rows (still failing + quarantined longer than threshold)
    stale = 0
    if not still_bad_df.empty and "_quarantined_at" in still_bad_df.columns:
        stale_cutoff = datetime.utcnow() - timedelta(hours=STALE_THRESHOLD_HOURS)
        quarantined_ts = pd.to_datetime(
            still_bad_df["_quarantined_at"], utc=True, errors="coerce"
        ).dt.tz_localize(None)
        stale = int((quarantined_ts < stale_cutoff).sum())

        if stale > 0:
            log.warning(
                "stale_quarantine_rows",
                count=stale,
                name=name,
                threshold_hours=STALE_THRESHOLD_HOURS,
                oldest_path=quarantine_path,
            )

    return {
        "name": name,
        "total": len(quarantine_df),
        "recovered": recovered,
        "still_bad": len(still_bad_df),
        "stale": stale,
    }


quarantine_review()
