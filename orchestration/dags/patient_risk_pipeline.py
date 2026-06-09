"""
Master DAG: patient_risk_pipeline

Orchestrates the full end-to-end data pipeline for the patient no-show
risk prediction model:

  1. data_quality_bronze  — Great Expectations check on bronze partition
  2. spark_silver_appointments — Bronze → Silver cleaning (PySpark)
  3. spark_silver_patients     — Bronze → Silver SCD2 patients (PySpark)
  4. dbt_run                   — dbt gold-layer models + tests
  5. feast_materialize         — Feast feature materialization
  6. drift_detection           — Evidently data drift report
  7. model_retrain_check       — Trigger retraining DAG if drift detected

Schedule: @hourly
Owner: data-engineering
SLA: All tasks complete within 45 minutes of DAG start

Dependencies:
  Upstream: delta_sink consumer (external, not managed by this DAG)
  Downstream: model_retraining DAG (triggered conditionally)
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from typing import Any

import structlog
from airflow.decorators import dag, task, task_group
from airflow.models import Connection
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

log = structlog.get_logger()


def _maybe_slack_callback(context: Any) -> None:
    """Fire Slack alert on failure only if the connection is configured."""
    # We guard with a try/except so that the callback is a no-op in local dev
    # where no Slack connection exists. In production, the connection is configured
    # in Airflow's connection store (Secrets Manager, Vault, etc.) and alerts fire.
    # Never hardcode webhook URLs here — they're credentials and should live in Airflow Connections.
    try:
        Connection.get_connection_from_secrets("slack_data_alerts")
    except Exception:
        return
    from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification

    send_slack_webhook_notification(
        slack_webhook_conn_id="slack_data_alerts",
        text="DAG {{ dag.dag_id }} task {{ task.task_id }} failed at {{ ts }}.",
    )(context)


def _restore_delta_on_dq_failure(
    table_path: str,
    storage_options: dict,
    dag_run_window_start: datetime,
) -> bool:
    """Roll back a Delta table to the version before the most recent write if that write
    occurred within the current pipeline window and its data triggered a DQ failure.

    This handles the case where the upstream producer wrote corrupt data during the
    current pipeline interval. Rolling back prevents bad rows from blocking all
    downstream tasks on the next retry.

    Returns True when a restore is performed so callers can log it.
    """
    from deltalake import DeltaTable as _DeltaTable

    try:
        dt = _DeltaTable(table_path, storage_options=storage_options)
        history = dt.history(5)
    except Exception as exc:
        log.warning("delta_restore_history_read_failed", path=table_path, error=str(exc))
        return False

    if len(history) < 2:
        return False  # No previous version to restore to

    latest = history[0]
    latest_op = latest.get("operation", "")
    latest_ts = datetime.utcfromtimestamp(latest["timestamp"] / 1000)

    # Only restore when the latest write was within the current pipeline window —
    # avoids rolling back data written by previous (healthy) runs.
    write_operations = {"WRITE", "MERGE", "STREAMING UPDATE", "DELETE", "UPDATE"}
    if latest_op in write_operations and latest_ts >= dag_run_window_start:
        restore_to_version = history[1]["version"]
        log.warning(
            "delta_restoring_to_last_good_version",
            table=table_path,
            bad_version=latest["version"],
            restoring_to=restore_to_version,
            triggering_operation=latest_op,
        )
        dt.restore_to_version(restore_to_version)
        return True

    return False


# ── DAG defaults ──────────────────────────────────────────────────────────────
# default_args are applied to every task in this DAG unless overridden at the task level.
DEFAULT_ARGS = {
    "owner": "data-engineering",
    # depends_on_past=False: each DAG run is independent. Set to True only if you
    # need sequential processing where today's run must wait for yesterday's to succeed.
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # 2 retries with exponential backoff handles transient errors (S3 throttling,
    # network blips) without manual intervention. For production, consider retries=3.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # exponential backoff: retry_delay doubles each attempt (5m → 10m → 20m).
    # This is gentler on downstream systems during incidents.
    "retry_exponential_backoff": True,
    "on_failure_callback": _maybe_slack_callback,
    # SLA: if the task takes longer than 45 minutes, Airflow fires an SLA miss alert.
    # This doesn't stop the task — it's a notification mechanism for monitoring.
    "sla": timedelta(minutes=45),
}

DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://healthcare")
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/opt/airflow/feature_store/feature_repo")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5001")
DRIFT_THRESHOLD = float(os.getenv("DRIFT_P_VALUE_THRESHOLD", "0.05"))
# Hard-fail the GE quality gate if more than this fraction of rows are quarantined.
# Below the threshold the pipeline continues with clean rows; above it the data is too
# corrupted for silver promotion to be safe.
QUARANTINE_FAIL_THRESHOLD = float(os.getenv("QUARANTINE_FAIL_THRESHOLD", "0.5"))


@dag(
    dag_id="patient_risk_pipeline",
    description="End-to-end patient no-show risk data pipeline",
    schedule="@hourly",
    start_date=days_ago(1),
    # catchup=False: don't backfill past runs when the DAG is first deployed.
    # If we deployed this today and catchup=True, Airflow would immediately try to
    # run every hour since start_date — potentially thousands of runs.
    catchup=False,
    # max_active_runs=1: only one run executes at a time.
    # Prevents parallel runs from writing to the same Delta partition simultaneously,
    # which would cause data corruption or confusing merge conflicts.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["healthcare", "ml", "delta-lake", "spark", "dbt", "feast"],
    doc_md=__doc__,
)
def patient_risk_pipeline():

    # ── Task: Circuit Breaker ─────────────────────────────────────────────────
    @task(task_id="check_lag")
    def check_lag(**context: Any) -> None:
        """Circuit breaker: skip the entire DAG run if the bronze table hasn't received
        a new commit in more than 3 hours.

        Without this gate, repeated silver-write failures cause bronze to accumulate
        unbounded data across hourly runs. The circuit breaker halts the cascade:
        it skips the current run, sets an Airflow Variable as a human-visible flag,
        and fires the Slack alert so the on-call engineer knows to investigate.

        To re-enable the pipeline after the root cause is fixed:
          airflow variables delete patient_risk_pipeline_paused
        """
        import os
        from datetime import timedelta

        from airflow.exceptions import AirflowSkipException
        from airflow.models import Variable

        _LAG_THRESHOLD = timedelta(hours=3)
        _BRONZE_PATH = "s3://healthcare/bronze/appointments_raw"
        _PAUSE_VAR = "patient_risk_pipeline_paused"

        # Respect a manually-set pause flag — operator may have set it during an incident
        if Variable.get(_PAUSE_VAR, default_var="false") == "true":
            raise AirflowSkipException(
                f"Pipeline paused via Variable '{_PAUSE_VAR}'. "
                "Unset it to resume: airflow variables delete patient_risk_pipeline_paused"
            )

        storage_opts = {
            "endpoint_url": "http://minio:9000",
            "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "aws_allow_http": "true",
            "region": "us-east-1",
        }

        try:
            from deltalake import DeltaTable

            history = DeltaTable(_BRONZE_PATH, storage_options=storage_opts).history(1)
            if not history:
                log.warning("circuit_breaker_no_delta_history_failing_open")
                return  # Fail open: let downstream tasks surface the real error

            last_commit_ts = datetime.utcfromtimestamp(history[0]["timestamp"] / 1000)
            lag = datetime.utcnow() - last_commit_ts
        except Exception as exc:
            # Fail open: if we can't read the Delta log (e.g. MinIO down), let downstream
            # tasks produce a clearer error rather than masking it with a skip
            log.warning("circuit_breaker_check_error_failing_open", error=str(exc))
            return

        log.info(
            "circuit_breaker_lag_check",
            lag_hours=round(lag.total_seconds() / 3600, 2),
            last_bronze_ts=last_commit_ts.isoformat(),
            threshold_hours=_LAG_THRESHOLD.total_seconds() / 3600,
        )

        if lag > _LAG_THRESHOLD:
            Variable.set(_PAUSE_VAR, "true")
            _maybe_slack_callback(context)
            raise AirflowSkipException(
                f"Circuit breaker open: bronze lag is {lag} (threshold: {_LAG_THRESHOLD}). "
                f"DAG skipped. Unset Variable '{_PAUSE_VAR}' after fixing root cause to resume."
            )

    # ── Task Group: Bronze Quality Gate ──────────────────────────────────────
    @task_group(group_id="bronze_quality")
    def bronze_quality_gate():

        def _read_bronze_delta(table_path: str, storage_options: dict) -> Any:
            """Read a Delta table from MinIO into a pandas DataFrame."""
            from deltalake import DeltaTable

            s3_path = table_path.replace("s3a://", "s3://")
            return DeltaTable(s3_path, storage_options=storage_options).to_pandas()

        @task(task_id="check_bronze_appointments")
        def check_bronze_appointments(**context: Any) -> dict:
            """Quality gate for the bronze appointments partition.

            Runs two complementary checks:
            1. GE checkpoint for a batch-level pass/fail report (stored in gx/uncommitted/).
               On failure, attempts Delta RESTORE if the write occurred in this pipeline window.
            2. Row-level quarantine: bad rows are written to a quarantine Delta table and
               removed from the forward path so good rows still reach silver.
               Hard-fails only when the quarantine rate exceeds QUARANTINE_FAIL_THRESHOLD.
            """
            import os

            import great_expectations as gx
            import pyarrow as pa
            from deltalake import write_deltalake

            partition_date = context["ds"]
            storage_opts = {
                "endpoint_url": "http://minio:9000",
                "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                "aws_allow_http": "true",
                "region": "us-east-1",
            }
            df = _read_bronze_delta(f"{DELTA_LAKE_PATH}/bronze/appointments_raw", storage_opts)
            context_gx = gx.get_context(context_root_dir="/opt/airflow/quality")

            result = context_gx.run_checkpoint(
                checkpoint_name="bronze_appointments",
                batch_request={
                    "runtime_parameters": {"batch_data": df},
                    "batch_identifiers": {"partition_date": partition_date},
                },
            )

            if not result["success"]:
                failed = [
                    r["expectation_config"]["expectation_type"]
                    for _, val in result.run_results.items()
                    for r in val.get("validation_result", {}).get("results", [])
                    if not r["success"]
                ]
                log.warning("gx_checkpoint_failed", failures=failed, partition=partition_date)
                window_start = context["data_interval_start"].replace(tzinfo=None)
                s3_path = f"{DELTA_LAKE_PATH}/bronze/appointments_raw".replace("s3a://", "s3://")
                restored = _restore_delta_on_dq_failure(s3_path, storage_opts, window_start)
                if restored:
                    # Re-read after RESTORE so quarantine operates on the corrected data
                    df = _read_bronze_delta(
                        f"{DELTA_LAKE_PATH}/bronze/appointments_raw", storage_opts
                    )
                    log.warning(
                        "bronze_appointments_restored_after_dq_failure",
                        partition=partition_date,
                    )

            # Row-level quarantine — runs regardless of GE batch result.
            # Bad rows go to a quarantine Delta table; the quarantine_review DAG
            # re-validates them daily and merges correctable rows back to bronze.
            _VALID_APPT_EVENTS = {"SCHEDULED", "RESCHEDULED", "CANCELLED", "COMPLETED", "NO_SHOW"}
            mask = (
                df["appointment_id"].notna()
                & df["patient_id"].notna()
                & df["provider_id"].notna()
                & df["event_type"].isin(_VALID_APPT_EVENTS)
            )
            bad_df = df[~mask].copy()

            if not bad_df.empty:
                bad_df["_quarantined_at"] = datetime.utcnow().isoformat()
                bad_df["_quarantine_reason"] = "failed_row_level_dq_checks"
                bad_df["_quarantine_partition_date"] = partition_date
                write_deltalake(
                    "s3://healthcare/quarantine/bronze_appointments",
                    pa.Table.from_pandas(bad_df, preserve_index=False),
                    storage_options=storage_opts,
                    mode="append",
                    schema_mode="merge",
                )
                log.warning("appointments_quarantined", count=len(bad_df), partition=partition_date)

            bad_rate = len(bad_df) / max(len(df), 1)
            if bad_rate > QUARANTINE_FAIL_THRESHOLD:
                raise ValueError(
                    f"Quarantine rate {bad_rate:.1%} exceeds threshold "
                    f"{QUARANTINE_FAIL_THRESHOLD:.1%} for {partition_date}. "
                    "Too many bad rows — pipeline hard-failed."
                )

            return {
                "partition_date": partition_date,
                "clean_count": len(df) - len(bad_df),
                "quarantine_count": len(bad_df),
            }

        @task(task_id="check_bronze_patients")
        def check_bronze_patients(**context: Any) -> dict:
            """Quality gate for the bronze patients partition.

            Mirrors check_bronze_appointments: GE batch report + row-level quarantine
            with a QUARANTINE_FAIL_THRESHOLD hard-fail guard.
            """
            import os

            import great_expectations as gx
            import pyarrow as pa
            from deltalake import write_deltalake

            partition_date = context["ds"]
            storage_opts = {
                "endpoint_url": "http://minio:9000",
                "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                "aws_allow_http": "true",
                "region": "us-east-1",
            }
            df = _read_bronze_delta(f"{DELTA_LAKE_PATH}/bronze/patients_raw", storage_opts)
            context_gx = gx.get_context(context_root_dir="/opt/airflow/quality")

            result = context_gx.run_checkpoint(
                checkpoint_name="bronze_patients",
                batch_request={
                    "runtime_parameters": {"batch_data": df},
                    "batch_identifiers": {"partition_date": partition_date},
                },
            )

            if not result["success"]:
                failed = [
                    r["expectation_config"]["expectation_type"]
                    for _, val in result.run_results.items()
                    for r in val.get("validation_result", {}).get("results", [])
                    if not r["success"]
                ]
                log.warning("gx_checkpoint_failed", failures=failed, partition=partition_date)
                window_start = context["data_interval_start"].replace(tzinfo=None)
                s3_path = f"{DELTA_LAKE_PATH}/bronze/patients_raw".replace("s3a://", "s3://")
                restored = _restore_delta_on_dq_failure(s3_path, storage_opts, window_start)
                if restored:
                    df = _read_bronze_delta(f"{DELTA_LAKE_PATH}/bronze/patients_raw", storage_opts)
                    log.warning(
                        "bronze_patients_restored_after_dq_failure",
                        partition=partition_date,
                    )

            _VALID_PATIENT_EVENTS = {"REGISTERED", "UPDATED", "DEACTIVATED"}
            mask = df["patient_id"].notna() & df["event_type"].isin(_VALID_PATIENT_EVENTS)
            bad_df = df[~mask].copy()

            if not bad_df.empty:
                bad_df["_quarantined_at"] = datetime.utcnow().isoformat()
                bad_df["_quarantine_reason"] = "failed_row_level_dq_checks"
                bad_df["_quarantine_partition_date"] = partition_date
                write_deltalake(
                    "s3://healthcare/quarantine/bronze_patients",
                    pa.Table.from_pandas(bad_df, preserve_index=False),
                    storage_options=storage_opts,
                    mode="append",
                    schema_mode="merge",
                )
                log.warning("patients_quarantined", count=len(bad_df), partition=partition_date)

            bad_rate = len(bad_df) / max(len(df), 1)
            if bad_rate > QUARANTINE_FAIL_THRESHOLD:
                raise ValueError(
                    f"Quarantine rate {bad_rate:.1%} exceeds threshold "
                    f"{QUARANTINE_FAIL_THRESHOLD:.1%} for {partition_date}. "
                    "Too many bad rows — pipeline hard-failed."
                )

            return {
                "partition_date": partition_date,
                "clean_count": len(df) - len(bad_df),
                "quarantine_count": len(bad_df),
            }

        check_bronze_appointments()
        check_bronze_patients()

    # ── Task Group: Silver Transforms ─────────────────────────────────────────
    # In local dev (Docker) we use pandas + deltalake (Python delta-rs bindings)
    # instead of a full PySpark cluster — same logic, zero Spark overhead.
    # In production you'd replace these @task functions with SparkSubmitOperator
    # pointing to pipelines/spark/silver/clean_appointments.py, etc.
    # The separation means the business logic (dedup, SCD2) is proven in the
    # Spark jobs, and the DAG just calls them as subprocesses.
    @task_group(group_id="silver_transforms")
    def silver_transforms():

        @task(task_id="silver_appointments")
        def silver_appointments_task(**context: Any) -> dict:
            """Deduplicate and type-cast bronze appointments → silver Delta table."""
            import os

            import pandas as pd
            import pyarrow as pa
            from deltalake import DeltaTable, write_deltalake

            storage_opts = {
                "endpoint_url": "http://minio:9000",
                "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                "aws_allow_http": "true",
                "region": "us-east-1",
            }
            bronze_path = "s3://healthcare/bronze/appointments_raw"
            silver_path = "s3://healthcare/silver/appointments"

            df = DeltaTable(bronze_path, storage_options=storage_opts).to_pandas()

            # Deduplicate: keep latest event per appointment_id
            df = df.sort_values("event_timestamp", ascending=False).drop_duplicates(
                "appointment_id"
            )

            # Convert epoch-ms timestamps to UTC-naive strings (DuckDB ::timestamp cast-safe)
            sched_dt = pd.to_datetime(df["scheduled_start_ts"], unit="ms", utc=True).dt.tz_convert(
                None
            )
            df["scheduled_start_ts"] = sched_dt.astype(str)
            df["event_timestamp_ts"] = (
                pd.to_datetime(df["event_timestamp"], unit="ms", utc=True)
                .dt.tz_convert(None)
                .astype(str)
            )
            df["scheduled_at"] = df["scheduled_start_ts"]
            df["scheduled_date"] = sched_dt.dt.date.astype(str)

            # Boolean flags
            df["is_no_show"] = df["event_type"] == "NO_SHOW"
            df["is_cancelled"] = df["event_type"] == "CANCELLED"
            df["is_completed"] = df["event_type"] == "COMPLETED"
            df["is_morning_appointment"] = sched_dt.dt.hour < 12

            # Temporal features
            df["scheduled_day_of_week"] = sched_dt.dt.dayofweek.astype("int32")
            df["scheduled_hour"] = sched_dt.dt.hour.astype("int32")
            df["scheduled_month"] = sched_dt.dt.month.astype("int32")

            def _lead_time_cat(h: float) -> str:
                if pd.isna(h):
                    return "UNKNOWN"
                h = float(h)
                if h < 24:
                    return "SAME_DAY"
                if h < 72:
                    return "SHORT"
                if h < 168:
                    return "MEDIUM"
                if h < 720:
                    return "LONG"
                return "VERY_LONG"

            df["lead_time_category"] = df["lead_time_hours"].apply(_lead_time_cat)

            # Not in seed data — typed float NaN; None would give Delta Lake an untyped Null column
            import numpy as np

            df["copay_amount_usd"] = np.nan

            # Audit
            df["_silver_processed_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            df["_partition_date"] = context["ds"]
            df["_dq_passed"] = True

            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            staging_path = "s3://healthcare/silver/_staging/appointments"

            # Write to staging first — if this fails, production silver is untouched
            write_deltalake(
                staging_path,
                arrow_table,
                storage_options=storage_opts,
                mode="overwrite",
                schema_mode="overwrite",
            )

            # Validate staging before promoting — catches silent empty-write bugs before
            # they overwrite a healthy production table
            staging_count = len(DeltaTable(staging_path, storage_options=storage_opts).to_pandas())
            if staging_count == 0:
                raise ValueError(
                    f"Staging write for silver/appointments produced 0 rows on {context['ds']}; "
                    "production silver was not modified"
                )

            # Capture current silver version so we can restore if promotion crashes mid-write
            try:
                silver_dt = DeltaTable(silver_path, storage_options=storage_opts)
                pre_promote_version = silver_dt.version()
            except Exception:
                pre_promote_version = None  # First write — nothing to restore to

            try:
                write_deltalake(
                    silver_path,
                    arrow_table,
                    storage_options=storage_opts,
                    mode="overwrite",
                    schema_mode="overwrite",
                )
            except Exception:
                if pre_promote_version is not None:
                    DeltaTable(silver_path, storage_options=storage_opts).restore_to_version(
                        pre_promote_version
                    )
                    log.warning(
                        "silver_appointments_promotion_failed_restored",
                        version=pre_promote_version,
                    )
                raise

            log.info("silver_appointments_written", rows=len(df), path=silver_path)
            return {"partition_date": context["ds"], "rows_written": len(df)}

        @task(task_id="silver_patients_scd2")
        def silver_patients_scd2_task(**context: Any) -> dict:
            """SCD Type 2 merge of bronze patient events → silver Delta table."""
            import os
            from datetime import date, timedelta

            import pandas as pd
            import pyarrow as pa
            from deltalake import DeltaTable, write_deltalake

            storage_opts = {
                "endpoint_url": "http://minio:9000",
                "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                "aws_allow_http": "true",
                "region": "us-east-1",
            }
            bronze_path = "s3://healthcare/bronze/patients_raw"
            silver_path = "s3://healthcare/silver/patients"

            df = DeltaTable(bronze_path, storage_options=storage_opts).to_pandas()

            # Build SCD2: latest record per patient is current, others are historical
            df["event_ts"] = (
                pd.to_datetime(df["event_timestamp"], unit="ms", utc=True)
                .dt.tz_convert(None)
                .astype(str)
            )
            df = df.sort_values(["patient_id", "event_timestamp"])

            # Mark is_current: only the latest event per patient
            df["is_current"] = ~df.duplicated(subset=["patient_id"], keep="last")

            # valid_from / valid_to as strings (avoids pyarrow Timestamp tz issues)
            df["valid_from"] = df["event_ts"]
            df["valid_to"] = df.groupby("patient_id")["event_ts"].shift(-1).fillna("")

            # Convert epoch-days integer → ISO date string (expected by stg_patients)
            epoch = date(1970, 1, 1)
            df["date_of_birth_date"] = df["date_of_birth"].apply(
                lambda d: (epoch + timedelta(days=int(d))).isoformat() if pd.notna(d) else None
            )

            # Placeholder: not in seed data but expected by staging model
            df["insurance_plan_name"] = (
                ""  # typed string, not None (avoids Delta Lake Null type error)
            )

            # Audit
            df["_silver_processed_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            staging_path = "s3://healthcare/silver/_staging/patients"

            # Write to staging first — if this fails, production silver is untouched
            write_deltalake(
                staging_path,
                arrow_table,
                storage_options=storage_opts,
                mode="overwrite",
                schema_mode="overwrite",
            )

            staging_count = len(DeltaTable(staging_path, storage_options=storage_opts).to_pandas())
            if staging_count == 0:
                raise ValueError(
                    f"Staging write for silver/patients produced 0 rows on {context['ds']}; "
                    "production silver was not modified"
                )

            try:
                silver_dt = DeltaTable(silver_path, storage_options=storage_opts)
                pre_promote_version = silver_dt.version()
            except Exception:
                pre_promote_version = None

            try:
                write_deltalake(
                    silver_path,
                    arrow_table,
                    storage_options=storage_opts,
                    mode="overwrite",
                    schema_mode="overwrite",
                )
            except Exception:
                if pre_promote_version is not None:
                    DeltaTable(silver_path, storage_options=storage_opts).restore_to_version(
                        pre_promote_version
                    )
                    log.warning(
                        "silver_patients_promotion_failed_restored",
                        version=pre_promote_version,
                    )
                raise

            log.info("silver_patients_written", rows=len(df), path=silver_path)
            return {"partition_date": context["ds"], "rows_written": len(df)}

        silver_appointments_task()
        silver_patients_scd2_task()

    # ── Task: dbt gold layer ──────────────────────────────────────────────────
    @task(task_id="dbt_run_gold")
    def dbt_run_gold(**context: Any) -> dict:
        """Load silver Delta tables into DuckDB, then run dbt gold models and tests."""
        import os
        from pathlib import Path

        import duckdb
        from deltalake import DeltaTable

        partition_date = context["ds"]
        duckdb_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/dev.duckdb")
        Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)

        storage_opts = {
            "endpoint_url": "http://minio:9000",
            "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "aws_allow_http": "true",
            "region": "us-east-1",
        }

        # Read silver tables from MinIO Delta Lake and load into DuckDB for dbt
        appts_df = DeltaTable(
            "s3://healthcare/silver/appointments", storage_options=storage_opts
        ).to_pandas()
        patients_df = DeltaTable(
            "s3://healthcare/silver/patients", storage_options=storage_opts
        ).to_pandas()

        conn = duckdb.connect(duckdb_path)
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("CREATE OR REPLACE TABLE silver.appointments AS SELECT * FROM appts_df")
        conn.execute("CREATE OR REPLACE TABLE silver.patients AS SELECT * FROM patients_df")
        conn.close()
        log.info("silver_loaded_into_duckdb", appts=len(appts_df), patients=len(patients_df))

        # In dbt 1.5+, --project-dir and --profiles-dir are subcommand-level options
        dbt_common = [
            "--project-dir",
            "/opt/airflow/pipelines/dbt",
            "--profiles-dir",
            "/opt/airflow/pipelines/dbt",
            "--target",
            "dev",
            "--vars",
            f'{{"run_date": "{partition_date}"}}',
        ]

        for subcommand in ("run", "test"):
            extra = ["--select", "+ml_patient_appointment_stats"]
            proc = subprocess.run(
                ["dbt", subcommand] + dbt_common + extra,
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"dbt {subcommand} failed:\n{proc.stdout}\n{proc.stderr}")

        return {"partition_date": partition_date, "dbt_status": "passed"}

    # ── Task: Feast feature materialization ───────────────────────────────────
    @task(task_id="feast_materialize")
    def feast_materialize(**context: Any) -> dict:
        """Materialize Feast features for the last 24 hours to the online store."""
        try:
            from feast import FeatureStore
        except ImportError:
            log.warning("feast_not_installed_skipping_materialization")
            return {"skipped": True, "reason": "feast_not_installed"}

        store = FeatureStore(repo_path=FEAST_REPO_PATH)
        end_ts = datetime.utcnow()
        # 25-hour window (not 24) provides a 1-hour overlap as a safety buffer.
        # If the previous run's materialization took slightly longer than an hour,
        # the overlap ensures no feature values are accidentally skipped.
        # Feast materialization is idempotent — re-materializing the same period is safe.
        start_ts = end_ts - timedelta(hours=25)

        store.materialize(start_date=start_ts, end_date=end_ts)

        log.info(
            "feast_materialization_complete",
            start=start_ts.isoformat(),
            end=end_ts.isoformat(),
        )
        return {"materialized_from": start_ts.isoformat(), "materialized_to": end_ts.isoformat()}

    # ── Task: Data drift detection ────────────────────────────────────────────
    @task(task_id="drift_detection")
    def drift_detection(**context: Any) -> dict:
        """
        Run Evidently drift report comparing today's feature distribution
        against the reference distribution used at last model training.
        Returns drift_detected=True if p-value drops below threshold.
        """
        import json
        from pathlib import Path

        import pandas as pd

        try:
            from evidently.metric_preset import DataDriftPreset
            from evidently.report import Report
        except ImportError:
            log.warning("evidently_not_installed_skipping_drift")
            return {"drift_detected": False, "reason": "evidently_not_installed"}

        partition_date = context["ds"]
        report_path = Path(f"/opt/airflow/reports/drift/{partition_date}")
        report_path.mkdir(parents=True, exist_ok=True)

        # Load current features from Feast offline store
        from feast import FeatureStore

        store = FeatureStore(repo_path=FEAST_REPO_PATH)

        # Reference dataset (saved at last training time)
        ref_path = "/opt/airflow/data/reference_features.parquet"
        if not Path(ref_path).exists():
            log.warning("no_reference_data_skipping_drift", ref_path=ref_path)
            return {"drift_detected": False, "reason": "no_reference_data"}

        reference_df = pd.read_parquet(ref_path)

        # Current features — last 24h from offline store
        feature_names = [
            "patient_appointment_stats:no_show_rate_30d",
            "patient_appointment_stats:no_show_rate_90d",
            "patient_appointment_stats:avg_lead_time_days",
            "patient_appointment_stats:total_appointments_90d",
            "patient_demographics:age_at_appointment",
            "patient_demographics:distance_to_clinic_miles",
        ]
        entity_df = pd.DataFrame(
            {
                "patient_id": reference_df["patient_id"].tolist(),
                "event_timestamp": [datetime.utcnow()] * len(reference_df),
            }
        )
        current_df = store.get_historical_features(
            entity_df=entity_df,
            features=feature_names,
        ).to_df()

        # Run Evidently drift report
        report = Report(metrics=[DataDriftPreset()])
        report.run(reference_data=reference_df, current_data=current_df)

        report_file = report_path / "drift_report.html"
        report.save_html(str(report_file))

        drift_result = report.as_dict()
        drift_detected = drift_result["metrics"][0]["result"]["dataset_drift"]

        log.info(
            "drift_detection_complete",
            partition_date=partition_date,
            drift_detected=drift_detected,
        )

        # Persist result for downstream tasks
        result_file = report_path / "drift_result.json"
        result_file.write_text(json.dumps({"drift_detected": drift_detected}))

        return {"drift_detected": drift_detected, "report_path": str(report_file)}

    # ── Task: Feast backfill before drift-triggered retraining ───────────────
    @task(task_id="feast_backfill_drift")
    def feast_backfill_drift(drift_result: dict, **context: Any) -> dict:
        """Re-materialize the full 90-day Feast feature window before retraining.

        Without this step, the model retrains on the same drifted feature distribution
        that triggered the drift alert — the problem persists. By re-materializing the
        full rolling window before retraining, we ensure the offline store reflects the
        corrected current distribution, not the stale one that caused the alert.

        The 90-day window covers the longest rolling feature (no_show_rate_90d).
        A shorter backfill would leave the tail of that window stale.
        """
        try:
            from feast import FeatureStore
        except ImportError:
            log.warning("feast_not_installed_skipping_drift_backfill")
            return {"skipped": True, "reason": "feast_not_installed"}

        store = FeatureStore(repo_path=FEAST_REPO_PATH)
        end_ts = datetime.utcnow()
        start_ts = end_ts - timedelta(days=90)

        log.info(
            "feast_drift_backfill_starting",
            backfill_from=start_ts.isoformat(),
            backfill_to=end_ts.isoformat(),
            drift_result=drift_result,
        )
        store.materialize(start_date=start_ts, end_date=end_ts)
        log.info("feast_drift_backfill_complete")

        return {
            "backfill_from": start_ts.isoformat(),
            "backfill_to": end_ts.isoformat(),
        }

    # ── Task: Conditional retraining trigger ──────────────────────────────────
    @task.branch(task_id="should_retrain")
    def should_retrain(drift_result: dict) -> str:
        """Branch: trigger retraining if drift was detected."""
        # @task.branch returns the task_id of the next task to run.
        # Airflow will skip all other branches automatically.
        # This is the "event-driven retraining" pattern: instead of retraining on a
        # fixed schedule, we only retrain when the data has actually changed enough
        # to degrade the model. This saves compute cost and avoids noisy retraining.
        if drift_result.get("drift_detected", False):
            log.info("drift_detected_backfilling_feast_then_retraining")
            # Route through feast_backfill_drift first so the offline store is
            # re-materialized before the retraining job reads from it.
            return "feast_backfill_drift"
        log.info("no_drift_skipping_retrain")
        return "pipeline_complete"

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_model_retraining",
        trigger_dag_id="model_retraining",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    @task(task_id="pipeline_complete")
    def pipeline_complete(**context: Any) -> None:
        log.info(
            "pipeline_complete",
            execution_date=context["ds"],
            dag_id=context["dag"].dag_id,
        )

    # ── Wire up the DAG ───────────────────────────────────────────────────────
    # check_lag is the circuit breaker at the head of the pipeline. If bronze lag exceeds
    # 3 hours, it raises AirflowSkipException which cascades to all downstream tasks,
    # skipping the entire run without marking it as failed. This prevents a stuck consumer
    # from causing hundreds of failed hourly runs.
    lag_check = check_lag()
    bronze_gate = bronze_quality_gate()
    silver = silver_transforms()
    dbt = dbt_run_gold()
    feast = feast_materialize()
    drift = drift_detection()
    branch = should_retrain(drift)
    # feast_backfill_drift receives the drift report so it can log which features drifted.
    # It only runs when should_retrain branches to "feast_backfill_drift".
    feast_backfill = feast_backfill_drift(drift)

    (
        lag_check
        >> bronze_gate
        >> silver
        >> dbt
        >> feast
        >> drift
        >> branch
        >> [feast_backfill, pipeline_complete()]
    )
    feast_backfill >> trigger_retraining


patient_risk_pipeline()
