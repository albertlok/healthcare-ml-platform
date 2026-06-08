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
from typing import TYPE_CHECKING, Any

import structlog
from airflow.decorators import dag, task, task_group
from airflow.models import Variable, Connection
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

log = structlog.get_logger()


def _maybe_slack_callback(context: Any) -> None:
    """Fire Slack alert on failure only if the connection is configured."""
    try:
        Connection.get_connection_from_secrets("slack_data_alerts")
    except Exception:
        return
    from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification
    send_slack_webhook_notification(
        slack_webhook_conn_id="slack_data_alerts",
        text="DAG {{ dag.dag_id }} task {{ task.task_id }} failed at {{ ts }}.",
    )(context)


# ── DAG defaults ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": _maybe_slack_callback,
    "sla": timedelta(minutes=45),
}

DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://healthcare")
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/opt/airflow/feature_store/feature_repo")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5001")
DRIFT_THRESHOLD = float(os.getenv("DRIFT_P_VALUE_THRESHOLD", "0.05"))



@dag(
    dag_id="patient_risk_pipeline",
    description="End-to-end patient no-show risk data pipeline",
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["healthcare", "ml", "delta-lake", "spark", "dbt", "feast"],
    doc_md=__doc__,
)
def patient_risk_pipeline():

    # ── Task Group: Bronze Quality Gate ──────────────────────────────────────
    @task_group(group_id="bronze_quality")
    def bronze_quality_gate():

        def _read_bronze_delta(table_path: str) -> "pd.DataFrame":
            """Read a Delta table from MinIO into a pandas DataFrame."""
            import os
            import pandas as pd
            from deltalake import DeltaTable

            storage_options = {
                "endpoint_url": "http://minio:9000",
                "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                "aws_allow_http": "true",
                "region": "us-east-1",
            }
            # s3a:// → s3:// for delta-rs
            s3_path = table_path.replace("s3a://", "s3://")
            return DeltaTable(s3_path, storage_options=storage_options).to_pandas()

        @task(task_id="check_bronze_appointments")
        def check_bronze_appointments(**context: Any) -> dict:
            """Run Great Expectations checkpoint on bronze appointments partition."""
            import great_expectations as gx

            partition_date = context["ds"]
            df = _read_bronze_delta(f"{DELTA_LAKE_PATH}/bronze/appointments_raw")
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
                raise ValueError(f"Bronze DQ failed for {partition_date}. Failures: {failed}")

            return {"partition_date": partition_date, "dq_passed": True}

        @task(task_id="check_bronze_patients")
        def check_bronze_patients(**context: Any) -> dict:
            import great_expectations as gx

            partition_date = context["ds"]
            df = _read_bronze_delta(f"{DELTA_LAKE_PATH}/bronze/patients_raw")
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
                raise ValueError(f"Bronze patients DQ failed for {partition_date}. Failures: {failed}")

            return {"partition_date": partition_date, "dq_passed": True}

        check_bronze_appointments()
        check_bronze_patients()

    # ── Task Group: Silver Transforms (pandas + deltalake for local dev) ─────────
    # Production equivalent: SparkSubmitOperator against pipelines/spark/silver/
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
            df = df.sort_values("event_timestamp", ascending=False).drop_duplicates("appointment_id")

            # Convert epoch-ms timestamps to UTC-naive strings (DuckDB ::timestamp cast-safe)
            sched_dt = pd.to_datetime(df["scheduled_start_ts"], unit="ms", utc=True).dt.tz_convert(None)
            df["scheduled_start_ts"] = sched_dt.astype(str)
            df["event_timestamp_ts"] = (
                pd.to_datetime(df["event_timestamp"], unit="ms", utc=True).dt.tz_convert(None).astype(str)
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

            write_deltalake(silver_path, pa.Table.from_pandas(df, preserve_index=False),
                            storage_options=storage_opts, mode="overwrite", schema_mode="overwrite")
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
                pd.to_datetime(df["event_timestamp"], unit="ms", utc=True).dt.tz_convert(None).astype(str)
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
            df["insurance_plan_name"] = ""  # typed string, not None (avoids Delta Lake Null type error)

            # Audit
            df["_silver_processed_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

            write_deltalake(silver_path, pa.Table.from_pandas(df, preserve_index=False),
                            storage_options=storage_opts, mode="overwrite", schema_mode="overwrite")
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
        appts_df = DeltaTable("s3://healthcare/silver/appointments", storage_options=storage_opts).to_pandas()
        patients_df = DeltaTable("s3://healthcare/silver/patients", storage_options=storage_opts).to_pandas()

        conn = duckdb.connect(duckdb_path)
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("CREATE OR REPLACE TABLE silver.appointments AS SELECT * FROM appts_df")
        conn.execute("CREATE OR REPLACE TABLE silver.patients AS SELECT * FROM patients_df")
        conn.close()
        log.info("silver_loaded_into_duckdb", appts=len(appts_df), patients=len(patients_df))

        # In dbt 1.5+, --project-dir and --profiles-dir are subcommand-level options
        dbt_common = [
            "--project-dir", "/opt/airflow/pipelines/dbt",
            "--profiles-dir", "/opt/airflow/pipelines/dbt",
            "--target", "dev",
            "--vars", f'{{"run_date": "{partition_date}"}}',
        ]

        for subcommand in ("run", "test"):
            extra = ["--select", "+ml_patient_appointment_stats"]
            proc = subprocess.run(
                ["dbt", subcommand] + dbt_common + extra,
                capture_output=True, text=True, check=False,
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
        start_ts = end_ts - timedelta(hours=25)  # 1-hour overlap for safety

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
            from evidently import ColumnMapping
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
            {"patient_id": reference_df["patient_id"].tolist(),
             "event_timestamp": [datetime.utcnow()] * len(reference_df)}
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

    # ── Task: Conditional retraining trigger ──────────────────────────────────
    @task.branch(task_id="should_retrain")
    def should_retrain(drift_result: dict) -> str:
        """Branch: trigger retraining if drift was detected."""
        if drift_result.get("drift_detected", False):
            log.info("drift_detected_triggering_retrain")
            return "trigger_model_retraining"
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
    bronze_gate = bronze_quality_gate()
    silver = silver_transforms()
    dbt = dbt_run_gold()
    feast = feast_materialize()
    drift = drift_detection()
    branch = should_retrain(drift)

    (
        bronze_gate
        >> silver
        >> dbt
        >> feast
        >> drift
        >> branch
        >> [trigger_retraining, pipeline_complete()]
    )


patient_risk_pipeline()
