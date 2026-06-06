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
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification
from airflow.utils.dates import days_ago

log = structlog.get_logger()

# ── DAG defaults ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": send_slack_webhook_notification(
        slack_webhook_conn_id="slack_data_alerts",
        text="DAG {{ dag.dag_id }} task {{ task.task_id }} failed at {{ ts }}.",
    ),
    "sla": timedelta(minutes=45),
}

DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "s3a://healthcare")
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/opt/airflow/feature_store/feature_repo")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
DRIFT_THRESHOLD = float(os.getenv("DRIFT_P_VALUE_THRESHOLD", "0.05"))

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.1.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.shuffle.partitions": "8",
}


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

        @task(task_id="check_bronze_appointments")
        def check_bronze_appointments(**context: Any) -> dict:
            """Run Great Expectations checkpoint on bronze appointments partition."""
            import great_expectations as gx

            partition_date = context["ds"]  # Airflow logical date
            context_gx = gx.get_context(context_root_dir="/opt/airflow/quality")

            result = context_gx.run_checkpoint(
                checkpoint_name="bronze_appointments",
                batch_request={
                    "runtime_parameters": {"path": f"{DELTA_LAKE_PATH}/bronze/appointments_raw"},
                    "batch_identifiers": {"partition_date": partition_date},
                },
            )

            if not result["success"]:
                failed = [
                    r["expectation_config"]["expectation_type"]
                    for r in result["results"]
                    if not r["success"]
                ]
                raise ValueError(f"Bronze DQ failed for {partition_date}. Failures: {failed}")

            return {"partition_date": partition_date, "dq_passed": True}

        @task(task_id="check_bronze_patients")
        def check_bronze_patients(**context: Any) -> dict:
            import great_expectations as gx

            partition_date = context["ds"]
            context_gx = gx.get_context(context_root_dir="/opt/airflow/quality")
            result = context_gx.run_checkpoint(checkpoint_name="bronze_patients")

            if not result["success"]:
                raise ValueError(f"Bronze patients DQ failed for {partition_date}")

            return {"partition_date": partition_date, "dq_passed": True}

        check_bronze_appointments()
        check_bronze_patients()

    # ── Task Group: Silver Spark Jobs ─────────────────────────────────────────
    @task_group(group_id="silver_transforms")
    def silver_transforms():

        silver_appointments = SparkSubmitOperator(
            task_id="silver_appointments",
            conn_id="spark_default",
            application="/opt/airflow/pipelines/spark/silver/clean_appointments.py",
            packages=SPARK_PACKAGES,
            conf=SPARK_CONF,
            application_args=["--date", "{{ ds }}"],
            name="silver-clean-appointments-{{ ds }}",
            verbose=False,
        )

        silver_patients = SparkSubmitOperator(
            task_id="silver_patients_scd2",
            conn_id="spark_default",
            application="/opt/airflow/pipelines/spark/silver/scd2_patients.py",
            packages=SPARK_PACKAGES,
            conf=SPARK_CONF,
            application_args=["--date", "{{ ds }}"],
            name="silver-scd2-patients-{{ ds }}",
            verbose=False,
        )

        return silver_appointments, silver_patients

    # ── Task: dbt gold layer ──────────────────────────────────────────────────
    @task(task_id="dbt_run_gold")
    def dbt_run_gold(**context: Any) -> dict:
        """Run dbt gold models and tests. Fails DAG if any test fails."""
        partition_date = context["ds"]

        result = subprocess.run(
            [
                "dbt", "run",
                "--project-dir", "/opt/airflow/pipelines/dbt",
                "--profiles-dir", "/opt/airflow/pipelines/dbt",
                "--target", "prod",
                "--vars", f'{{"run_date": "{partition_date}"}}',
                "--select", "marts.core+ marts.ml+",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt run failed:\n{result.stdout}\n{result.stderr}")

        test_result = subprocess.run(
            [
                "dbt", "test",
                "--project-dir", "/opt/airflow/pipelines/dbt",
                "--profiles-dir", "/opt/airflow/pipelines/dbt",
                "--target", "prod",
                "--select", "marts.core+ marts.ml+",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if test_result.returncode != 0:
            raise RuntimeError(f"dbt test failed:\n{test_result.stdout}\n{test_result.stderr}")

        return {"partition_date": partition_date, "dbt_status": "passed"}

    # ── Task: Feast feature materialization ───────────────────────────────────
    @task(task_id="feast_materialize")
    def feast_materialize(**context: Any) -> dict:
        """Materialize Feast features for the last 24 hours to the online store."""
        from feast import FeatureStore

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
        from evidently import ColumnMapping
        from evidently.metric_preset import DataDriftPreset
        from evidently.report import Report

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
