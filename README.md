# healthcare-ml-platform

> **End-to-end healthcare ML data platform** — streaming ingestion through feature serving and RAG inference, built with the modern data lakehouse stack and documented as a portfolio project for Senior Data Engineer roles.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Repository Structure](#repository-structure)
5. [Quick Start](#quick-start)
6. [Component Deep Dives](#component-deep-dives)
   - [Streaming Ingestion (Kafka)](#1-streaming-ingestion--kafka)
   - [Delta Lake Bronze Layer](#2-delta-lake-bronze-layer)
   - [PySpark Silver Transforms](#3-pyspark-silver-transforms)
   - [dbt Gold Layer](#4-dbt-gold-layer)
   - [Feast Feature Store](#5-feast-feature-store)
   - [Airflow Orchestration](#6-airflow-orchestration)
   - [DVC + MLflow ML Pipeline](#7-dvc--mlflow-ml-pipeline)
   - [Evidently Drift Detection](#8-evidently-drift-detection)
   - [ChromaDB RAG Endpoint](#9-chromadb-rag-endpoint)
   - [Data Quality (Great Expectations)](#10-data-quality--great-expectations)
7. [Cloud Deployment](#cloud-deployment)
8. [CI/CD](#cicd)
9. [Built with Claude Code](#built-with-claude-code)

---

## Overview

This project simulates a **production-grade ML data platform** for predicting patient appointment no-shows — a high-value use case in healthcare operations. It demonstrates:

- **Streaming** data ingestion from Kafka with Avro schema enforcement
- **Medallion architecture** (Bronze → Silver → Gold) on Delta Lake
- **Distributed batch processing** with PySpark including SCD Type 2 patient history
- **SQL transformation layer** with dbt (DuckDB locally, Snowflake in prod)
- **Feature engineering** with Feast (offline + online store, point-in-time joins)
- **ML pipeline** versioned with DVC and tracked in MLflow
- **Data drift monitoring** with Evidently AI
- **RAG inference endpoint** using ChromaDB + LangChain + FastAPI
- **Full orchestration** via Apache Airflow with conditional DAG branching
- **CI/CD** with GitHub Actions and pre-commit hooks

All services run **locally for free** using Docker Compose, with clear paths to AWS, Azure, and GCP equivalents documented throughout.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                   │
│   Synthetic patient appointments + patient registration events          │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ Avro / Schema Registry
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        STREAMING LAYER                                  │
│   Apache Kafka (topics: appointment.scheduled, patient.registered)      │
│   Confluent Schema Registry  │  Kafka Connect                           │
└──────────────┬──────────────┴──────────────────────────────────────────┘
               │ Spark Structured Streaming
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DELTA LAKE  —  BRONZE (raw, append-only)             │
│   s3a://healthcare/bronze/appointments_raw                              │
│   s3a://healthcare/bronze/patients_raw                                  │
│   Storage: MinIO (local) │ S3 / ADLS Gen2 / GCS (cloud)                │
└──────────────┬──────────────────────────────────────────────────────────┘
               │ PySpark batch jobs (Airflow-triggered)
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DELTA LAKE  —  SILVER (cleaned, typed)               │
│   silver.appointments  — deduplicated, derived cols, DQ-tagged          │
│   silver.patients      — SCD Type 2 with full change history            │
└──────────────┬──────────────────────────────────────────────────────────┘
               │ dbt (DuckDB dev / Snowflake prod)
               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER  (business-ready)                         │
│   fct_appointments  │  dim_patients  │  ml_patient_features             │
└──────────────┬──────────────────────────────────────────────────────────┘
               │ Feast materialization
               ▼
┌──────────────────────────────┐    ┌────────────────────────────────────┐
│   FEAST  FEATURE STORE       │    │   CHROMADB  VECTOR STORE           │
│   Offline: DuckDB / Snowflake│    │   Clinical note embeddings         │
│   Online:  SQLite / Redis    │    │   sentence-transformers (free)     │
└──────────────┬───────────────┘    └──────────────┬─────────────────────┘
               │ point-in-time features              │ semantic retrieval
               ▼                                     ▼
┌─────────────────────────────┐    ┌────────────────────────────────────┐
│   ML PIPELINE (DVC + MLflow)│    │   RAG  INFERENCE  API  (FastAPI)   │
│   XGBoost no-show classifier│    │   LangChain + Claude / local LLM   │
│   SHAP explainability        │    │   /query  /patient/{id}/risk       │
└─────────────────────────────┘    └────────────────────────────────────┘
                        ▲
         ┌──────────────┴──────────────────────────────┐
         │         APACHE AIRFLOW  (orchestrator)       │
         │   DAGs: patient_risk_pipeline (hourly)       │
         │         dbt_daily_refresh                    │
         │         model_retraining (triggered)         │
         │         feast_materialization                │
         └──────────────────────────────────────────────┘
                        ▲
         ┌──────────────┴──────────────────────────────┐
         │       EVIDENTLY  —  Drift Monitoring         │
         │   Data drift reports  │  Model performance   │
         │   Auto-triggers retraining DAG on drift      │
         └──────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Local | Cloud Equivalent |
|---|---|---|---|
| **Streaming** | Apache Kafka 3.7 | Docker | AWS MSK / Azure Event Hubs / Confluent Cloud |
| **Schema Registry** | Confluent Schema Registry | Docker | Confluent Cloud / AWS Glue SR |
| **Object Storage** | MinIO | Docker | AWS S3 / Azure ADLS Gen2 / GCS |
| **Lakehouse Format** | Delta Lake 3.1 | Local PySpark | Databricks / AWS EMR / Azure Synapse |
| **Batch Processing** | PySpark 3.5 | Spark standalone | Databricks / AWS EMR / GCP Dataproc |
| **SQL Transform** | dbt Core 1.8 | DuckDB (free) | Snowflake / BigQuery / Redshift |
| **Orchestration** | Airflow 2.9 | Docker (Celery) | AWS MWAA / GCP Cloud Composer |
| **Feature Store** | Feast 0.40 | SQLite + DuckDB | Tecton / AWS SageMaker FS |
| **Data Versioning** | DVC 3.x | Local / GDrive | DVC + S3 remote |
| **Experiment Tracking** | MLflow 2.13 | Docker | Databricks MLflow / Azure ML |
| **ML Model** | XGBoost + SHAP | Local | SageMaker / Vertex AI / AzureML |
| **Drift Monitoring** | Evidently AI | Local | Evidently Cloud / Arize |
| **Vector Store** | ChromaDB 0.5 | Docker | Pinecone / Weaviate / OpenSearch |
| **RAG Framework** | LangChain 0.2 | Local | Bedrock / Azure OpenAI |
| **Inference API** | FastAPI 0.111 | Local | AWS Lambda / Cloud Run / AKS |
| **Data Quality** | Great Expectations | Local | GX Cloud |
| **CI/CD** | GitHub Actions | — | — |

**Total infrastructure cost: $0** (all tools are open-source or have free tiers)

---

## Repository Structure

```
healthcare-ml-platform/
├── CLAUDE.md                          # Claude Code project instructions
├── README.md                          # This file
├── docker-compose.yml                 # Full local stack (13 services)
├── Makefile                           # 30+ dev shortcuts
├── pyproject.toml                     # Python deps + tool config
├── .env.example                       # All environment variables documented
│
├── ingestion/                         # ① Kafka layer
│   ├── schemas/
│   │   ├── appointment_event.avsc     # Avro schema — appointment lifecycle
│   │   └── patient_event.avsc         # Avro schema — patient registration
│   ├── producers/
│   │   ├── appointment_producer.py    # Synthetic appointment events → Kafka
│   │   └── patient_producer.py        # Synthetic patient events → Kafka
│   └── consumers/
│       └── delta_sink.py              # Kafka → Delta Lake bronze (micro-batch)
│
├── pipelines/
│   ├── spark/                         # ② ③ PySpark jobs
│   │   ├── bronze/
│   │   │   └── ingest_appointments.py # Spark Structured Streaming → Delta bronze
│   │   ├── silver/
│   │   │   ├── clean_appointments.py  # Dedup + cast + derived cols + DQ flag
│   │   │   └── scd2_patients.py       # SCD Type 2 patient dimension
│   │   └── gold/
│   │       └── patient_risk_features.py
│   └── dbt/                           # ④ dbt gold layer
│       ├── models/staging/            # 1:1 source casts
│       ├── models/intermediate/       # Business joins
│       └── models/marts/              # Core dims/facts + ML feature mart
│
├── orchestration/dags/                # ⑥ Airflow
│   ├── patient_risk_pipeline.py       # Master hourly DAG
│   ├── dbt_daily_refresh.py
│   ├── model_retraining.py
│   └── feast_materialization.py
│
├── feature_store/feature_repo/        # ⑤ Feast
│   ├── feature_store.yaml
│   ├── entities.py
│   ├── data_sources.py
│   ├── feature_views.py               # 3 feature views, 13 features
│   └── feature_services.py
│
├── ml/                                # ⑦ DVC + MLflow
│   ├── dvc.yaml                       # Pipeline: features→split→train→evaluate
│   ├── params.yaml                    # Versioned hyperparameters
│   ├── train.py                       # XGBoost training + MLflow logging
│   ├── evaluate.py                    # Metrics, confusion matrix, SHAP
│   └── drift_detection.py             # Evidently reports
│
├── rag/                               # ⑨ ChromaDB + LangChain
│   ├── embed_notes.py                 # Embed clinical notes → ChromaDB
│   ├── retriever.py                   # Semantic search
│   ├── chain.py                       # LangChain RAG chain
│   └── api/
│       ├── main.py                    # FastAPI app
│       └── schemas.py                 # Pydantic models
│
├── quality/                           # ⑩ Great Expectations
│   └── expectations/
│
├── infra/
│   ├── postgres/init-multiple-dbs.sh
│   ├── terraform/aws/
│   ├── terraform/azure/
│   └── snowflake/setup.sql
│
└── .github/workflows/
    ├── ci.yml                         # PR: lint + unit tests + dbt compile
    └── dvc_repro.yml                  # DVC pipeline on data change
```

---

## Quick Start

### Prerequisites

```bash
docker >= 24.0
docker compose >= 2.24
python >= 3.11
java >= 11          # Required for PySpark local mode
```

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/healthcare-ml-platform.git
cd healthcare-ml-platform
make env            # Copies .env.example → .env
```

### 2. Start the full stack

```bash
make up
```

This starts 13 Docker services. Wait ~60 seconds for Airflow to initialize, then open:

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8081 | admin / admin |
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8082 | — |
| MLflow | http://localhost:5000 | — |
| ChromaDB | http://localhost:8000 | — |

### 3. Create Kafka topics and seed data

```bash
make kafka-topics   # Creates 4 topics
make seed-once      # Produces 1000 appointment + 500 patient events
```

### 4. Run the data pipeline

```bash
# Option A: Run each stage manually
make spark-bronze                          # Kafka → Delta bronze
make spark-silver                          # Bronze → Silver (clean + SCD2)
make dbt-run                               # Silver → Gold (dbt)
make feast-apply && make feast-materialize # Feature store

# Option B: Trigger the Airflow DAG
# Visit http://localhost:8081, enable patient_risk_pipeline, click Run
```

### 5. Train the model and start the RAG API

```bash
make dvc-repro      # Features → train → evaluate (DVC pipeline)
make rag-embed      # Embed clinical notes into ChromaDB
make rag-start      # Start FastAPI at http://localhost:8888
```

### 6. Test everything

```bash
make test           # Unit tests with coverage
make lint           # black + isort + flake8
make dbt-test       # dbt schema + singular tests
```

---

## Component Deep Dives

### 1. Streaming Ingestion — Kafka

**Files:** `ingestion/producers/`, `ingestion/schemas/`, `ingestion/consumers/delta_sink.py`

Synthetic healthcare events are produced to Kafka topics using the [Confluent Python client](https://github.com/confluentinc/confluent-kafka-python) with Avro serialization enforced by Schema Registry.

**Avro schemas** (`ingestion/schemas/`) define strict contracts for:
- `AppointmentEvent` — lifecycle events (SCHEDULED, RESCHEDULED, CANCELLED, COMPLETED, NO_SHOW) with 17 typed fields
- `PatientEvent` — registration and profile updates with insurance, demographics, and preferences

**Producer design decisions:**
- `acks='all'` + `enable.idempotence=True` — exactly-once delivery semantics
- `compression.type='snappy'` — ~40% size reduction on JSON-heavy payloads
- `linger.ms=50` + `batch.size=65536` — throughput batching without sacrificing latency
- Stable patient/provider ID pools — ensures referential consistency across events

**Topic naming convention:** `{env}.{domain}.{entity}.{event}`
```
dev.healthcare.appointment.scheduled
dev.healthcare.patient.registered
```

**Delta sink consumer** (`delta_sink.py`) reads with manual offset commits — offsets are only committed *after* a successful Delta Lake write, preventing data loss on failure.

**Cloud equivalent:** Kafka Connect S3 Sink Connector (AWS MSK) or Databricks Auto Loader

---

### 2. Delta Lake Bronze Layer

**Files:** `pipelines/spark/bronze/ingest_appointments.py`

The bronze layer is **append-only and immutable** — raw events land here exactly as received from Kafka. Nothing is modified or filtered.

**Spark Structured Streaming** reads from Kafka and writes micro-batches to Delta every 60 seconds:

```python
df.writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", CHECKPOINT_PATH)
  .partitionBy("_partition_date")
  .trigger(processingTime="60 seconds")
  .start(BRONZE_PATH)
```

**Confluent wire format stripping:** The Avro payload from Schema Registry has a 5-byte prefix (1 magic byte + 4-byte schema ID). We strip this before `from_avro()` deserialization:
```python
F.expr("substring(value, 6, length(value) - 5)")
```

**Delta optimizations enabled:**
- `optimizeWrite` — auto-compacts small files on write
- `autoCompact` — merges small files in the background
- `mergeSchema=true` — handles Avro schema evolution gracefully

**Cloud equivalent (Databricks):** Delta Live Tables with Auto Loader cloudFiles source

---

### 3. PySpark Silver Transforms

**Files:** `pipelines/spark/silver/clean_appointments.py`, `pipelines/spark/silver/scd2_patients.py`

The silver layer applies **four transformation stages** using PySpark's `transform()` chaining pattern:

```python
silver_df = (
    bronze_df
    .transform(deduplicate)       # Window-based dedup on (appointment_id, event_type)
    .transform(cast_and_clean)    # Type casting, null handling, enum normalization
    .transform(add_derived_columns) # Lead-time buckets, day-of-week, temporal features
    .transform(tag_data_quality)  # _dq_passed boolean flag
)
```

**Deduplication** uses a window function (not `dropDuplicates()`) to keep the record with the highest Kafka offset — preserving the latest version of each event:
```python
window = Window.partitionBy("appointment_id", "event_type").orderBy(F.col("kafka_offset").desc())
```

**MERGE upsert** into the Silver Delta table on `(appointment_id, event_type)` ensures idempotent re-runs.

#### SCD Type 2 — Patient Dimension (`scd2_patients.py`)

Implements a full [Slowly Changing Dimension Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2) on Delta Lake:

| Column | Purpose |
|---|---|
| `valid_from` | Timestamp this version became active |
| `valid_to` | `9999-12-31` for current row; set on expiry |
| `is_current` | Boolean — exactly one `TRUE` per `patient_id` |

On a patient insurance update, the pipeline:
1. **Closes** the old row: sets `valid_to = new_event_timestamp`, `is_current = FALSE`
2. **Inserts** a new current row with the updated values

This gives us a complete audit trail of every patient attribute change, enabling point-in-time correct ML features via Feast.

---

### 4. dbt Gold Layer

**Files:** `pipelines/dbt/`

dbt handles all SQL transformations from Silver → Gold using a three-layer model structure:

```
staging/    → 1:1 with Silver Delta tables. Light casting only.
intermediate/ → Business logic joins (appointments ⋈ patients ⋈ providers)
marts/
  core/     → fct_appointments, dim_patients, dim_providers
  ml/       → ml_patient_risk_features (ML-ready aggregates)
```

**dbt profiles** support two targets via `DBT_TARGET` env var:
- `dev` → DuckDB (zero infra, free, Snowflake-compatible SQL)
- `prod` → Snowflake (same SQL, different connection)

**Every model has** a `.yml` file with column descriptions and at minimum:
```yaml
tests:
  - not_null
  - unique
```

**ML feature mart** (`marts/ml/ml_patient_risk_features.sql`) computes rolling-window aggregates used as Feast offline store source:

| Feature | Window | Type |
|---|---|---|
| `no_show_rate_30d` | 30 days | float |
| `no_show_rate_90d` | 90 days | float |
| `total_appointments_90d` | 90 days | int |
| `avg_lead_time_days` | 90 days | float |
| `last_appointment_days_ago` | — | int |
| `provider_no_show_rate` | 90 days | float |

**Run locally:**
```bash
make dbt-run    # dbt run --target dev
make dbt-test   # dbt test
make dbt-docs   # Serves docs at http://localhost:8085
```

---

### 5. Feast Feature Store

**Files:** `feature_store/feature_repo/`

[Feast](https://feast.dev) provides a unified offline + online feature store. The offline store is used for training (point-in-time correct historical features). The online store is used for low-latency inference serving.

**Entities:**
- `patient` — keyed on `patient_id`
- `provider` — keyed on `provider_id`

**Feature Views:**

| View | Features | Source |
|---|---|---|
| `patient_appointment_stats` | no_show_rate_30d/90d, lead_time, recency | dbt ML mart |
| `patient_demographics` | age, insurance_type, distance, language | Silver patients |
| `appointment_context` | type, day_of_week, hour, is_reminder_sent | Silver appointments |

**Materialization** (offline → online store) runs every 6 hours via Airflow:
```bash
feast materialize <start> <end>
```

**Point-in-time correct training data:**
```python
store.get_historical_features(
    entity_df=entity_df_with_timestamps,
    features=["patient_appointment_stats:no_show_rate_30d", ...]
).to_df()
```

**Cloud equivalent:** Feast with DynamoDB online store + S3 offline store (AWS), or Tecton

---

### 6. Airflow Orchestration

**Files:** `orchestration/dags/`

All pipelines are orchestrated by Airflow 2.9 using the **TaskFlow API** (`@task` decorators).

**DAG catalog:**

| DAG | Schedule | Description |
|---|---|---|
| `patient_risk_pipeline` | `@hourly` | Master pipeline — the main DAG |
| `dbt_daily_refresh` | `0 6 * * *` | Full dbt run + test |
| `model_retraining` | Triggered | Fires when drift detected |
| `feast_materialization` | `0 */6 * * *` | Online store refresh |

**Master DAG flow** (`patient_risk_pipeline`):

```
bronze_quality_gate ──► silver_transforms ──► dbt_run_gold
                                                    │
                                           feast_materialize
                                                    │
                                           drift_detection
                                                    │
                                         ┌─── should_retrain ───┐
                                         ▼                       ▼
                               trigger_model_retraining    pipeline_complete
```

**Key design patterns:**
- `TaskGroup` for logical grouping (bronze quality, silver transforms)
- `@task.branch` for conditional retraining trigger
- `TriggerDagRunOperator` for cross-DAG dependency
- `on_failure_callback` → Slack notification on every task failure
- `max_active_runs=1` + `catchup=False` — prevents runaway backfill

---

### 7. DVC + MLflow ML Pipeline

**Files:** `ml/`

The ML pipeline is fully reproducible via [DVC](https://dvc.org), with all experiments tracked in [MLflow](https://mlflow.org).

**DVC pipeline stages** (`ml/dvc.yaml`):

```
raw_features → split → train → evaluate → register
```

| Stage | Inputs | Outputs |
|---|---|---|
| `raw_features` | Feast offline export | `data/features/features.parquet` |
| `split` | features.parquet + params | train.parquet, test.parquet |
| `train` | train.parquet + params | model.pkl, mlflow run_id |
| `evaluate` | test.parquet + model | metrics.json, confusion_matrix.png, shap_summary.png |
| `register` | metrics + model | MLflow Model Registry entry |

**Versioned hyperparameters** (`ml/params.yaml`):
```yaml
model:
  n_estimators: 200
  max_depth: 6
  learning_rate: 0.05
  scale_pos_weight: 3.5     # class imbalance — no-shows are ~15% of data
  subsample: 0.8
split:
  test_size: 0.2
  random_state: 42
```

**MLflow logging** in `train.py`:
```python
mlflow.log_params(params)
mlflow.log_metrics({"roc_auc": auc, "f1": f1, "precision": precision})
mlflow.xgboost.log_model(model, artifact_path="model")
```

**Reproduce the full pipeline:**
```bash
make dvc-repro       # Runs all stages end-to-end
make dvc-metrics     # Show tracked metrics
dvc params diff      # Compare params to last commit
```

---

### 8. Evidently Drift Detection

**Files:** `ml/drift_detection.py`, integrated in `orchestration/dags/patient_risk_pipeline.py`

[Evidently AI](https://evidentlyai.com) compares the current feature distribution against the reference distribution captured at training time.

**Drift detection runs hourly** as the final step in the master Airflow DAG. If `dataset_drift=True` is detected, the DAG automatically triggers the `model_retraining` DAG.

**Reports generated:**
- `reports/drift/{date}/drift_report.html` — visual drift report
- `reports/drift/{date}/drift_result.json` — machine-readable result for Airflow branching

**Threshold:** `DRIFT_P_VALUE_THRESHOLD=0.05` (configurable in `.env`)

---

### 9. ChromaDB RAG Endpoint

**Files:** `rag/`

A [Retrieval-Augmented Generation](https://en.wikipedia.org/wiki/Retrieval-augmented_generation) pipeline that answers natural language questions about patient risk profiles.

**Architecture:**

```
Clinical notes (synthetic) → sentence-transformers embedding → ChromaDB
                                                                    │
User query → embed query → similarity search → top-k docs          │
                                                    │               │
                                          LangChain RAG chain ◄─────┘
                                                    │
                                          Claude / local LLM
                                                    │
                                          FastAPI response
```

**Embeddings:** `sentence-transformers/all-MiniLM-L6-v2` — runs fully locally, no API key required, 384-dimensional embeddings.

**API endpoints** (`rag/api/main.py`):

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/query` | Free-text RAG query |
| `GET` | `/patient/{patient_id}/risk` | Risk summary for a patient |
| `POST` | `/embed` | Embed new clinical notes |

**Example query:**
```bash
curl -X POST http://localhost:8888/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Which patients with Medicare are most likely to no-show for follow-up appointments?"}'
```

**Cloud equivalent:** AWS Bedrock Knowledge Bases / Azure AI Search + OpenAI / Vertex AI Search

---

### 10. Data Quality — Great Expectations

**Files:** `quality/expectations/`

[Great Expectations](https://greatexpectations.io) suites run as Airflow tasks, gating each medallion layer transition.

**Suites per layer:**

| Suite | Key expectations |
|---|---|
| `bronze_appointments` | Schema match, no null `appointment_id` / `patient_id`, event_type in allowed set |
| `silver_appointments` | No DQ-failed rows (`_dq_passed = true`), `scheduled_duration_minutes` in [5, 480] |
| `gold_features` | No null features for ML mart, `no_show_rate_*` in [0.0, 1.0] |

---

## Cloud Deployment

The local stack maps 1:1 to cloud services. Switch by changing `.env`:

### AWS
```
Kafka      → MSK (Kafka 3.6)
Storage    → S3 + Delta Lake
Spark      → EMR 7.x or Databricks on AWS
Warehouse  → Snowflake on AWS or Redshift Serverless
Airflow    → MWAA 2.9
Features   → Feast + DynamoDB online store
```

### Azure
```
Kafka      → Event Hubs (Kafka protocol)
Storage    → ADLS Gen2 + Delta Lake
Spark      → Databricks on Azure or Synapse Spark
Warehouse  → Snowflake on Azure or Synapse Dedicated
Airflow    → Self-hosted on AKS or Azure Data Factory
Features   → Feast + Azure Cache for Redis
```

### GCP
```
Kafka      → Pub/Sub (+ Dataflow) or Confluent on GCP
Storage    → GCS + Delta Lake
Spark      → Dataproc 2.x or Databricks on GCP
Warehouse  → BigQuery or Snowflake on GCP
Airflow    → Cloud Composer 2
Features   → Feast + Bigtable online store
```

Terraform modules for each cloud are in `infra/terraform/`.

---

## CI/CD

**GitHub Actions workflows** (`.github/workflows/`):

| Workflow | Trigger | Steps |
|---|---|---|
| `ci.yml` | PR to main | Install deps → lint (black/isort/flake8) → unit tests + coverage → dbt compile |
| `dvc_repro.yml` | Push to main with data changes | DVC repro → push metrics to DVC remote |

**Pre-commit hooks** (`.pre-commit-config.yaml`):
```
black        → code formatting
isort        → import sorting
flake8       → linting
sqlfluff     → SQL linting (snowflake dialect)
```

Install hooks locally:
```bash
make pre-commit   # pip install pre-commit && pre-commit install
```

---

## Built with Claude Code

This project was scaffolded, reviewed, and iterated on using **[Claude Code](https://claude.ai/code)** — Anthropic's AI-native CLI for software engineering.

**How Claude Code was used:**

| Task | Claude Code capability |
|---|---|
| Project scaffolding | Generated repo structure, `CLAUDE.md`, `docker-compose.yml` |
| PySpark jobs | Wrote SCD2 logic, window functions, Delta MERGE patterns |
| Airflow DAGs | Generated TaskFlow API DAGs with branching and callbacks |
| Feast definitions | Feature views, entities, and materialization scripts |
| dbt models | Staging/intermediate/mart SQL with test YAML |
| DVC pipeline | `dvc.yaml` stage definitions and `params.yaml` |
| RAG endpoint | LangChain chain, ChromaDB setup, FastAPI routes |
| Code review | `/code-review` on every major file before commit |

**CLAUDE.md** at the repo root contains the full project brief used to guide each Claude Code session — including coding standards, naming conventions, and architectural decisions.

See [`docs/claude-code-workflow.md`](docs/claude-code-workflow.md) for specific prompts and session notes.

---

*Built by Albert Lok · [LinkedIn](https://linkedin.com/in/albertlok) · [GitHub](https://github.com/albertlok)*
