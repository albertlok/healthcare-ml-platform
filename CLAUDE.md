# CLAUDE.md — healthcare-ml-platform

This file is the authoritative guide for Claude Code when working in this repository.
Read it fully before generating, editing, or reviewing any code.

---

## Project Purpose

End-to-end healthcare ML data platform simulating a production-grade system for
predicting patient appointment no-shows. The project demonstrates a modern data
lakehouse + MLOps stack across streaming ingestion, batch transformation, feature
engineering, model training, RAG inference, and data quality — all orchestrated
via Apache Airflow and documented with full lineage.

This is a portfolio project targeting Senior Data Engineer roles at companies
using Snowflake, Databricks, Kafka, Spark, Airflow, dbt, and AI/ML feature
infrastructure (Feast, DVC, vector databases).

---

## Repository Layout

```
healthcare-ml-platform/
├── CLAUDE.md                          # ← you are here
├── README.md
├── docker-compose.yml                 # Local: Kafka, Airflow, Postgres, MinIO, Weaviate
├── .env.example                       # Required env vars — never commit .env
├── Makefile                           # Shortcuts: make up, make test, make lint
│
├── ingestion/                         # Kafka producers & schema registry configs
│   ├── producers/
│   │   ├── appointment_producer.py    # Synthetic appointment event stream
│   │   └── patient_producer.py        # Patient demographic events
│   ├── schemas/                       # Avro schemas for all Kafka topics
│   │   ├── appointment_event.avsc
│   │   └── patient_event.avsc
│   └── consumers/
│       └── delta_sink.py              # Kafka → Delta Lake bronze writer
│
├── pipelines/
│   ├── spark/                         # PySpark jobs
│   │   ├── bronze/
│   │   │   └── ingest_appointments.py # Raw Kafka events → Delta bronze
│   │   ├── silver/
│   │   │   ├── clean_appointments.py  # Dedup, null handling, type casting
│   │   │   └── scd2_patients.py       # SCD Type 2 patient dimension
│   │   └── gold/
│   │       └── patient_risk_features.py  # Aggregated ML-ready features
│   │
│   ├── dbt/                           # dbt project (Snowflake + DuckDB targets)
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml.example
│   │   ├── models/
│   │   │   ├── staging/               # 1:1 with source tables, light casting
│   │   │   ├── intermediate/          # Business logic joins
│   │   │   └── marts/
│   │   │       ├── core/              # Patient, provider, appointment dims/facts
│   │   │       └── ml/                # Feature mart for model training
│   │   ├── tests/                     # dbt generic + singular tests
│   │   ├── macros/
│   │   └── docs/                      # dbt docs generate artifacts
│   │
│   └── quality/
│       └── expectations/              # Great Expectations suites per layer
│           ├── bronze_appointments.json
│           ├── silver_appointments.json
│           └── gold_features.json
│
├── orchestration/
│   └── dags/
│       ├── patient_risk_pipeline.py   # Master DAG: ingest → transform → features → train
│       ├── data_quality_checks.py     # Standalone GX validation DAG
│       ├── dbt_daily_refresh.py       # dbt run + test DAG
│       └── model_retraining.py        # Triggered when data drift detected
│
├── feature_store/
│   ├── feature_repo/                  # Feast feature repository root
│   │   ├── feature_store.yaml
│   │   ├── data_sources.py            # Snowflake + file offline sources
│   │   ├── entities.py                # patient_id, provider_id
│   │   ├── feature_views.py           # All FeatureView definitions
│   │   ├── feature_services.py        # FeatureService bundles for model serving
│   │   └── stream_feature_views.py    # Real-time features from Kafka
│   └── scripts/
│       ├── materialize.py             # feast materialize CLI wrapper
│       └── backfill.py
│
├── ml/
│   ├── train.py                       # Model training (XGBoost no-show classifier)
│   ├── evaluate.py                    # Metrics, confusion matrix, SHAP values
│   ├── predict.py                     # Batch inference against Feast online store
│   ├── drift_detection.py             # Evidently AI data + model drift reports
│   ├── params.yaml                    # DVC-tracked hyperparameters
│   └── dvc.yaml                       # DVC pipeline stages
│
├── rag/
│   ├── embed_notes.py                 # Embed synthetic clinical notes → ChromaDB
│   ├── retriever.py                   # Semantic search over patient notes
│   ├── chain.py                       # LangChain RAG chain (Claude / local LLM)
│   └── api/
│       ├── main.py                    # FastAPI app
│       └── schemas.py                 # Pydantic request/response models
│
├── infra/
│   ├── terraform/                     # IaC for cloud targets
│   │   ├── aws/                       # MSK, S3, Glue, EMR, MWAA modules
│   │   ├── azure/                     # Event Hubs, ADLS Gen2, ADF, Synapse
│   │   └── gcp/                       # Pub/Sub, GCS, Dataproc, Composer
│   └── snowflake/
│       ├── setup.sql                  # Warehouses, databases, roles, grants
│       └── storage_integration.sql    # S3/GCS external stage configs
│
├── ci/
│   └── .github/workflows/
│       ├── ci.yml                     # Lint, unit tests, dbt compile on PR
│       ├── dvc_repro.yml              # DVC pipeline reproduction on data change
│       └── great_expectations.yml     # GX checkpoint on schema change
│
└── docs/
    ├── architecture.md                # Full system design with diagrams
    ├── data_dictionary.md             # All tables, columns, types, owners
    ├── runbook.md                     # How to run locally end-to-end
    └── claude-code-workflow.md        # How Claude Code was used to build this
```

---

## Tech Stack Reference

### Streaming & Messaging
| Tool | Version | Purpose | Local Alternative |
|---|---|---|---|
| Apache Kafka | 3.7 | Event streaming backbone | Docker (`confluentinc/cp-kafka`) |
| Confluent Schema Registry | 7.6 | Avro schema enforcement | Docker (`confluentinc/cp-schema-registry`) |
| Kafka Connect | 3.7 | Managed source/sink connectors | Docker |
| AWS MSK | managed | Cloud Kafka (prod equiv) | Confluent Cloud free tier |
| Azure Event Hubs | managed | Kafka-protocol compatible | — |

### Compute & Processing
| Tool | Version | Purpose |
|---|---|---|
| Apache Spark (PySpark) | 3.5 | Distributed batch + streaming |
| Delta Lake | 3.1 | ACID lakehouse storage layer |
| Databricks | Runtime 14+ | Managed Spark + Unity Catalog |
| AWS EMR | 7.x | Managed Spark on S3 |
| Azure Synapse Spark | managed | Managed Spark on ADLS |
| GCP Dataproc | 2.x | Managed Spark on GCS |
| DuckDB | 0.10 | Local OLAP, fast iteration |

### Storage
| Layer | Local | AWS | Azure | GCP |
|---|---|---|---|---|
| Object store | MinIO (Docker) | S3 | ADLS Gen2 | GCS |
| Data warehouse | DuckDB / Snowflake trial | Redshift / Snowflake | Synapse / Snowflake | BigQuery / Snowflake |
| Online feature store | SQLite (Feast) | DynamoDB | Azure Cache | Bigtable |
| Vector store | ChromaDB (local) | OpenSearch k-NN | Azure AI Search | Vertex AI Matching Engine |

### Transformation
| Tool | Purpose |
|---|---|
| dbt Core | SQL transformations, testing, docs |
| dbt-snowflake | Snowflake adapter |
| dbt-duckdb | Local dev adapter |
| Great Expectations | Data quality suites |
| Soda Core | Lightweight data quality checks |

### Orchestration
| Tool | Version | Purpose | Local Setup |
|---|---|---|---|
| Apache Airflow | 2.9 | DAG orchestration | Astronomer `astro dev` or Docker |
| AWS MWAA | managed | Managed Airflow on AWS | — |
| GCP Cloud Composer | managed | Managed Airflow on GCP | — |
| Azure Data Factory | managed | Pipeline orchestration (ADF equiv) | — |

### Feature Store & MLOps
| Tool | Purpose |
|---|---|
| Feast | Open-source feature store (offline + online) |
| DVC | Data versioning, experiment tracking, pipeline reproducibility |
| MLflow | Experiment tracking, model registry |
| Evidently AI | Data drift + model performance monitoring |
| SHAP | Model explainability |

### AI / LLM / RAG
| Tool | Purpose |
|---|---|
| ChromaDB | Local vector database |
| LangChain | RAG chain construction |
| sentence-transformers | Free local embeddings (no API key needed) |
| FastAPI | Inference API layer |
| Anthropic Claude API | LLM for RAG generation (optional) |

### Infrastructure & CI/CD
| Tool | Purpose |
|---|---|
| Terraform | IaC for AWS / Azure / GCP modules |
| Docker + Docker Compose | Full local stack |
| GitHub Actions | CI: lint, test, dbt compile, DVC repro |
| Pre-commit hooks | Black, isort, flake8, sqlfluff |

---

## Coding Standards

### Python
- **Style:** Black (line length 100), isort, flake8
- **Type hints:** Required on all function signatures
- **Docstrings:** Google style, required for all public functions and classes
- **Logging:** Use `structlog` for structured JSON logging — never `print()`
- **Config:** All secrets via environment variables; use `pydantic-settings` for config classes
- **Error handling:** Raise specific exceptions; never bare `except:`; always log context

```python
# Good
def compute_no_show_rate(patient_id: str, lookback_days: int = 90) -> float:
    """Compute historical no-show rate for a patient.

    Args:
        patient_id: UUID string identifying the patient.
        lookback_days: Rolling window in days for the calculation.

    Returns:
        Float between 0.0 and 1.0 representing no-show rate.

    Raises:
        PatientNotFoundError: If patient_id does not exist in the feature store.
    """

# Bad — never do this
def compute(pid, days=90):
    # compute no show rate
    ...
```

### PySpark
- Always specify schemas explicitly — never infer on production reads
- Use `DataFrame` API over RDD unless there is a documented reason
- Partition writes by date (`year`, `month`, `day`) on Delta tables
- Use `spark.conf.set()` configurations at the top of each job, never inline
- Test with `pytest` + `chispa` for DataFrame equality assertions

```python
# Good — explicit schema, typed output
APPOINTMENT_SCHEMA = StructType([
    StructField("appointment_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("scheduled_at", TimestampType(), False),
    StructField("no_show", BooleanType(), True),
])

def read_bronze_appointments(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").schema(APPOINTMENT_SCHEMA).load(path)
```

### SQL / dbt
- **Naming:** `stg_<source>__<entity>`, `int_<description>`, `fct_<fact>`, `dim_<dimension>`
- All models must have a `.yml` file with `description`, `columns`, and at least one `not_null` + `unique` test on the primary key
- Use `{{ ref() }}` exclusively — never hardcode database/schema names
- CTEs over subqueries; each CTE has a comment explaining its purpose
- `sqlfluff` lint target: `snowflake` dialect

```sql
-- Good
with

source as (
    -- Raw appointments from bronze layer
    select * from {{ source('bronze', 'appointments') }}
),

renamed as (
    -- Standardize column names and cast types
    select
        appointment_id::varchar        as appointment_id,
        patient_id::varchar            as patient_id,
        scheduled_at::timestamp_ntz    as scheduled_at,
        coalesce(no_show, false)       as is_no_show
    from source
)

select * from renamed
```

### Airflow DAGs
- One DAG per logical domain pipeline; no god DAGs
- Use `@task` decorator (TaskFlow API) for Python operators
- All connections via Airflow Connections — never hardcode credentials
- Set `max_active_runs=1` and explicit `catchup=False` unless backfill is intentional
- DAG-level `default_args` must include `retries`, `retry_delay`, and `on_failure_callback`
- Document every DAG with a docstring including: purpose, schedule, upstream/downstream dependencies

### Kafka
- All topics use Avro schemas registered in Schema Registry
- Consumer groups named `<service>-<topic>-consumer`
- Producers use `acks='all'` and `enable.idempotence=True`
- Never commit offsets before successful downstream write

### Terraform
- One module per cloud service; no monolithic configs
- All variables have `description` and `type`
- Remote state in S3/GCS/Azure Blob — never local `terraform.tfstate`
- Tag all resources: `project`, `environment`, `owner`, `cost_center`

---

## Data Architecture

### Medallion Layers

| Layer | Format | Location | SLA | Owner |
|---|---|---|---|---|
| **Bronze** | Delta Lake (raw) | `s3://bucket/bronze/` | Append-only, immutable | Data Engineering |
| **Silver** | Delta Lake (cleaned) | `s3://bucket/silver/` | Deduplicated, typed, SCD2 | Data Engineering |
| **Gold** | Delta Lake / Snowflake | `s3://bucket/gold/` or Snowflake | Business-ready aggregates | Analytics + ML |
| **Feature Mart** | Feast offline store | Snowflake / file | Point-in-time correct | ML Engineering |

### Key Delta Tables

```
bronze.appointments_raw          -- Kafka events, append-only
bronze.patients_raw

silver.appointments              -- Cleaned, deduped, partitioned by scheduled_date
silver.patients                  -- SCD Type 2 with valid_from / valid_to

gold.fct_appointments            -- Fact table with all appointment attributes
gold.dim_patients                -- Current patient dimension
gold.patient_risk_features       -- ML feature aggregations (30/60/90-day windows)

feature_store.patient_features   -- Feast-materialized, point-in-time correct
```

### Naming Conventions

- **Databases:** `{env}_{domain}` → `dev_healthcare`, `prod_healthcare`
- **Schemas:** `bronze`, `silver`, `gold`, `feature_store`, `ml_outputs`
- **Tables:** `{layer}_{entity}` → `silver_appointments`
- **Columns:** `snake_case`; boolean columns prefixed `is_` or `has_`; timestamps suffixed `_at` (event time) or `_date` (date only)
- **Kafka topics:** `{env}-{domain}-{entity}-{event}` → `prod-healthcare-appointment-scheduled`

---

## Environment Setup

### Prerequisites
```bash
# Required
docker >= 24.0
docker-compose >= 2.24
python >= 3.11
java >= 11          # For PySpark local mode

# Optional (for cloud targets)
aws-cli >= 2.15
azure-cli >= 2.58
gcloud >= 468
terraform >= 1.7
snowsql >= 1.3
```

### Local Stack Startup
```bash
make up             # Starts: Kafka, Schema Registry, Airflow, MinIO, Postgres, ChromaDB
make seed           # Runs synthetic data producers for 5 minutes
make dbt-run        # dbt run + test against DuckDB
make feast-apply    # feast apply + materialize last 30 days
make dvc-repro      # Reproduce ML pipeline from features → trained model
make rag-start      # Start FastAPI RAG endpoint on :8000
```

### Python Environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"         # Installs all dev + test dependencies
pre-commit install               # Install hooks: black, isort, flake8, sqlfluff
```

### Environment Variables
Copy `.env.example` to `.env` and fill in values. Required keys:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081

# Storage
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
DELTA_LAKE_PATH=s3a://healthcare/

# Warehouse
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=dev_healthcare
SNOWFLAKE_SCHEMA=silver
DUCKDB_PATH=./data/dev.duckdb

# Feature Store
FEAST_REPO_PATH=./feature_store/feature_repo

# MLflow
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT_NAME=patient-no-show

# RAG
CHROMA_HOST=localhost
CHROMA_PORT=8000
ANTHROPIC_API_KEY=           # Optional; falls back to local LLM
```

---

## Testing Strategy

### Unit Tests (`pytest`)
- Location: `tests/unit/`
- Cover: PySpark transformations, feature computation logic, dbt macros
- Use `chispa` for Spark DataFrame equality; `pytest-mock` for external dependencies
- Run: `pytest tests/unit/ -v --cov=pipelines --cov-report=term-missing`

### Integration Tests
- Location: `tests/integration/`
- Require: Running Docker stack (`make up`)
- Cover: Full DAG runs on synthetic data, Feast materialize → online serve, DVC repro
- Run: `pytest tests/integration/ -v -m integration`

### dbt Tests
- Every model has `not_null` + `unique` on PK
- Custom singular tests for business rules (e.g., `scheduled_at` never in the past for future appts)
- Run: `dbt test --target dev`

### Data Quality (Great Expectations)
- Suites defined per medallion layer
- Run as Airflow tasks gating layer promotion
- Checkpoint results stored in `gx/uncommitted/validations/`

---

## Airflow DAG Catalog

| DAG ID | Schedule | Description |
|---|---|---|
| `patient_risk_pipeline` | `@hourly` | Master pipeline: ingest → silver → gold → features |
| `dbt_daily_refresh` | `0 6 * * *` | dbt run + test for all marts |
| `data_quality_checks` | `@daily` | Great Expectations checkpoints across all layers |
| `model_retraining` | triggered | Fires when drift_detection reports p-value < 0.05 |
| `feast_materialization` | `0 */6 * * *` | Materialize features to online store |
| `dvc_pipeline` | triggered | Runs DVC repro when training data changes |

---

## Feast Feature Catalog

### Entity: `patient`
Primary key: `patient_id` (string)

### Feature Views

**`patient_appointment_stats`** — Rolling appointment history
- `no_show_rate_30d`: float — No-show rate over last 30 days
- `no_show_rate_90d`: float — No-show rate over last 90 days
- `total_appointments_90d`: int — Appointment volume
- `avg_lead_time_days`: float — Avg days between scheduling and appointment
- `last_appointment_days_ago`: int — Recency feature

**`patient_demographics`** — Static patient attributes
- `age_at_appointment`: int
- `insurance_type`: string (encoded)
- `distance_to_clinic_miles`: float
- `preferred_communication_channel`: string

**`appointment_context`** — Appointment-level features
- `appointment_type`: string (encoded)
- `provider_no_show_rate`: float — Provider-level historical rate
- `day_of_week`: int
- `hour_of_day`: int
- `is_reminder_sent`: bool

---

## DVC Pipeline Stages

```
raw_features → split → train → evaluate → register
```

| Stage | Inputs | Outputs |
|---|---|---|
| `raw_features` | Feast offline store export | `data/features/features.parquet` |
| `split` | `features.parquet` | `data/train.parquet`, `data/test.parquet` |
| `train` | `train.parquet`, `params.yaml` | `models/no_show_model.pkl`, `mlflow run_id` |
| `evaluate` | `test.parquet`, model | `reports/metrics.json`, `reports/confusion_matrix.png` |
| `register` | metrics, model | MLflow Model Registry entry |

---

## Cloud Architecture Notes (Prod Equivalents)

### AWS
- **Kafka:** MSK with MSK Connect for S3 sink
- **Spark:** EMR on EKS or Glue 4.0 (Spark 3.3+)
- **Storage:** S3 with Delta Lake; Unity Catalog via Databricks
- **Warehouse:** Snowflake on AWS or Redshift Serverless
- **Airflow:** MWAA 2.9
- **Feature Store:** Feast with DynamoDB online store + S3 offline
- **Secrets:** AWS Secrets Manager via Airflow connections

### Azure
- **Kafka:** Event Hubs (Kafka protocol) or HDInsight Kafka
- **Spark:** Synapse Spark or Databricks on Azure
- **Storage:** ADLS Gen2 with Delta Lake
- **Warehouse:** Synapse Dedicated Pool or Snowflake on Azure
- **Airflow:** ADF pipelines or self-hosted on AKS
- **Secrets:** Azure Key Vault

### GCP
- **Kafka:** Pub/Sub (+ Dataflow for Kafka-compatible streaming) or Confluent on GCP
- **Spark:** Dataproc 2.x or Databricks on GCP
- **Storage:** GCS with Delta Lake
- **Warehouse:** BigQuery or Snowflake on GCP
- **Airflow:** Cloud Composer 2

---

## Claude Code Usage Notes

This project was scaffolded and developed using **Claude Code** (claude.ai/code).

### How to use Claude Code effectively in this repo:
- Run `/init` at the repo root to re-index before a new session
- Use `/code-review` before committing any Spark job or dbt model
- Use `@pipelines/spark/silver/` to scope questions to a specific layer
- Use `@orchestration/dags/` when debugging Airflow task failures
- Reference specific DAG or feature view files directly in prompts for precise edits

### Documented Claude Code sessions:
See `docs/claude-code-workflow.md` for prompts, outputs, and review notes from
each major component build.

---

## Common Commands

```bash
# Local dev
make up                          # Start full Docker stack
make down                        # Tear down stack
make logs service=airflow        # Tail logs for a service
make spark-submit job=silver/clean_appointments.py

# dbt
dbt run --select staging         # Run only staging models
dbt test --select marts.ml       # Test only ML mart models
dbt docs generate && dbt docs serve

# Feast
feast apply                      # Apply feature repo changes
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

# DVC
dvc repro                        # Reproduce full ML pipeline
dvc params diff                  # Compare params to last commit
dvc metrics show                 # Show all tracked metrics

# Airflow
astro dev start                  # Astronomer local dev server
astro dev run dags trigger patient_risk_pipeline

# Data quality
great_expectations checkpoint run bronze_appointments

# RAG API
uvicorn rag.api.main:app --reload --port 8000
```

---

## Key Design Decisions

1. **Delta Lake over Iceberg locally** — Better PySpark ergonomics, Databricks-native,
   and Snowflake external tables support Delta. Iceberg support can be added via a
   `delta-to-iceberg` bridge if needed for a Flink target.

2. **DuckDB for local dev, Snowflake for prod** — dbt profiles switch targets via
   `DBT_TARGET` env var. This lets all SQL logic be tested locally for free with
   identical semantics to Snowflake (ANSI SQL compatible).

3. **Feast over Tecton** — Feast is free and open source; the architecture mirrors
   Tecton's concepts (offline/online store, materialization jobs) so the patterns
   transfer directly.

4. **ChromaDB over Pinecone** — Zero cost, zero infra, runs in Docker. The
   `retriever.py` abstraction layer makes swapping to Pinecone/Weaviate a
   one-line config change.

5. **Synthetic data only** — No real PHI. The `ingestion/producers/` scripts
   generate realistic but fully synthetic patient and appointment records using
   the `Faker` library with healthcare-specific extensions.
