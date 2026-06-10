# ─────────────────────────────────────────────────────────────
# healthcare-ml-platform  —  Makefile
# ─────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help
SHELL := /bin/bash

# Prefer the venv python when it exists; fall back to python3 / python.
PYTHON := $(or $(wildcard .venv/bin/python3), $(shell command -v python3 2>/dev/null), python3)

# ── Docker ────────────────────────────────────────────────────

.PHONY: up
up: ## Start the full local stack
	docker compose up -d --build
	@echo "Stack starting. Services:"
	@echo "  Airflow UI     → http://localhost:8088  (admin/admin)"
	@echo "  Kafka UI       → http://localhost:8080"
	@echo "  MinIO Console  → http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  Spark Master   → http://localhost:8082"
	@echo "  MLflow         → http://localhost:5001"
	@echo "  ChromaDB       → http://localhost:8000"
	@echo "  Schema Registry→ http://localhost:8081"

.PHONY: down
down: ## Tear down the stack (preserve volumes)
	docker compose down

.PHONY: destroy
destroy: ## Tear down + delete all volumes (DESTRUCTIVE)
	docker compose down -v

.PHONY: restart
restart: ## Restart a single service: make restart service=airflow-scheduler
	docker compose restart $(service)

.PHONY: logs
logs: ## Tail logs for a service: make logs service=airflow-webserver
	docker compose logs -f $(service)

.PHONY: ps
ps: ## Show running containers
	docker compose ps

.PHONY: exec
exec: ## Exec into a container: make exec service=kafka cmd="bash"
	docker compose exec $(service) $(cmd)

# ── Kafka ─────────────────────────────────────────────────────

.PHONY: kafka-topics
kafka-topics: ## Create all required Kafka topics
	docker compose exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--create --if-not-exists \
		--topic dev-healthcare-appointment-scheduled \
		--partitions 3 --replication-factor 1
	docker compose exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--create --if-not-exists \
		--topic dev-healthcare-patient-registered \
		--partitions 3 --replication-factor 1
	docker compose exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--create --if-not-exists \
		--topic dev-healthcare-appointment-cancelled \
		--partitions 3 --replication-factor 1
	docker compose exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--create --if-not-exists \
		--topic dev-healthcare-appointment-completed \
		--partitions 3 --replication-factor 1
	@echo "Topics created."

.PHONY: kafka-list
kafka-list: ## List all Kafka topics
	docker compose exec kafka kafka-topics \
		--bootstrap-server localhost:29092 --list

.PHONY: kafka-describe
kafka-describe: ## Describe a topic: make kafka-describe topic=dev-healthcare-appointment-scheduled
	docker compose exec kafka kafka-topics \
		--bootstrap-server localhost:29092 \
		--describe --topic $(topic)

# ── Seeding ───────────────────────────────────────────────────

.PHONY: seed
seed: kafka-topics ## Stream synthetic events to Kafka for 5 minutes (requires stack running)
	docker compose exec airflow-worker bash -c \
		"KAFKA_BOOTSTRAP_SERVERS=kafka:29092 SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
		python /opt/airflow/ingestion/producers/appointment_producer.py --duration 300 &"
	docker compose exec airflow-worker bash -c \
		"KAFKA_BOOTSTRAP_SERVERS=kafka:29092 SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
		python /opt/airflow/ingestion/producers/patient_producer.py --duration 300"

.PHONY: seed-once
seed-once: kafka-topics ## Produce a single batch of 500 appointment + 200 patient events
	docker compose exec airflow-worker bash -c \
		"KAFKA_BOOTSTRAP_SERVERS=kafka:29092 SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
		python /opt/airflow/ingestion/producers/appointment_producer.py --count 500 --once"
	docker compose exec airflow-worker bash -c \
		"KAFKA_BOOTSTRAP_SERVERS=kafka:29092 SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
		python /opt/airflow/ingestion/producers/patient_producer.py --count 200 --once"

.PHONY: seed-bronze
seed-bronze: ## Seed bronze Delta tables directly (bypasses Kafka — no Avro deps needed)
	docker compose exec airflow-worker bash -c \
		"MINIO_ENDPOINT=http://minio:9000 python /opt/airflow/ingestion/seed_bronze.py --rows 2000"

.PHONY: kafka-sink
kafka-sink: ## Consume all Kafka messages and write to Delta Lake bronze (no Spark needed)
	docker compose exec airflow-worker bash -c \
		"KAFKA_BOOTSTRAP_SERVERS=kafka:29092 SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
		MINIO_ENDPOINT=http://minio:9000 \
		python /opt/airflow/ingestion/consumers/delta_sink_simple.py --max-messages 10000"

# ── dbt ───────────────────────────────────────────────────────

.PHONY: dbt-run
dbt-run: ## Load silver → DuckDB, then run all dbt models
	$(PYTHON) pipelines/dbt/scripts/load_silver.py
	cd pipelines/dbt && dbt run --target dev

.PHONY: dbt-test
dbt-test: dbt-run ## Build models then run all dbt tests
	cd pipelines/dbt && dbt test --target dev

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt docs at :8085
	cd pipelines/dbt && dbt docs generate --target dev && dbt docs serve --port 8085

.PHONY: dbt-compile
dbt-compile: ## Compile dbt project (checks SQL syntax)
	cd pipelines/dbt && dbt compile --target dev

.PHONY: dbt-lint
dbt-lint: ## Run sqlfluff lint on all dbt SQL
	sqlfluff lint pipelines/dbt/models --dialect snowflake

# ── Feast ─────────────────────────────────────────────────────

.PHONY: feast-export
feast-export: ## Export DuckDB gold tables → Feast parquet sources (run after dbt-run)
	$(PYTHON) feature_store/scripts/export_features.py

.PHONY: feast-apply
feast-apply: feast-export ## Export features then apply Feast feature repository
	cd feature_store/feature_repo && feast apply

.PHONY: feast-materialize
feast-materialize: ## Materialize features for the last 30 days
	cd feature_store/feature_repo && \
	feast materialize $$(date -u -v-30d +"%Y-%m-%dT%H:%M:%S") $$(date -u +"%Y-%m-%dT%H:%M:%S")

.PHONY: feast-ui
feast-ui: ## Launch Feast UI at :8888
	cd feature_store/feature_repo && feast ui

# ── DVC / ML ──────────────────────────────────────────────────

.PHONY: dvc-repro
dvc-repro: ## Reproduce DVC ML pipeline end-to-end
	dvc repro

.PHONY: dvc-status
dvc-status: ## Show DVC pipeline stage status
	dvc status

.PHONY: dvc-metrics
dvc-metrics: ## Show tracked ML metrics
	dvc metrics show

.PHONY: dvc-diff
dvc-diff: ## Compare metrics to last commit
	dvc metrics diff

.PHONY: train
train: ## Run model training directly (no DVC)
	$(PYTHON) ml/train.py

.PHONY: evaluate
evaluate: ## Run model evaluation and generate reports
	$(PYTHON) ml/evaluate.py

# ── Spark ─────────────────────────────────────────────────────

.PHONY: spark-submit
spark-submit: ## Submit a Spark job: make spark-submit job=bronze/ingest_appointments.py
	docker compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
		--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
		/opt/pipelines/spark/$(job)

.PHONY: spark-bronze
spark-bronze: ## Run bronze ingestion job
	$(MAKE) spark-submit job=bronze/ingest_appointments.py

.PHONY: spark-silver
spark-silver: ## Run silver cleaning job
	$(MAKE) spark-submit job=silver/clean_appointments.py

# ── RAG API ───────────────────────────────────────────────────

.PHONY: rag-embed
rag-embed: ## Embed synthetic clinical notes into ChromaDB
	$(PYTHON) rag/embed_notes.py

.PHONY: rag-start
rag-start: ## Start the FastAPI RAG inference endpoint at :8888
	uvicorn rag.api.main:app --reload --host 0.0.0.0 --port 8888

# ── Data Quality ──────────────────────────────────────────────

.PHONY: gx-init
gx-init: ## Initialize Great Expectations project
	great_expectations init

.PHONY: gx-run
gx-run: ## Run all Great Expectations checkpoints
	great_expectations checkpoint run bronze_appointments
	great_expectations checkpoint run silver_appointments
	great_expectations checkpoint run gold_features

# ── Testing ───────────────────────────────────────────────────

.PHONY: test
test: ## Run unit tests
	pytest tests/unit/ -v --cov=pipelines --cov=feature_store --cov=ml \
		--cov-report=term-missing --cov-report=html:htmlcov

.PHONY: test-integration
test-integration: ## Run integration tests (requires running stack)
	pytest tests/integration/ -v -m integration

.PHONY: lint
lint: ## Run all linters (black, isort, flake8)
	black --check .
	isort --check-only .
	flake8 .

.PHONY: format
format: ## Auto-format code (black + isort)
	black .
	isort .

.PHONY: typecheck
typecheck: ## Run mypy type checking
	mypy pipelines/ feature_store/ ml/ rag/ --ignore-missing-imports

# ── Setup ─────────────────────────────────────────────────────

.PHONY: install
install: ## Install Python dependencies
	pip install -e ".[dev]"

.PHONY: pre-commit
pre-commit: ## Install pre-commit hooks
	pre-commit install

.PHONY: env
env: ## Copy .env.example to .env
	@[ -f .env ] && echo ".env already exists — skipping." || (cp .env.example .env && echo ".env created from .env.example")

# ── Snowflake ─────────────────────────────────────────────────

.PHONY: snowflake-setup
snowflake-setup: ## Run Snowflake DDL setup scripts
	snowsql -f infra/snowflake/setup.sql

# ── Help ──────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}' | sort
