"""
XGBoost no-show classifier — training pipeline.

Supports multiple DVC stages via --stage flag:
  export-features  → Pull features from Feast offline store → Parquet
  split            → Train/val/test split with stratification
  train            → XGBoost training with early stopping + MLflow logging
  register         → Push best model to MLflow Model Registry

Usage:
  python ml/train.py --stage train
  python ml/train.py --stage export-features
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import mlflow
import mlflow.xgboost
import pandas as pd
import structlog
import xgboost as xgb
import yaml
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split

load_dotenv()

log = structlog.get_logger()

PARAMS_PATH = Path("ml/params.yaml")
FEAT_PATH = Path("data/features/features.parquet")
TRAIN_PATH = Path("data/train.parquet")
VAL_PATH = Path("data/val.parquet")
TEST_PATH = Path("data/test.parquet")
MODEL_PATH = Path("models/no_show_model.json")
FEAT_NAMES_PATH = Path("models/feature_names.json")
TRAIN_METRICS_PATH = Path("reports/train_metrics.json")
RUN_ID_PATH = Path("models/mlflow_run_id.txt")

FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "./feature_store/feature_repo")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT_NAME", "patient-no-show")


def load_params() -> dict:
    return yaml.safe_load(PARAMS_PATH.read_text())


def export_features(params: dict) -> None:
    """Pull training features from Feast offline store and save to Parquet."""
    from feast import FeatureStore

    log.info("exporting_features_from_feast")
    store = FeatureStore(repo_path=FEAST_REPO_PATH)

    # Build entity DataFrame — patient IDs with a range of timestamps
    # In production this would query the gold layer for all appointment events
    entity_df = _build_entity_df(lookback_days=params["features"]["entity_lookback_days"])

    features_df = store.get_historical_features(
        entity_df=entity_df,
        features=store.get_feature_service(params["features"]["service_name"]),
    ).to_df()

    FEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
    features_df.to_parquet(FEAT_PATH, index=False)
    log.info("features_exported", rows=len(features_df), path=str(FEAT_PATH))


def _build_entity_df(lookback_days: int) -> pd.DataFrame:
    """Build an entity DataFrame by reading appointment events from DuckDB gold mart."""
    import duckdb

    duckdb_path = os.getenv("DUCKDB_PATH", "./pipelines/dbt/data/dev.duckdb")
    conn = duckdb.connect(duckdb_path, read_only=True)
    df = conn.execute("""
        SELECT
            pa.patient_id,
            a.provider_id,
            pa.feature_timestamp AS event_timestamp,
            pa.label
        FROM main_ml.ml_patient_appointment_stats pa
        JOIN main_staging.stg_appointments a
            ON pa.appointment_id = a.appointment_id
        WHERE pa.feature_timestamp >= CURRENT_DATE - INTERVAL '{days} days'
    """.format(days=lookback_days)).df()
    conn.close()
    return df


def split(params: dict) -> None:
    """Stratified train/val/test split."""
    log.info("splitting_dataset")
    df = pd.read_parquet(FEAT_PATH)
    split_params = params["split"]

    # Drop entity keys and timestamps — these are Feast join artifacts, not model features.
    # provider_id and event_timestamp come from the Feast output DataFrame alongside
    # patient_id and feature_timestamp (the original entity_df columns).
    X = df.drop(
        columns=[
            "label",
            "patient_id",
            "provider_id",
            "appointment_id",
            "feature_timestamp",
            "event_timestamp",
        ],
        errors="ignore",
    )
    # Cast any remaining object-typed columns to float. Feast can return boolean
    # features (e.g. has_chronic_condition) as Python object dtype when they contain
    # nulls — pd.to_numeric handles both True/False and NaN gracefully.
    for col in X.select_dtypes(include=["object", "datetime64[ns, UTC]", "datetime64[ns]"]).columns:
        X[col] = pd.to_numeric(X[col], errors="coerce")
    y = df["label"].astype(int)

    X_trainval, X_test, y_trainval, y_test = train_test_split(
        X,
        y,
        test_size=split_params["test_size"],
        random_state=split_params["random_state"],
        # stratify=y ensures the same class ratio (no-show %) in train, val, and test.
        # Without this, a small test set could accidentally have 0% no-shows, making
        # metrics meaningless. Always stratify for imbalanced classification problems.
        stratify=y if split_params["stratify"] else None,
    )
    # Split the remaining trainval into train + val, adjusting the fraction so the
    # final val set is the right absolute size relative to the whole dataset.
    val_frac = split_params["val_size"] / (1 - split_params["test_size"])
    X_train, X_val, y_train, y_val = train_test_split(
        X_trainval,
        y_trainval,
        test_size=val_frac,
        random_state=split_params["random_state"],
        stratify=y_trainval if split_params["stratify"] else None,
    )

    for path, X_split, y_split in [
        (TRAIN_PATH, X_train, y_train),
        (VAL_PATH, X_val, y_val),
        (TEST_PATH, X_test, y_test),
    ]:
        pd.concat([X_split, y_split], axis=1).to_parquet(path, index=False)
        log.info("split_written", path=str(path), rows=len(X_split))


def train(params: dict) -> None:
    """Train XGBoost model with MLflow experiment tracking."""
    log.info("starting_training")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    train_df = pd.read_parquet(TRAIN_PATH)
    val_df = pd.read_parquet(VAL_PATH)

    feature_cols = [c for c in train_df.columns if c != "label"]
    X_train, y_train = train_df[feature_cols], train_df["label"].astype(int)
    X_val, y_val = val_df[feature_cols], val_df["label"].astype(int)

    model_params = params["model"]

    with mlflow.start_run() as run:
        mlflow.log_params(model_params)
        mlflow.log_param("train_rows", len(X_train))
        mlflow.log_param("val_rows", len(X_val))
        mlflow.log_param("n_features", len(feature_cols))

        dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=feature_cols)
        dval = xgb.DMatrix(X_val, label=y_val, feature_names=feature_cols)

        booster = xgb.train(
            params={
                # binary:logistic outputs probabilities [0, 1] — we then apply a threshold
                # to get a hard prediction. This is better than binary:hinge which outputs
                # class labels directly, because it lets us tune the threshold separately.
                "objective": "binary:logistic",
                "eval_metric": model_params["eval_metric"],
                "max_depth": model_params["max_depth"],
                "learning_rate": model_params["learning_rate"],
                # subsample + colsample_bytree are regularization params that randomly
                # sample rows and columns per tree. They reduce overfitting and speed up
                # training. Values in [0.6, 0.9] are common starting points.
                "subsample": model_params["subsample"],
                "colsample_bytree": model_params["colsample_bytree"],
                "min_child_weight": model_params["min_child_weight"],
                # scale_pos_weight handles class imbalance: set to (num_negative / num_positive)
                # to give more weight to the minority class (no-shows). Without this, the model
                # would learn to always predict "showed up" and still get ~95% accuracy.
                "scale_pos_weight": model_params["scale_pos_weight"],
                # L1 (alpha) and L2 (lambda) regularization reduce overfitting by penalizing
                # large leaf weights. Increase if the model is overfit on training data.
                "reg_alpha": model_params["reg_alpha"],
                "reg_lambda": model_params["reg_lambda"],
                "seed": 42,
            },
            dtrain=dtrain,
            num_boost_round=model_params["n_estimators"],
            # early_stopping_rounds: stop training if val metric doesn't improve for N rounds.
            # This prevents overfitting and reduces training time without sacrificing accuracy.
            evals=[(dtrain, "train"), (dval, "val")],
            early_stopping_rounds=model_params["early_stopping_rounds"],
            verbose_eval=50,
        )

        val_auc = booster.eval(dval, "val").split(":")[1]
        mlflow.log_metric("val_aucpr", float(val_auc))

        # Save model artifact
        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        booster.save_model(str(MODEL_PATH))
        FEAT_NAMES_PATH.write_text(json.dumps(feature_cols))

        mlflow.xgboost.log_model(booster, artifact_path="model")

        train_metrics = {"val_aucpr": float(val_auc), "mlflow_run_id": run.info.run_id}
        TRAIN_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        TRAIN_METRICS_PATH.write_text(json.dumps(train_metrics, indent=2))

        log.info("training_complete", val_aucpr=val_auc, run_id=run.info.run_id)


def register(params: dict) -> None:
    """Register the trained model in MLflow Model Registry."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    train_metrics = json.loads(TRAIN_METRICS_PATH.read_text())
    run_id = train_metrics["mlflow_run_id"]

    result = mlflow.register_model(
        model_uri=f"runs:/{run_id}/model",
        name="no_show_classifier",
    )
    log.info("model_registered", version=result.version, run_id=run_id)
    RUN_ID_PATH.write_text(run_id)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage",
        choices=["export-features", "split", "train", "register"],
        required=True,
    )
    args = parser.parse_args()
    params = load_params()

    stages = {
        "export-features": lambda: export_features(params),
        "split": lambda: split(params),
        "train": lambda: train(params),
        "register": lambda: register(params),
    }
    stages[args.stage]()


if __name__ == "__main__":
    main()
