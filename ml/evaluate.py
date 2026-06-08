"""
Model evaluation: metrics, confusion matrix, SHAP explainability plots.

Reads the trained XGBoost model and test set, computes classification metrics
at the configured threshold, generates visual reports, and logs everything to MLflow.

Usage:
  python ml/evaluate.py
  (called automatically by `dvc repro`)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import shap
import structlog
import xgboost as xgb
import yaml
from dotenv import load_dotenv
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

load_dotenv()

log = structlog.get_logger()

PARAMS_PATH = Path("ml/params.yaml")
TEST_PATH = Path("data/test.parquet")
MODEL_PATH = Path("models/no_show_model.json")
FEAT_NAMES_PATH = Path("models/feature_names.json")
TRAIN_METRICS_PATH = Path("reports/train_metrics.json")
EVAL_METRICS_PATH = Path("reports/eval_metrics.json")
CM_PATH = Path("reports/confusion_matrix.png")
SHAP_PATH = Path("reports/shap_summary.png")
FI_PATH = Path("reports/feature_importance.png")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")


def load_model_and_features() -> tuple[xgb.Booster, list[str]]:
    booster = xgb.Booster()
    booster.load_model(str(MODEL_PATH))
    feature_names = json.loads(FEAT_NAMES_PATH.read_text())
    return booster, feature_names


def evaluate() -> None:
    params = yaml.safe_load(PARAMS_PATH.read_text())
    threshold = params["thresholds"]["classification"]

    booster, feature_names = load_model_and_features()
    test_df = pd.read_parquet(TEST_PATH)

    X_test = test_df[feature_names]
    y_test = test_df["label"].astype(int)

    dtest = xgb.DMatrix(X_test, feature_names=feature_names)
    y_proba = booster.predict(dtest)
    y_pred = (y_proba >= threshold).astype(int)

    # ── Classification metrics ────────────────────────────────────────────────
    metrics = {
        "roc_auc": round(roc_auc_score(y_test, y_proba), 4),
        "avg_precision": round(average_precision_score(y_test, y_proba), 4),
        "f1": round(f1_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred, zero_division=0), 4),
        "recall": round(recall_score(y_test, y_pred, zero_division=0), 4),
        "threshold": threshold,
        "test_rows": len(y_test),
        "positive_rate": round(y_test.mean(), 4),
    }
    EVAL_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVAL_METRICS_PATH.write_text(json.dumps(metrics, indent=2))
    log.info("eval_metrics", **metrics)

    # ── Confusion matrix plot ─────────────────────────────────────────────────
    cm = confusion_matrix(y_test, y_pred)
    fig, ax = plt.subplots(figsize=(6, 5))
    disp = ConfusionMatrixDisplay(cm, display_labels=["Showed Up", "No-Show"])
    disp.plot(ax=ax, cmap="Blues", colorbar=False)
    ax.set_title(f"Confusion Matrix (threshold={threshold})")
    fig.tight_layout()
    fig.savefig(CM_PATH, dpi=150)
    plt.close(fig)

    # ── SHAP summary plot ─────────────────────────────────────────────────────
    explainer = shap.TreeExplainer(booster)
    shap_values = explainer.shap_values(X_test.sample(min(500, len(X_test)), random_state=42))

    fig, ax = plt.subplots(figsize=(10, 8))
    shap.summary_plot(
        shap_values,
        X_test.sample(min(500, len(X_test)), random_state=42),
        show=False,
        plot_size=None,
    )
    plt.tight_layout()
    plt.savefig(SHAP_PATH, dpi=150, bbox_inches="tight")
    plt.close()

    # ── Feature importance plot ───────────────────────────────────────────────
    importance = booster.get_score(importance_type="gain")
    fi_df = (
        pd.DataFrame(list(importance.items()), columns=["feature", "gain"])
        .sort_values("gain", ascending=True)
        .tail(20)
    )

    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(fi_df["feature"], fi_df["gain"], color="#2196F3")
    ax.set_xlabel("Feature Importance (Gain)")
    ax.set_title("Top 20 Features — XGBoost Gain")
    fig.tight_layout()
    fig.savefig(FI_PATH, dpi=150)
    plt.close(fig)

    # ── Log to MLflow ─────────────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    train_metrics = (
        json.loads(TRAIN_METRICS_PATH.read_text()) if TRAIN_METRICS_PATH.exists() else {}
    )
    run_id = train_metrics.get("mlflow_run_id")

    if run_id:
        with mlflow.start_run(run_id=run_id):
            mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, float)})
            mlflow.log_artifact(str(CM_PATH))
            mlflow.log_artifact(str(SHAP_PATH))
            mlflow.log_artifact(str(FI_PATH))
            mlflow.log_artifact(str(EVAL_METRICS_PATH))

    print("\n" + "=" * 50)
    print("EVALUATION RESULTS")
    print("=" * 50)
    for k, v in metrics.items():
        print(f"  {k:25s}: {v}")
    print(classification_report(y_test, y_pred, target_names=["Showed Up", "No-Show"]))


if __name__ == "__main__":
    evaluate()
