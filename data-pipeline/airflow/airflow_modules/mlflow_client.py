"""
MLflow Client - Core integration module for MLflow.

Responsibilities:
  - Connect to MLflow tracking server
  - Log training runs (params, metrics, artifacts)
  - Upload trained models to MLflow Model Registry
  - Download models from MLflow Model Registry for deployment
"""

import os
import shutil
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, Optional

import mlflow
import mlflow.artifacts
import mlflow.transformers
from mlflow.tracking import MlflowClient


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class MLflowConfig:
    """Holds all connection settings for MLflow server."""
    tracking_uri: str                           # e.g. http://localhost:5000
    s3_endpoint_url: str                        # MinIO endpoint, e.g. http://localhost:9000
    aws_access_key_id: str                      # MinIO root user
    aws_secret_access_key: str                  # MinIO root password
    default_experiment: str = "Default"         # Experiment name shown in MLflow UI
    aws_default_region: str = "us-east-1"


def load_config_from_env() -> MLflowConfig:
    """
    Build MLflowConfig from environment variables.
    Raises EnvironmentError if required vars are missing.
    """
    required = {
        "MLFLOW_TRACKING_URI":    os.getenv("MLFLOW_TRACKING_URI"),
        "MLFLOW_S3_ENDPOINT_URL": os.getenv("MLFLOW_S3_ENDPOINT_URL"),
        "AWS_ACCESS_KEY_ID":      os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY":  os.getenv("AWS_SECRET_ACCESS_KEY"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")

    return MLflowConfig(
        tracking_uri=required["MLFLOW_TRACKING_URI"],
        s3_endpoint_url=required["MLFLOW_S3_ENDPOINT_URL"],
        aws_access_key_id=required["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=required["AWS_SECRET_ACCESS_KEY"],
        default_experiment=os.getenv("MLFLOW_DEFAULT_EXPERIMENT", "Default"),
        aws_default_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect(config: MLflowConfig) -> None:
    """
    Configure MLflow + S3/MinIO environment.
    Must be called once before any other mlflow operation.
    """
    mlflow.set_tracking_uri(config.tracking_uri)

    # Set MinIO as S3 artifact backend
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = config.s3_endpoint_url
    os.environ["AWS_ACCESS_KEY_ID"] = config.aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = config.aws_secret_access_key
    os.environ["AWS_DEFAULT_REGION"] = config.aws_default_region
    os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"

    # set_experiment() auto-creates the experiment if it doesn't exist
    mlflow.set_experiment(config.default_experiment)
    print(f"[MLflow] Connected → {config.tracking_uri}  experiment='{config.default_experiment}'")


# ---------------------------------------------------------------------------
# Logging (Write side — used by training & evaluation)
# ---------------------------------------------------------------------------

@contextmanager
def start_run(
    run_name: str,
    experiment_name: Optional[str] = None,
    tags: Optional[dict] = None,
) -> Generator[mlflow.ActiveRun, None, None]:
    """
    Context manager that wraps mlflow.start_run().
    Creates the experiment automatically if it doesn't exist.

    Usage:
        with start_run("qlora-run-v1", tags={"dataset": "hilo-v1"}) as run:
            log_params(...)
            log_metrics(...)
            artifact_uri = upload_model(...)
        print(run.info.run_id)
    """
    if experiment_name:
        mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=run_name, tags=tags or {}) as run:
        print(f"[MLflow] Run started  run_id={run.info.run_id}  name='{run_name}'")
        yield run
        print(f"[MLflow] Run finished run_id={run.info.run_id}")


def log_params(params: dict) -> None:
    """Log hyperparameters to the active run (lora_r, lr, batch_size, ...)."""
    mlflow.log_params(params)


def log_metrics(metrics: dict, step: Optional[int] = None) -> None:
    """Log numeric metrics to the active run (train_loss, rouge1, bleu, ...)."""
    mlflow.log_metrics(metrics, step=step)


def upload_model(
    model_local_path: str,
    artifact_path: str = "model",
    registered_model_name: Optional[str] = None,
    registry_tags: Optional[dict] = None,
) -> tuple[str, Optional[str]]:
    """
    Upload a model directory to MLflow artifact store (MinIO).

    Uses mlflow.log_artifacts() to upload files, then optionally registers
    the artifact URI into the Model Registry (required for promote/stage).

    Args:
        registry_tags: Tags to attach to the registered model version
                       (e.g. training_type, parent_version, dataset_version).

    Returns:
        (artifact_uri, registry_version)  e.g. ("runs:/<run_id>/adapter", "2")
    """
    mlflow.log_artifacts(model_local_path, artifact_path=artifact_path)

    run_id = mlflow.active_run().info.run_id
    artifact_uri = f"runs:/{run_id}/{artifact_path}"
    print(f"[MLflow] Model uploaded → {artifact_uri}")

    registry_version = None
    if registered_model_name:
        registry_version = register_model(artifact_uri, registered_model_name, tags=registry_tags)
        print(f"[MLflow] Registered as '{registered_model_name}' version {registry_version}")

    return artifact_uri, registry_version


def register_model(
    artifact_uri: str,
    model_name: str,
    tags: Optional[dict] = None,
) -> str:
    """
    Register an artifact URI into the MLflow Model Registry.
    Uses MlflowClient directly to avoid MLflow v3 fluent API calling
    search_logged_models (which requires logged-model format).
    Returns the version number string.
    """
    client = MlflowClient()
    # Ensure registered model exists
    try:
        client.create_registered_model(model_name)
    except Exception:
        pass  # already exists

    mv = client.create_model_version(
        name=model_name,
        source=artifact_uri,
        run_id=mlflow.active_run().info.run_id,
        tags=tags,
    )
    return mv.version


def set_model_stage(
    model_name: str,
    version: str,
    stage: str,  # "Staging" | "Production" | "Archived"
) -> None:
    """
    Transition a registered model version to the given stage.
    e.g. promote version "2" of "typhoon-finetuned" to "Production".
    """
    client = MlflowClient()
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage=stage,
        archive_existing_versions=(stage == "Production"),  # auto-archive old Production
    )
    print(f"[MLflow] '{model_name}' v{version} → stage='{stage}'")


# ---------------------------------------------------------------------------
# Retrieval (Read side — used by deploy.py)
# ---------------------------------------------------------------------------

def get_production_model(model_name: str) -> dict:
    """
    Query Model Registry for the latest version at stage='Production'.

    Returns:
        {
            "name": str,
            "version": str,
            "artifact_uri": str,   # e.g. "s3://mlflow/<run_id>/artifacts/model"
            "run_id": str,
        }

    Raises:
        ValueError if no Production version found.
    """
    client = MlflowClient()
    versions = client.get_latest_versions(model_name, stages=["Production"])
    if not versions:
        raise ValueError(
            f"No model version in stage='Production' for '{model_name}'. "
            "Please promote a version first via MLflow UI or set_model_stage()."
        )
    mv = versions[0]
    return {
        "name":         mv.name,
        "version":      mv.version,
        "artifact_uri": mv.source,   # s3://mlflow/<run_id>/artifacts/model
        "run_id":       mv.run_id,
    }


def download_model(
    artifact_uri: str,
    download_dir: str,
) -> str:
    """
    Download model artifacts from MLflow/MinIO to a local directory.
    Uses mlflow.artifacts.download_artifacts() (handles S3/MinIO auth via env).

    Args:
        artifact_uri:  e.g. "s3://mlflow/<run_id>/artifacts/model"
                       or   "runs:/<run_id>/model"
        download_dir:  Target local path, e.g. "~/.cache/huggingface/hub/<name>/"

    Returns:
        Absolute path to the downloaded model directory.
    """
    dest = os.path.expanduser(download_dir)
    os.makedirs(dest, exist_ok=True)

    tmp_path = mlflow.artifacts.download_artifacts(artifact_uri=artifact_uri)

    # Move contents into dest
    if tmp_path != dest:
        for item in os.listdir(tmp_path):
            src = os.path.join(tmp_path, item)
            dst = os.path.join(dest, item)
            if os.path.exists(dst):
                shutil.rmtree(dst) if os.path.isdir(dst) else os.remove(dst)
            shutil.move(src, dst)

    print(f"[MLflow] Model downloaded → {dest}")
    return dest
