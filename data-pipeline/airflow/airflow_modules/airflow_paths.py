from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]

AIRFLOW_DIR = PROJECT_ROOT / "airflow"
AIRFLOW_CONFIG_DIR = AIRFLOW_DIR / "airflow_configs"
