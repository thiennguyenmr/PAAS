from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SPARK_DIR = PROJECT_ROOT / "spark"

CONFIG_DIR = SPARK_DIR / "spark_configs"
