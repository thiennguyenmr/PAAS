from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ── Default arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=6),
    "start_date": pendulum.datetime(2026, 3, 1, tz="Asia/Ho_Chi_Minh"),
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ml_training_pipeline",
    default_args=default_args,
    description="QLoRA fine-tuning scheduled pipeline with MLflow logging",
    schedule="0 2 * * 0",   # Every Sunday at 2:00 AM
    catchup=False,
    tags=["ml", "training", "qlora", "mlflow"],
    params={
        # Can be overridden when triggered manually from the Airflow UI
        "model_id":        "scb10x/llama3.2-typhoon2-3b-instruct",
        "dataset_version": "v1",
        "lora_r":          16,
        "lora_alpha":      32,
        "learning_rate":   2e-4,
        "max_steps":             10,
        "skip_eval":             True,    # Run evaluation after training
        "registered_model_name": "typhoon-finetuned",  # "" = no registry, "typhoon-finetuned" = register to this name
        "resume_from_adapter_version": "",  # "" = fresh training, "2" = continue from v2
    },
) as dag:

    train_model = BashOperator(
        task_id="train_qlora",
        bash_command="""
            RUN_DATE=$(date +%Y-%m-%d)
            /opt/airflow/venv/bin/python /opt/airflow/airflow_jobs/train_qlora_v1.py \
                --model-id          "{{ params.model_id }}" \
                --dataset-version   "{{ params.dataset_version }}" \
                --output-dir        "/tmp/ml_outputs/${RUN_DATE}" \\
                --lora-r            {{ params.lora_r }} \\
                --lora-alpha        {{ params.lora_alpha }} \\
                --lr                {{ params.learning_rate }} \\
                --max-steps             {{ params.max_steps }} \\
                {% if params.registered_model_name %}--registered-model-name "{{ params.registered_model_name }}"{% endif %} \\
                {% if params.resume_from_adapter_version %}--resume-from-version "{{ params.resume_from_adapter_version }}"{% endif %} \\
                {% if params.skip_eval %}--skip-eval{% endif %}
        """,
        env={
            "MLFLOW_TRACKING_URI":         "{{ var.value.MLFLOW_TRACKING_URI }}",
            "MLFLOW_S3_ENDPOINT_URL":       "{{ var.value.MLFLOW_S3_ENDPOINT_URL }}",
            "AWS_ACCESS_KEY_ID":            "{{ var.value.AWS_ACCESS_KEY_ID }}",
            "AWS_SECRET_ACCESS_KEY":        "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
            "AWS_DEFAULT_REGION":           "{{ var.value.get('AWS_DEFAULT_REGION', 'us-east-1') }}",
            "MLFLOW_DEFAULT_EXPERIMENT":    "{{ var.value.get('MLFLOW_DEFAULT_EXPERIMENT', 'typhoon-finetuning') }}",
            "MLFLOW_REGISTERED_MODEL_NAME": "{{ var.value.get('MLFLOW_REGISTERED_MODEL_NAME', 'typhoon-finetuned') }}",
            # Ensure the training script can find mlflow_client and other modules in /opt/airflow/airflow_modules
            "PYTHONPATH": "/opt/spark/work-dir:/opt/airflow:/opt/airflow/airflow_modules",
        },
    )

