#!/usr/bin/env bash
set -Eeuo pipefail

########################################
# CONFIG
########################################
SPARK_HOME=${SPARK_HOME:-/opt/spark}
LOG_DIR=${SPARK_LOG_DIR:-/tmp}
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MINIO_BUCKET=${MINIO_BUCKET:-spark-logs}
MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}

########################################
# USAGE
########################################
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <spark-submit args>"
  exit 64
fi

########################################
# EXTRACT JOB NAME
########################################
JOB_FILE=$(basename "${!#}")
JOB_NAME="${JOB_FILE%.*}"
LOG_FILE="$LOG_DIR/${JOB_NAME}_$TIMESTAMP.log"

########################################
# RUN SPARK JOB
########################################
echo "========== Spark submit started =========="
echo "Job name   : $JOB_NAME"
echo "Log file   : $LOG_FILE"
echo "Command    : spark-submit $*"
echo "=========================================="

set +e
# Capture the appId from Spark output
APP_ID=$("$SPARK_HOME/bin/spark-submit" "$@" 2>&1 | tee "$LOG_FILE" | grep -oP "(?<=application_)[0-9_]+")
EXIT_CODE=${PIPESTATUS[0]}
set -e

########################################
# UPLOAD LOG TO MINIO
########################################
if command -v mc &>/dev/null; then
  mc alias set localminio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
  mc cp "$LOG_FILE" "localminio/$MINIO_BUCKET/${JOB_NAME}_$TIMESTAMP.log"
fi

########################################
# RESULT
########################################
if [[ $EXIT_CODE -eq 0 ]]; then
  echo "Spark job SUCCESS"
else
  echo "Spark job FAILED (exit code=$EXIT_CODE)"
  echo "---------- Last 200 lines of log ----------"
  tail -n 200 "$LOG_FILE"
  echo "------------------------------------------"
fi

########################################
# OUTPUT APP ID FOR AIRFLOW XCom
########################################
echo "::set-output name=spark_app_id::$APP_ID"
echo "Spark appId: $APP_ID"

exit $EXIT_CODE
