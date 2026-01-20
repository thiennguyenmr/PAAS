#!/bin/bash
set -e

# 1. Create a connection from Airflow to Spark-Master.
echo "Initializing Airflow connections..."

airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077 \
  || echo "spark_default connection already exists"


