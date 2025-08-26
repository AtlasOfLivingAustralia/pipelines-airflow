#!/bin/bash

set -e

# Prompt for S3 bucket name
read -p "Enter the S3 bucket name (e.g., ala-prod-pipelines-airflow): " S3_BUCKET
if [[ -z "$S3_BUCKET" ]]; then
  echo "Bucket name cannot be empty. Exiting."
  exit 1
fi

# Set timestamp
TS=$(date +%Y%m%d-%H%M%S)

# Check if preingestion directory exists in the bucket
if aws s3 ls "s3://$S3_BUCKET/airflow/dags/" > /dev/null 2>&1; then
  echo "Renaming existing airflow/dag directory to airflow/dag_$TS ..."
  aws s3 mv "s3://$S3_BUCKET/airflow/dags/" "s3://$S3_BUCKET/airflow/dags/$TS/" --recursive
fi

echo "Copying local dags directory to s3://$S3_BUCKET/airflow/dags/ ..."
aws s3 cp ./ "s3://$S3_BUCKET/airflow/dags/" \
  --recursive \
  --include "*.*" \
  --exclude "*__pycache__" \
  --exclude ".*" \
  --exclude "*.iml" \
  --exclude "*.pyc" \
  --exclude ".pytest_cache" \
  --exclude "venv" \
  --exclude ".git" \
  --exclude "*.log" \
  --exclude "tests*" \
  --exclude "temps*" \
  --exclude ".idea"

echo "Deploy complete."