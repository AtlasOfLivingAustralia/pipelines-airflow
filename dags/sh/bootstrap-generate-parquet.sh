#!/bin/bash
set -x
export S3_BUCKET=$1
echo "S3 bucket to use: $S3_BUCKET"

# create directories
sudo ln -s /mnt /data

sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/generate_parquet.py /tmp/generate_parquet.py