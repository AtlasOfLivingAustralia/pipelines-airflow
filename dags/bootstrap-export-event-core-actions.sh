#!/bin/bash
set -x
# pip not accessible to sudo for some reason
sudo pip3 install -U FrictionlessDarwinCore
sudo pip3 install -U boto3
sudo pip3 install -U argparse
sudo pip3 install -U requests_oauth2client

export S3_BUCKET=$1
echo "S3 bucket to use: $S3_BUCKET"

# create directories
sudo ln -s /mnt /data
sudo mkdir -p /data/la-pipelines/config
sudo mkdir -p /data/biocache-load
sudo mkdir -p /data/pipelines-shp
sudo mkdir -p /data/pipelines-vocabularies
sudo mkdir -p /data/dwca-tmp/
sudo chmod -R 777 /data/dwca-tmp/
sudo mkdir -p /data/spark-tmp
sudo chown hadoop:hadoop -R /mnt/dwca-tmp
sudo chown hadoop:hadoop -R /data/*

# config files and JAR
sudo aws s3 cp s3://$S3_BUCKET/logback.xml  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines.yaml  /data/la-pipelines/config/la-pipelines.yaml
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines-emr.yaml  /data/la-pipelines/config/la-pipelines-local.yaml
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines.jar  /usr/share/la-pipelines/la-pipelines.jar

# set up la-pipeline script
sudo wget https://github.com/mikefarah/yq/releases/download/v4.16.1/yq_linux_arm64.tar.gz -O - | tar xz
sudo mv yq_linux_arm64 /usr/bin/yq
sudo curl -o /usr/bin/docopts -LJO https://github.com/docopt/docopts/releases/download/v0.6.3-rc2/docopts_linux_arm
sudo chmod +x /usr/bin/docopts
sudo aws s3 cp s3://$S3_BUCKET/logging_lib.sh /usr/share/la-pipelines/logging_lib.sh
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines /usr/bin/la-pipelines
sudo chmod -R 777 /usr/bin/la-pipelines
sudo chmod -R 777 /usr/share/la-pipelines/logging_lib.sh

# Download / Upload scripts
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/download-event-avro-hdfs.sh  /tmp/download-event-avro-hdfs.sh
sudo chmod -R 777 /tmp/download-event-avro-hdfs.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/download-datasets-hdfs.sh  /tmp/download-datasets-hdfs.sh
sudo chmod -R 777 /tmp/download-datasets-hdfs.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/download-avro-hdfs-for-downloads.sh  /tmp/download-avro-hdfs-for-downloads.sh
sudo chmod -R 777 /tmp/download-avro-hdfs-for-downloads.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/upload-hdfs-datasets.sh  /tmp/upload-hdfs-datasets.sh
sudo chmod -R 777 /tmp/upload-hdfs-datasets.sh
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/frictionless.sh  /tmp/frictionless.sh
sudo chmod -R 777 /tmp/frictionless.sh

sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/ala/README.HTML.template  /tmp/README.HTML.template

# DOI script
sudo aws s3 cp s3://$S3_BUCKET/airflow/dags/ala/doi_service.py  /tmp/doi_service.py
