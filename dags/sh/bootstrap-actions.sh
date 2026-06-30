#!/bin/bash
set -x
export S3_BUCKET=$1
echo 'S3 bucket to use: $S3_BUCKET'

# create directories
sudo mkdir -p /data/la-pipelines/config
sudo mkdir -p /data/pipelines-vocabularies
sudo mkdir -p /data/pipelines-shp

# get SHP files
sudo aws s3 cp s3://$S3_BUCKET/pipelines-shp/ /data/pipelines-shp/ --recursive --include "*"
sudo aws s3 cp s3://$S3_BUCKET/pipelines-vocabularies/  /data/pipelines-vocabularies/ --recursive --include "*"

# config files and JAR
sudo aws s3 cp s3://$S3_BUCKET/logback.xml  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines-emr.yaml  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines.yaml  /data/la-pipelines/config
sudo aws s3 cp s3://$S3_BUCKET/la-pipelines.jar  /tmp/la-pipelines.jar