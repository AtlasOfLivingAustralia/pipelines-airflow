#!/bin/bash
set -x
s3_bucket=$1

sudo chown hadoop:hadoop -R /data/la-pipelines/*
echo "This script will copy the datasets from s3://$s3_bucket/pipelines-data to hdfs:///pipelines-data"
echo "It will show all objects it found but exclude backup files/folder and the final number to copy have already excluded them."
# Exclude backup, identifiers-backup, and S3 folder markers (*_$folder$)

function list_dr(){
  for ((i = 2; i <= $#; i++ )); do
    datasetId=${!i}
    echo "$s3_bucket $datasetId"
  done
}

function download_dr(){
  local s3_bucket=$1
  local datasetId=$2
  # Download file from s3 using s3-dist-cp
  # does not work to exclude backup if exist: --srcPattern='.*\/identifiers(?!\/ala_uuid_backup\/).*'
  echo  "Downloading $datasetId from $s3_bucket"
  # Exclude backup/identifiers-backup and S3 folder markers
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/$datasetId/1/ \
    --srcPattern='^(?!.*backup).*' \
    --dest=hdfs:///pipelines-data/$datasetId/1/

  echo "$datasetId Download finished"
}

if (( $# > 30 )); then

  echo "More than 30 datasets to download: $# , using bulk copy."
  # Download all datasets from s3. This is faster than looping and downloading by datasets.
  echo "Download datasets from $s3_bucket"
  # Include only dr*/1/ subtree to implicitly exclude backup/identifiers-backup
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/ \
    --srcPattern='^(?!.*backup).*' \
    --dest=hdfs:///pipelines-data/

else

  export -f download_dr
  echo "Downloading $# datasets in parallel"
  list_dr $@ | xargs -P 5 -n 2 -I{} sh -c  'download_dr  $@' _ {}

fi
echo 'Completed download of datasets'