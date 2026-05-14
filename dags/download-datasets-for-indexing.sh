#!/bin/bash
set -x
s3_bucket=$1

for ((i = 2; i <= $#; i++ )); do

  export datasetId=${!i}
  echo 'Download ' $datasetId

  # Download file from s3
  echo $datasetId ' - Download existing identifiers'
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/$datasetId \
    --dest=hdfs:///pipelines-data/$datasetId

  echo "$datasetId  Download finished"

done

echo 'Completed download of datasets'