#!/bin/bash
set -x
s3_bucket=$1

for ((i = 2; i <= $#; i++ )); do
  export datasetId=${!i}
  echo 'Download ' $datasetId

  # Download archives from S3
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
      --src=s3://$s3_bucket/pipelines-annotations/$datasetId/ \
      --dest=hdfs:///pipelines-annotations/$datasetId/ \
      --srcPattern=".*.zip"

done

echo 'Completed download of annotation datasets'