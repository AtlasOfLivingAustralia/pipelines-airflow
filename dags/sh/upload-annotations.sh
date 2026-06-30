#!/bin/bash
set -x
s3_bucket=$1

for ((i = 2; i <= $#; i++ )); do
  export datasetId=${!i}

  echo $datasetId ' - Removing existing index records'
  sudo -u hadoop aws s3 rm --recursive s3://$s3_bucket/pipelines-annotations/$datasetId/1

  echo $datasetId ' - Uploading index records'
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=hdfs:///pipelines-annotations/$datasetId/1 \
    --dest=s3://$s3_bucket/pipelines-annotations/$datasetId/1

  echo $datasetId ' - Upload finished'

done

echo 'Completed upload of datasets'