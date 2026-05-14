#!/bin/bash
set -x
s3_bucket=$1

for ((i = 2; i <= $#; i++ )); do

  export datasetId=${!i}

  echo "Download existing AVRO $datasetId"

  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/$datasetId/1/search \
    --dest=hdfs:///pipelines-data/$datasetId/1/search

  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/$datasetId/1/verbatim \
    --dest=hdfs:///pipelines-data/$datasetId/1/verbatim

  echo "Download finished for $datasetId"

done

echo 'Completed download of datasets'