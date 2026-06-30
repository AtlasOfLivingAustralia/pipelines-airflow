#!/bin/bash
set -x
s3_bucket=$1

function list_dr(){
  for ((i = 2; i <= $#; i++ )); do
    datasetId=${!i}
    echo "$s3_bucket $datasetId"
  done

}


function upload_dr(){
  local s3_bucket=$1
  local datasetId=$2

    echo $datasetId ' - Removing existing index records from bucket '$s3_bucket
    sudo -u hadoop aws s3 rm --recursive s3://$s3_bucket/pipelines-all-datasets/index-record/$datasetId

    echo $datasetId ' - Uploading index records'
    sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
      --src=hdfs:///pipelines-all-datasets/index-record/$datasetId \
      --dest=s3://$s3_bucket/pipelines-all-datasets/index-record/$datasetId

    echo $datasetId ' - Delete existing interpretation'
    sudo -u hadoop aws s3 rm --recursive s3://$s3_bucket/pipelines-data/$datasetId/1/images

    echo $datasetId ' - Uploading interpretation'
    sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
      --src=hdfs:///pipelines-data/$datasetId/1/images \
      --dest=s3://$s3_bucket/pipelines-data/$datasetId/1/images

    echo $datasetId ' - Upload finished'

  }

export -f upload_dr
list_dr $@ | xargs -P 15 -n 2 -I{} sh -c  'upload_dr  $@' _ {}

echo 'Completed upload of datasets'