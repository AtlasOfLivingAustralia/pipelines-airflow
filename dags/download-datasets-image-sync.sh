#!/bin/bash
set -x
s3_bucket=$1

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
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/$datasetId/1/ \
    --dest=hdfs:///pipelines-data/$datasetId/1/

  echo "$datasetId Download finished"
}

if (( $# > 30 )); then

  # Download all datasets from s3. This is faster than looping and downloading by datasets.
  echo "Download datasets from $s3_bucket"
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/ --dest=hdfs:///pipelines-data/

else

  export -f download_dr
  list_dr $@ | xargs -P 15 -n 2 -I{} sh -c  'download_dr  $@' _ {}

fi
echo 'Completed download of datasets'