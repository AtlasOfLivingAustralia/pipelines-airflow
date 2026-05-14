#!/bin/bash
set -x
s3_bucket=$1

for ((i = 2; i <= $#; i++ )); do

  export datasetId=${!i}
  echo 'Download ' $datasetId

  # Download file from s3
  echo "Downloading s3://$s3_bucket/pipelines-data/1/$datasetId/verbatim.avro"
  sudo -u hadoop aws s3 cp s3://$s3_bucket/pipelines-data/$datasetId/1/verbatim/ /data/pipelines-data/$datasetId/1/verbatim/ --recursive
  sudo -u hadoop aws s3 cp s3://$s3_bucket/pipelines-data/$datasetId/1/dwca-metrics.yml /data/pipelines-data/$datasetId/1/dwca-metrics.yml

  # Download existing identifiers from s3.
  # Note: For new drs, ala_uuid folder should not exist on the emr, if it does, the uuid step will error as there are not avro files inside
  export identifier_path=s3://$s3_bucket/pipelines-data/$datasetId/1/identifiers/ala_uuid
  files_exists=`aws s3 ls $identifier_path`
  if [ ! -z "$files_exists" ]
  then
    echo "Downloading $identifier_path"
    sudo -u hadoop aws s3 cp $identifier_path /data/pipelines-data/$datasetId/1/identifiers/ala_uuid --recursive || true
  else
      echo "Identifier path does not exist. This may be a new dr."
  fi

done

echo 'Completed download of datasets'