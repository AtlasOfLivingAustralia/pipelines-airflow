#!/bin/bash
set -x
s3_bucket=$1

for ((i = 2; i <= $#; i++ )); do
  export datasetId=${!i}

  echo "Uploading pipeline-data $datasetId"
  sudo -u hadoop aws s3 sync /data/pipelines-data/$datasetId/1 s3://$s3_bucket/pipelines-data/$datasetId/1 --exclude "/data/pipelines-data/$datasetId/1/identifiers/ala_uuid_backup*" --delete

  echo $datasetId " - Backup existing identifiers in $s3_bucket"
  sudo -u hadoop aws s3 rm --recursive s3://$s3_bucket/pipelines-data/$datasetId/identifiers-backup/ala_uuid

  # Warning: do not pass --delete option here as it will remove all existing ala_uuid_backup folders in identifiers-backup
  sudo -u hadoop aws s3 sync /data/pipelines-data/$datasetId/1/identifiers/ s3://$s3_bucket/pipelines-data/$datasetId/identifiers-backup/

  echo "Uploading indexed record $datasetId"
  sudo -u hadoop aws s3 rm s3://$s3_bucket/pipelines-all-datasets/index-record/$datasetId --recursive
  sudo -u hadoop aws s3 sync /data/pipelines-all-datasets/index-record/$datasetId s3://$s3_bucket/pipelines-all-datasets/index-record/$datasetId --delete
done

echo 'Completed upload of datasets'