#!/bin/bash
set -x
s3_bucket=$1

for ((i = 2; i <= $#; i++ )); do
  export datasetId=${!i}

  sudo -u hadoop aws s3 rm -r s3://$s3_bucket/pipelines-all-datasets/index-record/$datasetId

  echo "${datasetId} - Uploading index records"
  sudo -u hadoop aws s3 rm s3://$s3_bucket/pipelines-all-datasets/index-record/$datasetId --recursive
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=hdfs:///pipelines-all-datasets/index-record/$datasetId \
    --dest=s3://$s3_bucket/pipelines-all-datasets/index-record/$datasetId

  echo "${datasetId} Removing existing content"
  sudo -u hadoop aws s3 rm s3://$s3_bucket/pipelines-data/$datasetId/backup --recursive
  sudo -u hadoop aws s3 mv s3://$s3_bucket/pipelines-data/$datasetId/1 s3://$s3_bucket/pipelines-data/$datasetId/backup --recursive

  echo "${datasetId}  Uploading interpretation"
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=hdfs:///pipelines-data/$datasetId/1 \
    --dest=s3://$s3_bucket/pipelines-data/$datasetId/1

  echo $datasetId ' - Backup identifiers'
  sudo -u hadoop aws s3 rm s3://$s3_bucket/pipelines-data/$datasetId/identifiers-backup/ala_uuid --recursive

  # Warning: do not pass --delete option here as it will remove all existing ala_uuid_backup folders in identifiers-backup
  sudo -u hadoop aws s3 sync s3://$s3_bucket/pipelines-data/$datasetId/1/identifiers/ s3://$s3_bucket/pipelines-data/$datasetId/identifiers-backup/

  # Only ala_uuid stays but not the ala_uuid_backup
  sudo -u hadoop aws s3 rm s3://$s3_bucket/pipelines-data/$datasetId/1/identifiers --exclude "*" --include "*ala_uuid_backup*" --recursive

  echo $datasetId ' - Upload finished'

done

echo 'Completed upload of datasets'