#!/bin/bash
set -x
s3_bucket=$1

for ((i = 2; i <= $#; i++ )); do

  export datasetId=${!i}
  echo 'Download ' $datasetId

  # Download file from s3

  # Download existing identifiers from s3.
  # Note: For new drs, ala_uuid folder should not exist on the emr, if it does, the uuid step will error as there are not avro files inside
  export identifier_path=s3://$s3_bucket/pipelines-data/$datasetId/1/identifiers/ala_uuid
  files_exists=`aws s3 ls $identifier_path`
  if [ ! -z "$files_exists" ]
  then
    echo "Downloading $identifier_path"
    # Download existing identifiers from s3 into HDFS
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
      --src=$identifier_path \
      --dest=hdfs:///pipelines-data/$datasetId/1/identifiers/ala_uuid
  else
    echo "Identifier path does not exist. This may be a new dr."
  fi

  echo $datasetId ' - Download existing verbatim.avro'
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/$datasetId/1/verbatim \
    --dest=hdfs:///pipelines-data/$datasetId/1/verbatim

  echo $datasetId ' - Download existing YAML'
  sudo -u hadoop java -jar /usr/share/aws/emr/command-runner/lib/command-runner.jar s3-dist-cp \
    --src=s3://$s3_bucket/pipelines-data/$datasetId/1/dwca-metrics.yml \
    --dest=hdfs:///pipelines-data/$datasetId/1

  echo $datasetId ' - Download finished'

done

echo 'Completed download of datasets'