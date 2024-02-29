#!/bin/bash
set -x
s3_bucket_dwca=$1
s3_bucket_avro=$2

for ((i = 3; i <= $#; i++ )); do
  (
    export datasetId=${!i}
    echo 'Download ' $datasetId

    # Download file from s3
    echo "Downloading s3://$s3_bucket_dwca/dwca-imports/$datasetId"
    sudo -u hadoop aws s3 cp s3://$s3_bucket_dwca/dwca-imports/$datasetId /data/biocache-load/$datasetId --recursive

    # Unzip dataset
    echo "cd /data/biocache-load/$datasetId"
    cd "/data/biocache-load/$datasetId"
    echo $PWD
    echo "jar xf $datasetId.zip"
    sudo -u hadoop jar xf $datasetId.zip

    # Download existing identifiers from s3.
    # Note: For new drs, ala_uuid folder should not exist on the emr, if it does, the uuid step will error as there are not avro files inside
    export identifier_path=s3://$s3_bucket_avro/pipelines-data/$datasetId/1/identifiers/ala_uuid
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
  ) &
done

wait
echo 'Completed download of datasets'